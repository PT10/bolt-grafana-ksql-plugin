import defaults from 'lodash/defaults';
//import { fromFetch } from 'rxjs/fetch';
//import { switchMap, catchError } from 'rxjs/operators';

import {
  DataQueryRequest,
  DataQueryResponse,
  DataSourceApi,
  DataSourceInstanceSettings,
  FieldType,
  CircularDataFrame,
  MutableDataFrame,
} from '@grafana/data';

import { BoltQuery, BoltOptions, defaultQuery } from './types';
import { Observable, merge } from 'rxjs';

export class BoltDataSource extends DataSourceApi<BoltQuery, BoltOptions> {
  baseUrl: string;
  connMap: any = {};
  frameRefMap: any = {}; // Panel ID, data frame map. Holds all data frames across panels
  //cleanUpMap: any = {}; // panel ID, cleanup status map
  cleanUpThresholdMap: any = {}; // panel ID, last timestamp cleaned up
  panelDataStateMap: any = {}; // panel ID, data state (dataFound/headerFound) map used to periodically emit data
  dataThreadStateMap: any = {}; // panel ID, data state map
  panelQuerySubscriptionMap: any = {}; // panel ID, query, subscription object map
  //panelRefreshSequence = 0;
  constructor(instanceSettings: DataSourceInstanceSettings<BoltOptions>) {
    super(instanceSettings);
    this.baseUrl = instanceSettings.url || '';

    //this.checkAndEmit();
    this.checkAndClean();
  }

  query(options: DataQueryRequest<BoltQuery>): Observable<DataQueryResponse> {
    const panelId = options.panelId;
    this.cleanUpThresholdMap = {};
    const streams = options.targets.map(target => {
      const query = defaults(target, defaultQuery);
      return Observable.create((s: any) => {
        //this.queryMap[panelId] = query;
        this.panelQuerySubscriptionMap[panelId] = {};
        this.panelQuerySubscriptionMap[panelId]['query'] = query;
        this.panelQuerySubscriptionMap[panelId]['subscription'] = s;
        this.panelQuerySubscriptionMap[panelId]['refId'] = query.refId;
        const { range, rangeRaw } = options;

        const from = new Date(range!.from.valueOf());
        let to: any = undefined;

        if (typeof rangeRaw!.from !== 'string') {
          // Absolute timewindow
          to = new Date(range!.to.valueOf()).toISOString().slice(0, -1) + '+0000';
        }

        const queryStr = this.getQueryString(query, from.toISOString().slice(0, -1) + '+0000', to);
        const q = {
          ksql: queryStr,
          streamsProperties: { 'auto.offset.reset': 'earliest', 'commit.interval.ms': 1000 },
        };

        if (this.connMap[panelId]) {
          this.connMap[panelId].close();
        }

        const wsConn = new WebSocket(this.baseUrl);
        this.connMap[panelId] = wsConn;

        if (!this.frameRefMap[panelId]) {
          this.frameRefMap[panelId] = {};
        }

        // if (query.cleanupData === 'true' && !this.cleanUpMap[panelId]) {
        //   this.cleanUpMap[panelId] = true;
        //   this.checkAndClean(panelId);
        // }

        if (!this.dataThreadStateMap[panelId]) {
          this.dataThreadStateMap[panelId] = true;
          this.checkAndEmit2(panelId);
        }

        this.frameRefMap[panelId]['frameRef'] = {};
        this.frameRefMap[panelId]['subscription'] = s;
        this.frameRefMap[panelId]['refId'] = query.refId;
        this.frameRefMap[panelId]['fromTime'] = rangeRaw!.from;

        this.setWsConn(this.connMap[panelId], panelId, q, query, s, to);
      });
    });

    return merge(...streams);
  }

  setWsConn(wsConn: any, panelId: any, q: any, query: BoltQuery, s: any, toDate?: Date) {
    const me = this;
    const columnSeq: any = {};
    let partialChunk = '';
    //const frameRef = {};
    const headerFields: any = {};

    if (wsConn.readyState === WebSocket.OPEN) {
      wsConn.send(JSON.stringify({ panelId: panelId, query: q, type: 'query' }));
      return;
    } else if (wsConn.readyState === WebSocket.CLOSED) {
      wsConn = new WebSocket(this.baseUrl);
    }

    wsConn.onopen = function() {
      wsConn.send(JSON.stringify({ panelId: panelId, query: q, type: 'query' }));
      console.log('WebSocket Client Connected for panel: ' + panelId);
    };

    wsConn.onmessage = function(m: any) {
      if (m.error) {
        console.log(m.error);
        s.next({ data: [], error: { message: m.error } });
      } else {
        //console.log(m.data);
        const data = JSON.parse(m.data);
        if (data.error) {
          s.next({ data: [], error: { message: data.error.code || 'Erorr in connection' } });
        } else {
          partialChunk =
            me.updateData(
              data.data,
              query,
              s,
              headerFields,
              columnSeq,
              me.frameRefMap[panelId]['frameRef'],
              partialChunk,
              panelId,
              toDate
            ) || '';
        }
      }
    };

    wsConn.onerror = function(e: any) {
      if (e.type === 'error') {
        s.next({ data: [], error: { message: 'Error in web socket connection' } });
      } else {
        s.next({ data: [], error: { message: 'Unknown error in connection' } });
      }
      me.connMap[panelId] = undefined;
    };

    wsConn.onclose = function() {};
  }

  updateData(
    data: any,
    query: BoltQuery,
    subscription: any,
    headerFields: any,
    columnSeq: any,
    frameRef: any,
    partialChunk: string,
    panelId: string,
    toDate?: Date
  ) {
    try {
      const resp = this.parseResponse(data, partialChunk);
      if (!resp.value && !resp.partialChunk) {
        return;
      } else if (resp.partialChunk) {
        return resp.partialChunk;
      }

      const valJson: any = resp.value;

      this.panelDataStateMap[panelId] = {};
      this.panelDataStateMap[panelId]['dataFound'] = false;
      this.panelDataStateMap[panelId]['headerFound'] = false;
      let dataFound = false;
      let headerFound = false;
      valJson.forEach((rowObj: any) => {
        const rowResultData: any = {};
        if (rowObj['header']) {
          this.panelDataStateMap[panelId]['headerFound'] = true;
          headerFound = true;
          rowObj['header']['schema'].split(',').map((f: any) => {
            const matches = f.match(/`(.*?)` (.*)/);
            headerFields[matches[1]] = matches[2];
          });

          Object.keys(headerFields).forEach((f: string, index: number) => {
            if (query.dimention === 'single') {
              let frame = frameRef['default'];
              if (!frame) {
                frame = this.createCircularDataFrame(query);
                frameRef['default'] = frame;
              }

              //if (f === 'WINDOWSTART' || f === 'INGESTIONTIME') {
              if (index === 0) {
                frame.addField({ name: f, type: FieldType.time });
              } else if (['VARCHAR', 'STRING'].includes(headerFields[f].toUpperCase())) {
                frame.addField({ name: f, type: FieldType.string });
              } else {
                frame.addField({ name: f, type: FieldType.number });
              }
            } else {
              //if (f !== 'WINDOWSTART') {
              if (index !== 0) {
                const frame = this.createCircularDataFrame(query);

                frame.addField({ name: 'WINDOWEND', type: FieldType.time });
                if (['VARCHAR', 'STRING'].includes(headerFields[f].toUpperCase())) {
                  frame.addField({ name: f, type: FieldType.string });
                } else {
                  frame.addField({ name: f, type: FieldType.number });
                }
                frameRef[f] = frame;
              }
            }
            columnSeq[index] = f;
          });
        } else if (rowObj['@type'] && rowObj['@type'] === 'statement_error') {
          subscription.next({ data: [], error: { message: rowObj['message'] } });
          return;
        } else {
          const columns: any[] = rowObj['row']['columns'];

          if (this.isOutsideTimeFrame(columns[0], panelId, rowObj['row'], toDate)) {
            return;
          }

          columns.forEach((col: any, i: number) => {
            const columnName = columnSeq[i];

            rowResultData[columnName] = col;
          });
        }
        if (Object.keys(rowResultData).length > 0) {
          this.panelDataStateMap[panelId]['dataFound'] = true;
          dataFound = true;
          if (query.dimention === 'single') {
            //frameRef.default.add(rowResultData);
            this.addUpdateValueToFrame(frameRef.default, rowResultData, columnSeq);
          } else {
            Object.keys(rowResultData).forEach((key: any, i: number) => {
              //if (key === 'WINDOWSTART') {
              if (i === 0) {
                return;
              }

              const d = {
                [columnSeq[0]]: rowResultData[columnSeq[0]],
                [key]: rowResultData[key],
              };
              this.addUpdateValueToFrame(frameRef[key], d, columnSeq);
            });
          }
        }
      });

      // if (dataFound) {
      //   this.emitToPanel(subscription, frameRef, query.refId);
      // } else if (headerFound) {
      //   setTimeout(() => {
      //     this.emitToPanel(subscription, frameRef, query.refId);
      //   }, 30000);
      // }

      return;
    } catch (ex) {
      console.log('Exception in fetching the response. skipping the results for this batch. ' + ex);
      subscription.next({ data: [], error: { message: 'Error in parsing the response' } });
      return;
    }
  }

  isOutsideTimeFrame(_timeVal: any, panelId: any, data: any, _toDate?: Date): boolean {
    let toDate = new Date();
    if (_toDate) {
      toDate = new Date(_toDate);
    }
    const bufferredMaxDate = new Date(toDate);
    bufferredMaxDate.setSeconds(toDate.getSeconds() + 300);

    const outOfRange = new Date(_timeVal) > bufferredMaxDate;

    if (outOfRange) {
      console.log(
        'Data discarded for ' +
          'Panel: ' +
          panelId +
          ' Data= ' +
          data.columns.toString() +
          ' Current time (with buffer): ' +
          bufferredMaxDate.valueOf()
      );
    }
    return outOfRange;
  }

  addUpdateValueToFrame(frame: CircularDataFrame, rowResultData: any, columnSeq: any) {
    const stringFields = frame.fields
      .filter(f => f.type === 'string' || f.type === 'time')
      .map(so => {
        return so.name;
      });

    const frameTimeStamps = frame.values[columnSeq[0]].toArray();
    let updateAt = -1;
    if (frame.values[columnSeq[0]]) {
      updateAt = frameTimeStamps.findIndex((v: any) => v === rowResultData[columnSeq[0]]);
    }

    let unique = true;
    if (updateAt >= 0) {
      stringFields.every((fieldName: any) => {
        const columnVals: any = frame.values[fieldName];
        if (columnVals && columnVals.get(updateAt) === rowResultData[fieldName]) {
          unique = true;
        } else {
          unique = false;
          return false;
        }
        return true;
      });
      if (unique) {
        frame.set(updateAt, rowResultData);
      }
    }

    if (updateAt === -1 || !unique) {
      if (rowResultData[columnSeq[0]] < frameTimeStamps[frameTimeStamps.length - 1]) {
        //old data point, received late
        let insertAt = frameTimeStamps.findIndex((v: any) => rowResultData[columnSeq[0]] < v);

        const nextPoints: any[] = [rowResultData];
        for (let i = insertAt; i < frameTimeStamps.length; i++) {
          nextPoints.push(frame.get(i));
        }

        let i = 0;
        for (; i < nextPoints.length - 1; i++) {
          frame.set(insertAt++, nextPoints[i]);
        }
        frame.add(nextPoints[i]);
      } else {
        frame.add(rowResultData);
      }
    }
  }

  emitToPanel(subscription: any, frameRef: any, refId: any) {
    subscription.next({
      data: Object.values(frameRef),
      key: refId,
    });
  }

  checkAndEmit() {
    Object.keys(this.frameRefMap).forEach(panelId => {
      const frameRef = this.frameRefMap[panelId]['frameRef'];
      const subscription = this.panelQuerySubscriptionMap[panelId]['subscription'];
      const refId = this.panelQuerySubscriptionMap[panelId]['refId'];

      if (this.panelDataStateMap[panelId] && this.panelDataStateMap[panelId]['dataFound']) {
        this.panelDataStateMap[panelId]['dataFound'] = false;
        this.emitToPanel(subscription, frameRef, refId);
      } else if (this.panelDataStateMap[panelId] && this.panelDataStateMap[panelId]['headerFound']) {
        this.panelDataStateMap[panelId]['headerFound'] = false;
        setTimeout(() => {
          this.emitToPanel(subscription, frameRef, refId);
        }, 10000);
      }
    });

    setTimeout(() => this.checkAndEmit(), 500);
  }

  checkAndEmit2(panelId: any) {
    const frameRef = this.frameRefMap[panelId]['frameRef'];
    const subscription = this.panelQuerySubscriptionMap[panelId]['subscription'];
    const refId = this.panelQuerySubscriptionMap[panelId]['refId'];

    if (this.panelDataStateMap[panelId] && this.panelDataStateMap[panelId]['dataFound']) {
      this.panelDataStateMap[panelId]['dataFound'] = false;
      this.emitToPanel(subscription, frameRef, refId);
    } else if (this.panelDataStateMap[panelId] && this.panelDataStateMap[panelId]['headerFound']) {
      this.panelDataStateMap[panelId]['headerFound'] = false;
      setTimeout(() => {
        this.emitToPanel(subscription, frameRef, refId);
      }, 30000);
    }

    setTimeout(() => this.checkAndEmit(), 3000);
  }

  // Cleanup thred
  checkAndClean() {
    Object.keys(this.panelQuerySubscriptionMap).forEach((panelId: any) => {
      const query: BoltQuery = this.panelQuerySubscriptionMap[panelId]['query']; //this.queryMap[panelId]; // get latest query object
      if (query.cleanupData !== 'true') {
        //this.cleanUpMap[panelId] = false;
        return;
      }
      const frameRef = this.frameRefMap[panelId]['frameRef'];
      const from = this.frameRefMap[panelId]['fromTime'];

      if (frameRef && Object.keys(frameRef).length > 0 && typeof from === 'string') {
        // relative time only
        const s = this.frameRefMap[panelId]['subscription'];
        const refId = this.frameRefMap[panelId]['refId'];
        let timeThreshold = new Date();

        if (query.cleanupThreshold === 'startTime') {
          const timeSec = from.match(/now-(\d+)(\w)/);
          switch (timeSec![2]) {
            case 'm':
              timeThreshold = new Date(timeThreshold.getTime() - 1000 * 60 * +timeSec![1]);
              break;
            case 'h':
              timeThreshold = new Date(timeThreshold.getTime() - 1000 * 60 * 60 * +timeSec![1]);
              break;
            case 'd':
              timeThreshold = new Date(timeThreshold.getTime() - 1000 * 60 * 60 * 24 * +timeSec![1]);
              break;
          }
        } else {
          timeThreshold = new Date(timeThreshold.getTime() - 1000 * 60 * query.customCleanupThreshold);
        }

        let dataFound = false;
        let oldPointTimings: number[] = [];
        Object.values(frameRef).forEach((frame: any) => {
          const timeField = frame.fields.find((f: any) => f.type === 'time');
          const stringFields = frame.fields.filter((f: any) => f.type === 'string').map((f: any) => f.name);
          const numberFields = frame.fields.filter((f: any) => f.type === 'number').map((f: any) => f.name);

          if (!timeField) {
            return;
          }

          const oldPointPositions: any[] = [];
          frame.values[timeField.name].toArray().forEach((v: any, i: number) => {
            if (this.cleanUpThresholdMap[panelId] && v < this.cleanUpThresholdMap[panelId]) {
              return;
            }
            if (v < timeThreshold.getTime()) {
              oldPointPositions.push(i);
              oldPointTimings.push(v);
            }
          });

          this.cleanUpThresholdMap[panelId] = timeThreshold;

          if (oldPointPositions.length > 0) {
            dataFound = true;
          } else {
            return;
          }

          const resultObj: any = {};
          stringFields.forEach((f: any) => {
            if (f === 'NAME') {
              resultObj[f] = true;
              return;
            }
            resultObj[f] = undefined;
          });
          numberFields.forEach((f: any) => {
            if (f === 'LATITUDE' || f === 'LONGITUDE') {
              resultObj[f] = true;
              return;
            }
            resultObj[f] = query.oldDataReplacementVal === 'null' ? undefined : 0;
          });
          oldPointPositions.forEach(pos => {
            resultObj[timeField.name] = frame.values[timeField.name].toArray()[pos];
            if (resultObj['LATITUDE']) {
              resultObj['LATITUDE'] = frame.values['LATITUDE'].toArray()[pos];
            }
            if (resultObj['LONGITUDE']) {
              resultObj['LONGITUDE'] = frame.values['LONGITUDE'].toArray()[pos];
            }
            if (resultObj['NAME']) {
              resultObj['NAME'] = frame.values['NAME'].toArray()[pos];
            }
            frame.set(pos, resultObj);
          });
        });

        if (dataFound) {
          console.log('PanelId: ' + panelId + ' cleaning up old points: ' + oldPointTimings.join(', '));
          console.log('Current time: ' + new Date().valueOf());
          s.next({
            data: Object.values(frameRef),
            key: refId,
          });
        }
      }
    });

    //if (this.frameRefMap[panelId]['action'] !== 'stop') {
    setTimeout(() => this.checkAndClean(), 30000);
    //}
  }

  getQueryString(query: BoltQuery, rowTimeStr: string, rowEndTime?: string) {
    let queryText = query.query;
    let clause = " where ROWTIME >= '" + rowTimeStr + "'" + (rowEndTime ? " AND ROWTIME < '" + rowEndTime + "'" : '');
    if (queryText.match(/ where /i)) {
      queryText = queryText.replace(/ where /i, clause + ' AND ');
    } else if (queryText.match(/ WINDOW\s+TUMBLING /i)) {
      const matches: any[] | null = queryText.match(/ WINDOW\s+TUMBLING (\(.*?\))/i);
      if (matches) {
        queryText = queryText.replace(matches[1], matches[1] + clause);
      }
    } else {
      const matches: any[] | null = queryText.match(/ from (.*?) /i);
      if (matches && matches[1]) {
        queryText = queryText.replace(matches[1], matches[1] + clause);
      }
    }

    if (!queryText.endsWith(';')) {
      queryText += ';';
    }

    const endTime = rowEndTime ? new Date(rowEndTime!.replace('+0000', '')).valueOf() : new Date().valueOf();
    const diff = Math.abs(endTime - new Date(rowTimeStr.replace('+0000', '')).valueOf()) / 1000;

    //queryText = queryText.replace('_RANGE_', 1800);
    queryText = queryText.replace('_RANGE_', diff.toFixed(0));

    return queryText;
  }

  parseResponse(data: any, partialChunk: string): { value?: string; partialChunk?: string } {
    let val = data;
    //console.log('New chunk reveived \n ' + val);

    if (partialChunk) {
      val = partialChunk + val;
      partialChunk = '';
    }

    let valJson: any;
    if (!val) {
      return { value: undefined, partialChunk: undefined };
    }

    val = val.replace(/\r|\n/g, '');
    if (!val) {
      return { value: undefined, partialChunk: undefined };
    }

    if (val.startsWith(',')) {
      val = val.slice(1);
    }
    const orgVal = val;

    if (!val.startsWith('[')) {
      val = '[' + val;
    }
    val = (val.endsWith(',') ? val.slice(0, -1) : val) + (!val.endsWith(']') ? ']' : '');

    try {
      valJson = JSON.parse(val);
    } catch (ex) {
      //console.log(' Error in parsing json. Saving as partial chunk: ' + orgVal);
      //console.log(ex);
      partialChunk = orgVal;
      return { value: undefined, partialChunk: partialChunk };
    }

    return { value: valJson, partialChunk: undefined };
  }

  createCircularDataFrame(query: BoltQuery): MutableDataFrame {
    const frame = new MutableDataFrame();
    //   {
    //   append: 'tail',
    //   capacity: query.frameSize,
    // }
    frame.refId = query.refId;

    return frame;
  }

  async testDatasource() {
    return new Promise((resolve, reject) => {
      const wsConn = new WebSocket(this.baseUrl);
      wsConn.onopen = function() {
        wsConn.close();
        resolve({
          status: 'success',
          message: 'Data source is working',
          title: 'Success',
        });
      };

      wsConn.onerror = function(error: any) {
        reject({
          status: 'error',
          message: 'Data source not accessible or access type is not set to Browser',
          title: 'Error',
        });
      };
    });
  }
}
