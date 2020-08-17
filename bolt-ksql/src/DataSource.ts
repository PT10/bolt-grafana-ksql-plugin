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
} from '@grafana/data';

import { BoltQuery, BoltOptions, defaultQuery } from './types';
import { Observable, merge } from 'rxjs';

export class BoltDataSource extends DataSourceApi<BoltQuery, BoltOptions> {
  baseUrl: string;
  connMap: any = {};
  constructor(instanceSettings: DataSourceInstanceSettings<BoltOptions>) {
    super(instanceSettings);
    this.baseUrl = instanceSettings.url || '';
  }

  query(options: DataQueryRequest<BoltQuery>): Observable<DataQueryResponse> {
    const panelId = options.panelId;
    const streams = options.targets.map(target => {
      const query = defaults(target, defaultQuery);
      return Observable.create((s: any) => {
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
          streamsProperties: { 'auto.offset.reset': 'earliest' },
        };

        if (this.connMap[panelId]) {
          this.connMap[panelId].close();
        }

        const wsConn = new WebSocket(this.baseUrl);
        this.connMap[panelId] = wsConn;

        //this.runQuery(q, query.refId, s, panelId);

        this.setWsConn(this.connMap[panelId], panelId, q, query, s, to);
      });
    });

    return merge(...streams);
  }

  setWsConn(wsConn: any, panelId: any, q: any, query: BoltQuery, s: any, toDate?: Date) {
    const me = this;
    const columnSeq: any = {};
    let partialChunk = '';
    const frameRef = {};
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
            me.updateData(data.data, query, s, headerFields, columnSeq, frameRef, partialChunk, panelId, toDate) || '';
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
    panelId?: string,
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

      let dataFound = false;
      let headerFound = false;
      valJson.forEach((rowObj: any) => {
        const rowResultData: any = {};
        if (rowObj['header']) {
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

                frame.addField({ name: 'WINDOWSTART', type: FieldType.time });
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
          dataFound = true;
          if (query.dimention === 'single') {
            //frameRef.default.add(rowResultData);
            this.addUpdateValueToFrame(frameRef.default, rowResultData, columnSeq, headerFields);
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
              this.addUpdateValueToFrame(frameRef[key], d, columnSeq, headerFields);

              // frameRef[key].add({
              //   WINDOWSTART: rowResultData['WINDOWSTART'],
              //   [key]: rowResultData[key],
              // });
            });
          }
        }
      });

      if (dataFound) {
        this.emitToPanel(subscription, frameRef, query.refId);
      } else if (headerFound) {
        setTimeout(() => {
          this.emitToPanel(subscription, frameRef, query.refId);
        }, 30000);
      }

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
    bufferredMaxDate.setSeconds(toDate.getSeconds() + 30);

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

  addUpdateValueToFrame(frame: CircularDataFrame, rowResultData: any, columnSeq: any, headerFields: any) {
    let updateAt = -1;
    const stringFieldsObj = frame.fields.filter(f => f.type === 'string' || f.type === 'time');
    if (stringFieldsObj.length === 0) {
      frame.add(rowResultData);
      return;
    }

    const stringFields = stringFieldsObj.map(so => {
      return so.name;
    });
    if (frame.values[columnSeq[0]]) {
      frame.values[columnSeq[0]].toArray().forEach((v: any, i: number) => {
        if (v === rowResultData[columnSeq[0]]) {
          updateAt = i;
        }
      });
    }

    if (updateAt > 0) {
      let unique = true;
      stringFields.forEach((fieldName: any) => {
        const columnVals: any = frame.values[fieldName];
        if (columnVals && columnVals[updateAt] && columnVals[updateAt] === rowResultData[fieldName]) {
          unique = true;
        } else {
          unique = false;
          return;
        }
      });
      if (unique) {
        frame.set(updateAt, rowResultData);
      } else {
        frame.add(rowResultData);
      }
    } else {
      frame.add(rowResultData);
    }
  }

  emitToPanel(subscription: any, frameRef: any, refId: any) {
    subscription.next({
      data: Object.values(frameRef),
      key: refId,
    });
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

    const endTime = rowEndTime ? new Date(rowEndTime!).valueOf() : new Date().valueOf();
    const diff = Math.abs(endTime - new Date(rowTimeStr).valueOf()) / 1000;

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

  createCircularDataFrame(query: BoltQuery): CircularDataFrame {
    const frame = new CircularDataFrame({
      append: 'tail',
      capacity: query.frameSize,
    });
    frame.refId = query.refId;

    return frame;
  }

  // Test method
  async runQuery(q: any, refId: string, subscription: any, panelId: any) {
    const fetchedResource: any = await fetch(this.baseUrl + '/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(q),
    });

    console.log('Connection established for: ' + panelId);
    const reader = await fetchedResource.body.getReader();

    const decoder = new TextDecoder('utf-8');
    const columnSeq: any = {};
    let partialChunk = '';

    const frame = new CircularDataFrame({
      append: 'tail',
      capacity: 1000,
    });

    frame.refId = refId;
    const headerFields: any = {};

    reader.read().then(function pt(done: any) {
      if (done.done) {
        return;
      }

      try {
        let val = decoder.decode(done.value);
        console.log('New chunk reveived \n ' + val);

        if (partialChunk) {
          val = partialChunk + val;
          partialChunk = '';
          //console.log('prepended to previous partial chunk \n ' + val);
        }

        let valJson: any;
        if (!val) {
          return reader.read().then(pt);
        }

        val = val.replace(/\r|\n/g, '');
        if (!val) {
          return reader.read().then(pt);
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
          return reader.read().then(pt);
        }

        const savedColumnData: any = {};
        valJson.forEach((rowObj: any) => {
          if (rowObj['header']) {
            rowObj['header']['schema'].split(',').map((f: any) => {
              const matches = f.match(/`(.*?)` (.*)/);
              headerFields[matches[1]] = matches[2];
            });
            Object.keys(headerFields).forEach((f: string, index: number) => {
              if (f === 'WINDOWSTART') {
                frame.addField({ name: f, type: FieldType.time });
              } else if (['VARCHAR', 'STRING'].includes(headerFields[f].toUpperCase())) {
                frame.addField({ name: f, type: FieldType.string });
              } else {
                frame.addField({ name: f, type: FieldType.number });
              }
              columnSeq[index] = f;
            });
          } else {
            const columns: any[] = rowObj['row']['columns'];
            let stringConcat = '';
            const numericFieldValues: any = {};
            columns.forEach((col: any, i: number) => {
              const columnName = columnSeq[i];
              if (['VARCHAR', 'STRING'].includes(headerFields[columnName])) {
                stringConcat += col;
              } else if (columnName !== 'WINDOWSTART') {
                numericFieldValues[columnName] = col;
              }
              savedColumnData[columnName] = col;
            });
            if (false) {
              // stringConcat
              Object.keys(numericFieldValues).forEach((numericFieldName: any) => {
                const concatStr = stringConcat + '_' + numericFieldName;
                savedColumnData[concatStr] = numericFieldValues[numericFieldName];
                if (!frame.fields.find(f => f.name === concatStr)) {
                  frame.addField({ name: concatStr, type: FieldType.number });
                }
              });
            }
          }

          frame.add(savedColumnData);
          subscription.next({
            data: [frame],
            key: refId,
          });
        });

        return reader.read().then(pt);
      } catch (ex) {
        console.log('Exception in fetching the response. skipping the results for this batch. ' + ex);
        return reader.read().then(pt);
      }
    });
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
