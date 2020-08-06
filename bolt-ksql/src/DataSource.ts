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
        const { range } = options;
        const from = new Date(range!.from.valueOf());

        const queryStr = this.getQueryString(query.query, from.toISOString().slice(0, -1));
        const q = {
          ksql: queryStr,
          streamsProperties: { 'auto.offset.reset': 'earliest' },
        };

        if (!this.connMap[panelId]) {
          const wsConn = new WebSocket(this.baseUrl);
          this.connMap[panelId] = wsConn;
        }

        //this.runQuery(q, query.refId, s, panelId);

        this.setWsConn(this.connMap[panelId], panelId, q, query.refId, s);
      });
    });

    return merge(...streams);
  }

  setWsConn(wsConn: any, panelId: any, q: any, refId: string, s: any) {
    const me = this;
    const columnSeq: any = {};
    let partialChunk = '';

    const frame = new CircularDataFrame({
      append: 'tail',
      capacity: 1000,
    });

    frame.refId = refId;
    const headerFields: any = {};

    if (wsConn.readyState === WebSocket.OPEN) {
      wsConn.send(JSON.stringify({ panelId: panelId, query: q }));
      return;
    } else if (wsConn.readyState === WebSocket.CLOSED) {
      wsConn = new WebSocket(this.baseUrl);
    }

    wsConn.onopen = function() {
      wsConn.send(JSON.stringify({ panelId: panelId, query: q }));
      console.log('WebSocket Client Connected');
    };

    wsConn.onmessage = function(m: any) {
      console.log(m.data);
      const data = JSON.parse(m.data);
      partialChunk = me.updateData(data.data, refId, s, headerFields, columnSeq, frame, partialChunk) || '';
    };

    wsConn.onerror = function(e: any) {
      console.log(e);
      me.connMap[panelId] = undefined;
    };

    wsConn.onclose = function() {};

    return wsConn;
  }

  updateData(
    data: any,
    refId: string,
    subscription: any,
    headerFields: any,
    columnSeq: any,
    frame: any,
    partialChunk: string
  ) {
    try {
      let val = data;
      console.log('New chunk reveived \n ' + val);

      if (partialChunk) {
        val = partialChunk + val;
        partialChunk = '';
        //console.log('prepended to previous partial chunk \n ' + val);
      }

      let valJson: any;
      if (!val) {
        return;
      }

      val = val.replace(/\r|\n/g, '');
      if (!val) {
        return;
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
        console.log(' Error in parsing json. Saving as partial chunk: ' + orgVal);
        console.log(ex);
        partialChunk = orgVal;
        return partialChunk;
      }

      const savedColumnData: any = {};
      valJson.forEach((rowObj: any) => {
        if (rowObj['header']) {
          Object.keys(headerFields).forEach(function(key) {
            delete headerFields[key];
          });
          Object.keys(columnSeq).forEach(function(key) {
            delete columnSeq[key];
          });
          if (frame.fields) {
            frame.fields.splice(0, frame.fields.length);
          }

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
              if (!frame.fields.find((f: any) => f.name === concatStr)) {
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

      return;
    } catch (ex) {
      console.log('Exception in fetching the response. skipping the results for this batch. ' + ex);
      return;
    }
  }

  getQueryString(queryText: string, rowTimeStr: string) {
    if (queryText.match(/ where /i)) {
      queryText = queryText.replace(/ where /i, " where ROWTIME >= '" + rowTimeStr + "' AND ");
    } else if (queryText.match(/ WINDOW\s+TUMBLING /i)) {
      const matches: any[] | null = queryText.match(/ WINDOW\s+TUMBLING (\(.*?\))/i);
      if (matches) {
        queryText = queryText.replace(matches[1], matches[1] + " where ROWTIME >= '" + rowTimeStr + "'");
      }
    } else {
      const matches: any[] | null = queryText.match(/ from (.*?) /i);
      if (matches && matches[1]) {
        queryText = queryText.replace(matches[1], matches[1] + " where ROWTIME >= '" + rowTimeStr + "' ");
      }
    }

    if (!queryText.endsWith(';')) {
      queryText += ';';
    }

    return queryText;
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
          console.log(' Error in parsing json. Saving as partial chunk: ' + orgVal);
          console.log(ex);
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
