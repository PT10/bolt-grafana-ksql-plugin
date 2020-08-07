/*
 *
 *  Copyright (C) 2019 Bolt Analytics Corporation
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 *
 */
import React from 'react';
import { PureComponent } from 'react';

// Types
import { BoltQuery, BoltOptions } from './types';

//import { QueryEditorProps, FormField } from '@grafana/ui';
import { LegacyForms, TextArea, InlineFormLabel } from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';

const { FormField } = LegacyForms;
import { getBackendSrv } from '@grafana/runtime';
import { BoltDataSource } from 'DataSource';

type Props = QueryEditorProps<BoltDataSource, BoltQuery, BoltOptions>;

interface State extends BoltQuery {}

export class BoltQueryEditor extends PureComponent<Props, State> {
  query: BoltQuery;
  backendSrv: any;
  ksqlUrl = '';
  baseUrl = '';

  constructor(props: Props) {
    super(props);
    this.baseUrl = props.datasource.baseUrl;
    this.ksqlUrl = props.datasource.baseUrl + '/ksql';
    this.backendSrv = getBackendSrv();

    const { query } = this.props;
    this.query = query;

    this.state = {
      ...this.state,
      query: query.query || '',
      error: query.error || '',
      info: query.info || '',
      parsingStream: query.parsingStream || '',
      filteringStream: query.filteringStream || '',
    };

    const { onChange } = this.props;
    onChange({
      ...this.props.query,
      ...this.state,
    });
  }

  render() {
    const { error, info, parsingStream, filteringStream, query } = this.state;
    const labelRed = {
      color: 'red',
    };
    const labelGreen = {
      color: 'green',
    };
    return (
      <div>
        {error && (
          <div className="gf-form-inline">
            <div className="gf-form">
              <div className="gf-form">
                <label style={labelRed}>{error}</label>
              </div>
            </div>
          </div>
        )}
        {info && (
          <div className="gf-form-inline">
            <div className="gf-form">
              <div className="gf-form">
                <label style={labelGreen}>{info}</label>
              </div>
            </div>
          </div>
        )}
        <div className="gf-form-inline">
          <div className="gf-form">
            <div className="gf-form">
              {/*<InlineFormLabel>Parse Stream</InlineFormLabel>*/}
              <FormField
                label="Parse Stream (Optional)"
                labelWidth={0}
                type="text"
                value={parsingStream}
                inputWidth={30}
                name="parsingStream"
                onChange={this.onFieldValueChange}
                onBlur={this.onChangeQueryDetected}
              ></FormField>
            </div>
          </div>
        </div>
        {parsingStream && (
          <div className="gf-form-inline">
            <div className="gf-form">
              <div className="gf-form">
                {/*<InlineFormLabel>Filter Stream (Optional)</InlineFormLabel>*/}
                <FormField
                  label="Filter Stream (Optional)"
                  labelWidth={0}
                  type="text"
                  value={filteringStream}
                  inputWidth={30}
                  name="filteringStream"
                  onChange={this.onFieldValueChange}
                  onBlur={this.onChangeQueryDetected}
                ></FormField>
              </div>
            </div>
          </div>
        )}
        <div className="gf-form-inline">
          <div className="gf-form">
            <div className="gf-form" style={{ width: '600px' }}>
              <InlineFormLabel>Query</InlineFormLabel>
              <TextArea
                type="text"
                value={query}
                height={180}
                onChange={this.onFieldValueChange}
                name="query"
              ></TextArea>
            </div>
          </div>
        </div>
      </div>
    );
  }

  onFieldValueChange = (event: any, _name?: string) => {
    const name = _name ? _name : event.target.name;
    const value = event.target.value;

    this.setState({
      ...this.state,
      [name]: value,
    });

    const { onChange } = this.props;
    onChange({
      ...this.props.query,
      [name]: value,
    });
  };

  onChangeQueryDetected = (event: any, _name?: string) => {
    this.setInfo('Stream/Filter creation in progress..');

    this.executeQuery(event.target.value);
  };

  executeQuery = (q: any) => {
    const me = this;
    let wsConn = new WebSocket(this.baseUrl);
    const ksqlQuery = { ksql: q, streamsProperties: { 'ksql.streams.auto.offset.reset': 'earliest' } };

    if (wsConn.readyState === WebSocket.OPEN) {
      wsConn.send(JSON.stringify({ query: ksqlQuery, type: 'stream' }));
      return;
    } else if (wsConn.readyState === WebSocket.CLOSED) {
      wsConn = new WebSocket(this.baseUrl);
    }

    wsConn.onopen = function() {
      wsConn.send(JSON.stringify({ query: ksqlQuery, type: 'stream' }));
      console.log('WebSocket Client Connected');
    };

    wsConn.onmessage = function(m: any) {
      if (m.error) {
        console.log(m.error);
        me.setError(m.error);
      } else {
        console.log(m.data);
        try {
          const data = JSON.parse(m.data);
          if (data.error) {
            me.setError(JSON.stringify(data.error));
          } else if (data.data) {
            try {
              const details = JSON.parse(data.data);
              if (details['@type'] === 'statement_error') {
                me.setError(details.message);
              } else if (details[0] && details[0]['commandStatus'] && details[0]['commandStatus']['status']) {
                if (details[0]['commandStatus']['status'].toUpperCase() === 'SUCCESS') {
                  me.setInfo(details[0]['commandStatus']['message']);
                } else {
                  me.setError(details[0]['commandStatus']['message']);
                }
              } else {
                me.setError('Unknown status');
              }
            } catch (ex) {
              me.setError('Unsupported output format');
            }
          }
        } catch (ex) {
          me.setError('Unsupported output format');
        }
      }
    };

    wsConn.onerror = function(e: any) {
      console.log(e);
      me.setError(e);
    };

    wsConn.onclose = function() {
      console.log('WebSocket Client Closed');
    };

    return wsConn;
  };

  setInfo = (info: string) => {
    this.setState({
      ...this.state,
      info: info,
      error: '',
    });
  };

  clear = () => {
    this.setState({
      ...this.state,
      info: '',
      error: '',
    });
  };

  setError = (error: string) => {
    this.setState({
      ...this.state,
      error: error,
      info: '',
    });
  };
}
