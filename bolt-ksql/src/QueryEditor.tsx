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

  constructor(props: Props) {
    super(props);
    this.ksqlUrl = props.datasource.baseUrl + '/ksql';
    this.backendSrv = getBackendSrv();

    const { query } = this.props;
    this.query = query;

    this.state = {
      ...this.state,
      query: query.query || '',
      error: query.error || '',
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
    const { error, parsingStream, filteringStream, query } = this.state;
    const labelStyle = {
      color: 'red',
    };
    return (
      <div>
        {error && (
          <div className="gf-form-inline">
            <div className="gf-form">
              <div className="gf-form">
                <label style={labelStyle}>{error}</label>
              </div>
            </div>
          </div>
        )}
        <div className="gf-form-inline">
          <div className="gf-form">
            <div className="gf-form">
              {/*<InlineFormLabel>Parse Stream</InlineFormLabel>*/}
              <FormField
                label="Parse Stream"
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
    this.clearError();
    const stream: any = event.target.value.match(/CREATE STREAM (.*?) .*/i);
    const table: any = event.target.value.match(/CREATE TABLE (.*?) .*/i);

    if (stream && event.target.name === 'parsingStream') {
      this.createStreamIfNotExists(stream[1], event.target.value);
    } else if (table && event.target.name === 'filteringStream') {
      this.createTable(event.target.value);
    } else {
    }
  };

  createStreamIfNotExists = (_name: string, _query: string) => {
    const options = {
      url: this.ksqlUrl,
      method: 'POST',
      headers: { 'Content-Type': 'application/vnd.ksql.v1+json; charset=utf-8' },
      params: undefined,
      data: '{"ksql": "LIST STREAMS;"}',
    };

    this.backendSrv.datasourceRequest(options).then(
      (response: any) => {
        if (response.status === 200) {
          console.log(response);
          // Check if exists and if not then create
          const data = response.data;
          if (data.length > 0) {
            const str = data[0].streams.find((s: any) => s.type === 'STREAM' && s.name === _name.toUpperCase());
            if (!str) {
              this.createParsingStream(_name, _query);
            } else {
              console.log('Stream ' + _name + ' already exists');
            }
          }
        } else {
          console.log('Error');
        }
      },
      (error: any) => {
        console.log('Error while listing the stream. ' + error);
      }
    );
  };

  createParsingStream = (_name: string, _query: string) => {
    const payload = { ksql: _query, streamsProperties: { 'ksql.streams.auto.offset.reset': 'earliest' } };
    const options = {
      url: this.ksqlUrl,
      method: 'POST',
      headers: { 'Content-Type': 'application/vnd.ksql.v1+json; charset=utf-8' },
      params: undefined,
      data: JSON.stringify(payload),
    };

    this.backendSrv.datasourceRequest(options).then(
      (response: any) => {
        if (response.status === 200) {
          //this.createTable(_query);
          console.log('Stream ' + _name + ' created successfully');
        }
      },
      (error: any) => {
        console.log('Error while creating the stream. ' + error);
        this.setError(error.data.message);
      }
    );
  };

  createTable = (_query: string) => {
    const payload = { ksql: _query, streamsProperties: { 'ksql.streams.auto.offset.reset': 'earliest' } };
    const options = {
      url: this.ksqlUrl,
      method: 'POST',
      headers: { 'Content-Type': 'application/vnd.ksql.v1+json; charset=utf-8' },
      params: undefined,
      data: JSON.stringify(payload),
    };

    this.backendSrv.datasourceRequest(options).then(
      (response: any) => {
        if (response.status === 200) {
          console.log('Filter creation successful');
        }
      },
      (error: any) => {
        console.log('Error while creating the filter. ' + error.data.message);
        this.setError(error.data.message);
      }
    );
  };

  setError = (error: string) => {
    this.setState({
      ...this.state,
      error: error,
    });
  };

  clearError = () => {
    this.setState({
      ...this.state,
      error: '',
    });
  };
}
