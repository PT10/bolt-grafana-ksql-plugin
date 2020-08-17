import { DataQuery, DataSourceJsonData } from '@grafana/data';

export interface BoltQuery extends DataQuery {
  query: string;
  dimention: string;
  frameSize: number;
  error?: string;
  info?: string;
  parsingStream: string;
  filteringStream: string;
}

export const defaultQuery: Partial<BoltQuery> = {
  query: '',
  dimention: 'single',
  frameSize: 1000,
  error: '',
  info: '',
  parsingStream: '',
  filteringStream: '',
};

/**
 * These are options configured for each DataSource instance
 */
export interface BoltOptions extends DataSourceJsonData {
  //path?: string;
}

/**
 * Value that is used in the backend, but never sent over HTTP to the frontend
 */
export interface MySecureJsonData {
  //apiKey?: string;
}
