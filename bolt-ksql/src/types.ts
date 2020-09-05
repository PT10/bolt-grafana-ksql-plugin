import { DataQuery, DataSourceJsonData } from '@grafana/data';

export interface BoltQuery extends DataQuery {
  query: string;
  dimention: string;
  visualType: string;
  frameSize: number;
  error?: string;
  info?: string;
  parsingStream: string;
  filteringStream: string;
  cleanupData: string;
  cleanupThreshold: string;
  customCleanupThreshold: number;
  oldDataReplacementVal: string;
}

export const defaultQuery: Partial<BoltQuery> = {
  query: '',
  dimention: 'single',
  visualType: 'metric',
  frameSize: 1000,
  error: '',
  info: '',
  parsingStream: '',
  filteringStream: '',
  cleanupData: 'false',
  cleanupThreshold: 'startTime',
  customCleanupThreshold: 30,
  oldDataReplacementVal: 'null',
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
