import { DataSourcePlugin } from '@grafana/data';
import { BoltDataSource } from './DataSource';
import { ConfigEditor } from './ConfigEditor';
import { BoltQueryEditor } from './QueryEditor';
import { BoltQuery, BoltOptions } from './types';

export const plugin = new DataSourcePlugin<BoltDataSource, BoltQuery, BoltOptions>(BoltDataSource)
  .setConfigEditor(ConfigEditor)
  .setQueryEditor(BoltQueryEditor);
