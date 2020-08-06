import React from 'react';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { BoltOptions } from './types';
import { DataSourceHttpSettings } from '@grafana/ui';

interface Props extends DataSourcePluginOptionsEditorProps<BoltOptions> {}

export const ConfigEditor = (props: Props) => {
  const { options, onOptionsChange } = props;
  return (
    <>
      <DataSourceHttpSettings
        defaultUrl="http://localhost:8088"
        dataSourceConfig={options}
        showAccessOptions={true}
        onChange={onOptionsChange}
      />
    </>
  );
};
