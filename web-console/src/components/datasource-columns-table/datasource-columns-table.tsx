/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React from 'react';
import ReactTable from 'react-table';

import { queryDruidSql, QueryManager } from '../../utils';
import { ColumnMetadata } from '../../utils/column-metadata';
import { Loader } from '../loader/loader';

import './datasource-columns-table.scss';

interface TableRow {
  columnName: string;
  columnType: string;
}

export interface DatasourceColumnsTableProps {
  datasourceId: string;
  downloadFilename?: string;
}

export interface DatasourceColumnsTableState {
  columns?: TableRow[];
  loading: boolean;
  error?: string;
}

export class DatasourceColumnsTable extends React.PureComponent<
  DatasourceColumnsTableProps,
  DatasourceColumnsTableState
> {
  private datasourceColumnsQueryManager: QueryManager<null, TableRow[]>;

  constructor(props: DatasourceColumnsTableProps, context: any) {
    super(props, context);
    this.state = {
      loading: true,
    };

    this.datasourceColumnsQueryManager = new QueryManager({
      processQuery: async () => {
        const { datasourceId } = this.props;

        const resp = await queryDruidSql<ColumnMetadata>({
          query: `SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = '${datasourceId}'`,
        });

        return resp.map(object => {
          return { columnName: object.COLUMN_NAME, columnType: object.DATA_TYPE };
        });
      },
      onStateChange: ({ result, error, loading }) => {
        this.setState({ columns: result, error, loading });
      },
    });
  }

  componentDidMount(): void {
    this.datasourceColumnsQueryManager.runQuery(null);
  }

  renderTable(error?: string) {
    const { columns } = this.state;

    return (
      <ReactTable
        data={columns || []}
        defaultPageSize={20}
        filterable
        columns={[
          {
            Header: 'Column name',
            accessor: 'columnName',
          },
          {
            Header: 'Data type',
            accessor: 'columnType',
          },
        ]}
        noDataText={error ? error : 'No column data found'}
      />
    );
  }

  render(): JSX.Element {
    const { loading, error } = this.state;
    this.renderTable(error);
    return (
      <div className="datasource-columns-table">
        <div className="main-area">
          {loading ? <Loader loadingText="" loading /> : this.renderTable()}
        </div>
      </div>
    );
  }
}
