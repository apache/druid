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
import ReactTable, { Column } from 'react-table';

import { Loader } from '..';
import { queryDruidSql, QueryManager } from '../../utils';
import { ColumnMetadata } from '../../utils/column-metadata';

import './datasource-columns-table.scss';

interface TableRow {
  columnsName: string;
  columnType: string;
}

export interface DatasourceColumnsTableProps {
  datasourceId: string;
  downloadFilename?: string;
}

export interface DatasourceColumnsTableState {
  columns?: any;
  loading: boolean;
  error?: string;
}

export class DatasourceColumnsTable extends React.PureComponent<
  DatasourceColumnsTableProps,
  DatasourceColumnsTableState
> {
  private supervisorStatisticsTableQueryManager: QueryManager<null, TableRow[]>;

  constructor(props: DatasourceColumnsTableProps, context: any) {
    super(props, context);
    this.state = {
      loading: true,
    };
    this.supervisorStatisticsTableQueryManager = new QueryManager({
      processQuery: async () => {
        const { datasourceId } = this.props;
        const resp = await queryDruidSql<ColumnMetadata>({
          query: `SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = '${datasourceId}'`,
        });
        const dimensionArray = resp.map(object => {
          return { columnsName: object.COLUMN_NAME, columnType: object.DATA_TYPE };
        });
        return dimensionArray;
      },
      onStateChange: ({ result, error, loading }) => {
        this.setState({ columns: result, error, loading });
      },
    });
  }

  componentDidMount(): void {
    this.supervisorStatisticsTableQueryManager.runQuery(null);
  }

  renderTable(error?: string) {
    const { columns } = this.state;
    console.log(columns);
    const tableColumns: Column<TableRow>[] = [
      {
        Header: 'Column Name',
        accessor: 'columnsName',
      },
      {
        Header: 'Data Type',
        accessor: 'columnType',
      },
    ];

    return (
      <ReactTable
        data={this.state.columns ? this.state.columns : []}
        showPagination={false}
        defaultPageSize={15}
        columns={tableColumns}
        noDataText={error ? error : 'No statistics data found'}
      />
    );
  }

  render(): JSX.Element {
    const { loading, error } = this.state;
    this.renderTable(error);
    return (
      <div className="datasource-columns-table">
        <div className="main-area">
          {loading ? <Loader loadingText="" loading /> : !loading && this.renderTable()}
        </div>
      </div>
    );
  }
}
