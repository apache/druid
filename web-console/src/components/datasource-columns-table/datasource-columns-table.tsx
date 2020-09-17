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

import { SqlLiteral } from 'druid-query-toolkit';
import React from 'react';
import ReactTable from 'react-table';

import { useQueryManager } from '../../hooks';
import { ColumnMetadata, queryDruidSql } from '../../utils';
import { Loader } from '../loader/loader';

import './datasource-columns-table.scss';

export interface DatasourceColumnsTableRow {
  COLUMN_NAME: string;
  DATA_TYPE: string;
}

export interface DatasourceColumnsTableProps {
  datasourceId: string;
  downloadFilename?: string;
}

export const DatasourceColumnsTable = React.memo(function DatasourceColumnsTable(
  props: DatasourceColumnsTableProps,
) {
  const [columnsState] = useQueryManager<string, DatasourceColumnsTableRow[]>({
    processQuery: async (datasourceId: string) => {
      return await queryDruidSql<ColumnMetadata>({
        query: `SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = ${SqlLiteral.create(datasourceId)}`,
      });
    },
    initQuery: props.datasourceId,
  });

  function renderTable() {
    return (
      <ReactTable
        data={columnsState.data || []}
        defaultPageSize={20}
        filterable
        columns={[
          {
            Header: 'Column name',
            accessor: 'COLUMN_NAME',
          },
          {
            Header: 'Data type',
            accessor: 'DATA_TYPE',
          },
        ]}
        noDataText={columnsState.getErrorMessage() || 'No column data found'}
      />
    );
  }

  return (
    <div className="datasource-columns-table">
      <div className="main-area">{columnsState.loading ? <Loader /> : renderTable()}</div>
    </div>
  );
});
