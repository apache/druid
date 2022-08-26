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

import { SqlRef } from 'druid-query-toolkit';
import React from 'react';
import ReactTable from 'react-table';

import { Loader } from '../../../components/loader/loader';
import { useQueryManager } from '../../../hooks';
import { SMALL_TABLE_PAGE_SIZE, SMALL_TABLE_PAGE_SIZE_OPTIONS } from '../../../react-table';
import { queryDruidSql } from '../../../utils';

import './lookup-values-table.scss';

interface LookupRow {
  k: string;
  v: string;
}

export interface LookupValuesTableProps {
  lookupId: string;
  downloadFilename?: string;
}

export const LookupValuesTable = React.memo(function LookupValuesTable(
  props: LookupValuesTableProps,
) {
  const [entriesState] = useQueryManager<string, LookupRow[]>({
    processQuery: async (lookupId: string) => {
      return await queryDruidSql<LookupRow>({
        query: `SELECT "k", "v" FROM ${SqlRef.column(lookupId, 'lookup')} LIMIT 5000`,
      });
    },
    initQuery: props.lookupId,
  });

  function renderTable() {
    const entries = entriesState.data || [];
    return (
      <ReactTable
        data={entries}
        defaultPageSize={SMALL_TABLE_PAGE_SIZE}
        pageSizeOptions={SMALL_TABLE_PAGE_SIZE_OPTIONS}
        showPagination={entries.length > SMALL_TABLE_PAGE_SIZE}
        filterable
        noDataText={
          entriesState.getErrorMessage() ||
          'Lookup data not found. If this is a new lookup it might not have propagated yet.'
        }
        columns={[
          {
            Header: 'Key',
            accessor: 'k',
            className: 'padded',
            width: 300,
          },
          {
            Header: 'Value',
            accessor: 'v',
            className: 'padded',
          },
        ]}
      />
    );
  }

  return (
    <div className="lookup-columns-table">
      <div className="main-area">{entriesState.loading ? <Loader /> : renderTable()}</div>
    </div>
  );
});
