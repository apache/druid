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

import { HeaderRows } from 'druid-query-toolkit';
import React from 'react';
import ReactTable from 'react-table';

import { TableCell } from '../../../components';

import './query-output.scss';

export interface QueryOutputProps {
  loading: boolean;
  result?: HeaderRows;
  error?: string;
}

export class QueryOutput extends React.PureComponent<QueryOutputProps> {
  render(): JSX.Element {
    const { result, loading, error } = this.props;

    return (
      <div className="query-output">
        <ReactTable
          data={result ? result.rows : []}
          loading={loading}
          noDataText={!loading && result && !result.rows.length ? 'No results' : error || ''}
          sortable={false}
          columns={(result ? result.header : []).map((h: any, i) => {
            return {
              Header: h,
              accessor: String(i),
              Cell: row => <TableCell value={row.value} />,
            };
          })}
        />
      </div>
    );
  }
}
