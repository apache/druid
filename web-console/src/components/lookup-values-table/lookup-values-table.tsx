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
import { Loader } from '../loader/loader';

import './lookup-values-table.scss';

interface LookupRow {
  k: string;
  v: string;
}

export interface LookupColumnsTableProps {
  lookupId: string;
  downloadFilename?: string;
}

export interface LookupColumnsTableState {
  columns?: LookupRow[];
  loading: boolean;
  error?: string;
}

export class LookupValuesTable extends React.PureComponent<
  LookupColumnsTableProps,
  LookupColumnsTableState
> {
  private LookupColumnsQueryManager: QueryManager<null, LookupRow[]>;

  constructor(props: LookupColumnsTableProps, context: any) {
    super(props, context);
    this.state = {
      loading: true,
    };

    this.LookupColumnsQueryManager = new QueryManager({
      processQuery: async () => {
        const { lookupId } = this.props;

        const resp = await queryDruidSql<LookupRow>({
          query: `SELECT "k", "v" FROM lookup.${lookupId}
          LIMIT 5000`,
        });

        return resp;
      },
      onStateChange: ({ result, error, loading }) => {
        this.setState({ columns: result, error, loading });
      },
    });
  }

  componentDidMount(): void {
    this.LookupColumnsQueryManager.runQuery(null);
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
            Header: 'Key',
            accessor: 'k',
          },
          {
            Header: 'Value',
            accessor: 'v',
          },
        ]}
        noDataText={
          error
            ? error
            : 'Lookup data not found. If this is a new lookup it might not have propagated yet.'
        }
      />
    );
  }

  render(): JSX.Element {
    const { loading, error } = this.state;
    this.renderTable(error);
    return (
      <div className="lookup-columns-table">
        <div className="main-area">
          {loading ? <Loader loadingText="" loading /> : this.renderTable()}
        </div>
      </div>
    );
  }
}
