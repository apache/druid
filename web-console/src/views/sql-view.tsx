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

import * as Hjson from 'hjson';
import * as React from 'react';
import ReactTable from 'react-table';

import { SqlControl } from '../components/sql-control';
import { QueryPlanDialog } from '../dialogs/query-plan-dialog';
import {
  BasicQueryExplanation,
  decodeRune,
  HeaderRows,
  localStorageGet, LocalStorageKeys,
  localStorageSet, parseQueryPlan,
  queryDruidRune,
  queryDruidSql, QueryManager,
  SemiJoinQueryExplanation
} from '../utils';

import './sql-view.scss';

interface QueryWithFlags {
  queryString: string;
  bypassCache?: boolean;
  wrapQuery?: boolean;
}

export interface SqlViewProps extends React.Props<any> {
  initSql: string | null;
}

export interface SqlViewState {
  loading: boolean;
  result: HeaderRows | null;
  error: string | null;
  explainDialogOpen: boolean;
  explainResult: BasicQueryExplanation | SemiJoinQueryExplanation | string | null;
  loadingExplain: boolean;
  explainError: Error | null;
  queryElapsed: number | null;
}

interface SqlQueryResult {
  queryResult: HeaderRows;
  queryElapsed: number;
}

export class SqlView extends React.Component<SqlViewProps, SqlViewState> {

  private sqlQueryManager: QueryManager<QueryWithFlags, SqlQueryResult>;
  private explainQueryManager: QueryManager<string, any>;

  constructor(props: SqlViewProps, context: any) {
    super(props, context);
    this.state = {
      loading: false,
      result: null,
      error: null,
      explainDialogOpen: false,
      loadingExplain: false,
      explainResult: null,
      explainError: null,
      queryElapsed: null
    };
  }

  componentDidMount(): void {
    this.sqlQueryManager = new QueryManager({
      processQuery: async (queryWithFlags: QueryWithFlags) => {
        const { queryString, bypassCache, wrapQuery } = queryWithFlags;
        const startTime = new Date();

        if (queryString.trim().startsWith('{')) {
          // Secret way to issue a native JSON "rune" query
          const runeQuery = Hjson.parse(queryString);

          if (bypassCache) {
            runeQuery.context = runeQuery.context || {};
            runeQuery.context.useCache = false;
            runeQuery.context.populateCache = false;
          }

          const result = await queryDruidRune(runeQuery);
          return {
            queryResult: decodeRune(runeQuery, result),
            queryElapsed: new Date().valueOf() - startTime.valueOf()
          };

        } else {
          const actualQuery = wrapQuery ?
            `SELECT * FROM (${queryString.trim().replace(/;+$/, '')}) LIMIT 5000` :
            queryString;

          const queryPayload: Record<string, any> = {
            query: actualQuery,
            resultFormat: 'array',
            header: true
          };

          if (wrapQuery) {
            queryPayload.context = {
              useCache: false,
              populateCache: false
            };
          }

          const result = await queryDruidSql(queryPayload);

          return {
            queryResult: {
              header: (result && result.length) ? result[0] : [],
              rows: (result && result.length) ? result.slice(1) : []
            },
            queryElapsed: new Date().valueOf() - startTime.valueOf()
          };
        }
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          result: result ? result.queryResult : null,
          queryElapsed: result ? result.queryElapsed : null,
          loading,
          error
        });
      }
    });

    this.explainQueryManager = new QueryManager({
      processQuery: async (query: string) => {
        const explainQuery = `explain plan for ${query}`;
        const result = await queryDruidSql({
          query: explainQuery,
          resultFormat: 'object'
        });
        const data: BasicQueryExplanation | SemiJoinQueryExplanation | string = parseQueryPlan(result[0]['PLAN']);
        return data;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          explainResult: result,
          loadingExplain: loading,
          explainError: error !== null ? new Error(error) : null
        });
      }
    });
  }

  componentWillUnmount(): void {
    this.sqlQueryManager.terminate();
    this.explainQueryManager.terminate();
  }

  getExplain = (q: string) => {
    this.setState({
      explainDialogOpen: true,
      loadingExplain: true,
      explainError: null
    });
    this.explainQueryManager.runQuery(q);
  }

  renderExplainDialog() {
    const {explainDialogOpen, explainResult, loadingExplain, explainError} = this.state;
    if (!loadingExplain && explainDialogOpen) {
      return <QueryPlanDialog
        explainResult={explainResult}
        explainError={explainError}
        onClose={() => this.setState({explainDialogOpen: false})}
      />;
    }
    return null;
  }

  renderResultTable() {
    const { result, loading, error } = this.state;

    return <ReactTable
      data={result ? result.rows : []}
      loading={loading}
      noDataText={!loading && result && !result.rows.length ? 'No results' : (error || '')}
      sortable={false}
      columns={(result ? result.header : []).map((h: any, i) => ({ Header: h, accessor: String(i) }))}
      defaultPageSize={10}
      className="-striped -highlight"
    />;
  }

  render() {
    const { initSql } = this.props;
    const { queryElapsed } = this.state;

    return <div className="sql-view app-view">
      <SqlControl
        initSql={initSql || localStorageGet(LocalStorageKeys.QUERY_KEY)}
        onRun={(queryString, bypassCache, wrapQuery) => {
          localStorageSet(LocalStorageKeys.QUERY_KEY, queryString);
          this.sqlQueryManager.runQuery({ queryString, bypassCache, wrapQuery });
        }}
        onExplain={this.getExplain}
        queryElapsed={queryElapsed}
      />
      {this.renderResultTable()}
      {this.renderExplainDialog()}
    </div>;
  }
}
