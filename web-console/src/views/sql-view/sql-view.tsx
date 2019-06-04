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
import SplitterLayout from 'react-splitter-layout';
import ReactTable from 'react-table';

import { SqlControl, TableCell } from '../../components';
import { QueryPlanDialog } from '../../dialogs';
import {
  BasicQueryExplanation,
  decodeRune,
  HeaderRows,
  localStorageGet, LocalStorageKeys,
  localStorageSet, parseQueryPlan,
  queryDruidRune,
  queryDruidSql, QueryManager,
  SemiJoinQueryExplanation
} from '../../utils';

import './sql-view.scss';

interface QueryWithContext {
  queryString: string;
  context?: Record<string, any>;
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
  static trimSemicolon(query: string): string {
    // Trims out a trailing semicolon while preserving space (https://bit.ly/1n1yfkJ)
    return query.replace(/;+(\s*)$/, '$1');
  }

  private sqlQueryManager: QueryManager<QueryWithContext, SqlQueryResult>;
  private explainQueryManager: QueryManager<QueryWithContext, BasicQueryExplanation | SemiJoinQueryExplanation | string>;

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
      processQuery: async (queryWithContext: QueryWithContext) => {
        const { queryString, context, wrapQuery } = queryWithContext;
        const startTime = new Date();

        if (queryString.trim().startsWith('{')) {
          // Secret way to issue a native JSON "rune" query
          const runeQuery = Hjson.parse(queryString);

          if (context) runeQuery.context = context;
          const result = await queryDruidRune(runeQuery);
          return {
            queryResult: decodeRune(runeQuery, result),
            queryElapsed: new Date().valueOf() - startTime.valueOf()
          };

        } else {
          const actualQuery = wrapQuery ?
            `SELECT * FROM (${SqlView.trimSemicolon(queryString)}) LIMIT 2000` :
            queryString;

          const queryPayload: Record<string, any> = {
            query: actualQuery,
            resultFormat: 'array',
            header: true
          };

          if (context) queryPayload.context = context;
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
      processQuery: async (queryWithContext: QueryWithContext) => {
        const { queryString, context } = queryWithContext;
        const explainPayload: Record<string, any> = {
          query: `EXPLAIN PLAN FOR (${SqlView.trimSemicolon(queryString)})`,
          resultFormat: 'object'
        };

        if (context) explainPayload.context = context;
        const result = await queryDruidSql(explainPayload);

        return parseQueryPlan(result[0]['PLAN']);
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

  onSecondaryPaneSizeChange(secondaryPaneSize: number) {
    localStorageSet(LocalStorageKeys.QUERY_VIEW_PANE_SIZE, String(secondaryPaneSize));
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
      columns={
        (result ? result.header : []).map((h: any, i) => {
          return {
            Header: h,
            accessor: String(i),
            Cell: row => <TableCell value={row.value}/>
          };
        })
      }
    />;
  }

  render() {
    const { initSql } = this.props;
    const { queryElapsed } = this.state;

    return <SplitterLayout
      customClassName="sql-view app-view"
      vertical
      percentage
      secondaryInitialSize={Number(localStorageGet(LocalStorageKeys.QUERY_VIEW_PANE_SIZE) as string) || 60}
      primaryMinSize={30}
      secondaryMinSize={30}
      onSecondaryPaneSizeChange={this.onSecondaryPaneSizeChange}
    >
      <div className="top-pane">
        <SqlControl
          initSql={initSql || localStorageGet(LocalStorageKeys.QUERY_KEY)}
          onRun={(queryString, context, wrapQuery) => {
            localStorageSet(LocalStorageKeys.QUERY_KEY, queryString);
            this.sqlQueryManager.runQuery({ queryString, context, wrapQuery });
          }}
          onExplain={(queryString, context) => {
            this.setState({ explainDialogOpen: true });
            this.explainQueryManager.runQuery({ queryString, context });
          }}
          queryElapsed={queryElapsed}
        />
      </div>
      <div className="bottom-pane">
        {this.renderResultTable()}
        {this.renderExplainDialog()}
      </div>
    </SplitterLayout>;
  }
}
