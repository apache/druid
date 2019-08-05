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

import { Intent } from '@blueprintjs/core';
import axios from 'axios';
import classNames from 'classnames';
import Hjson from 'hjson';
import React from 'react';
import SplitterLayout from 'react-splitter-layout';

import { QueryPlanDialog } from '../../dialogs';
import { EditContextDialog } from '../../dialogs/edit-context-dialog/edit-context-dialog';
import { AppToaster } from '../../singletons/toaster';
import {
  BasicQueryExplanation,
  decodeRune,
  downloadFile,
  getDruidErrorMessage,
  HeaderRows,
  localStorageGet,
  LocalStorageKeys,
  localStorageSet,
  parseQueryPlan,
  queryDruidSql,
  QueryManager,
  SemiJoinQueryExplanation,
} from '../../utils';
import { ColumnMetadata } from '../../utils/column-metadata';
import { isEmptyContext, QueryContext } from '../../utils/query-context';

import { ColumnTree } from './column-tree/column-tree';
import { QueryExtraInfo, QueryExtraInfoData } from './query-extra-info/query-extra-info';
import { QueryInput } from './query-input/query-input';
import { QueryOutput } from './query-output/query-output';
import { RunButton } from './run-button/run-button';

import './query-view.scss';

interface QueryWithContext {
  queryString: string;
  queryContext: QueryContext;
  wrapQuery?: boolean;
}

export interface QueryViewProps {
  initQuery: string | undefined;
}

export interface QueryViewState {
  queryString: string;
  queryContext: QueryContext;

  columnMetadataLoading: boolean;
  columnMetadata?: ColumnMetadata[];
  columnMetadataError?: string;

  loading: boolean;
  result?: HeaderRows;
  queryExtraInfo?: QueryExtraInfoData;
  error?: string;

  explainDialogOpen: boolean;
  explainResult?: BasicQueryExplanation | SemiJoinQueryExplanation | string;
  loadingExplain: boolean;
  explainError?: string;

  editContextDialogOpen: boolean;
}

interface QueryResult {
  queryResult: HeaderRows;
  queryExtraInfo: QueryExtraInfoData;
}

export class QueryView extends React.PureComponent<QueryViewProps, QueryViewState> {
  static trimSemicolon(query: string): string {
    // Trims out a trailing semicolon while preserving space (https://bit.ly/1n1yfkJ)
    return query.replace(/;+((?:\s*--[^\n]*)?\s*)$/, '$1');
  }

  static isRune(queryString: string): boolean {
    return queryString.trim().startsWith('{');
  }

  static validRune(queryString: string): boolean {
    try {
      Hjson.parse(queryString);
      return true;
    } catch {
      return false;
    }
  }

  static formatStr(s: string | number, format: 'csv' | 'tsv') {
    if (format === 'csv') {
      // remove line break, single quote => double quote, handle ','
      return `"${String(s)
        .replace(/(?:\r\n|\r|\n)/g, ' ')
        .replace(/"/g, '""')}"`;
    } else {
      // tsv
      // remove line break, single quote => double quote, \t => ''
      return String(s)
        .replace(/(?:\r\n|\r|\n)/g, ' ')
        .replace(/\t/g, '')
        .replace(/"/g, '""');
    }
  }

  private metadataQueryManager: QueryManager<null, ColumnMetadata[]>;
  private sqlQueryManager: QueryManager<QueryWithContext, QueryResult>;
  private explainQueryManager: QueryManager<
    QueryWithContext,
    BasicQueryExplanation | SemiJoinQueryExplanation | string
  >;

  constructor(props: QueryViewProps, context: any) {
    super(props, context);
    this.state = {
      queryString: props.initQuery || localStorageGet(LocalStorageKeys.QUERY_KEY) || '',
      queryContext: {},

      columnMetadataLoading: false,

      loading: false,

      explainDialogOpen: false,
      loadingExplain: false,

      editContextDialogOpen: false,
    };

    this.metadataQueryManager = new QueryManager({
      processQuery: async () => {
        return await queryDruidSql<ColumnMetadata>({
          query: `SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS`,
        });
      },
      onStateChange: ({ result, loading, error }) => {
        if (error) {
          AppToaster.show({
            message: 'Could not load SQL metadata',
            intent: Intent.DANGER,
          });
        }
        this.setState({
          columnMetadataLoading: loading,
          columnMetadata: result,
          columnMetadataError: error,
        });
      },
    });

    this.sqlQueryManager = new QueryManager({
      processQuery: async (queryWithContext: QueryWithContext): Promise<QueryResult> => {
        const { queryString, queryContext, wrapQuery } = queryWithContext;
        let queryId: string | undefined;
        let sqlQueryId: string | undefined;
        let wrappedLimit: number | undefined;

        let queryResult: HeaderRows;
        const startTime = new Date();
        let endTime: Date;

        if (QueryView.isRune(queryString)) {
          // Secret way to issue a native JSON "rune" query
          const runeQuery = Hjson.parse(queryString);

          if (!isEmptyContext(queryContext)) runeQuery.context = queryContext;
          let runeResult: any[];
          try {
            const runeResultResp = await axios.post('/druid/v2', runeQuery);
            endTime = new Date();
            runeResult = runeResultResp.data;
            queryId = runeResultResp.headers['x-druid-query-id'];
          } catch (e) {
            throw new Error(getDruidErrorMessage(e));
          }

          queryResult = decodeRune(runeQuery, runeResult);
        } else {
          const actualQuery = wrapQuery
            ? `SELECT * FROM (${QueryView.trimSemicolon(queryString)}\n) LIMIT 1000`
            : queryString;

          if (wrapQuery) wrappedLimit = 1000;

          const queryPayload: Record<string, any> = {
            query: actualQuery,
            resultFormat: 'array',
            header: true,
          };

          if (!isEmptyContext(queryContext)) queryPayload.context = queryContext;
          let sqlResult: any[];
          try {
            const sqlResultResp = await axios.post('/druid/v2/sql', queryPayload);
            endTime = new Date();
            sqlResult = sqlResultResp.data;
            sqlQueryId = sqlResultResp.headers['x-druid-sql-query-id'];
          } catch (e) {
            throw new Error(getDruidErrorMessage(e));
          }

          queryResult = {
            header: sqlResult && sqlResult.length ? sqlResult[0] : [],
            rows: sqlResult && sqlResult.length ? sqlResult.slice(1) : [],
          };
        }

        return {
          queryResult,
          queryExtraInfo: {
            queryId,
            sqlQueryId,
            startTime,
            endTime,
            numResults: queryResult.rows.length,
            wrappedLimit,
          },
        };
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          result: result ? result.queryResult : undefined,
          queryExtraInfo: result ? result.queryExtraInfo : undefined,
          loading,
          error,
        });
      },
    });

    this.explainQueryManager = new QueryManager({
      processQuery: async (queryWithContext: QueryWithContext) => {
        const { queryString, queryContext } = queryWithContext;
        const explainPayload: Record<string, any> = {
          query: `EXPLAIN PLAN FOR (${QueryView.trimSemicolon(queryString)}\n)`,
          resultFormat: 'object',
        };

        if (!isEmptyContext(queryContext)) explainPayload.context = queryContext;
        const result = await queryDruidSql(explainPayload);

        return parseQueryPlan(result[0]['PLAN']);
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          explainResult: result,
          loadingExplain: loading,
          explainError: error,
        });
      },
    });
  }

  componentDidMount(): void {
    this.metadataQueryManager.runQuery(null);
  }

  componentWillUnmount(): void {
    this.metadataQueryManager.terminate();
    this.sqlQueryManager.terminate();
    this.explainQueryManager.terminate();
  }

  handleDownload = (filename: string, format: string) => {
    const { result } = this.state;
    if (!result) return;
    let lines: string[] = [];
    let separator: string = '';

    if (format === 'csv' || format === 'tsv') {
      separator = format === 'csv' ? ',' : '\t';
      lines.push(result.header.map(str => QueryView.formatStr(str, format)).join(separator));
      lines = lines.concat(
        result.rows.map(r => r.map(cell => QueryView.formatStr(cell, format)).join(separator)),
      );
    } else {
      // json
      lines = result.rows.map(r => {
        const outputObject: Record<string, any> = {};
        for (let k = 0; k < r.length; k++) {
          const newName = result.header[k];
          if (newName) {
            outputObject[newName] = r[k];
          }
        }
        return JSON.stringify(outputObject);
      });
    }

    const lineBreak = '\n';
    downloadFile(lines.join(lineBreak), format, filename);
  };

  renderExplainDialog() {
    const { explainDialogOpen, explainResult, loadingExplain, explainError } = this.state;
    if (loadingExplain || !explainDialogOpen) return;

    return (
      <QueryPlanDialog
        explainResult={explainResult}
        explainError={explainError}
        onClose={() => this.setState({ explainDialogOpen: false })}
      />
    );
  }

  renderEditContextDialog() {
    const { editContextDialogOpen, queryContext } = this.state;
    if (!editContextDialogOpen) return;

    return (
      <EditContextDialog
        onSubmit={(queryContext: QueryContext) =>
          this.setState({ queryContext, editContextDialogOpen: false })
        }
        onClose={() => this.setState({ editContextDialogOpen: false })}
        queryContext={queryContext}
      />
    );
  }

  renderMainArea() {
    const {
      queryString,
      queryContext,
      loading,
      result,
      queryExtraInfo,
      error,
      columnMetadata,
    } = this.state;
    const runeMode = QueryView.isRune(queryString);

    return (
      <SplitterLayout
        vertical
        percentage
        secondaryInitialSize={
          Number(localStorageGet(LocalStorageKeys.QUERY_VIEW_PANE_SIZE) as string) || 60
        }
        primaryMinSize={30}
        secondaryMinSize={30}
        onSecondaryPaneSizeChange={this.handleSecondaryPaneSizeChange}
      >
        <div className="control-pane">
          <QueryInput
            queryString={queryString}
            onQueryStringChange={this.handleQueryStringChange}
            runeMode={runeMode}
            columnMetadata={columnMetadata}
          />
          <div className="control-bar">
            <RunButton
              renderEditContextDialog={() => this.setState({ editContextDialogOpen: true })}
              runeMode={runeMode}
              queryContext={queryContext}
              onQueryContextChange={this.handleQueryContextChange}
              onRun={this.handleRun}
              onExplain={this.handleExplain}
            />
            {queryExtraInfo && (
              <QueryExtraInfo queryExtraInfo={queryExtraInfo} onDownload={this.handleDownload} />
            )}
          </div>
        </div>
        <QueryOutput loading={loading} result={result} error={error} />
      </SplitterLayout>
    );
  }

  private handleQueryStringChange = (queryString: string): void => {
    this.setState({ queryString });
  };

  private handleQueryContextChange = (queryContext: QueryContext) => {
    this.setState({ queryContext });
  };

  private handleRun = (wrapQuery: boolean) => {
    const { queryString, queryContext } = this.state;

    if (QueryView.isRune(queryString) && !QueryView.validRune(queryString)) return;

    localStorageSet(LocalStorageKeys.QUERY_KEY, queryString);
    this.sqlQueryManager.runQuery({ queryString, queryContext, wrapQuery });
  };

  private handleExplain = () => {
    const { queryString, queryContext } = this.state;
    this.setState({ explainDialogOpen: true });
    this.explainQueryManager.runQuery({ queryString, queryContext });
  };

  private handleSecondaryPaneSizeChange = (secondaryPaneSize: number) => {
    localStorageSet(LocalStorageKeys.QUERY_VIEW_PANE_SIZE, String(secondaryPaneSize));
  };

  render(): JSX.Element {
    const { columnMetadata, columnMetadataLoading, columnMetadataError } = this.state;

    return (
      <div
        className={classNames('query-view app-view', { 'hide-column-tree': columnMetadataError })}
      >
        {!columnMetadataError && (
          <ColumnTree
            columnMetadataLoading={columnMetadataLoading}
            columnMetadata={columnMetadata}
            onQueryStringChange={this.handleQueryStringChange}
          />
        )}
        {this.renderMainArea()}
        {this.renderExplainDialog()}
        {this.renderEditContextDialog()}
      </div>
    );
  }
}
