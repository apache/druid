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

import { Intent, Switch, Tooltip } from '@blueprintjs/core';
import axios from 'axios';
import classNames from 'classnames';
import {
  HeaderRows,
  isFirstRowHeader,
  normalizeQueryResult,
  parseSqlQuery,
  shouldIncludeTimestamp,
  SqlQuery,
} from 'druid-query-toolkit';
import Hjson from 'hjson';
import memoizeOne from 'memoize-one';
import React from 'react';
import SplitterLayout from 'react-splitter-layout';

import { QueryPlanDialog } from '../../dialogs';
import { EditContextDialog } from '../../dialogs/edit-context-dialog/edit-context-dialog';
import { QueryHistoryDialog } from '../../dialogs/query-history-dialog/query-history-dialog';
import { AppToaster } from '../../singletons/toaster';
import {
  BasicQueryExplanation,
  downloadFile,
  getDruidErrorMessage,
  localStorageGet,
  localStorageGetJson,
  LocalStorageKeys,
  localStorageSet,
  localStorageSetJson,
  parseQueryPlan,
  queryDruidSql,
  QueryManager,
  SemiJoinQueryExplanation,
} from '../../utils';
import { ColumnMetadata } from '../../utils/column-metadata';
import { isEmptyContext, QueryContext } from '../../utils/query-context';
import { QueryRecord, QueryRecordUtil } from '../../utils/query-history';

import { ColumnTree } from './column-tree/column-tree';
import { QueryExtraInfo, QueryExtraInfoData } from './query-extra-info/query-extra-info';
import { QueryInput } from './query-input/query-input';
import { QueryOutput } from './query-output/query-output';
import { RunButton } from './run-button/run-button';

import './query-view.scss';

const parser = memoizeOne((sql: string): SqlQuery | undefined => {
  try {
    return parseSqlQuery(sql);
  } catch {
    return;
  }
});

interface QueryWithContext {
  queryString: string;
  queryContext: QueryContext;
  wrapQueryLimit: number | undefined;
}

export interface QueryViewProps {
  initQuery: string | undefined;
  defaultQueryContext?: Record<string, any>;
  mandatoryQueryContext?: Record<string, any>;
}

export interface QueryViewState {
  queryString: string;
  parsedQuery?: SqlQuery;
  queryContext: QueryContext;
  wrapQueryLimit: number | undefined;
  autoRun: boolean;

  columnMetadataLoading: boolean;
  columnMetadata?: readonly ColumnMetadata[];
  columnMetadataError?: string;

  loading: boolean;
  result?: QueryResult;
  error?: string;

  explainDialogOpen: boolean;
  explainResult?: BasicQueryExplanation | SemiJoinQueryExplanation | string;
  loadingExplain: boolean;
  explainError?: string;

  defaultSchema?: string;
  defaultTable?: string;

  editContextDialogOpen: boolean;
  historyDialogOpen: boolean;
  queryHistory: readonly QueryRecord[];
}

interface QueryResult {
  queryResult: HeaderRows;
  queryExtraInfo: QueryExtraInfoData;
  parsedQuery?: SqlQuery;
}

export class QueryView extends React.PureComponent<QueryViewProps, QueryViewState> {
  static trimSemicolon(query: string): string {
    // Trims out a trailing semicolon while preserving space (https://bit.ly/1n1yfkJ)
    return query.replace(/;+((?:\s*--[^\n]*)?\s*)$/, '$1');
  }

  static isEmptyQuery(query: string): boolean {
    return query.trim() === '';
  }

  static isExplainQuery(query: string): boolean {
    return /EXPLAIN\sPLAN\sFOR/i.test(query);
  }

  static wrapInExplainIfNeeded(query: string): string {
    query = QueryView.trimSemicolon(query);
    if (QueryView.isExplainQuery(query)) return query;
    return `EXPLAIN PLAN FOR (${query}\n)`;
  }

  static isJsonLike(queryString: string): boolean {
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
    const { mandatoryQueryContext } = props;

    const queryString = props.initQuery || localStorageGet(LocalStorageKeys.QUERY_KEY) || '';
    const parsedQuery = queryString ? parser(queryString) : undefined;

    const queryContext =
      localStorageGetJson(LocalStorageKeys.QUERY_CONTEXT) || props.defaultQueryContext || {};

    const possibleQueryHistory = localStorageGetJson(LocalStorageKeys.QUERY_HISTORY);
    const queryHistory = Array.isArray(possibleQueryHistory) ? possibleQueryHistory : [];

    const possibleAutoRun = localStorageGetJson(LocalStorageKeys.AUTO_RUN);
    const autoRun = typeof possibleAutoRun === 'boolean' ? possibleAutoRun : true;

    this.state = {
      queryString,
      parsedQuery,
      queryContext,
      wrapQueryLimit: 100,
      autoRun,

      columnMetadataLoading: false,

      loading: false,

      explainDialogOpen: false,
      loadingExplain: false,

      editContextDialogOpen: false,
      historyDialogOpen: false,
      queryHistory,
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
        const { queryString, queryContext, wrapQueryLimit } = queryWithContext;

        let parsedQuery: SqlQuery | undefined;
        let jsonQuery: any;

        try {
          parsedQuery = parser(queryString);
        } catch {}

        if (!(parsedQuery instanceof SqlQuery)) {
          parsedQuery = undefined;
        }
        if (QueryView.isJsonLike(queryString)) {
          jsonQuery = Hjson.parse(queryString);
        } else {
          jsonQuery = {
            query: queryString,
            resultFormat: 'array',
            header: true,
          };
        }

        if (!isEmptyContext(queryContext) || wrapQueryLimit || mandatoryQueryContext) {
          jsonQuery.context = Object.assign(
            {},
            jsonQuery.context || {},
            queryContext,
            mandatoryQueryContext || {},
          );
          jsonQuery.context.sqlOuterLimit = wrapQueryLimit;
        }

        let rawQueryResult: unknown;
        let queryId: string | undefined;
        let sqlQueryId: string | undefined;
        const startTime = new Date();
        let endTime: Date;
        if (!jsonQuery.queryType && typeof jsonQuery.query === 'string') {
          try {
            const sqlResultResp = await axios.post('/druid/v2/sql', jsonQuery);
            endTime = new Date();
            rawQueryResult = sqlResultResp.data;
            sqlQueryId = sqlResultResp.headers['x-druid-sql-query-id'];
          } catch (e) {
            throw new Error(getDruidErrorMessage(e));
          }
        } else {
          try {
            const runeResultResp = await axios.post('/druid/v2', jsonQuery);
            endTime = new Date();
            rawQueryResult = runeResultResp.data;
            queryId = runeResultResp.headers['x-druid-query-id'];
          } catch (e) {
            throw new Error(getDruidErrorMessage(e));
          }
        }

        const queryResult = normalizeQueryResult(
          rawQueryResult,
          shouldIncludeTimestamp(jsonQuery),
          isFirstRowHeader(jsonQuery),
        );
        return {
          queryResult,
          queryExtraInfo: {
            queryId,
            sqlQueryId,
            startTime,
            endTime,
            numResults: queryResult.rows.length,
            wrapQueryLimit,
          },
          parsedQuery,
        };
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          result,
          loading,
          error,
        });
      },
    });

    this.explainQueryManager = new QueryManager({
      processQuery: async (queryWithContext: QueryWithContext) => {
        const { queryString, queryContext, wrapQueryLimit } = queryWithContext;

        const explainPayload: Record<string, any> = {
          query: QueryView.wrapInExplainIfNeeded(queryString),
          resultFormat: 'object',
        };

        if (!isEmptyContext(queryContext) || wrapQueryLimit) {
          explainPayload.context = Object.assign({}, queryContext || {});
          explainPayload.context.sqlOuterLimit = wrapQueryLimit;
        }
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

  prettyPrintJson(): void {
    this.setState(prevState => ({
      queryString: JSON.stringify(Hjson.parse(prevState.queryString), null, 2),
    }));
  }

  handleDownload = (filename: string, format: string) => {
    const { result } = this.state;
    if (!result) return;
    const { queryResult } = result;

    let lines: string[] = [];
    let separator: string = '';

    if (format === 'csv' || format === 'tsv') {
      separator = format === 'csv' ? ',' : '\t';
      lines.push(queryResult.header.map(str => QueryView.formatStr(str, format)).join(separator));
      lines = lines.concat(
        queryResult.rows.map(r => r.map(cell => QueryView.formatStr(cell, format)).join(separator)),
      );
    } else {
      // json
      lines = queryResult.rows.map(r => {
        const outputObject: Record<string, any> = {};
        for (let k = 0; k < r.length; k++) {
          const newName = queryResult.header[k];
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
        setQueryString={this.handleQueryStringChange}
        onClose={() => this.setState({ explainDialogOpen: false })}
      />
    );
  }

  renderHistoryDialog() {
    const { historyDialogOpen, queryHistory } = this.state;
    if (!historyDialogOpen) return;

    return (
      <QueryHistoryDialog
        queryRecords={queryHistory}
        setQueryString={(queryString, queryContext) => {
          this.handleQueryContextChange(queryContext);
          this.handleQueryStringChange(queryString);
        }}
        onClose={() => this.setState({ historyDialogOpen: false })}
      />
    );
  }

  renderEditContextDialog() {
    const { editContextDialogOpen, queryContext } = this.state;
    if (!editContextDialogOpen) return;

    return (
      <EditContextDialog
        onQueryContextChange={this.handleQueryContextChange}
        onClose={() => {
          this.setState({ editContextDialogOpen: false });
        }}
        queryContext={queryContext}
      />
    );
  }

  renderAutoRunSwitch() {
    const { autoRun, queryString } = this.state;
    if (QueryView.isJsonLike(queryString)) return;

    return (
      <Tooltip
        content="Automatically run queries when modified via helper action menus."
        hoverOpenDelay={800}
      >
        <Switch
          className="auto-run"
          checked={autoRun}
          label="Auto run"
          onChange={() => this.handleAutoRunChange(!autoRun)}
        />
      </Tooltip>
    );
  }

  renderWrapQueryLimitSelector() {
    const { wrapQueryLimit, queryString } = this.state;
    if (QueryView.isJsonLike(queryString)) return;

    return (
      <Tooltip
        content="Automatically wrap the query with a limit to protect against queries with very large result sets."
        hoverOpenDelay={800}
      >
        <Switch
          className="smart-query-limit"
          checked={Boolean(wrapQueryLimit)}
          label="Smart query limit"
          onChange={() => this.handleWrapQueryLimitChange(wrapQueryLimit ? undefined : 100)}
        />
      </Tooltip>
    );
  }

  renderMainArea() {
    const { queryString, queryContext, loading, result, error, columnMetadata } = this.state;
    const emptyQuery = QueryView.isEmptyQuery(queryString);

    let currentSchema: string | undefined;
    let currentTable: string | undefined;

    if (result && result.parsedQuery) {
      currentSchema = result.parsedQuery.getSchema();
      currentTable = result.parsedQuery.getTableName();
    } else if (localStorageGet(LocalStorageKeys.QUERY_KEY)) {
      const defaultQueryString = localStorageGet(LocalStorageKeys.QUERY_KEY);

      const defaultQueryAst: SqlQuery | undefined = defaultQueryString
        ? parser(defaultQueryString)
        : undefined;

      if (defaultQueryAst) {
        currentSchema = defaultQueryAst.getSchema();
        currentTable = defaultQueryAst.getTableName();
      }
    }

    const runeMode = QueryView.isJsonLike(queryString);
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
            currentSchema={currentSchema ? currentSchema : 'druid'}
            currentTable={currentTable}
            queryString={queryString}
            onQueryStringChange={this.handleQueryStringChange}
            runeMode={runeMode}
            columnMetadata={columnMetadata}
          />
          <div className="control-bar">
            <RunButton
              onEditContext={() => this.setState({ editContextDialogOpen: true })}
              runeMode={runeMode}
              queryContext={queryContext}
              onQueryContextChange={this.handleQueryContextChange}
              onRun={emptyQuery ? undefined : this.handleRun}
              onExplain={emptyQuery ? undefined : this.handleExplain}
              onHistory={() => this.setState({ historyDialogOpen: true })}
              onPrettier={() => this.prettyPrintJson()}
              loading={loading}
            />
            {this.renderAutoRunSwitch()}
            {this.renderWrapQueryLimitSelector()}
            {result && (
              <QueryExtraInfo
                queryExtraInfo={result.queryExtraInfo}
                onDownload={this.handleDownload}
              />
            )}
          </div>
        </div>
        <QueryOutput
          runeMode={runeMode}
          loading={loading}
          error={error}
          queryResult={result ? result.queryResult : undefined}
          parsedQuery={result ? result.parsedQuery : undefined}
          onQueryChange={this.handleQueryStringChange}
        />
      </SplitterLayout>
    );
  }

  private handleQueryStringChange = (
    queryString: string | SqlQuery,
    preferablyRun?: boolean,
  ): void => {
    if (queryString instanceof SqlQuery) queryString = queryString.toString();
    this.setState({ queryString, parsedQuery: parser(queryString) }, () => {
      const { autoRun } = this.state;
      if (preferablyRun && autoRun) this.handleRun();
    });
  };

  private handleQueryContextChange = (queryContext: QueryContext) => {
    this.setState({ queryContext });
  };

  private handleAutoRunChange = (autoRun: boolean) => {
    this.setState({ autoRun });
    localStorageSetJson(LocalStorageKeys.AUTO_RUN, autoRun);
  };

  private handleWrapQueryLimitChange = (wrapQueryLimit: number | undefined) => {
    this.setState({ wrapQueryLimit });
  };

  private handleRun = () => {
    const { queryString, queryContext, wrapQueryLimit, queryHistory } = this.state;
    if (QueryView.isJsonLike(queryString) && !QueryView.validRune(queryString)) return;

    const newQueryHistory = QueryRecordUtil.addQueryToHistory(
      queryHistory,
      queryString,
      queryContext,
    );

    localStorageSetJson(LocalStorageKeys.QUERY_HISTORY, newQueryHistory);
    localStorageSet(LocalStorageKeys.QUERY_KEY, queryString);
    localStorageSetJson(LocalStorageKeys.QUERY_CONTEXT, queryContext);

    this.setState({ queryHistory: newQueryHistory });
    this.sqlQueryManager.runQuery({ queryString, queryContext, wrapQueryLimit });
  };

  private handleExplain = () => {
    const { queryString, queryContext, wrapQueryLimit } = this.state;

    this.setState({ explainDialogOpen: true });
    this.explainQueryManager.runQuery({ queryString, queryContext, wrapQueryLimit });
  };

  private handleSecondaryPaneSizeChange = (secondaryPaneSize: number) => {
    localStorageSet(LocalStorageKeys.QUERY_VIEW_PANE_SIZE, String(secondaryPaneSize));
  };

  private getParsedQuery = () => {
    const { parsedQuery } = this.state;
    return parsedQuery;
  };

  render(): JSX.Element {
    const { columnMetadata, columnMetadataLoading, columnMetadataError, parsedQuery } = this.state;

    let defaultSchema;
    let defaultTable;
    if (parsedQuery instanceof SqlQuery) {
      defaultSchema = parsedQuery.getSchema();
      defaultTable = parsedQuery.getTableName();
    }

    return (
      <div
        className={classNames('query-view app-view', { 'hide-column-tree': columnMetadataError })}
      >
        {!columnMetadataError && (
          <ColumnTree
            getParsedQuery={this.getParsedQuery}
            columnMetadataLoading={columnMetadataLoading}
            columnMetadata={columnMetadata}
            onQueryStringChange={this.handleQueryStringChange}
            defaultSchema={defaultSchema ? defaultSchema : 'druid'}
            defaultTable={defaultTable}
          />
        )}
        {this.renderMainArea()}
        {this.renderExplainDialog()}
        {this.renderHistoryDialog()}
        {this.renderEditContextDialog()}
      </div>
    );
  }
}
