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

import { Code, Intent, Switch, Tooltip } from '@blueprintjs/core';
import classNames from 'classnames';
import { QueryResult, QueryRunner, SqlQuery } from 'druid-query-toolkit';
import Hjson from 'hjson';
import * as JSONBig from 'json-bigint-native';
import memoizeOne from 'memoize-one';
import React, { RefObject } from 'react';
import SplitterLayout from 'react-splitter-layout';

import { Loader } from '../../components';
import { QueryPlanDialog } from '../../dialogs';
import { EditContextDialog } from '../../dialogs/edit-context-dialog/edit-context-dialog';
import { QueryHistoryDialog } from '../../dialogs/query-history-dialog/query-history-dialog';
import { Api, AppToaster } from '../../singletons';
import {
  BasicQueryExplanation,
  ColumnMetadata,
  downloadFile,
  DruidError,
  findEmptyLiteralPosition,
  getDruidErrorMessage,
  localStorageGet,
  localStorageGetJson,
  LocalStorageKeys,
  localStorageSet,
  localStorageSetJson,
  parseQueryPlan,
  queryDruidSql,
  QueryManager,
  QueryState,
  RowColumn,
  SemiJoinQueryExplanation,
} from '../../utils';
import { isEmptyContext, QueryContext } from '../../utils/query-context';
import { QueryRecord, QueryRecordUtil } from '../../utils/query-history';

import { ColumnTree } from './column-tree/column-tree';
import {
  LIVE_QUERY_MODES,
  LiveQueryMode,
  LiveQueryModeSelector,
} from './live-query-mode-selector/live-query-mode-selector';
import { QueryError } from './query-error/query-error';
import { QueryExtraInfo } from './query-extra-info/query-extra-info';
import { QueryInput } from './query-input/query-input';
import { QueryOutput } from './query-output/query-output';
import { RunButton } from './run-button/run-button';

import './query-view.scss';

const parser = memoizeOne((sql: string): SqlQuery | undefined => {
  try {
    return SqlQuery.parse(sql);
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
  liveQueryMode: LiveQueryMode;

  columnMetadataState: QueryState<readonly ColumnMetadata[]>;

  queryResultState: QueryState<QueryResult, DruidError>;

  explainDialogOpen: boolean;
  explainResultState: QueryState<BasicQueryExplanation | SemiJoinQueryExplanation | string>;

  defaultSchema?: string;
  defaultTable?: string;

  editContextDialogOpen: boolean;
  historyDialogOpen: boolean;
  queryHistory: readonly QueryRecord[];
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
  private queryManager: QueryManager<QueryWithContext, QueryResult>;
  private explainQueryManager: QueryManager<
    QueryWithContext,
    BasicQueryExplanation | SemiJoinQueryExplanation | string
  >;

  private queryInputRef: RefObject<QueryInput>;

  constructor(props: QueryViewProps, context: any) {
    super(props, context);
    const { mandatoryQueryContext } = props;

    this.queryInputRef = React.createRef();

    const queryString = props.initQuery || localStorageGet(LocalStorageKeys.QUERY_KEY) || '';
    const parsedQuery = queryString ? parser(queryString) : undefined;

    const queryContext =
      localStorageGetJson(LocalStorageKeys.QUERY_CONTEXT) || props.defaultQueryContext || {};

    const possibleQueryHistory = localStorageGetJson(LocalStorageKeys.QUERY_HISTORY);
    const queryHistory = Array.isArray(possibleQueryHistory) ? possibleQueryHistory : [];

    const possibleLiveQueryMode = localStorageGetJson(LocalStorageKeys.LIVE_QUERY_MODE);
    const liveQueryMode = LIVE_QUERY_MODES.includes(possibleLiveQueryMode)
      ? possibleLiveQueryMode
      : 'auto';

    this.state = {
      queryString,
      parsedQuery,
      queryContext,
      wrapQueryLimit: 100,
      liveQueryMode,

      columnMetadataState: QueryState.INIT,

      queryResultState: QueryState.INIT,

      explainDialogOpen: false,
      explainResultState: QueryState.INIT,

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
      onStateChange: columnMetadataState => {
        if (columnMetadataState.error) {
          AppToaster.show({
            message: 'Could not load SQL metadata',
            intent: Intent.DANGER,
          });
        }
        this.setState({
          columnMetadataState,
        });
      },
    });

    const queryRunner = new QueryRunner((payload, isSql, cancelToken) => {
      return Api.instance.post(`/druid/v2${isSql ? '/sql' : ''}`, payload, { cancelToken });
    });

    this.queryManager = new QueryManager({
      processQuery: async (
        queryWithContext: QueryWithContext,
        cancelToken,
      ): Promise<QueryResult> => {
        const { queryString, queryContext, wrapQueryLimit } = queryWithContext;

        const query = QueryView.isJsonLike(queryString) ? Hjson.parse(queryString) : queryString;

        let context: Record<string, any> | undefined;
        if (!isEmptyContext(queryContext) || wrapQueryLimit || mandatoryQueryContext) {
          context = Object.assign({}, queryContext, mandatoryQueryContext || {});
          if (typeof wrapQueryLimit !== 'undefined') {
            context.sqlOuterLimit = wrapQueryLimit;
          }
        }

        try {
          return await queryRunner.runQuery({
            query,
            extraQueryContext: context,
            cancelToken,
          });
        } catch (e) {
          throw new DruidError(e);
        }
      },
      onStateChange: queryResultState => {
        this.setState({
          queryResultState,
        });
      },
    });

    this.explainQueryManager = new QueryManager({
      processQuery: async (queryWithContext: QueryWithContext) => {
        const { queryString, queryContext, wrapQueryLimit } = queryWithContext;

        let context: Record<string, any> | undefined;
        if (!isEmptyContext(queryContext) || wrapQueryLimit || mandatoryQueryContext) {
          context = Object.assign({}, queryContext, mandatoryQueryContext || {});
          if (typeof wrapQueryLimit !== 'undefined') {
            context.sqlOuterLimit = wrapQueryLimit;
          }
        }

        let result: QueryResult | undefined;
        try {
          result = await queryRunner.runQuery({
            query: QueryView.wrapInExplainIfNeeded(queryString),
            extraQueryContext: context,
          });
        } catch (e) {
          throw new Error(getDruidErrorMessage(e));
        }

        return parseQueryPlan(result!.rows[0][0]);
      },
      onStateChange: explainResultState => {
        this.setState({
          explainResultState,
        });
      },
    });
  }

  componentDidMount(): void {
    const { liveQueryMode, queryString } = this.state;

    this.metadataQueryManager.runQuery(null);

    if (liveQueryMode !== 'off' && queryString) {
      this.handleRun();
    }
  }

  componentWillUnmount(): void {
    this.metadataQueryManager.terminate();
    this.queryManager.terminate();
    this.explainQueryManager.terminate();
  }

  prettyPrintJson(): void {
    this.setState(prevState => {
      let parsed: any;
      try {
        parsed = Hjson.parse(prevState.queryString);
      } catch {
        return null;
      }
      return {
        queryString: JSONBig.stringify(parsed, undefined, 2),
      };
    });
  }

  handleDownload = (filename: string, format: string) => {
    const { queryResultState } = this.state;
    const queryResult = queryResultState.data;
    if (!queryResult) return;

    let lines: string[] = [];
    let separator: string = '';

    if (format === 'csv' || format === 'tsv') {
      separator = format === 'csv' ? ',' : '\t';
      lines.push(
        queryResult.header.map(column => QueryView.formatStr(column.name, format)).join(separator),
      );
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
            outputObject[newName.name] = r[k];
          }
        }
        return JSONBig.stringify(outputObject);
      });
    }

    const lineBreak = '\n';
    downloadFile(lines.join(lineBreak), format, filename);
  };

  renderExplainDialog() {
    const { explainDialogOpen, explainResultState } = this.state;
    if (explainResultState.loading || !explainDialogOpen) return;

    return (
      <QueryPlanDialog
        explainResult={explainResultState.data}
        explainError={explainResultState.error}
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

  renderLiveQueryModeSelector() {
    const { liveQueryMode, queryString } = this.state;
    if (QueryView.isJsonLike(queryString)) return;

    return (
      <LiveQueryModeSelector
        liveQueryMode={liveQueryMode}
        onLiveQueryModeChange={this.handleLiveQueryModeChange}
        autoLiveQueryModeShouldRun={this.autoLiveQueryModeShouldRun()}
      />
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
          className="auto-limit"
          checked={Boolean(wrapQueryLimit)}
          label="Auto limit"
          onChange={() => this.handleWrapQueryLimitChange(wrapQueryLimit ? undefined : 100)}
        />
      </Tooltip>
    );
  }

  renderMainArea() {
    const { queryString, queryContext, queryResultState, columnMetadataState } = this.state;
    const emptyQuery = QueryView.isEmptyQuery(queryString);
    const queryResult = queryResultState.data;

    let currentSchema: string | undefined;
    let currentTable: string | undefined;

    if (queryResult && queryResult.sqlQuery) {
      currentSchema = queryResult.sqlQuery.getFirstSchema();
      currentTable = queryResult.sqlQuery.getFirstTableName();
    } else if (localStorageGet(LocalStorageKeys.QUERY_KEY)) {
      const defaultQueryString = localStorageGet(LocalStorageKeys.QUERY_KEY);

      const defaultQueryAst: SqlQuery | undefined = defaultQueryString
        ? parser(defaultQueryString)
        : undefined;

      if (defaultQueryAst) {
        currentSchema = defaultQueryAst.getFirstSchema();
        currentTable = defaultQueryAst.getFirstTableName();
      }
    }

    const someQueryResult = queryResultState.getSomeData();
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
            ref={this.queryInputRef}
            currentSchema={currentSchema ? currentSchema : 'druid'}
            currentTable={currentTable}
            queryString={queryString}
            onQueryStringChange={this.handleQueryStringChange}
            runeMode={runeMode}
            columnMetadata={columnMetadataState.data}
          />
          <div className="query-control-bar">
            <RunButton
              onEditContext={() => this.setState({ editContextDialogOpen: true })}
              runeMode={runeMode}
              queryContext={queryContext}
              onQueryContextChange={this.handleQueryContextChange}
              onRun={emptyQuery ? undefined : this.handleRun}
              onExplain={emptyQuery ? undefined : this.handleExplain}
              onHistory={() => this.setState({ historyDialogOpen: true })}
              onPrettier={() => this.prettyPrintJson()}
              loading={queryResultState.loading}
            />
            {this.renderWrapQueryLimitSelector()}
            {this.renderLiveQueryModeSelector()}
            {queryResult && (
              <QueryExtraInfo queryResult={queryResult} onDownload={this.handleDownload} />
            )}
          </div>
        </div>
        <div className="output-pane">
          {someQueryResult && (
            <QueryOutput
              runeMode={runeMode}
              queryResult={someQueryResult}
              onQueryChange={this.handleQueryChange}
            />
          )}
          {queryResultState.error && (
            <QueryError
              error={queryResultState.error}
              moveCursorTo={position => {
                this.moveToPosition(position);
              }}
              queryString={queryString}
              onQueryStringChange={this.handleQueryStringChange}
            />
          )}
          {queryResultState.loading && (
            <Loader
              cancelText="Cancel query"
              onCancel={() => {
                this.queryManager.cancelCurrent();
              }}
            />
          )}
          {queryResultState.isInit() && (
            <div className="init-state">
              <p>
                Enter a query and click <Code>Run</Code>
              </p>
            </div>
          )}
        </div>
      </SplitterLayout>
    );
  }

  private moveToPosition(position: RowColumn) {
    const currentQueryInput = this.queryInputRef.current;
    if (!currentQueryInput) return;
    currentQueryInput.goToPosition(position);
  }

  private handleQueryChange = (query: SqlQuery, preferablyRun?: boolean): void => {
    this.handleQueryStringChange(query.toString(), preferablyRun);

    // Possibly move the cursor of the QueryInput to the empty literal position
    const emptyLiteralPosition = findEmptyLiteralPosition(query);
    if (emptyLiteralPosition) {
      // Introduce a delay to let the new text appear
      setTimeout(() => {
        this.moveToPosition(emptyLiteralPosition);
      }, 10);
    }
  };

  private handleQueryStringChange = (queryString: string, preferablyRun?: boolean): void => {
    const parsedQuery = parser(queryString);
    const newSate = { queryString, parsedQuery };
    this.setState(newSate, preferablyRun ? this.handleRunIfLive : undefined);
  };

  private handleQueryContextChange = (queryContext: QueryContext) => {
    this.setState({ queryContext });
  };

  private handleLiveQueryModeChange = (liveQueryMode: LiveQueryMode) => {
    this.setState({ liveQueryMode });
    localStorageSetJson(LocalStorageKeys.LIVE_QUERY_MODE, liveQueryMode);
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
    this.queryManager.runQuery({ queryString, queryContext, wrapQueryLimit });
  };

  private autoLiveQueryModeShouldRun() {
    const { queryResultState } = this.state;
    return (
      !queryResultState.data ||
      !queryResultState.data.queryDuration ||
      queryResultState.data.queryDuration < 10000
    );
  }

  private handleRunIfLive = () => {
    const { liveQueryMode } = this.state;
    if (liveQueryMode === 'off') return;
    if (liveQueryMode === 'auto' && !this.autoLiveQueryModeShouldRun()) return;
    this.handleRun();
  };

  private handleExplain = () => {
    const { queryString, queryContext, wrapQueryLimit } = this.state;

    this.setState({ explainDialogOpen: true });
    this.explainQueryManager.runQuery({
      queryString,
      queryContext,
      wrapQueryLimit,
    });
  };

  private handleSecondaryPaneSizeChange = (secondaryPaneSize: number) => {
    localStorageSet(LocalStorageKeys.QUERY_VIEW_PANE_SIZE, String(secondaryPaneSize));
  };

  private getParsedQuery = () => {
    const { parsedQuery } = this.state;
    return parsedQuery;
  };

  render(): JSX.Element {
    const { columnMetadataState, parsedQuery } = this.state;

    let defaultSchema;
    let defaultTable;
    if (parsedQuery instanceof SqlQuery) {
      defaultSchema = parsedQuery.getFirstSchema();
      defaultTable = parsedQuery.getFirstTableName();
    }

    return (
      <div
        className={classNames('query-view app-view', {
          'hide-column-tree': columnMetadataState.isError(),
        })}
      >
        {!columnMetadataState.isError() && (
          <ColumnTree
            getParsedQuery={this.getParsedQuery}
            columnMetadataLoading={columnMetadataState.loading}
            columnMetadata={columnMetadataState.data}
            onQueryChange={this.handleQueryChange}
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
