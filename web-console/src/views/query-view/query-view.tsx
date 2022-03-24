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

import { Code, Intent, Switch } from '@blueprintjs/core';
import { Tooltip2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import { QueryResult, QueryRunner, SqlExpression, SqlQuery } from 'druid-query-toolkit';
import Hjson from 'hjson';
import * as JSONBig from 'json-bigint-native';
import memoizeOne from 'memoize-one';
import React, { RefObject } from 'react';
import SplitterLayout from 'react-splitter-layout';
import { v4 as uuidv4 } from 'uuid';

import { Loader } from '../../components';
import { EditContextDialog } from '../../dialogs/edit-context-dialog/edit-context-dialog';
import { QueryHistoryDialog } from '../../dialogs/query-history-dialog/query-history-dialog';
import { Api, AppToaster } from '../../singletons';
import {
  ColumnMetadata,
  downloadFile,
  DruidError,
  findEmptyLiteralPosition,
  localStorageGet,
  localStorageGetJson,
  LocalStorageKeys,
  localStorageSet,
  localStorageSetJson,
  QueryAction,
  queryDruidSql,
  QueryManager,
  QueryState,
  QueryWithContext,
  RowColumn,
  stringifyValue,
} from '../../utils';
import { QueryContext } from '../../utils/query-context';
import { QueryRecord, QueryRecordUtil } from '../../utils/query-history';

import { ColumnTree } from './column-tree/column-tree';
import { ExplainDialog } from './explain-dialog/explain-dialog';
import {
  LIVE_QUERY_MODES,
  LiveQueryMode,
  LiveQueryModeSelector,
} from './live-query-mode-selector/live-query-mode-selector';
import { QueryError } from './query-error/query-error';
import { QueryExtraInfo } from './query-extra-info/query-extra-info';
import { QueryInput } from './query-input/query-input';
import { QueryOutput } from './query-output/query-output';
import { QueryTimer } from './query-timer/query-timer';
import { RunButton } from './run-button/run-button';

import './query-view.scss';

const LAST_DAY = SqlExpression.parse(`__time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY`);

const parser = memoizeOne((sql: string): SqlQuery | undefined => {
  try {
    return SqlQuery.parse(sql);
  } catch {
    return;
  }
});

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

  explainDialogQuery?: QueryWithContext;

  defaultSchema?: string;
  defaultTable?: string;

  editContextDialogOpen: boolean;
  historyDialogOpen: boolean;
  queryHistory: readonly QueryRecord[];
}

export class QueryView extends React.PureComponent<QueryViewProps, QueryViewState> {
  static isEmptyQuery(query: string): boolean {
    return query.trim() === '';
  }

  static isJsonLike(queryString: string): boolean {
    return queryString.trim().startsWith('{');
  }

  static isSql(query: any): boolean {
    if (typeof query === 'string') return true;
    return typeof query.query === 'string';
  }

  static validRune(queryString: string): boolean {
    try {
      Hjson.parse(queryString);
      return true;
    } catch {
      return false;
    }
  }

  static formatStr(s: null | string | number | Date, format: 'csv' | 'tsv') {
    // stringify and remove line break
    const str = stringifyValue(s).replace(/(?:\r\n|\r|\n)/g, ' ');

    if (format === 'csv') {
      // csv: single quote => double quote, handle ','
      return `"${str.replace(/"/g, '""')}"`;
    } else {
      // tsv: single quote => double quote, \t => ''
      return str.replace(/\t/g, '').replace(/"/g, '""');
    }
  }

  private readonly metadataQueryManager: QueryManager<null, ColumnMetadata[]>;
  private readonly queryManager: QueryManager<QueryWithContext, QueryResult>;

  private readonly queryInputRef: RefObject<QueryInput>;

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

    const queryRunner = new QueryRunner();

    this.queryManager = new QueryManager({
      processQuery: async (
        queryWithContext: QueryWithContext,
        cancelToken,
      ): Promise<QueryResult> => {
        const { queryString, queryContext, wrapQueryLimit } = queryWithContext;
        const query = QueryView.isJsonLike(queryString) ? Hjson.parse(queryString) : queryString;
        const isSql = QueryView.isSql(query);
        const extraQueryContext = { ...queryContext, ...(mandatoryQueryContext || {}) };

        if (isSql && typeof wrapQueryLimit !== 'undefined') {
          extraQueryContext.sqlOuterLimit = wrapQueryLimit + 1;
        }

        const queryIdKey = isSql ? 'sqlQueryId' : 'queryId';
        // Look for an existing queryId in the JSON itself or in the extra context object.
        let cancelQueryId = query.context?.[queryIdKey] || extraQueryContext[queryIdKey];
        if (!cancelQueryId) {
          // If the queryId (sqlQueryId) is not explicitly set on the context generate one thus making it possible to cancel the query.
          cancelQueryId = extraQueryContext[queryIdKey] = uuidv4();
        }

        void cancelToken.promise
          .then(() => {
            return Api.instance.delete(
              `/druid/v2${isSql ? '/sql' : ''}/${Api.encodePath(cancelQueryId)}`,
            );
          })
          .catch(() => {});

        try {
          return await queryRunner.runQuery({
            query,
            extraQueryContext,
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
  }

  componentDidMount(): void {
    const { initQuery } = this.props;
    const { liveQueryMode } = this.state;

    this.metadataQueryManager.runQuery(null);

    if (liveQueryMode !== 'off' && initQuery) {
      this.handleRun();
    }
  }

  componentWillUnmount(): void {
    this.metadataQueryManager.terminate();
    this.queryManager.terminate();
  }

  private prettyPrintJson(): void {
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

  private readonly handleDownload = (filename: string, format: string) => {
    const { queryResultState } = this.state;
    const queryResult = queryResultState.data;
    if (!queryResult) return;

    let lines: string[] = [];
    let separator = '';

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

  private readonly handleLoadMore = () => {
    this.setState(
      ({ wrapQueryLimit }) => ({
        wrapQueryLimit: wrapQueryLimit ? wrapQueryLimit * 10 : undefined,
      }),
      this.handleRun,
    );
  };

  private renderExplainDialog() {
    const { mandatoryQueryContext } = this.props;
    const { explainDialogQuery } = this.state;
    if (!explainDialogQuery) return;

    return (
      <ExplainDialog
        queryWithContext={explainDialogQuery}
        mandatoryQueryContext={mandatoryQueryContext}
        setQueryString={this.handleQueryStringChange}
        onClose={() => this.setState({ explainDialogQuery: undefined })}
      />
    );
  }

  private renderHistoryDialog() {
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

  private renderEditContextDialog() {
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

  private renderLiveQueryModeSelector() {
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

  private renderWrapQueryLimitSelector() {
    const { wrapQueryLimit, queryString } = this.state;
    if (QueryView.isJsonLike(queryString)) return;

    return (
      <Tooltip2
        content="Automatically wrap the query with a limit to protect against queries with very large result sets."
        hoverOpenDelay={800}
      >
        <Switch
          className="auto-limit"
          checked={Boolean(wrapQueryLimit)}
          label="Auto limit"
          onChange={() => this.handleWrapQueryLimitChange(wrapQueryLimit ? undefined : 100)}
        />
      </Tooltip2>
    );
  }

  private renderMainArea() {
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
        secondaryInitialSize={Number(localStorageGet(LocalStorageKeys.QUERY_VIEW_PANE_SIZE)!) || 60}
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
              <QueryExtraInfo
                queryResult={queryResult}
                onDownload={this.handleDownload}
                onLoadMore={this.handleLoadMore}
              />
            )}
            {queryResultState.loading && <QueryTimer />}
          </div>
        </div>
        <div className="output-pane">
          {someQueryResult && (
            <QueryOutput
              runeMode={runeMode}
              queryResult={someQueryResult}
              onQueryAction={this.handleQueryAction}
              onLoadMore={this.handleLoadMore}
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
              {emptyQuery ? (
                <p>
                  Enter a query and click <Code>Run</Code>
                </p>
              ) : (
                <p>
                  Click <Code>Run</Code> to execute the query
                </p>
              )}
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

  private readonly handleQueryChange = (query: SqlQuery, preferablyRun?: boolean): void => {
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

  private readonly handleQueryAction = (queryAction: QueryAction): void => {
    const { parsedQuery } = this.state;
    if (!parsedQuery) return;
    this.handleQueryChange(parsedQuery.apply(queryAction), true);
  };

  private readonly handleQueryStringChange = (
    queryString: string,
    preferablyRun?: boolean,
  ): void => {
    const parsedQuery = parser(queryString);
    this.setState({ queryString, parsedQuery }, preferablyRun ? this.handleRunIfLive : undefined);
  };

  private readonly handleQueryContextChange = (queryContext: QueryContext) => {
    this.setState({ queryContext });
  };

  private readonly handleLiveQueryModeChange = (liveQueryMode: LiveQueryMode) => {
    this.setState({ liveQueryMode });
    localStorageSetJson(LocalStorageKeys.LIVE_QUERY_MODE, liveQueryMode);
  };

  private readonly handleWrapQueryLimitChange = (wrapQueryLimit: number | undefined) => {
    this.setState({ wrapQueryLimit });
  };

  private readonly handleRun = () => {
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

  private readonly handleRunIfLive = () => {
    const { liveQueryMode } = this.state;
    if (liveQueryMode === 'off') return;
    if (liveQueryMode === 'auto' && !this.autoLiveQueryModeShouldRun()) return;
    this.handleRun();
  };

  private readonly handleExplain = () => {
    const { queryString, queryContext, wrapQueryLimit } = this.state;

    this.setState({
      explainDialogQuery: {
        queryString,
        queryContext,
        wrapQueryLimit,
      },
    });
  };

  private readonly handleSecondaryPaneSizeChange = (secondaryPaneSize: number) => {
    localStorageSet(LocalStorageKeys.QUERY_VIEW_PANE_SIZE, String(secondaryPaneSize));
  };

  private readonly getParsedQuery = () => {
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
            defaultWhere={LAST_DAY}
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
