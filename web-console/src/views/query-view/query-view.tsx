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
import {
  AdditiveExpression,
  Alias,
  FilterClause,
  HeaderRows,
  isFirstRowHeader,
  normalizeQueryResult,
  RefExpression,
  shouldIncludeTimestamp,
  sqlParserFactory,
  SqlQuery,
  StringType,
  Timestamp,
} from 'druid-query-toolkit';
import Hjson from 'hjson';
import memoizeOne from 'memoize-one';
import React from 'react';
import SplitterLayout from 'react-splitter-layout';

import { SQL_FUNCTIONS, SyntaxDescription } from '../../../lib/sql-function-doc';
import { QueryPlanDialog } from '../../dialogs';
import { EditContextDialog } from '../../dialogs/edit-context-dialog/edit-context-dialog';
import {
  QueryHistoryDialog,
  QueryRecord,
} from '../../dialogs/query-history-dialog/query-history-dialog';
import { AppToaster } from '../../singletons/toaster';
import {
  BasicQueryExplanation,
  downloadFile,
  getDruidErrorMessage,
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

const parserRaw = sqlParserFactory(
  SQL_FUNCTIONS.map((sqlFunction: SyntaxDescription) => {
    return sqlFunction.syntax.substr(0, sqlFunction.syntax.indexOf('('));
  }),
);

const parser = memoizeOne((sql: string) => {
  try {
    return parserRaw(sql);
  } catch {
    return;
  }
});

interface QueryWithContext {
  queryString: string;
  queryContext: QueryContext;
  wrapQuery: boolean;
}

export interface QueryViewProps {
  initQuery: string | undefined;
}

export interface RowFilter {
  row: string | number | AdditiveExpression | Timestamp | StringType;
  header: string | Timestamp | StringType;
  operator: '!=' | '=' | '>' | '<' | 'like' | '>=' | '<=' | 'LIKE';
}

export interface QueryViewState {
  queryString: string;
  queryAst: SqlQuery;
  queryContext: QueryContext;
  wrapQuery: boolean;
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

  static wrapInLimitIfNeeded(query: string, limit = 1000): string {
    query = QueryView.trimSemicolon(query);
    if (QueryView.isExplainQuery(query)) return query;
    return `SELECT * FROM (${query}\n) LIMIT ${limit}`;
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

    let queryString: string | undefined;
    if (props.initQuery) {
      queryString = props.initQuery;
    } else if (localStorageGet(LocalStorageKeys.QUERY_KEY)) {
      queryString = localStorageGet(LocalStorageKeys.QUERY_KEY);
    }
    const queryAst = queryString ? parser(queryString) : undefined;

    const localStorageQueryHistory = localStorageGet(LocalStorageKeys.QUERY_HISTORY);
    let queryHistory = [];
    if (localStorageQueryHistory) {
      let possibleQueryHistory: unknown;
      try {
        possibleQueryHistory = JSON.parse(localStorageQueryHistory);
      } catch {}
      if (Array.isArray(possibleQueryHistory)) queryHistory = possibleQueryHistory;
    }

    const localStorageAutoRun = localStorageGet(LocalStorageKeys.AUTO_RUN);
    let autoRun = true;
    if (localStorageAutoRun) {
      let possibleAutoRun: unknown;
      try {
        possibleAutoRun = JSON.parse(localStorageAutoRun);
      } catch {}
      if (typeof possibleAutoRun === 'boolean') autoRun = possibleAutoRun;
    }

    this.state = {
      queryString: queryString ? queryString : '',
      queryAst,
      queryContext: {},
      wrapQuery: true,
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
        const { queryString, queryContext, wrapQuery } = queryWithContext;

        let ast: SqlQuery | undefined;
        let wrappedLimit: number | undefined;
        let jsonQuery: any;

        try {
          ast = parser(queryString);
        } catch {}

        if (!(ast instanceof SqlQuery)) {
          ast = undefined;
        }
        if (QueryView.isJsonLike(queryString)) {
          jsonQuery = Hjson.parse(queryString);
        } else {
          const actualQuery = wrapQuery
            ? QueryView.wrapInLimitIfNeeded(queryString)
            : QueryView.trimSemicolon(queryString);

          if (wrapQuery) wrappedLimit = 1000;

          jsonQuery = {
            query: actualQuery,
            resultFormat: 'array',
            header: true,
          };
        }

        if (!isEmptyContext(queryContext)) {
          jsonQuery.context = Object.assign(jsonQuery.context || {}, queryContext);
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
            wrappedLimit,
          },
          parsedQuery: ast,
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
        const { queryString, queryContext, wrapQuery } = queryWithContext;

        const actualQuery = wrapQuery
          ? QueryView.wrapInLimitIfNeeded(queryString)
          : QueryView.trimSemicolon(queryString);

        const explainPayload: Record<string, any> = {
          query: QueryView.wrapInExplainIfNeeded(actualQuery),
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
        onClose={() => this.setState({ explainDialogOpen: false })}
        setQueryString={(queryString: string) =>
          this.setState({ queryString, explainDialogOpen: false, queryAst: parser(queryString) })
        }
      />
    );
  }

  renderHistoryDialog() {
    const { historyDialogOpen, queryHistory } = this.state;
    if (!historyDialogOpen) return;

    return (
      <QueryHistoryDialog
        queryRecords={queryHistory}
        setQueryString={queryString =>
          this.setState({ queryString, queryAst: parser(queryString), historyDialogOpen: false })
        }
        onClose={() => this.setState({ historyDialogOpen: false })}
      />
    );
  }

  renderEditContextDialog() {
    const { editContextDialogOpen, queryContext } = this.state;
    if (!editContextDialogOpen) return;

    return (
      <EditContextDialog
        onQueryContextChange={(queryContext: QueryContext) =>
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
      error,
      columnMetadata,
      autoRun,
      wrapQuery,
    } = this.state;
    const emptyQuery = QueryView.isEmptyQuery(queryString);

    let currentSchema;
    let currentTable;

    if (result && result.parsedQuery instanceof SqlQuery) {
      currentSchema = result.parsedQuery.getSchema();
      currentTable = result.parsedQuery.getTableName();
    } else if (localStorageGet(LocalStorageKeys.QUERY_KEY)) {
      const defaultQueryString = localStorageGet(LocalStorageKeys.QUERY_KEY);
      const tempAst = defaultQueryString ? parser(defaultQueryString) : undefined;
      if (tempAst) {
        currentSchema = tempAst.getSchema();
        currentTable = tempAst.getTableName();
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
              autoRun={autoRun}
              setAutoRun={this.setAutoRun}
              wrapQuery={wrapQuery}
              setWrapQuery={this.setWrapQuery}
              onEditContext={() => this.setState({ editContextDialogOpen: true })}
              runeMode={runeMode}
              queryContext={queryContext}
              onQueryContextChange={this.handleQueryContextChange}
              onRun={emptyQuery ? undefined : this.handleRun}
              onExplain={emptyQuery ? undefined : this.handleExplain}
              onHistory={() => this.setState({ historyDialogOpen: true })}
            />
            {result && (
              <QueryExtraInfo
                queryExtraInfo={result.queryExtraInfo}
                onDownload={this.handleDownload}
              />
            )}
          </div>
        </div>
        <QueryOutput
          sqlExcludeColumn={this.sqlExcludeColumn}
          sqlFilterRow={this.sqlFilterRow}
          sqlOrderBy={this.sqlOrderBy}
          runeMode={runeMode}
          loading={loading}
          queryResult={result ? result.queryResult : undefined}
          parsedQuery={result ? result.parsedQuery : undefined}
          error={error}
        />
      </SplitterLayout>
    );
  }

  private addFunctionToGroupBy = (
    functionName: string,
    spacing: string[],
    argumentsArray: (StringType | number)[],
    preferablyRun: boolean,
    alias: Alias,
  ): void => {
    const { queryAst } = this.state;
    if (!queryAst) return;
    const modifiedAst = queryAst.addFunctionToGroupBy(functionName, spacing, argumentsArray, alias);
    this.handleQueryStringChange(modifiedAst.toString(), preferablyRun);
  };

  private addToGroupBy = (columnName: string, preferablyRun: boolean): void => {
    const { queryAst } = this.state;
    if (!queryAst) return;
    const modifiedAst = queryAst.addToGroupBy(columnName);
    this.handleQueryStringChange(modifiedAst.toString(), preferablyRun);
  };

  private addAggregateColumn = (
    columnName: string | RefExpression,
    functionName: string,
    preferablyRun: boolean,
    alias?: Alias,
    distinct?: boolean,
    filter?: FilterClause,
  ): void => {
    const { queryAst } = this.state;
    if (!queryAst) return;
    const modifiedAst = queryAst.addAggregateColumn(
      columnName,
      functionName,
      alias,
      distinct,
      filter,
    );
    this.handleQueryStringChange(modifiedAst.toString(), preferablyRun);
  };

  private sqlOrderBy = (
    header: string,
    direction: 'ASC' | 'DESC',
    preferablyRun: boolean,
  ): void => {
    const { queryAst } = this.state;
    if (!queryAst) return;
    const modifiedAst = queryAst.orderBy(header, direction);
    this.handleQueryStringChange(modifiedAst.toString(), preferablyRun);
  };

  private sqlExcludeColumn = (header: string, preferablyRun: boolean): void => {
    const { queryAst } = this.state;
    if (!queryAst) return;
    const modifiedAst = queryAst.excludeColumn(header);
    this.handleQueryStringChange(modifiedAst.toString(), preferablyRun);
  };

  private sqlFilterRow = (filters: RowFilter[], preferablyRun: boolean): void => {
    const { queryAst } = this.state;
    if (!queryAst) return;

    let modifiedAst: SqlQuery = queryAst;
    for (const filter of filters) {
      modifiedAst = modifiedAst.filterRow(filter.header, filter.row, filter.operator);
    }
    this.handleQueryStringChange(modifiedAst.toString(), preferablyRun);
  };

  private sqlClearWhere = (column: string, preferablyRun: boolean): void => {
    const { queryAst } = this.state;

    if (!queryAst) return;
    this.handleQueryStringChange(queryAst.removeFilter(column).toString(), preferablyRun);
  };

  private handleQueryStringChange = (queryString: string, preferablyRun?: boolean): void => {
    this.setState({ queryString, queryAst: parser(queryString) }, () => {
      const { autoRun } = this.state;
      if (preferablyRun && autoRun) this.handleRun();
    });
  };

  private handleQueryContextChange = (queryContext: QueryContext) => {
    this.setState({ queryContext });
  };

  private setAutoRun = (autoRun: boolean) => {
    this.setState({ autoRun });
    localStorageSet(LocalStorageKeys.AUTO_RUN, String(autoRun));
  };

  private setWrapQuery = (wrapQuery: boolean) => {
    this.setState({ wrapQuery });
  };

  private handleRun = () => {
    const { queryString, queryContext, wrapQuery, queryHistory } = this.state;
    if (QueryView.isJsonLike(queryString) && !QueryView.validRune(queryString)) return;

    const newQueryHistory = QueryHistoryDialog.addQueryToHistory(queryHistory, queryString);

    localStorageSet(LocalStorageKeys.QUERY_HISTORY, JSON.stringify(newQueryHistory));
    localStorageSet(LocalStorageKeys.QUERY_KEY, queryString);

    this.setState({ queryHistory: newQueryHistory });
    this.sqlQueryManager.runQuery({ queryString, queryContext, wrapQuery });
  };

  private handleExplain = () => {
    const { queryString, queryContext, wrapQuery } = this.state;

    this.setState({ explainDialogOpen: true });
    this.explainQueryManager.runQuery({ queryString, queryContext, wrapQuery });
  };

  private handleSecondaryPaneSizeChange = (secondaryPaneSize: number) => {
    localStorageSet(LocalStorageKeys.QUERY_VIEW_PANE_SIZE, String(secondaryPaneSize));
  };

  private getGroupBySetting = () => {
    const { queryString, queryAst } = this.state;
    const ast = queryAst;
    let tempAst: SqlQuery | undefined;
    if (!ast) {
      tempAst = parser(queryString);
    }

    let hasGroupBy = false;
    if (ast && ast instanceof SqlQuery) {
      hasGroupBy = !!ast.groupByClause;
    } else if (tempAst && tempAst instanceof SqlQuery) {
      hasGroupBy = !!tempAst.groupByClause;
    }
    return hasGroupBy;
  };

  private getQueryAst = () => {
    const { queryAst } = this.state;
    return queryAst;
  };

  private getCurrentFilters = () => {
    const { queryAst } = this.state;
    return queryAst.getCurrentFilters();
  };

  render(): JSX.Element {
    const { columnMetadata, columnMetadataLoading, columnMetadataError, queryAst } = this.state;

    let defaultSchema;
    let defaultTable;
    if (queryAst instanceof SqlQuery) {
      defaultSchema = queryAst.getSchema();
      defaultTable = queryAst.getTableName();
    }

    return (
      <div
        className={classNames('query-view app-view', { 'hide-column-tree': columnMetadataError })}
      >
        {!columnMetadataError && (
          <ColumnTree
            clear={this.sqlClearWhere}
            filterByRow={this.sqlFilterRow}
            addFunctionToGroupBy={this.addFunctionToGroupBy}
            addAggregateColumn={this.addAggregateColumn}
            addToGroupBy={this.addToGroupBy}
            hasGroupBy={this.getGroupBySetting}
            queryAst={this.getQueryAst}
            columnMetadataLoading={columnMetadataLoading}
            columnMetadata={columnMetadata}
            onQueryStringChange={this.handleQueryStringChange}
            defaultSchema={defaultSchema ? defaultSchema : 'druid'}
            defaultTable={defaultTable}
            currentFilters={this.getCurrentFilters}
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
