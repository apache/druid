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

import { Button, ControlGroup, Intent, Tooltip } from '@blueprintjs/core';
import { Position } from '@blueprintjs/core/lib/esm/common/position';
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import classNames from 'classnames';
import {
  AdditiveExpression,
  Alias,
  FilterClause,
  HeaderRows,
  isFirstRowHeader,
  normalizeQueryResult,
  shouldIncludeTimestamp,
  sqlParserFactory,
  SqlQuery,
  StringType,
} from 'druid-query-toolkit';
import Hjson from 'hjson';
import React from 'react';
import SplitterLayout from 'react-splitter-layout';

import { SQL_FUNCTIONS, SyntaxDescription } from '../../../lib/sql-function-doc';
import { QueryPlanDialog } from '../../dialogs';
import { EditContextDialog } from '../../dialogs/edit-context-dialog/edit-context-dialog';
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

const parser = sqlParserFactory(
  SQL_FUNCTIONS.map((sql_function: SyntaxDescription) => {
    return sql_function.syntax.substr(0, sql_function.syntax.indexOf('('));
  }),
);

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

  defaultSchema?: string;
  defaultTable?: string;
  ast?: SqlQuery;

  editContextDialogOpen: boolean;
  historyDialogOpen: boolean;
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
    this.state = {
      queryString: props.initQuery || localStorageGet(LocalStorageKeys.QUERY_KEY) || '',
      queryContext: {},

      columnMetadataLoading: false,

      loading: false,

      explainDialogOpen: false,
      loadingExplain: false,

      editContextDialogOpen: false,
      historyDialogOpen: false,
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
            ? `SELECT * FROM (${QueryView.trimSemicolon(queryString)}\n) LIMIT 1000`
            : queryString;

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
          result: result ? result.queryResult : undefined,
          queryExtraInfo: result ? result.queryExtraInfo : undefined,
          loading,
          error,
          ast: result ? result.parsedQuery : undefined,
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
        setQueryString={(queryString: string) =>
          this.setState({ queryString, explainDialogOpen: false })
        }
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
      ast,
    } = this.state;
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
            queryString={queryString}
            onQueryStringChange={this.handleQueryStringChange}
            runeMode={runeMode}
            columnMetadata={columnMetadata}
          />
          <ControlGroup vertical>
            <Tooltip content="QueryHistory" position={Position.LEFT}>
              <Button
                icon={IconNames.HISTORY}
                onClick={() => this.setState({ historyDialogOpen: true })}
              />
            </Tooltip>
            <Tooltip content="QueryHistory" position={Position.LEFT}>
              <Button icon="filter" />
            </Tooltip>
            <Tooltip content="QueryHistory" position={Position.LEFT}>
              <Button icon="filter" />
            </Tooltip>
          </ControlGroup>
          <div className="control-bar">
            <RunButton
              onEditContext={() => this.setState({ editContextDialogOpen: true })}
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
        <QueryOutput
          aggregateColumns={ast ? ast.getAggregateColumns() : undefined}
          disabled={!ast}
          sorted={ast ? ast.getSorted() : undefined}
          sqlExcludeColumn={this.sqlExcludeColumn}
          sqlFilterRow={this.sqlFilterRow}
          sqlOrderBy={this.sqlOrderBy}
          runeMode={runeMode}
          loading={loading}
          result={result}
          error={error}
        />
      </SplitterLayout>
    );
  }

  private addFunctionToGroupBy = (
    functionName: string,
    spacing: string[],
    argumentsArray: (StringType | number)[],
    run: boolean,
  ): void => {
    let { ast } = this.state;
    if (!ast) return;
    ast = ast.addFunctionToGroupBy(functionName, spacing, argumentsArray);
    this.setState({
      queryString: ast.toString(),
    });
    if (run) {
      this.handleRun(true, ast.toString());
    }
  };

  private addToGroupBy = (columnName: string, run: boolean): void => {
    let { ast } = this.state;
    if (!ast) return;
    ast = ast.addToGroupBy(columnName);
    this.setState({
      queryString: ast.toString(),
    });
    if (run) {
      this.handleRun(true, ast.toString());
    }
  };

  private addAggregateColumn = (
    columnName: string,
    functionName: string,
    run: boolean,
    alias?: Alias,
    distinct?: boolean,
    filter?: FilterClause,
  ): void => {
    let { ast } = this.state;
    if (!ast) return;
    ast = ast.addAggregateColumn(columnName, functionName, alias, distinct, filter);
    this.setState({
      queryString: ast.toString(),
    });
    if (run) {
      this.handleRun(true, ast.toString());
    }
  };

  private sqlOrderBy = (header: string, direction: 'ASC' | 'DESC', run: boolean): void => {
    let { ast } = this.state;
    if (!ast) return;
    ast = ast.orderBy(header, direction);
    this.setState({
      queryString: ast.toString(),
    });
    if (run) {
      this.handleRun(true, ast.toString());
    }
  };

  private sqlExcludeColumn = (header: string, run: boolean): void => {
    let { ast } = this.state;
    if (!ast) return;
    ast = ast.excludeColumn(header);
    this.setState({
      queryString: ast.toString(),
    });
    if (run) {
      this.handleRun(true, ast.toString());
    }
  };

  private sqlFilterRow = (
    row: string | number | AdditiveExpression,
    header: string,
    operator: '!=' | '=' | '>' | '<' | 'like' | '>=' | '<=' | 'LIKE',
    run: boolean,
  ): void => {
    let { ast } = this.state;
    if (!ast) return;
    ast = ast.filterRow(header, row, operator);
    this.setState({
      queryString: ast.toString(),
    });
    if (run) {
      this.handleRun(true, ast.toString());
    }
  };

  private handleQueryStringChange = (queryString: string): void => {
    this.setState({ queryString });
  };

  private handleQueryContextChange = (queryContext: QueryContext) => {
    this.setState({ queryContext });
  };

  private handleRun = (wrapQuery: boolean, customQueryString?: string) => {
    const { queryString, queryContext } = this.state;

    if (!customQueryString) {
      customQueryString = queryString;
    }

    if (QueryView.isJsonLike(customQueryString) && !QueryView.validRune(customQueryString)) return;

    localStorageSet(LocalStorageKeys.QUERY_KEY, customQueryString);
    this.sqlQueryManager.runQuery({ queryString: customQueryString, queryContext, wrapQuery });
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
    const {
      columnMetadata,
      columnMetadataLoading,
      columnMetadataError,
      ast,
      queryString,
    } = this.state;

    let tempAst: SqlQuery | undefined;
    if (!ast) {
      try {
        tempAst = parser(queryString);
      } catch {}
    }
    let defaultSchema;
    if (ast && ast instanceof SqlQuery) {
      defaultSchema = ast.getSchema();
    } else if (tempAst && tempAst instanceof SqlQuery) {
      defaultSchema = tempAst.getSchema();
    }
    let defaultTable;
    if (ast && ast instanceof SqlQuery) {
      defaultTable = ast.getTableName();
    } else if (tempAst && tempAst instanceof SqlQuery) {
      defaultTable = tempAst.getTableName();
    }
    let hasGroupBy;
    if (ast && ast instanceof SqlQuery) {
      hasGroupBy = !!ast.groupByClause;
    } else if (tempAst && tempAst instanceof SqlQuery) {
      hasGroupBy = !!tempAst.groupByClause;
    }

    return (
      <div
        className={classNames('query-view app-view', { 'hide-column-tree': columnMetadataError })}
      >
        {!columnMetadataError && (
          <ColumnTree
            filterByRow={this.sqlFilterRow}
            addFunctionToGroupBy={this.addFunctionToGroupBy}
            addAggregateColumn={this.addAggregateColumn}
            addToGroupBy={this.addToGroupBy}
            hasGroupBy={hasGroupBy}
            columnMetadataLoading={columnMetadataLoading}
            columnMetadata={columnMetadata}
            onQueryStringChange={this.handleQueryStringChange}
            defaultSchema={defaultSchema}
            defaultTable={defaultTable}
          />
        )}
        {this.renderMainArea()}
        {this.renderExplainDialog()}
        {this.renderEditContextDialog()}
      </div>
    );
  }
}
