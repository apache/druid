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

import { Classes, Icon, IconName, Intent, ITreeNode, Position, Tooltip, Tree } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import axios, { AxiosResponse } from 'axios';
import Hjson from 'hjson';
import React from 'react';
import SplitterLayout from 'react-splitter-layout';

import { QueryPlanDialog } from '../../dialogs';
import { AppToaster } from '../../singletons/toaster';
import {
  BasicQueryExplanation,
  decodeRune,
  downloadFile, getDruidErrorMessage,
  groupBy,
  HeaderRows,
  localStorageGet, LocalStorageKeys,
  localStorageSet, parseQueryPlan,
  queryDruidSql, QueryManager,
  SemiJoinQueryExplanation, uniq
} from '../../utils';
import { isEmptyContext, QueryContext } from '../../utils/query-context';

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

export interface QueryViewProps extends React.Props<any> {
  initQuery: string | null;
}

export interface QueryViewState {
  queryString: string;
  queryContext: QueryContext;

  loading: boolean;
  result: HeaderRows | null;
  queryExtraInfo: QueryExtraInfoData | null;
  error: string | null;

  explainDialogOpen: boolean;
  explainResult: BasicQueryExplanation | SemiJoinQueryExplanation | string | null;
  loadingExplain: boolean;
  explainError: Error | null;

  tableNames: string[] | null;
  columnNames: string[] | null;
  nav: ITreeNode[] | null;
}

interface QueryResult {
  queryResult: HeaderRows;
  queryExtraInfo: QueryExtraInfoData;
}

export class QueryView extends React.Component<QueryViewProps, QueryViewState> {
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

  private sqlQueryManager: QueryManager<QueryWithContext, QueryResult>;
  private explainQueryManager: QueryManager<QueryWithContext, BasicQueryExplanation | SemiJoinQueryExplanation | string>;

  constructor(props: QueryViewProps, context: any) {
    super(props, context);
    this.state = {
      queryString: props.initQuery || localStorageGet(LocalStorageKeys.QUERY_KEY) || '',
      queryContext: {},

      loading: false,
      result: null,
      queryExtraInfo: null,
      error: null,

      explainDialogOpen: false,
      loadingExplain: false,
      explainResult: null,
      explainError: null,

      tableNames: null,
      columnNames: null,
      nav: null
    };
  }

  componentDidMount(): void {
    this.sqlQueryManager = new QueryManager({
      processQuery: async (queryWithContext: QueryWithContext) => {
        const { queryString, queryContext, wrapQuery } = queryWithContext;
        let queryId: string = '';

        let queryResult: HeaderRows;
        const startTime = Date.now();
        let endTime: number;

        if (QueryView.isRune(queryString)) {
          // Secret way to issue a native JSON "rune" query
          const runeQuery = Hjson.parse(queryString);

          if (!isEmptyContext(queryContext)) runeQuery.context = queryContext;
          let runeResult: any[];
          try {
            const runeResultResp = await axios.post('/druid/v2', runeQuery);
            endTime = Date.now();
            runeResult = runeResultResp.data;
            queryId = runeResultResp.headers['x-druid-query-id'];
          } catch (e) {
            throw new Error(getDruidErrorMessage(e));
          }

          queryResult = decodeRune(runeQuery, runeResult);

        } else {
          const actualQuery = wrapQuery ?
            `SELECT * FROM (${QueryView.trimSemicolon(queryString)}\n) LIMIT 2000` :
            queryString;

          const queryPayload: Record<string, any> = {
            query: actualQuery,
            resultFormat: 'array',
            header: true
          };

          if (!isEmptyContext(queryContext)) queryPayload.context = queryContext;
          let sqlResult: any[];
          try {
            const sqlResultResp = await axios.post('/druid/v2/sql', queryPayload);
            endTime = Date.now();
            sqlResult = sqlResultResp.data;
            queryId = sqlResultResp.headers['x-druid-sql-query-id'];
          } catch (e) {
            throw new Error(getDruidErrorMessage(e));
          }

          queryResult = {
            header: (sqlResult && sqlResult.length) ? sqlResult[0] : [],
            rows: (sqlResult && sqlResult.length) ? sqlResult.slice(1) : []
          };
        }

        return {
          queryResult,
          queryExtraInfo: {
            id: queryId,
            elapsed: endTime - startTime
          }
        };
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          result: result ? result.queryResult : null,
          queryExtraInfo: result ? result.queryExtraInfo : null,
          loading,
          error
        });
      }
    });

    this.explainQueryManager = new QueryManager({
      processQuery: async (queryWithContext: QueryWithContext) => {
        const { queryString, queryContext } = queryWithContext;
        const explainPayload: Record<string, any> = {
          query: `EXPLAIN PLAN FOR (${QueryView.trimSemicolon(queryString)}\n)`,
          resultFormat: 'object'
        };

        if (!isEmptyContext(queryContext)) explainPayload.context = queryContext;
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

    this.getMetadata();
  }

  componentWillUnmount(): void {
    this.sqlQueryManager.terminate();
    this.explainQueryManager.terminate();
  }

  private getMetadata = async () => {
    let metadataResp: AxiosResponse | null = null;
    try {
      metadataResp = await axios.post('/druid/v2/sql', {
        query: `SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS`
      });
    } catch (e) {
      AppToaster.show({
        message: 'Failed to load SQL metadata',
        intent: Intent.DANGER
      });
    }

    interface Metadata {
      TABLE_SCHEMA: string;
      TABLE_NAME: string;
      COLUMN_NAME: string;
    }

    if (!metadataResp) return;
    const metadata: Metadata[] = metadataResp.data;

    this.setState({
      tableNames: uniq(metadata.map((d: any) => d.TABLE_NAME)),
      columnNames: uniq(metadata.map((d: any) => d.COLUMN_NAME)),
      nav: groupBy(
        metadata,
        (r) => r.TABLE_SCHEMA,
        (metadata, schema): ITreeNode => ({
          id: schema,
          icon: IconNames.DATABASE,
          label: schema,
          isExpanded: schema === 'druid',
          childNodes: groupBy(
            metadata,
            (r) => r.TABLE_NAME,
            (metadata, table) => ({
              id: table,
              icon: IconNames.TH,
              label: table,
              childNodes: groupBy(
                metadata,
                (r) => r.COLUMN_NAME,
                (metadata, column) => ({
                  id: column,
                  icon: column === '__time' ? IconNames.TIME : IconNames.FONT,
                  label: column
                })
              )
            })
          )
        })
      ).reverse()
    });
  }

  onSecondaryPaneSizeChange(secondaryPaneSize: number) {
    localStorageSet(LocalStorageKeys.QUERY_VIEW_PANE_SIZE, String(secondaryPaneSize));
  }

  formatStr(s: string | number, format: 'csv' | 'tsv') {
    if (format === 'csv') {
      // remove line break, single quote => double quote, handle ','
      return `"${s.toString().replace(/(?:\r\n|\r|\n)/g, ' ').replace(/"/g, '""')}"`;
    } else { // tsv
      // remove line break, single quote => double quote, \t => ''
      return `${s.toString().replace(/(?:\r\n|\r|\n)/g, ' ').replace(/\t/g, '').replace(/"/g, '""')}`;
    }
  }

  handleDownload = (format: string) => {
    const { result } = this.state;
    if (!result) return;
    let data: string = '';
    let separator: string = '';
    const lineBreak = '\n';

    if (format === 'csv' || format === 'tsv') {
      separator = format === 'csv' ? ',' : '\t';
      data = result.header.map(str => this.formatStr(str, format)).join(separator) + lineBreak;
      data += result.rows.map(r => r.map(cell => this.formatStr(cell, format)).join(separator)).join(lineBreak);
    } else { // json
      data = result.rows.map(r => {
        const outputObject: Record<string, any> = {};
        for (let k = 0; k < r.length; k++) {
          const newName = result.header[k];
          if (newName) {
            outputObject[newName] = r[k];
          }
        }
        return JSON.stringify(outputObject);
      }).join(lineBreak);
    }
    downloadFile(data, format, 'query_result.' + format);
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

  renderIndexTree() {
    const { nav } = this.state;
    if (!nav) return null;

    const INITIAL_STATE: ITreeNode[] = [
      {
        id: 0,
        hasCaret: true,
        icon: 'folder-close',
        label: 'Folder 0'
      },
      {
        id: 1,
        icon: 'folder-close',
        isExpanded: true,
        label: (
          <Tooltip content="I'm a folder <3" position={Position.RIGHT}>
            Folder 1
          </Tooltip>
        ),
        childNodes: [
          {
            id: 2,
            icon: 'document',
            label: 'Item 0',
            secondaryLabel: (
              <Tooltip content="An eye!">
                <Icon icon="eye-open" />
              </Tooltip>
            )
          },
          {
            id: 3,
            icon: <Icon icon="tag" intent={Intent.PRIMARY} className={Classes.TREE_NODE_ICON} />,
            label: 'Organic meditation gluten-free, sriracha VHS drinking vinegar beard man.'
          },
          {
            id: 4,
            hasCaret: true,
            icon: 'folder-close',
            label: (
              <Tooltip content="foo" position={Position.RIGHT}>
                Folder 2
              </Tooltip>
            )
          }
        ]
      },
      {
        id: 2,
        hasCaret: true,
        icon: 'folder-close',
        label: 'Super secret files',
        disabled: true
      }
    ];

    return <div className="index-tree">
      <Tree
        contents={nav}
        onNodeClick={this.handleNodeClick}
        onNodeCollapse={this.handleNodeCollapse}
        onNodeExpand={this.handleNodeExpand}
      />
    </div>;
  }

  private handleNodeClick = (nodeData: ITreeNode, _nodePath: number[], e: React.MouseEvent<HTMLElement>) => {
    console.log('nodeData', _nodePath, nodeData);
    this.setState({ queryString: String(nodeData.label) });
  }

  private handleNodeCollapse = (nodeData: ITreeNode) => {
    nodeData.isExpanded = false;
    this.setState(this.state);
  }

  private handleNodeExpand = (nodeData: ITreeNode) => {
    nodeData.isExpanded = true;
    this.setState(this.state);
  }

  renderMainArea() {
    const { queryString, queryContext, loading, result, queryExtraInfo, error } = this.state;
    const runeMode = QueryView.isRune(queryString);

    return <SplitterLayout
      vertical
      percentage
      secondaryInitialSize={Number(localStorageGet(LocalStorageKeys.QUERY_VIEW_PANE_SIZE) as string) || 60}
      primaryMinSize={30}
      secondaryMinSize={30}
      onSecondaryPaneSizeChange={this.onSecondaryPaneSizeChange}
    >
      <div className="control-pane">
        <QueryInput
          queryString={queryString}
          onQueryStringChange={this.handleQueryStringChange}
          runeMode={runeMode}
        />
        <div className="control-bar">
          <RunButton
            runeMode={runeMode}
            queryContext={queryContext}
            onQueryContextChange={this.handleQueryContextChange}
            onRun={this.handleRun}
            onExplain={this.handleExplain}
          />
          {
            queryExtraInfo &&
            <QueryExtraInfo
              queryExtraInfo={queryExtraInfo}
              onDownload={this.handleDownload}
            />
          }
        </div>
      </div>
      <QueryOutput
        loading={loading}
        result={result}
        error={error}
      />
    </SplitterLayout>;
  }

  private handleQueryStringChange = (queryString: string): void => {
    this.setState({ queryString });
  }

  private handleQueryContextChange = (queryContext: QueryContext) => {
    this.setState({ queryContext });
  }

  private handleRun = (wrapQuery: boolean) => {
    const { queryString, queryContext } = this.state;

    if (QueryView.isRune(queryString) && !QueryView.validRune(queryString)) return;

    localStorageSet(LocalStorageKeys.QUERY_KEY, queryString);
    this.sqlQueryManager.runQuery({ queryString, queryContext, wrapQuery });
  }

  private handleExplain = () => {
    const { queryString, queryContext } = this.state;
    this.setState({ explainDialogOpen: true });
    this.explainQueryManager.runQuery({ queryString, queryContext });
  }

  render() {
    return <div className="query-view app-view">
      {this.renderIndexTree()}
      {this.renderMainArea()}
      {this.renderExplainDialog()}
    </div>;
  }
}
