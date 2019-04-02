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

import { Button, Classes, Dialog, FormGroup, InputGroup, Label, TextArea } from "@blueprintjs/core";
import axios from 'axios';
import * as classNames from 'classnames';
import * as Hjson from "hjson";
import * as React from 'react';
import ReactTable from "react-table";

import { SqlControl } from '../components/sql-control';
import {
  decodeRune, getDruidErrorMessage,
  HeaderRows,
  localStorageGet, LocalStorageKeys,
  localStorageSet, parseQueryPlan,
  queryDruidRune,
  queryDruidSql, QueryManager
} from '../utils';

import "./sql-view.scss";

export interface SqlViewProps extends React.Props<any> {
  initSql: string | null;
}

export interface SqlViewState {
  loading: boolean;
  result: HeaderRows | null;
  error: string | null;
  explainDialogOpen: boolean;
  explainResult: any;
  loadingExplain: boolean;
  explainError: Error | null;
}

export class SqlView extends React.Component<SqlViewProps, SqlViewState> {

  private sqlQueryManager: QueryManager<string, HeaderRows>;
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
      explainError: null
    };
  }

  componentDidMount(): void {
    this.sqlQueryManager = new QueryManager({
      processQuery: async (query: string) => {
        if (query.trim().startsWith('{')) {
          // Secret way to issue a native JSON "rune" query
          const runeQuery = Hjson.parse(query);
          return decodeRune(runeQuery, await queryDruidRune(runeQuery));

        } else {
          const result = await queryDruidSql({
            query,
            resultFormat: "array",
            header: true
          });

          return {
            header: (result && result.length) ? result[0] : [],
            rows: (result && result.length) ? result.slice(1) : []
          };
        }
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          result,
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
          resultFormat: "object"
        });
        const data = parseQueryPlan(result[0]["PLAN"]);
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
    const { explainDialogOpen, explainResult, loadingExplain, explainError } = this.state;
    let content: JSX.Element;
    if (loadingExplain) {
      content = <div>Loading...</div>;
    } else if (explainError) {
      content = <div>{explainError.message}</div>;
    } else if (explainResult == null) {
      content = <div/>;
    } else if (explainResult.query) {
      content = <div className={"one-query"}>
        <FormGroup
          label={"Query"}
        >
          <TextArea
            readOnly
            value={JSON.stringify(explainResult.query[0], undefined, 2)}
          />
        </FormGroup>
        <FormGroup
          label={"Signature"}
        >
          <InputGroup defaultValue={explainResult.signature} readOnly/>
        </FormGroup>
      </div>;
    } else if (explainResult.mainQuery && explainResult.subQueryRight) {
      content = <div className={"two-queries"}>
        <FormGroup
          label={"Main query"}
        >
          <TextArea
            readOnly
            value={JSON.stringify(explainResult.mainQuery, undefined, 2)}
          />
        </FormGroup>
        <FormGroup
          label={"Sub query"}
        >
          <TextArea
            readOnly
            value={JSON.stringify(explainResult.subQueryRight, undefined, 2)}
          />
        </FormGroup>
      </div>;
    } else {
      content = <div>{explainResult}</div>;
    }

    return <Dialog
      className={'sql-view-explain-dialog'}
      isOpen={explainDialogOpen}
      onClose={() => this.setState({explainDialogOpen: false})}
      title={"Query plan"}
    >
      <div className={Classes.DIALOG_BODY}>
        {content}
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button
            text="Close"
            onClick={() => this.setState({explainDialogOpen: false})}
          />
        </div>
      </div>
    </Dialog>;
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

    return <div className="sql-view app-view">
      <SqlControl
        initSql={initSql || localStorageGet(LocalStorageKeys.QUERY_KEY)}
        onRun={q => {
          localStorageSet(LocalStorageKeys.QUERY_KEY, q);
          this.sqlQueryManager.runQuery(q);
        }}
        onExplain={(q: string) => this.getExplain(q)}
      />
      {this.renderResultTable()}
      {this.renderExplainDialog()}
    </div>;
  }
}
