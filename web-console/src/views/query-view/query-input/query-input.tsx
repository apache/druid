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

import { IResizeEntry, ResizeSensor } from '@blueprintjs/core';
import ace from 'brace';
import escape from 'lodash.escape';
import React from 'react';
import AceEditor from 'react-ace';

import {
  SQL_CONSTANTS,
  SQL_DYNAMICS,
  SQL_EXPRESSION_PARTS,
  SQL_KEYWORDS,
} from '../../../../lib/keywords';
import { SQL_DATA_TYPES, SQL_FUNCTIONS } from '../../../../lib/sql-docs';
import { uniq } from '../../../utils';
import { ColumnMetadata } from '../../../utils/column-metadata';

import './query-input.scss';

const langTools = ace.acequire('ace/ext/language_tools');

export interface QueryInputProps {
  queryString: string;
  onQueryStringChange: (newQueryString: string) => void;
  runeMode: boolean;
  columnMetadata?: readonly ColumnMetadata[];
  currentSchema?: string;
  currentTable?: string;
}

export interface QueryInputState {
  // For reasons (https://github.com/securingsincity/react-ace/issues/415) react ace editor needs an explicit height
  // Since this component will grown and shrink dynamically we will measure its height and then set it.
  editorHeight: number;
  completions: any[];
  prevColumnMetadata?: readonly ColumnMetadata[];
  prevCurrentTable?: string;
  prevCurrentSchema?: string;
}

export class QueryInput extends React.PureComponent<QueryInputProps, QueryInputState> {
  static replaceDefaultAutoCompleter(): void {
    if (!langTools) return;

    const keywordList = ([] as any[]).concat(
      SQL_KEYWORDS.map(v => ({ name: v, value: v, score: 0, meta: 'keyword' })),
      SQL_EXPRESSION_PARTS.map(v => ({ name: v, value: v, score: 0, meta: 'keyword' })),
      SQL_CONSTANTS.map(v => ({ name: v, value: v, score: 0, meta: 'constant' })),
      SQL_DYNAMICS.map(v => ({ name: v, value: v, score: 0, meta: 'dynamic' })),
      SQL_DATA_TYPES.map(v => ({ name: v.name, value: v.name, score: 0, meta: 'type' })),
    );

    const keywordCompleter = {
      getCompletions: (_editor: any, _session: any, _pos: any, _prefix: any, callback: any) => {
        return callback(null, keywordList);
      },
    };

    langTools.setCompleters([
      langTools.snippetCompleter,
      langTools.textCompleter,
      keywordCompleter,
    ]);
  }

  static addFunctionAutoCompleter(): void {
    if (!langTools) return;

    const functionList: any[] = SQL_FUNCTIONS.map(entry => {
      return {
        value: entry.name,
        score: 80,
        meta: 'function',
        syntax: entry.name + entry.arguments,
        description: entry.description,
        completer: {
          insertMatch: (editor: any, data: any) => {
            editor.completer.insertMatch({ value: data.caption });
            const pos = editor.getCursorPosition();
            editor.gotoLine(pos.row + 1, pos.column - 1);
          },
        },
      };
    });

    langTools.addCompleter({
      getCompletions: (_editor: any, _session: any, _pos: any, _prefix: any, callback: any) => {
        callback(null, functionList);
      },
      getDocTooltip: (item: any) => {
        if (item.meta === 'function') {
          item.docHTML = QueryInput.completerToHtml(item);
        }
      },
    });
  }

  static completerToHtml(item: any) {
    return `
<div class="function-doc">
  <div class="function-doc-name">
    <b>${escape(item.caption)}</b>
  </div>
  <hr />
  <div>
    <b>Syntax:</b>
  </div>
  <div>${escape(item.syntax)}</div>
  <br />
  <div>
    <b>Description:</b>
  </div>
  <div>${escape(item.description)}</div>
</div>`;
  }

  static getDerivedStateFromProps(props: QueryInputProps, state: QueryInputState) {
    const { columnMetadata, currentSchema, currentTable } = props;

    if (
      columnMetadata &&
      (columnMetadata !== state.prevColumnMetadata ||
        currentSchema !== state.prevCurrentSchema ||
        currentTable !== state.prevCurrentTable)
    ) {
      const completions = ([] as any[]).concat(
        uniq(columnMetadata.map(d => d.TABLE_SCHEMA)).map(v => ({
          value: v,
          score: 10,
          meta: 'schema',
        })),
        uniq(
          columnMetadata
            .filter(d => (currentSchema ? d.TABLE_SCHEMA === currentSchema : true))
            .map(d => d.TABLE_NAME),
        ).map(v => ({
          value: v,
          score: 49,
          meta: 'datasource',
        })),
        uniq(
          columnMetadata
            .filter(d =>
              currentTable && currentSchema
                ? d.TABLE_NAME === currentTable && d.TABLE_SCHEMA === currentSchema
                : true,
            )
            .map(d => d.COLUMN_NAME),
        ).map(v => ({
          value: v,
          score: 50,
          meta: 'column',
        })),
      );

      return {
        completions,
        prevColumnMetadata: columnMetadata,
        prevCurrentSchema: currentSchema,
        prevCurrentTable: currentTable,
      };
    }
    return null;
  }

  constructor(props: QueryInputProps, context: any) {
    super(props, context);
    this.state = {
      editorHeight: 200,
      completions: [],
    };
  }

  componentDidMount(): void {
    QueryInput.replaceDefaultAutoCompleter();
    QueryInput.addFunctionAutoCompleter();
    if (langTools) {
      langTools.addCompleter({
        getCompletions: (_editor: any, _session: any, _pos: any, _prefix: any, callback: any) => {
          callback(null, this.state.completions);
        },
      });
    }
  }

  private handleAceContainerResize = (entries: IResizeEntry[]) => {
    if (entries.length !== 1) return;
    this.setState({ editorHeight: entries[0].contentRect.height });
  };

  private handleChange = (value: string) => {
    // This gets the event as a second arg
    const { onQueryStringChange } = this.props;
    onQueryStringChange(value);
  };

  render(): JSX.Element {
    const { queryString, runeMode } = this.props;
    const { editorHeight } = this.state;

    // Set the key in the AceEditor to force a rebind and prevent an error that happens otherwise
    return (
      <div className="query-input">
        <ResizeSensor onResize={this.handleAceContainerResize}>
          <div className="ace-container">
            <AceEditor
              mode={runeMode ? 'hjson' : 'dsql'}
              theme="solarized_dark"
              name="ace-editor"
              onChange={this.handleChange}
              focus
              fontSize={14}
              width="100%"
              height={`${editorHeight}px`}
              showPrintMargin={false}
              value={queryString}
              editorProps={{
                $blockScrolling: Infinity,
              }}
              setOptions={{
                enableBasicAutocompletion: !runeMode,
                enableLiveAutocompletion: !runeMode,
                showLineNumbers: true,
                tabSize: 2,
              }}
              style={{}}
              placeholder="SELECT * FROM ..."
            />
          </div>
        </ResizeSensor>
      </div>
    );
  }
}
