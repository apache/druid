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

import { IResizeEntry, ITreeNode, ResizeSensor } from '@blueprintjs/core';
import ace from 'brace';
import React from 'react';
import AceEditor from 'react-ace';
import ReactDOMServer from 'react-dom/server';

import { SQLFunctionDoc } from '../../../../lib/sql-function-doc';
import { uniq } from '../../../utils';
import { ColumnMetadata } from '../../../utils/column-metadata';
import { ColumnTreeProps, ColumnTreeState } from '../column-tree/column-tree';

import './query-input.scss';

const langTools = ace.acequire('ace/ext/language_tools');

export interface QueryInputProps extends React.Props<any> {
  queryString: string;
  onQueryStringChange: (newQueryString: string) => void;
  runeMode: boolean;
  columnMetadata: ColumnMetadata[] | null;
}

export interface QueryInputState {
  // For reasons (https://github.com/securingsincity/react-ace/issues/415) react ace editor needs an explicit height
  // Since this component will grown and shrink dynamically we will measure its height and then set it.
  editorHeight: number;
  prevColumnMetadata: ColumnMetadata[] | null;
}

export class QueryInput extends React.PureComponent<QueryInputProps, QueryInputState> {
  static getDerivedStateFromProps(props: ColumnTreeProps, state: ColumnTreeState) {
    const { columnMetadata } = props;

    if (columnMetadata && columnMetadata !== state.prevColumnMetadata) {
      const completions = ([] as any[]).concat(
        uniq(columnMetadata.map(d => d.TABLE_SCHEMA)).map(v => ({
          value: v,
          score: 10,
          meta: 'schema',
        })),
        uniq(columnMetadata.map(d => d.TABLE_NAME)).map(v => ({
          value: v,
          score: 49,
          meta: 'datasource',
        })),
        uniq(columnMetadata.map(d => d.COLUMN_NAME)).map(v => ({
          value: v,
          score: 50,
          meta: 'column',
        })),
      );

      langTools.addCompleter({
        getCompletions: (editor: any, session: any, pos: any, prefix: any, callback: any) => {
          callback(null, completions);
        },
      });

      return {
        prevColumnMetadata: columnMetadata,
      };
    }
    return null;
  }

  constructor(props: QueryInputProps, context: any) {
    super(props, context);
    this.state = {
      editorHeight: 200,
      prevColumnMetadata: null,
    };
  }

  private replaceDefaultAutoCompleter = () => {
    if (!langTools) return;
    /*
     Please refer to the source code @
     https://github.com/ajaxorg/ace/blob/9b5b63d1dc7c1b81b58d30c87d14b5905d030ca5/lib/ace/ext/language_tools.js#L41
     for the implementation of keyword completer
    */
    const keywordCompleter = {
      getCompletions: (editor: any, session: any, pos: any, prefix: any, callback: any) => {
        if (session.$mode.completer) {
          return session.$mode.completer.getCompletions(editor, session, pos, prefix, callback);
        }
        const state = editor.session.getState(pos.row);
        let keywordCompletions = session.$mode.getCompletions(state, session, pos, prefix);
        keywordCompletions = keywordCompletions.map((d: any) => {
          return Object.assign(d, { name: d.name.toUpperCase(), value: d.value.toUpperCase() });
        });
        return callback(null, keywordCompletions);
      },
    };
    langTools.setCompleters([
      langTools.snippetCompleter,
      langTools.textCompleter,
      keywordCompleter,
    ]);
  };

  private addFunctionAutoCompleter = (): void => {
    if (!langTools) return;

    const functionList: any[] = SQLFunctionDoc.map((entry: any) => {
      let funcName: string = entry.syntax.replace(/\(.*\)/, '()');
      if (!funcName.includes('(')) funcName = funcName.substr(0, 10);
      return {
        value: funcName,
        score: 80,
        meta: 'function',
        syntax: entry.syntax,
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
      getCompletions: (editor: any, session: any, pos: any, prefix: any, callback: any) => {
        callback(null, functionList);
      },
      getDocTooltip: (item: any) => {
        if (item.meta === 'function') {
          const functionName = item.caption.slice(0, -2);
          item.docHTML = ReactDOMServer.renderToStaticMarkup(
            <div className="function-doc">
              <div className="function-doc-name">
                <b>{functionName}</b>
              </div>
              <hr />
              <div>
                <b>Syntax:</b>
              </div>
              <div>{item.syntax}</div>
              <br />
              <div>
                <b>Description:</b>
              </div>
              <div>{item.description}</div>
            </div>,
          );
        }
      },
    });
  };

  componentDidMount(): void {
    this.replaceDefaultAutoCompleter();
    this.addFunctionAutoCompleter();
  }

  private handleAceContainerResize = (entries: IResizeEntry[]) => {
    if (entries.length !== 1) return;
    this.setState({ editorHeight: entries[0].contentRect.height });
  };

  render() {
    const { queryString, runeMode, onQueryStringChange } = this.props;
    const { editorHeight } = this.state;

    // Set the key in the AceEditor to force a rebind and prevent an error that happens otherwise
    return (
      <div className="query-input">
        <ResizeSensor onResize={this.handleAceContainerResize}>
          <div className="ace-container">
            <AceEditor
              key={runeMode ? 'hjson' : 'sql'}
              mode={runeMode ? 'hjson' : 'sql'}
              theme="solarized_dark"
              name="ace-editor"
              onChange={onQueryStringChange}
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
            />
          </div>
        </ResizeSensor>
      </div>
    );
  }
}
