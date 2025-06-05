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

import type { Ace } from 'ace-builds';
import type { Column } from 'druid-query-toolkit';
import { C } from 'druid-query-toolkit';
import React from 'react';
import AceEditor from 'react-ace';

import { getSqlCompletions } from '../../../../ace-completions/sql-completions';
import type { RowColumn } from '../../../../utils';
import { uniq } from '../../../../utils';

const V_PADDING = 10;

export interface SqlInputProps {
  value: string;
  onValueChange?: (newValue: string) => void;
  placeholder?: string;
  editorHeight?: number;
  columns?: readonly Column[];
  autoFocus?: boolean;
  showGutter?: boolean;
}

export class SqlInput extends React.PureComponent<SqlInputProps> {
  static aceTheme = 'solarized_dark';

  private aceEditor: Ace.Editor | undefined;

  static getCompletions(columns: readonly Column[], quote: boolean): Ace.Completion[] {
    return ([] as Ace.Completion[]).concat(
      uniq(columns.map(column => column.name)).map(v => ({
        value: quote ? String(C(v)) : v,
        score: 50,
        meta: 'column',
      })),
    );
  }

  constructor(props: SqlInputProps) {
    super(props);
    this.state = {};
  }

  componentWillUnmount() {
    delete this.aceEditor;
  }

  private readonly handleChange = (value: string) => {
    const { onValueChange } = this.props;
    if (!onValueChange) return;
    onValueChange(value);
  };

  public goToPosition(rowColumn: RowColumn) {
    const { aceEditor } = this;
    if (!aceEditor) return;
    aceEditor.focus(); // Grab the focus
    aceEditor.getSelection().moveCursorTo(rowColumn.row, rowColumn.column);
    // If we had an end we could also do
    // aceEditor.getSelection().selectToPosition({ row: endRow, column: endColumn });
  }

  render() {
    const { value, onValueChange, placeholder, autoFocus, editorHeight, showGutter } = this.props;

    const getColumns = () => this.props.columns?.map(column => column.name);
    const cmp: Ace.Completer[] = [
      {
        getCompletions: (_state, session, pos, prefix, callback) => {
          const allText = session.getValue();
          const line = session.getLine(pos.row);
          const charBeforePrefix = line[pos.column - prefix.length - 1];
          const lineBefore = line.slice(0, pos.column - prefix.length - 1);
          const keywordMatch = /(\w+)\s*$/.exec(lineBefore) || [];
          callback(
            null,
            getSqlCompletions({
              allText,
              keywordBeforePrefix: keywordMatch[1],
              charBeforePrefix,
              prefix,
              columns: getColumns(),
            }),
          );
        },
      },
    ];

    return (
      <AceEditor
        mode="dsql"
        theme={SqlInput.aceTheme}
        className="sql-input placeholder-padding"
        // 'react-ace' types are incomplete. Completion options can accept completers array.
        enableBasicAutocompletion={cmp as any}
        enableLiveAutocompletion={cmp as any}
        name="ace-editor"
        onChange={this.handleChange}
        focus
        fontSize={12}
        width="100%"
        height={editorHeight ? `${editorHeight}px` : '100%'}
        showGutter={Boolean(showGutter)}
        showPrintMargin={false}
        tabSize={2}
        value={value}
        readOnly={!onValueChange}
        editorProps={{
          $blockScrolling: Infinity,
        }}
        setOptions={{
          showLineNumbers: true,
          newLineMode: 'unix' as any, // This type is specified incorrectly in AceEditor
        }}
        placeholder={placeholder || 'SELECT * FROM ...'}
        onLoad={(editor: Ace.Editor) => {
          editor.renderer.setPadding(V_PADDING);
          editor.renderer.setScrollMargin(V_PADDING, V_PADDING, 0, 0);
          this.aceEditor = editor;

          if (autoFocus) {
            editor.focus();
          }
        }}
      />
    );
  }
}
