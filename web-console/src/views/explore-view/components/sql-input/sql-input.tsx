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
import React from 'react';
import AceEditor from 'react-ace';

import { getSqlCompletions } from '../../../../ace-completions/sql-completions';
import { useAvailableSqlFunctions } from '../../../../contexts/sql-functions-context';
import type { RowColumn } from '../../../../utils';

const V_PADDING = 10;
const ACE_THEME = 'solarized_dark';

export interface SqlInputProps {
  value: string;
  onValueChange?: (newValue: string) => void;
  placeholder?: string;
  editorHeight?: number;
  columns?: readonly Column[];
  autoFocus?: boolean;
  showGutter?: boolean;
  includeAggregates?: boolean;
}

export const SqlInput = React.forwardRef<
  { goToPosition: (rowColumn: RowColumn) => void } | undefined,
  SqlInputProps
>(function SqlInput(props, ref) {
  const {
    value,
    onValueChange,
    placeholder,
    autoFocus,
    editorHeight,
    showGutter,
    columns,
    includeAggregates,
  } = props;

  const availableSqlFunctions = useAvailableSqlFunctions();
  const aceEditorRef = React.useRef<Ace.Editor | undefined>();

  const goToPosition = React.useCallback((rowColumn: RowColumn) => {
    const aceEditor = aceEditorRef.current;
    if (!aceEditor) return;
    aceEditor.focus(); // Grab the focus
    aceEditor.getSelection().moveCursorTo(rowColumn.row, rowColumn.column);
  }, []);

  React.useImperativeHandle(ref, () => ({ goToPosition }), [goToPosition]);

  const handleChange = React.useCallback(
    (value: string) => {
      if (!onValueChange) return;
      onValueChange(value);
    },
    [onValueChange],
  );

  const handleAceLoad = React.useCallback((editor: Ace.Editor) => {
    editor.renderer.setPadding(V_PADDING);
    editor.renderer.setScrollMargin(V_PADDING, V_PADDING, 0, 0);
    aceEditorRef.current = editor;
  }, []);

  const getColumns = () => columns?.map(column => column.name);
  const cmp: Ace.Completer[] = [
    {
      getCompletions: (_state, session, pos, prefix, callback) => {
        const allText = session.getValue();
        const line = session.getLine(pos.row);
        const charBeforePrefix = line[pos.column - prefix.length - 1];
        const lineBeforePrefix = line.slice(0, pos.column - prefix.length - 1);
        callback(
          null,
          getSqlCompletions({
            allText,
            lineBeforePrefix,
            charBeforePrefix,
            prefix,
            columns: getColumns(),
            availableSqlFunctions,
            skipAggregates: !includeAggregates,
          }),
        );
      },
    },
  ];

  return (
    <AceEditor
      mode="dsql"
      theme={ACE_THEME}
      className="sql-input placeholder-padding"
      // 'react-ace' types are incomplete. Completion options can accept an array of completers.
      enableBasicAutocompletion={cmp as any}
      enableLiveAutocompletion={cmp as any}
      name="ace-editor"
      onChange={handleChange}
      focus={autoFocus}
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
      placeholder={placeholder || 'SQL filter'}
      onLoad={handleAceLoad}
    />
  );
});
