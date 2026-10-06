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

import type { EditorView } from '@codemirror/view';
import type { Column } from 'druid-query-toolkit';
import React from 'react';

import { CodeEditor, focusEditorAt } from '../../../../components';
import { useAvailableSqlFunctions } from '../../../../contexts/sql-functions-context';
import { dsql } from '../../../../editor-languages/dsql';
import type { RowColumn } from '../../../../utils';

const V_PADDING = 10;

export interface SqlInputHandle {
  goToPosition(rowColumn: RowColumn): void;
}

export interface SqlInputProps {
  ref?: React.Ref<SqlInputHandle | undefined>;
  value: string;
  onValueChange?: (newValue: string) => void;
  placeholder?: string;
  editorHeight?: number;
  columns?: readonly Column[];
  autoFocus?: boolean;
  showGutter?: boolean;
  includeAggregates?: boolean;
}

export function SqlInput(props: SqlInputProps) {
  const {
    ref,
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
  const editorViewRef = React.useRef<EditorView | undefined>(undefined);

  const goToPosition = React.useCallback((rowColumn: RowColumn) => {
    const editorView = editorViewRef.current;
    if (!editorView) return;
    focusEditorAt(editorView, rowColumn);
  }, []);

  React.useImperativeHandle(ref, () => ({ goToPosition }), [goToPosition]);

  const language = React.useMemo(
    () =>
      dsql({
        columns: columns?.map(column => column.name),
        availableSqlFunctions,
        skipAggregates: !includeAggregates,
      }),
    [columns, availableSqlFunctions, includeAggregates],
  );

  return (
    <CodeEditor
      ref={editorViewRef}
      className="sql-input"
      language={language}
      value={value}
      onChange={onValueChange}
      autoFocus={autoFocus}
      width="100%"
      height={editorHeight ? `${editorHeight}px` : '100%'}
      showGutter={Boolean(showGutter)}
      padding={V_PADDING}
      placeholder={placeholder || 'SQL filter'}
    />
  );
}
