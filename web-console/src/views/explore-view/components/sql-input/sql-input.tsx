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

import { CodeEditor } from '../../../../components';
import { useAvailableSqlFunctions } from '../../../../contexts/sql-functions-context';
import { dsql } from '../../../../editor-languages/dsql';

const V_PADDING = 10;

export interface SqlInputProps {
  /** Gives you the CodeMirror EditorView (to use with focusEditorAt for example) */
  ref?: React.Ref<EditorView | undefined>;
  value: string;
  onValueChange: (newValue: string) => void;
  placeholder?: string;
  /** In px, by default the input fills its container */
  height?: number;
  columns?: readonly Column[];
  autoFocus?: boolean;
  showLineNumbers?: boolean;
  includeAggregates?: boolean;
}

export function SqlInput(props: SqlInputProps) {
  const {
    ref,
    value,
    onValueChange,
    placeholder,
    autoFocus,
    height,
    showLineNumbers,
    columns,
    includeAggregates,
  } = props;

  const availableSqlFunctions = useAvailableSqlFunctions();
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
      ref={ref}
      className="sql-input"
      language={language}
      value={value}
      onChange={onValueChange}
      autoFocus={autoFocus}
      style={{ width: '100%', height: height ? `${height}px` : '100%' }}
      showLineNumbers={showLineNumbers}
      padding={V_PADDING}
      placeholder={placeholder || 'SQL filter'}
    />
  );
}
