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
import { IconNames } from '@blueprintjs/icons';
import { Compartment } from '@codemirror/state';
import type { EditorView } from '@codemirror/view';
import React from 'react';

import { CodeEditor } from '../../../components';
import { useAvailableSqlFunctions } from '../../../contexts/sql-functions-context';
import { NATIVE_JSON_QUERY_COMPLETIONS } from '../../../druid-models';
import { dsql } from '../../../editor-languages/dsql';
import { hjson } from '../../../editor-languages/hjson';
import { useConstant, usePermanentCallback } from '../../../hooks';
import { AppToaster } from '../../../singletons';
import type { ColumnMetadata, QuerySlice } from '../../../utils';

import { subQueryMarkers } from './sub-query-markers';

import './flexible-query-input.scss';

const V_PADDING = 10;

export interface FlexibleQueryInputProps {
  /** Gives you the CodeMirror EditorView (to use with focusEditorAt for example) */
  ref?: React.Ref<EditorView | undefined>;
  queryString: string;
  onQueryStringChange?: (newQueryString: string) => void;
  readOnly?: boolean;
  runQuerySlice?: (querySlice: QuerySlice) => void;
  running?: boolean;
  /** Defaults to true */
  showLineNumbers?: boolean;
  placeholder?: string;
  columnMetadata?: readonly ColumnMetadata[];
  /** Makes the editor remember its state (undo history, selection) between mounts, see CodeEditor */
  stateCacheId?: string;
  /** Defaults to true */
  transparentBackground?: boolean;
}

export function FlexibleQueryInput(props: FlexibleQueryInputProps) {
  const {
    ref,
    queryString,
    onQueryStringChange,
    readOnly,
    runQuerySlice,
    running,
    showLineNumbers = true,
    placeholder,
    columnMetadata,
    stateCacheId,
    transparentBackground = true,
  } = props;

  const availableSqlFunctions = useAvailableSqlFunctions();
  const editorViewRef = React.useRef<EditorView | undefined>(undefined);
  React.useImperativeHandle(ref, () => editorViewRef.current);

  const handleRunSubQuery = usePermanentCallback((slice: QuerySlice) => {
    if (!runQuerySlice) return;
    if (running) {
      AppToaster.show({
        icon: IconNames.WARNING_SIGN,
        intent: Intent.WARNING,
        message: `Another query is currently running`,
      });
      return;
    }

    runQuerySlice(slice);
  });

  // The sub query markers live on the line numbers, so they are only there when the line numbers are
  const subQueriesEnabled = Boolean(runQuerySlice) && showLineNumbers;
  const subQueryCompartment = useConstant(() => new Compartment());
  const subQueryExtension = React.useMemo(
    () => (subQueriesEnabled ? subQueryMarkers(handleRunSubQuery) : []),
    [subQueriesEnabled, handleRunSubQuery],
  );
  const extensions = useConstant(() => subQueryCompartment.of(subQueryExtension));

  React.useEffect(() => {
    editorViewRef.current?.dispatch({
      effects: subQueryCompartment.reconfigure(subQueryExtension),
    });
  }, [subQueryCompartment, subQueryExtension]);

  const isJson = queryString.trim().startsWith('{');
  const language = React.useMemo(
    () =>
      isJson
        ? hjson({ jsonCompletions: NATIVE_JSON_QUERY_COMPLETIONS })
        : dsql({ columnMetadata, availableSqlFunctions }),
    [isJson, columnMetadata, availableSqlFunctions],
  );

  return (
    <div className="flexible-query-input">
      <div className="editor-container">
        <CodeEditor
          ref={editorViewRef}
          language={language}
          transparentBackground={transparentBackground}
          value={queryString}
          onChange={onQueryStringChange}
          readOnly={readOnly}
          autoFocus
          showLineNumbers={showLineNumbers}
          padding={V_PADDING}
          placeholder={placeholder || 'SELECT * FROM ...'}
          stateCacheId={stateCacheId}
          extensions={extensions}
        />
      </div>
    </div>
  );
}
