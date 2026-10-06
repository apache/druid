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

import { CodeEditor, focusEditorAt } from '../../../components';
import { useAvailableSqlFunctions } from '../../../contexts/sql-functions-context';
import { NATIVE_JSON_QUERY_COMPLETIONS } from '../../../druid-models';
import { dsql } from '../../../editor-languages/dsql';
import { hjson } from '../../../editor-languages/hjson';
import { useConstant, usePermanentCallback } from '../../../hooks';
import { AppToaster } from '../../../singletons';
import type { ColumnMetadata, QuerySlice, RowColumn } from '../../../utils';

import { subQueryMarkers } from './sub-query-markers';

import './flexible-query-input.scss';

const V_PADDING = 10;

export interface FlexibleQueryInputHandle {
  goToPosition(rowColumn: RowColumn): void;
}

export interface FlexibleQueryInputProps {
  ref?: React.Ref<FlexibleQueryInputHandle | undefined>;
  queryString: string;
  onQueryStringChange?: (newQueryString: string) => void;
  runQuerySlice?: (querySlice: QuerySlice) => void;
  running?: boolean;
  showGutter?: boolean;
  placeholder?: string;
  columnMetadata?: readonly ColumnMetadata[];
  editorStateId?: string;
  leaveBackground?: boolean;
}

export function FlexibleQueryInput(props: FlexibleQueryInputProps) {
  const {
    ref,
    queryString,
    onQueryStringChange,
    runQuerySlice,
    running,
    showGutter = true,
    placeholder,
    columnMetadata,
    editorStateId,
    leaveBackground,
  } = props;

  const availableSqlFunctions = useAvailableSqlFunctions();
  const editorViewRef = React.useRef<EditorView | undefined>(undefined);

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

  // The sub query markers live on the line numbers, so they are only there when the gutter is
  const subQueriesEnabled = Boolean(runQuerySlice) && showGutter;
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

  const goToPosition = React.useCallback((rowColumn: RowColumn) => {
    const editorView = editorViewRef.current;
    if (!editorView) return;
    focusEditorAt(editorView, rowColumn);
  }, []);

  React.useImperativeHandle(ref, () => ({ goToPosition }), [goToPosition]);

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
          transparentBackground={!leaveBackground}
          value={queryString}
          onChange={onQueryStringChange}
          autoFocus
          height="100%"
          showGutter={showGutter}
          padding={V_PADDING}
          placeholder={placeholder || 'SELECT * FROM ...'}
          stateCacheId={editorStateId}
          extensions={extensions}
        />
      </div>
    </div>
  );
}
