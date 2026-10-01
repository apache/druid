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

import { Intent, ResizeSensor } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { Ace } from 'ace-builds';
import { Range } from 'ace-builds';
import classNames from 'classnames';
import { dedupe } from 'druid-query-toolkit';
import React from 'react';
import AceEditor from 'react-ace';

import { getHjsonCompletions } from '../../../ace-completions/hjson-completions';
import { getSqlCompletions } from '../../../ace-completions/sql-completions';
import { useAvailableSqlFunctions } from '../../../contexts/sql-functions-context';
import { NATIVE_JSON_QUERY_COMPLETIONS } from '../../../druid-models';
import { usePermanentCallback } from '../../../hooks';
import { AppToaster } from '../../../singletons';
import { AceEditorStateCache } from '../../../singletons/ace-editor-state-cache';
import type { ColumnMetadata, QuerySlice, RowColumn } from '../../../utils';
import { findAllSqlQueriesInText, findMap } from '../../../utils';

import './flexible-query-input.scss';

const V_PADDING = 10;
const ACE_THEME = 'solarized_dark';

/**
 * The row of the sub query gutter marker (set with `session.setBreakpoint` in `markQueries`) that the event is on
 */
function getSubQueryMarkerRow(e: React.MouseEvent): number | undefined {
  const marker = (e.target as Element).closest('.sub-query-gutter-marker');
  if (!marker) return;
  return findMap([...marker.classList], c => {
    const m = /^query-(\d+)$/.exec(c);
    return m ? Number(m[1]) : undefined;
  });
}

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
  const [editorHeight, setEditorHeight] = React.useState(200);
  const aceEditorRef = React.useRef<Ace.Editor | undefined>(undefined);
  const lastFoundQueriesRef = React.useRef<QuerySlice[]>([]);
  const highlightFoundQueryRef = React.useRef<{ row: number; marker: number } | undefined>(
    undefined,
  );

  const findAllQueriesByLine = React.useCallback(() => {
    const found = dedupe(findAllSqlQueriesInText(queryString), ({ startRowColumn }) =>
      String(startRowColumn.row),
    );
    if (!found.length) return [];

    // Do not report the first query if it is basically the main query minus whitespace
    const firstQuery = found[0].sql;
    if (firstQuery === queryString.trim()) return found.slice(1);

    return found;
  }, [queryString]);

  const markQueries = React.useCallback(() => {
    if (!runQuerySlice) return;
    const aceEditor = aceEditorRef.current;
    if (!aceEditor) return;
    const session = aceEditor.getSession();
    lastFoundQueriesRef.current = findAllQueriesByLine();

    session.clearBreakpoints();
    lastFoundQueriesRef.current.forEach(({ startRowColumn }) => {
      session.setBreakpoint(
        startRowColumn.row,
        `sub-query-gutter-marker query-${startRowColumn.row}`,
      );
    });
  }, [runQuerySlice, findAllQueriesByLine]);

  React.useEffect(() => {
    markQueries();
  }, [markQueries]);

  // Re-mark the queries once the query string has not changed for a bit
  React.useEffect(() => {
    const timeout = setTimeout(markQueries, 900);
    return () => clearTimeout(timeout);
  }, [queryString, markQueries]);

  React.useEffect(() => {
    return () => {
      if (editorStateId && aceEditorRef.current) {
        AceEditorStateCache.saveState(editorStateId, aceEditorRef.current);
      }
    };
  }, [editorStateId]);

  const goToPosition = React.useCallback((rowColumn: RowColumn) => {
    const aceEditor = aceEditorRef.current;
    if (!aceEditor) return;
    aceEditor.focus(); // Grab the focus
    aceEditor.getSelection().moveCursorTo(rowColumn.row, rowColumn.column);
  }, []);

  React.useImperativeHandle(ref, () => ({ goToPosition }), [goToPosition]);

  const handleAceContainerResize = React.useCallback((entries: ResizeObserverEntry[]) => {
    if (entries.length !== 1) return;
    setEditorHeight(entries[0].contentRect.height);
  }, []);

  const handleChange = React.useCallback(
    (value: string) => {
      if (!onQueryStringChange) return;
      onQueryStringChange(value);
    },
    [onQueryStringChange],
  );

  const handleAceLoad = React.useCallback(
    (editor: Ace.Editor) => {
      editor.renderer.setPadding(V_PADDING);
      editor.renderer.setScrollMargin(V_PADDING, V_PADDING, 0, 0);

      if (editorStateId) {
        AceEditorStateCache.applyState(editorStateId, editor);
      }

      aceEditorRef.current = editor;
    },
    [editorStateId],
  );

  const handleContainerClick = React.useCallback(
    (e: React.MouseEvent) => {
      if (!runQuerySlice) return;
      const row = getSubQueryMarkerRow(e);
      if (typeof row === 'undefined') return;

      const slice = lastFoundQueriesRef.current.find(
        ({ startRowColumn }) => startRowColumn.row === row,
      );
      if (!slice) return;

      if (running) {
        AppToaster.show({
          icon: IconNames.WARNING_SIGN,
          intent: Intent.WARNING,
          message: `Another query is currently running`,
        });
        return;
      }

      runQuerySlice(slice);
    },
    [runQuerySlice, running],
  );

  const handleContainerMouseOver = React.useCallback(
    (e: React.MouseEvent) => {
      if (!runQuerySlice) return;
      const aceEditor = aceEditorRef.current;
      if (!aceEditor) return;

      const row = getSubQueryMarkerRow(e);
      if (typeof row === 'undefined' || highlightFoundQueryRef.current?.row === row) return;

      const slice = lastFoundQueriesRef.current.find(
        ({ startRowColumn }) => startRowColumn.row === row,
      );
      if (!slice) return;
      const marker = aceEditor
        .getSession()
        .addMarker(
          new Range(
            slice.startRowColumn.row,
            slice.startRowColumn.column,
            slice.endRowColumn.row,
            slice.endRowColumn.column,
          ),
          'sub-query-highlight',
          'text',
          false,
        );
      highlightFoundQueryRef.current = { row, marker };
    },
    [runQuerySlice],
  );

  const handleContainerMouseOut = React.useCallback(() => {
    if (!highlightFoundQueryRef.current) return;
    const aceEditor = aceEditorRef.current;
    if (!aceEditor) return;
    aceEditor.getSession().removeMarker(highlightFoundQueryRef.current.marker);
    highlightFoundQueryRef.current = undefined;
  }, []);

  const jsonMode = queryString.trim().startsWith('{');

  // Ace reads the completers once, when autocompletion is enabled, so they must not change. The callback always sees
  // the latest props.
  const getCompletions = usePermanentCallback<Ace.Completer['getCompletions']>(
    (_editor, session, pos, prefix, callback) => {
      const allText = session.getValue();
      const line = session.getLine(pos.row);
      const charBeforePrefix = line[pos.column - prefix.length - 1];
      if (allText.trim().startsWith('{')) {
        const lines = allText.split('\n').slice(0, pos.row + 1);
        const lastLineIndex = lines.length - 1;
        lines[lastLineIndex] = lines[lastLineIndex].slice(0, pos.column - prefix.length - 1);
        callback(
          null,
          getHjsonCompletions({
            jsonCompletions: NATIVE_JSON_QUERY_COMPLETIONS,
            textBefore: lines.join('\n'),
            charBeforePrefix,
            prefix,
          }),
        );
      } else {
        const lineBeforePrefix = line.slice(0, pos.column - prefix.length - 1);
        callback(
          null,
          getSqlCompletions({
            allText,
            lineBeforePrefix,
            charBeforePrefix,
            prefix,
            columnMetadata,
            availableSqlFunctions,
          }),
        );
      }
    },
  );
  const completers = React.useMemo<Ace.Completer[]>(() => [{ getCompletions }], [getCompletions]);

  return (
    <div className="flexible-query-input">
      <ResizeSensor onResize={handleAceContainerResize}>
        <div
          className={classNames('ace-container', running ? 'query-running' : 'query-idle')}
          onClick={handleContainerClick}
          onMouseOver={handleContainerMouseOver}
          onMouseOut={handleContainerMouseOut}
        >
          <AceEditor
            mode={jsonMode ? 'hjson' : 'dsql'}
            theme={ACE_THEME}
            className={classNames(
              'placeholder-padding',
              leaveBackground ? undefined : 'no-background',
            )}
            editorProps={{ completers }}
            enableBasicAutocompletion
            enableLiveAutocompletion
            onChange={handleChange}
            focus
            fontSize={12}
            width="100%"
            height={editorHeight + 'px'}
            showGutter={showGutter}
            showPrintMargin={false}
            tabSize={2}
            value={queryString}
            readOnly={!onQueryStringChange}
            setOptions={{
              showLineNumbers: true,
              newLineMode: 'unix',
            }}
            placeholder={placeholder || 'SELECT * FROM ...'}
            onLoad={handleAceLoad}
          />
        </div>
      </ResizeSensor>
    </div>
  );
}
