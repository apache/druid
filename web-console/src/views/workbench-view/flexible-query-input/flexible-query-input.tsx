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
import type { Extension } from '@codemirror/state';
import { RangeSet, StateEffect, StateField } from '@codemirror/state';
import type { DecorationSet } from '@codemirror/view';
import { Decoration, EditorView, GutterMarker, lineNumberMarkers } from '@codemirror/view';
import { dedupe } from 'druid-query-toolkit';
import React from 'react';

import type { CompletionRequest } from '../../../components';
import { CodeEditor, focusEditorAt } from '../../../components';
import { useAvailableSqlFunctions } from '../../../contexts/sql-functions-context';
import { NATIVE_JSON_QUERY_COMPLETIONS } from '../../../druid-models';
import { getHjsonCompletions } from '../../../editor-completions/hjson-completions';
import { getSqlCompletions } from '../../../editor-completions/sql-completions';
import { AppToaster } from '../../../singletons';
import type { ColumnMetadata, QuerySlice, RowColumn } from '../../../utils';
import { findAllSqlQueriesInText, findMap } from '../../../utils';

import './flexible-query-input.scss';

const V_PADDING = 10;

class SubQueryGutterMarker extends GutterMarker {
  constructor(readonly row: number) {
    super();
    this.elementClass = `sub-query-gutter-marker query-${row}`;
  }

  eq(other: GutterMarker): boolean {
    return other instanceof SubQueryGutterMarker && other.row === this.row;
  }
}

/**
 * Sets the (0 based) rows that get a sub query gutter marker
 */
const setSubQueryRows = StateEffect.define<number[]>();

const subQueryMarkers = StateField.define<RangeSet<GutterMarker>>({
  create: () => RangeSet.empty,
  update(markers, tr) {
    markers = markers.map(tr.changes);
    for (const effect of tr.effects) {
      if (!effect.is(setSubQueryRows)) continue;
      const { doc } = tr.state;
      markers = RangeSet.of(
        effect.value
          .filter(row => row < doc.lines)
          .map(row => new SubQueryGutterMarker(row).range(doc.line(row + 1).from)),
        true,
      );
    }
    return markers;
  },
  provide: field => lineNumberMarkers.from(field),
});

const setSubQueryHighlight = StateEffect.define<{ from: number; to: number } | undefined>();

const subQueryHighlightMark = Decoration.mark({ class: 'sub-query-highlight' });

const subQueryHighlight = StateField.define<DecorationSet>({
  create: () => Decoration.none,
  update(highlight, tr) {
    highlight = highlight.map(tr.changes);
    for (const effect of tr.effects) {
      if (!effect.is(setSubQueryHighlight)) continue;
      const range = effect.value;
      highlight =
        range && range.from < range.to
          ? Decoration.set(subQueryHighlightMark.range(range.from, range.to))
          : Decoration.none;
    }
    return highlight;
  },
  provide: field => EditorView.decorations.from(field),
});

const SUB_QUERY_EXTENSIONS: Extension = [subQueryMarkers, subQueryHighlight];

/**
 * The row of the sub query gutter marker (set in `markQueries`) that the event is on
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
  const editorViewRef = React.useRef<EditorView | undefined>(undefined);
  const lastFoundQueriesRef = React.useRef<QuerySlice[]>([]);
  const highlightFoundQueryRowRef = React.useRef<number | undefined>(undefined);

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
    const editorView = editorViewRef.current;
    if (!editorView) return;
    lastFoundQueriesRef.current = findAllQueriesByLine();

    editorView.dispatch({
      effects: setSubQueryRows.of(
        lastFoundQueriesRef.current.map(({ startRowColumn }) => startRowColumn.row),
      ),
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

  const goToPosition = React.useCallback((rowColumn: RowColumn) => {
    const editorView = editorViewRef.current;
    if (!editorView) return;
    focusEditorAt(editorView, rowColumn);
  }, []);

  React.useImperativeHandle(ref, () => ({ goToPosition }), [goToPosition]);

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
      const editorView = editorViewRef.current;
      if (!editorView) return;

      const row = getSubQueryMarkerRow(e);
      if (typeof row === 'undefined' || highlightFoundQueryRowRef.current === row) return;

      const slice = lastFoundQueriesRef.current.find(
        ({ startRowColumn }) => startRowColumn.row === row,
      );
      if (!slice) return;
      const docLength = editorView.state.doc.length;
      editorView.dispatch({
        effects: setSubQueryHighlight.of({
          from: Math.min(slice.startOffset, docLength),
          to: Math.min(slice.endOffset, docLength),
        }),
      });
      highlightFoundQueryRowRef.current = row;
    },
    [runQuerySlice],
  );

  const handleContainerMouseOut = React.useCallback(() => {
    if (typeof highlightFoundQueryRowRef.current === 'undefined') return;
    editorViewRef.current?.dispatch({ effects: setSubQueryHighlight.of(undefined) });
    highlightFoundQueryRowRef.current = undefined;
  }, []);

  const getCompletions = React.useCallback(
    ({
      allText,
      prefix,
      charBeforePrefix,
      textBeforePrefix,
      lineBeforePrefix,
    }: CompletionRequest) => {
      if (allText.trim().startsWith('{')) {
        return getHjsonCompletions({
          jsonCompletions: NATIVE_JSON_QUERY_COMPLETIONS,
          textBefore: textBeforePrefix,
          charBeforePrefix,
          prefix,
        });
      } else {
        return getSqlCompletions({
          allText,
          lineBeforePrefix,
          charBeforePrefix,
          prefix,
          columnMetadata,
          availableSqlFunctions,
        });
      }
    },
    [columnMetadata, availableSqlFunctions],
  );

  return (
    <div className="flexible-query-input">
      <div
        className="editor-container"
        onClick={handleContainerClick}
        onMouseOver={handleContainerMouseOver}
        onMouseOut={handleContainerMouseOut}
      >
        <CodeEditor
          ref={editorViewRef}
          mode={queryString.trim().startsWith('{') ? 'hjson' : 'dsql'}
          transparentBackground={!leaveBackground}
          value={queryString}
          onChange={onQueryStringChange}
          autoFocus
          height="100%"
          showGutter={showGutter}
          padding={V_PADDING}
          placeholder={placeholder || 'SELECT * FROM ...'}
          getCompletions={getCompletions}
          stateCacheId={editorStateId}
          extensions={SUB_QUERY_EXTENSIONS}
        />
      </div>
    </div>
  );
}
