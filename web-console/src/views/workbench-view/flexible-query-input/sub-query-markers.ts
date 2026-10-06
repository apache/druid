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

import type { EditorState, Extension, Text } from '@codemirror/state';
import { RangeSet, StateEffect, StateField } from '@codemirror/state';
import type { DecorationSet } from '@codemirror/view';
import {
  Decoration,
  EditorView,
  GutterMarker,
  lineNumberMarkers,
  lineNumbers,
} from '@codemirror/view';
import { dedupe } from 'druid-query-toolkit';

import type { QuerySlice } from '../../../utils';
import { findAllSqlQueriesInText } from '../../../utils';

/**
 * The queries in the text that can be run on their own, at most one per line
 */
export function findSubQueries(text: string): QuerySlice[] {
  const found = dedupe(findAllSqlQueriesInText(text), ({ startRowColumn }) =>
    String(startRowColumn.row),
  );
  if (!found.length) return [];

  // Do not report the first query if it is basically the main query minus whitespace
  if (found[0].sql === text.trim()) return found.slice(1);

  return found;
}

const subQueryMarker = new (class extends GutterMarker {
  elementClass = 'sub-query-gutter-marker';
})();

const subQueryHighlightMark = Decoration.mark({ class: 'sub-query-highlight' });

interface SubQueriesValue {
  /** The sub queries of the current text */
  slices: readonly QuerySlice[];
  /** The "run" markers on the line numbers, one at the start line of each slice */
  markers: RangeSet<GutterMarker>;
  /** The slice whose marker is hovered */
  hovered?: QuerySlice;
  /** The hovered slice highlighted in the text */
  highlight: DecorationSet;
}

function makeValue(doc: Text, slices: readonly QuerySlice[]): SubQueriesValue {
  return {
    slices,
    markers: RangeSet.of(
      slices.map(slice => subQueryMarker.range(doc.lineAt(slice.startOffset).from)),
    ),
    highlight: Decoration.none,
  };
}

/**
 * Highlights the given slice (or nothing)
 */
const setHighlightedSubQuery = StateEffect.define<QuerySlice | undefined>();

const subQueries = StateField.define<SubQueriesValue>({
  create: state => makeValue(state.doc, findSubQueries(state.doc.toString())),
  update(value, tr) {
    if (tr.docChanged) {
      value = makeValue(tr.state.doc, findSubQueries(tr.state.doc.toString()));
    }
    for (const effect of tr.effects) {
      if (!effect.is(setHighlightedSubQuery)) continue;
      const slice = effect.value;
      value = {
        ...value,
        hovered: slice,
        highlight:
          slice && slice.startOffset < slice.endOffset
            ? Decoration.set(subQueryHighlightMark.range(slice.startOffset, slice.endOffset))
            : Decoration.none,
      };
    }
    return value;
  },
  provide: field => [
    lineNumberMarkers.from(field, value => value.markers),
    EditorView.decorations.from(field, value => value.highlight),
  ],
});

/**
 * The sub queries of the current text
 */
export function getSubQueries(state: EditorState): readonly QuerySlice[] {
  return state.field(subQueries, false)?.slices || [];
}

/**
 * The sub query that starts on the given line (given by its start offset)
 */
function findSubQueryOnLine(state: EditorState, lineFrom: number): QuerySlice | undefined {
  return getSubQueries(state).find(slice => state.doc.lineAt(slice.startOffset).from === lineFrom);
}

/**
 * Puts a "run" marker on the line numbers of every line where a query starts (other than the whole text). Hovering a
 * marker highlights its query and clicking it calls onRun with it. Shows the line numbers.
 */
export function subQueryMarkers(onRun: (slice: QuerySlice) => void): Extension {
  return [
    subQueries,
    lineNumbers({
      domEventHandlers: {
        click(view, line) {
          const slice = findSubQueryOnLine(view.state, line.from);
          if (!slice) return false;
          onRun(slice);
          return true;
        },
        mouseover(view, line) {
          const slice = findSubQueryOnLine(view.state, line.from);
          if (!slice || view.state.field(subQueries).hovered === slice) return false;
          view.dispatch({ effects: setHighlightedSubQuery.of(slice) });
          return false;
        },
        mouseout(view) {
          if (view.state.field(subQueries).hovered) {
            view.dispatch({ effects: setHighlightedSubQuery.of(undefined) });
          }
          return false;
        },
      },
    }),
  ];
}
