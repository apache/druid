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

import type { EditorState } from '@codemirror/state';
import { EditorState as State } from '@codemirror/state';
import { EditorView, lineNumberMarkers } from '@codemirror/view';

import { findSubQueries, getSubQueries, subQueryMarkers } from './sub-query-markers';

const TEXT = `-- Two queries
SELECT 1;
SELECT
  2`;

function markerLines(state: EditorState): number[] {
  const lines: number[] = [];
  for (const markers of state.facet(lineNumberMarkers)) {
    markers.between(0, state.doc.length, from => {
      lines.push(state.doc.lineAt(from).number);
    });
  }
  return lines;
}

describe('sub-query-markers', () => {
  it('finds the queries other than the whole text', () => {
    expect(findSubQueries(TEXT).map(slice => slice.sql)).toEqual(['SELECT 1;', 'SELECT\n  2']);
    expect(findSubQueries('SELECT 1')).toEqual([]);
  });

  it('marks the lines where the queries start and keeps them up to date', () => {
    let state = State.create({ doc: TEXT, extensions: subQueryMarkers(() => {}) });
    expect(markerLines(state)).toEqual([2, 3]);

    state = state.update({ changes: { from: 0, insert: 'SELECT 0;\n' } }).state;
    expect(markerLines(state)).toEqual([1, 3, 4]);
    // The offsets are for the current text
    expect(
      getSubQueries(state).map(slice => state.sliceDoc(slice.startOffset, slice.startOffset + 8)),
    ).toEqual(['SELECT 0', 'SELECT 1', 'SELECT\n ']);
  });

  it('draws a run button next to the line number', () => {
    const view = new EditorView({
      state: State.create({ doc: TEXT, extensions: subQueryMarkers(() => {}) }),
    });
    const markers = Array.from(view.dom.querySelectorAll('.sub-query-gutter-marker'));
    expect(markers.map(marker => marker.textContent)).toEqual(['2', '3']);
    expect(markers.every(marker => marker.querySelector('.sub-query-run-button svg'))).toBe(true);
    expect(
      markers.map(marker =>
        marker.querySelector('.sub-query-run-button')?.getAttribute('data-tooltip'),
      ),
    ).toEqual(['Run this query', 'Run this query']);
    view.destroy();
  });

  it('has no queries without the extension', () => {
    expect(getSubQueries(State.create({ doc: TEXT }))).toEqual([]);
  });
});
