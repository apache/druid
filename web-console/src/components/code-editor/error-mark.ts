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

import { syntaxTree } from '@codemirror/language';
import type { EditorState, Text } from '@codemirror/state';
import { StateEffect, StateField } from '@codemirror/state';
import type { DecorationSet } from '@codemirror/view';
import { Decoration, EditorView } from '@codemirror/view';

import type { LineColumn } from '../../utils';

/**
 * An error to mark in the text, like a parse error
 */
export interface EditorError {
  /** Where the error is (1-based), as reported by Druid and Hjson errors */
  position: LineColumn;
  /** Shown in a tooltip when hovering the mark */
  message: string;
}

/**
 * The offset of a (1-based) line and column. Positions past the end of the line or the text are clamped.
 */
export function lineColumnToOffset(doc: Text, { line, column }: LineColumn): number {
  const docLine = doc.line(Math.min(Math.max(line, 1), doc.lines));
  return Math.min(docLine.from + Math.max(column - 1, 0), docLine.to);
}

/**
 * The text to mark for an error at the given position: the syntax node there (if it is on one line), otherwise the
 * rest of the line. At the end of a line it is the last character before the position that is not a space.
 */
export function getErrorRange(
  state: EditorState,
  position: LineColumn,
): { from: number; to: number } | undefined {
  const { doc } = state;
  const pos = lineColumnToOffset(doc, position);
  const line = doc.lineAt(pos);

  const node = syntaxTree(state).resolveInner(pos, 1);
  if (!node.type.isTop && node.from <= pos && pos < node.to && node.to <= line.to) {
    return { from: node.from, to: node.to };
  }

  if (pos < line.to) return { from: pos, to: line.to };

  let before = pos;
  while (before > 0 && /\s/.test(doc.sliceString(before - 1, before))) before--;
  if (!before) return;
  return { from: before - 1, to: before };
}

const setEditorError = StateEffect.define<EditorError | undefined>();

/**
 * The mark of the error (if any), it goes away on the next change to the text
 */
export const editorErrorField = StateField.define<DecorationSet>({
  create: () => Decoration.none,
  update(marks, tr) {
    for (const effect of tr.effects) {
      if (!effect.is(setEditorError)) continue;
      const error = effect.value;
      const range = error && getErrorRange(tr.state, error.position);
      if (!range) return Decoration.none;
      return Decoration.set(
        Decoration.mark({
          class: 'cm-errorMark',
          attributes: { 'data-tooltip': error.message },
        }).range(range.from, range.to),
      );
    }
    return tr.docChanged ? Decoration.none : marks;
  },
  provide: field => EditorView.decorations.from(field),
});

/**
 * Underlines the given error in the editor (until the text changes), or removes the mark if there is no error
 */
export function showEditorError(view: EditorView, error: EditorError | undefined): void {
  view.dispatch({ effects: setEditorError.of(error) });
}
