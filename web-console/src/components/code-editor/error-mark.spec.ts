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

import { EditorState } from '@codemirror/state';
import { EditorView } from '@codemirror/view';
import Hjson from 'hjson';

import { hjson } from '../../editor-languages/hjson';
import { getHjsonEditorError } from '../json-input/json-input';

import { editorErrorField, getErrorRange, showEditorError } from './error-mark';

// The text that would be marked for the error that Hjson.parse gives
function markedText(text: string): string | undefined {
  let message: string | undefined;
  try {
    Hjson.parse(text);
  } catch (e) {
    message = e.message;
  }
  const error = getHjsonEditorError(message!);
  if (!error) return;
  const state = EditorState.create({ doc: text, extensions: hjson() });
  const range = getErrorRange(state, error.position);
  return range && state.sliceDoc(range.from, range.to);
}

describe('error mark', () => {
  it('marks the token at the position of an Hjson error', () => {
    expect(markedText('{\n  "a": 1\n  "b" 2\n}')).toEqual('2');
    expect(markedText('{\n  "a": [1, 2\n}')).toEqual('}');
    expect(markedText('{\n  "a": {}}}')).toEqual('}');
    expect(markedText('{\n  "a": 1,\n  }\n}')).toEqual('}');
  });

  it('marks the rest of the line, or the character before the end of a line', () => {
    // The key that the error recovery made of the text
    expect(markedText('{\n  a b: 1\n}')).toEqual('a ');
    // Without a language there is no syntax node
    const plainState = EditorState.create({ doc: 'abc def\nghi' });
    const range = getErrorRange(plainState, { line: 1, column: 3 });
    expect(range && plainState.sliceDoc(range.from, range.to)).toEqual('c def');
    // The position is past the end of the text, the last character is marked
    expect(markedText('{\n  "a": 1\n')).toEqual('1');
    expect(markedText('')).toBeUndefined();
  });

  it('describes Hjson errors without their position', () => {
    expect(
      getHjsonEditorError(`Expected ':' instead of '2' at line 3,7 >>>  "b" 2\n} ...`),
    ).toEqual({ position: { line: 3, column: 7 }, message: `Expected ':' instead of '2'` });
    expect(getHjsonEditorError('No position')).toBeUndefined();
  });

  it('shows the message as a tooltip until the text changes', () => {
    const view = new EditorView({
      state: EditorState.create({ doc: 'abc def', extensions: editorErrorField }),
    });
    showEditorError(view, { position: { line: 1, column: 5 }, message: 'Bad' });
    const mark = view.dom.querySelector('.cm-errorMark');
    expect(mark?.textContent).toEqual('def');
    expect(mark?.getAttribute('data-tooltip')).toEqual('Bad');

    view.dispatch({ changes: { from: 0, insert: 'x' } });
    expect(view.dom.querySelector('.cm-errorMark')).toBeNull();

    showEditorError(view, { position: { line: 1, column: 1 }, message: 'Bad' });
    showEditorError(view, undefined);
    expect(view.dom.querySelector('.cm-errorMark')).toBeNull();
    view.destroy();
  });
});
