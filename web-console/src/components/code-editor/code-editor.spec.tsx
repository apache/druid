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

import { undo } from '@codemirror/commands';
import type { EditorView } from '@codemirror/view';
import { render } from '@testing-library/react';
import { createRef } from 'react';

import { CodeEditor, forgetEditorState } from './code-editor';

describe('CodeEditor', () => {
  it('remembers the undo history between mounts until it is forgotten', () => {
    function mount(value: string) {
      const ref = createRef<EditorView | undefined>();
      const rendered = render(
        <CodeEditor ref={ref} value={value} onChange={() => {}} stateCacheId="spec" />,
      );
      return { view: ref.current!, unmount: rendered.unmount };
    }

    const first = mount('SELECT 1');
    first.view.dispatch({ changes: { from: 7, to: 8, insert: '2' }, userEvent: 'input' });
    first.unmount();

    const second = mount('SELECT 2');
    expect(undo(second.view)).toBe(true);
    expect(second.view.state.doc.toString()).toEqual('SELECT 1');
    second.unmount();

    forgetEditorState('spec');
    const third = mount('SELECT 2');
    expect(undo(third.view)).toBe(false);
    third.unmount();
  });
});
