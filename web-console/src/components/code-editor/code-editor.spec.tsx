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
import { openSearchPanel } from '@codemirror/search';
import type { EditorView } from '@codemirror/view';
import { act, fireEvent, render, screen } from '@testing-library/react';
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

  it('finds and replaces with the search panel', () => {
    const ref = createRef<EditorView | undefined>();
    const onChange = jest.fn();
    const { unmount } = render(<CodeEditor ref={ref} value="foo bar foo" onChange={onChange} />);
    const view = ref.current!;

    act(() => {
      openSearchPanel(view);
    });
    const searchField = screen.getByLabelText<HTMLInputElement>('Find');
    expect(document.activeElement).toBe(searchField);

    fireEvent.change(searchField, { target: { value: 'foo' } });
    fireEvent.keyDown(searchField, { key: 'Enter', keyCode: 13 });
    expect(view.state.selection.main).toMatchObject({ from: 0, to: 3 });
    fireEvent.keyDown(searchField, { key: 'Enter', keyCode: 13 });
    expect(view.state.selection.main).toMatchObject({ from: 8, to: 11 });

    fireEvent.change(screen.getByLabelText('Replace'), { target: { value: 'baz' } });
    fireEvent.click(screen.getByText('Replace all'));
    expect(onChange).toHaveBeenLastCalledWith('baz bar baz');

    fireEvent.keyDown(searchField, { key: 'Escape', keyCode: 27 });
    expect(screen.queryByLabelText('Find')).toBeNull();
    unmount();
  });
});
