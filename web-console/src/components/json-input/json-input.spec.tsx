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

import { EditorView } from '@codemirror/view';
import { act, fireEvent, render } from '@testing-library/react';
import Hjson from 'hjson';

import { extractLineColumnFromHjsonError, JsonInput } from './json-input';

describe('JsonInput', () => {
  it('matches snapshot (null)', () => {
    const jsonCollapse = <JsonInput onChange={() => {}} value={null} />;
    const { container } = render(jsonCollapse);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('matches snapshot (value)', () => {
    const value = {
      hello: ['world', { a: 1, b: 2 }],
    };
    const jsonCollapse = <JsonInput onChange={() => {}} value={value} />;
    const { container } = render(jsonCollapse);
    expect(container.firstChild).toMatchSnapshot();
  });

  it('extractLineColumnFromHjsonError is ok with non matching error', () => {
    expect(extractLineColumnFromHjsonError(new Error('blah blah'))).toBeUndefined();
  });

  it('extractLineColumnFromHjsonError works with real error', () => {
    let error: Error | undefined;
    try {
      Hjson.parse(`{\n"Hello" "World"\n}`);
    } catch (e) {
      error = e;
    }

    expect(error).toBeDefined();

    expect(extractLineColumnFromHjsonError(error!)).toEqual({
      line: 2,
      column: 9,
    });
  });

  it('underlines the error once the editor loses focus', () => {
    const { container } = render(<JsonInput onChange={() => {}} value={{ a: 1 }} />);
    const view = EditorView.findFromDOM(container.querySelector<HTMLElement>('.cm-editor')!)!;
    act(() => {
      view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: '{\n"a" 1\n}' } });
    });
    expect(container.querySelector('.cm-errorMark')).toBeNull();

    act(() => {
      fireEvent.blur(view.contentDOM);
    });
    const mark = container.querySelector('.cm-errorMark');
    expect(mark?.textContent).toEqual('1');
    expect(mark?.getAttribute('data-tooltip')).toEqual(`Expected ':' instead of '1'`);
  });
});
