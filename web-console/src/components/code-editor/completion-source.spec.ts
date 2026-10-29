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

import type { CompletionResult } from '@codemirror/autocomplete';
import { CompletionContext } from '@codemirror/autocomplete';
import type { Extension } from '@codemirror/state';
import { EditorState } from '@codemirror/state';

import { dsql } from '../../editor-languages/dsql';
import { hjson } from '../../editor-languages/hjson';
import { completionContextAt } from '../../test-utils/completion-context';

import { makeCompletionSource } from './completion-source';

describe('completion source', () => {
  // The word being completed with the cursor at the |
  function wordAt(extension: Extension, textWithCursor: string): string {
    return completionContextAt(extension, textWithCursor)[1].text;
  }

  it('uses the word characters of the language', () => {
    expect(wordAt(dsql(), 'SELECT a-co|')).toEqual('co');
    expect(wordAt(dsql(), 'SELECT $my_v|')).toEqual('$my_v');
    expect(wordAt(dsql(), 'SELECT "café|')).toEqual('café');
    expect(wordAt(hjson(), '{ type: mv-fil|')).toEqual('mv-fil');
    expect(wordAt(hjson(), '{ type: "c5.lar|')).toEqual('lar');
    expect(wordAt(dsql(), 'SELECT (|')).toEqual('');
  });

  it('only completes a word that ends at the cursor', () => {
    expect(wordAt(dsql(), 'SELECT co|unt')).toEqual('co');
  });

  it('keeps the result while the word grows', () => {
    const source = makeCompletionSource(() => [{ label: 'my-value' }]);
    const state = EditorState.create({ doc: '{ a: my', extensions: hjson() });
    const result = source(new CompletionContext(state, 7, false)) as CompletionResult;
    const validFor = result.validFor as Exclude<CompletionResult['validFor'], RegExp | undefined>;
    expect(validFor('my-va', 5, 10, state)).toBe(true);
    expect(validFor('my va', 5, 10, state)).toBe(false);
  });
});
