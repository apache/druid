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

import type {
  Completion,
  CompletionContext,
  CompletionResult,
  CompletionSource,
} from '@codemirror/autocomplete';
import { ensureSyntaxTree, syntaxTree } from '@codemirror/language';
import type { EditorState } from '@codemirror/state';
import type { Tree } from '@lezer/common';

import type { EditorCompletion } from '../../editor-completions/editor-completion';

import { renderCompletionDoc } from './completion-doc';

// The characters that make up the word being completed
const PREFIX_REGEXP = /[\w$\-\u00A2-\u2000\u2070-\uFFFF]*/;
const VALID_PREFIX_REGEXP = new RegExp(`^${PREFIX_REGEXP.source}$`);

/**
 * The (partial) word being completed, it ends at the cursor
 */
export interface CompletionWord {
  from: number;
  text: string;
}

/**
 * Builds the completions for the word being completed, CodeMirror filters and ranks them against it
 */
export type EditorCompletionBuilder = (
  context: CompletionContext,
  word: CompletionWord,
) => readonly EditorCompletion[];

/**
 * The word before the cursor, or undefined when there is nothing to complete (no word typed and the completion was not
 * asked for explicitly)
 */
export function matchCompletionWord(context: CompletionContext): CompletionWord | undefined {
  const { state, pos } = context;
  const from = context.matchBefore(PREFIX_REGEXP)?.from ?? pos;
  if (from === pos && !context.explicit) return;
  return { from, text: state.sliceDoc(from, pos) };
}

/**
 * The syntax node (the token) that the cursor is in or right after, like 'LineComment' or 'String'
 */
export function tokenBefore(
  state: EditorState,
  pos: number,
): { name: string; from: number; to: number } {
  const { name, from, to } = syntaxTree(state).resolveInner(pos, -1);
  return { name, from, to };
}

/**
 * The syntax tree of the whole text, for completions that look at more than the text around the cursor. The editor
 * parses in the background, so the end of a long text might not be parsed yet. It is parsed here (for a short while),
 * otherwise the tree is what has been parsed so far.
 */
export function completeSyntaxTree(state: EditorState): Tree {
  return ensureSyntaxTree(state, state.doc.length, 50) ?? syntaxTree(state);
}

function toCompletion({ doc, ...completion }: EditorCompletion): Completion {
  return doc ? { ...completion, info: () => renderCompletionDoc(doc) } : completion;
}

/**
 * Makes a CodeMirror completion source out of a completion builder (like getSqlCompletions)
 */
export function makeCompletionSource(build: EditorCompletionBuilder): CompletionSource {
  return (context): CompletionResult | null => {
    if (context.state.readOnly) return null;
    const word = matchCompletionWord(context);
    if (!word) return null;

    const options = build(context, word).map(toCompletion);
    if (!options.length) return null;

    return {
      from: word.from,
      options,
      validFor: VALID_PREFIX_REGEXP,
    };
  };
}
