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

import type { Completion, CompletionSource } from '@codemirror/autocomplete';

import type { EditorCompletion } from '../../editor-completions/editor-completion';

import { renderCompletionDoc } from './completion-doc';

// The characters that make up the word being completed
const PREFIX_REGEXP = /[\w$\-\u00A2-\u2000\u2070-\uFFFF]*/;
const VALID_PREFIX_REGEXP = new RegExp(`^${PREFIX_REGEXP.source}$`);

export interface CompletionRequest {
  allText: string;
  /** The (partial) word being completed */
  prefix: string;
  /** The character right before the prefix ('\n' at the start of a line, '' at the start of the text) */
  charBeforePrefix: string;
  /** All of the text before charBeforePrefix */
  textBeforePrefix: string;
  /** The part of the line before charBeforePrefix */
  lineBeforePrefix: string;
}

function toCompletion({ doc, ...completion }: EditorCompletion): Completion {
  return doc ? { ...completion, info: () => renderCompletionDoc(doc) } : completion;
}

/**
 * Makes a CodeMirror completion source out of a completion builder (like getSqlCompletions) that takes the text around
 * the cursor split up into a CompletionRequest
 */
export function makeCompletionSource(
  getCompletions: (request: CompletionRequest) => readonly EditorCompletion[],
): CompletionSource {
  return context => {
    const { state, pos } = context;
    if (state.readOnly) return null;
    const from = context.matchBefore(PREFIX_REGEXP)?.from ?? pos;
    const prefix = state.sliceDoc(from, pos);
    if (!prefix && !context.explicit) return null;

    const line = state.doc.lineAt(from);
    const options = getCompletions({
      allText: state.doc.toString(),
      prefix,
      charBeforePrefix: state.sliceDoc(from - 1, from),
      textBeforePrefix: state.sliceDoc(0, Math.max(0, from - 1)),
      lineBeforePrefix: from > line.from ? state.sliceDoc(line.from, from - 1) : '',
    }).map(toCompletion);
    if (!options.length) return null;

    return {
      from,
      options,
      validFor: VALID_PREFIX_REGEXP,
    };
  };
}
