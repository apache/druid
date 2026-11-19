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

import { CompletionContext } from '@codemirror/autocomplete';
import type { Extension } from '@codemirror/state';
import { EditorState } from '@codemirror/state';

import type { CompletionWord } from '../components/code-editor/completion-source';
import { matchCompletionWord } from '../components/code-editor/completion-source';

/**
 * The completion context and word that the editor would have with the cursor at the | in the text, as if the completion
 * was asked for explicitly (Ctrl-Space)
 */
export function completionContextAt(
  extension: Extension,
  textWithCursor: string,
): [CompletionContext, CompletionWord] {
  const pos = textWithCursor.indexOf('|');
  if (pos < 0) throw new Error(`no | in ${textWithCursor}`);
  const state = EditorState.create({
    doc: textWithCursor.slice(0, pos) + textWithCursor.slice(pos + 1),
    extensions: extension,
  });
  const context = new CompletionContext(state, pos, true);
  return [context, matchCompletionWord(context)!];
}
