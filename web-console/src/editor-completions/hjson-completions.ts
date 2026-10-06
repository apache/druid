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

import type { CompletionContext } from '@codemirror/autocomplete';

import type { CompletionWord } from '../components/code-editor/completion-source';
import { tokenBefore } from '../components/code-editor/completion-source';
import type { JsonCompletionItem, JsonCompletionRule } from '../utils';
import { getCompletionsForPath, getHjsonContext } from '../utils';

import type { EditorCompletion } from './editor-completion';

/**
 * The completions for the word being typed in Hjson. Needs the hjson language for the syntax tree.
 */
export function getHjsonCompletions(
  { state, pos }: CompletionContext,
  { from }: CompletionWord,
  jsonCompletions: JsonCompletionRule[],
): EditorCompletion[] {
  // Don't provide completions if we're in a comment
  if (tokenBefore(state, pos).name === 'comment') {
    return [];
  }

  // Get the context of where we are in the JSON structure
  const hjsonContext = getHjsonContext(state.sliceDoc(0, pos));
  const quote = state.sliceDoc(from - 1, from) === '"';

  // Get completions based on the current path and object context
  let pathForCompletions = hjsonContext.path;

  // If we're editing a value, add the current key to the path
  if (!hjsonContext.isEditingKey && hjsonContext.currentKey) {
    pathForCompletions = [...hjsonContext.path, hjsonContext.currentKey];
  }

  const completionItems = getCompletionsForPath(
    jsonCompletions,
    pathForCompletions,
    hjsonContext.isEditingKey,
    hjsonContext.currentObject,
  );

  // Filter completions based on whether we're editing a key or value
  const filteredCompletions = filterCompletionsByContext(completionItems, hjsonContext, quote);

  return filteredCompletions.map(item =>
    convertToEditorCompletion(item, hjsonContext.isEditingKey),
  );
}

/**
 * Filter completions based on the current editing context
 */
function filterCompletionsByContext(
  completions: JsonCompletionItem[],
  hjsonContext: { isEditingKey: boolean; currentKey?: string; currentObject: any },
  quote: boolean,
): JsonCompletionItem[] {
  if (hjsonContext.isEditingKey) {
    // We're editing a key - only show property completions
    // Filter out properties that already exist in the current object
    return completions.filter(completion => {
      return !(completion.value in hjsonContext.currentObject);
    });
  } else {
    // We're editing a value - show value completions
    const valueCompletions = completions;

    // If we're inside quotes, only show string-like values
    if (quote) {
      return valueCompletions.filter(
        c => !['true', 'false', 'null'].includes(c.value.toLowerCase()),
      );
    }

    return valueCompletions;
  }
}

/**
 * Convert a CompletionItem to an EditorCompletion
 */
function convertToEditorCompletion(
  item: JsonCompletionItem,
  isEditingKey: boolean,
): EditorCompletion {
  const completion: EditorCompletion = {
    label: item.value,
    boost: 5,
    detail: isEditingKey ? 'property' : 'value',
  };

  // Add documentation if available
  if (item.documentation) {
    completion.doc = {
      name: item.value,
      description: item.documentation,
    };
  }

  return completion;
}
