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

import type { Ace } from 'ace-builds';

import { DRUID_QUERY_COMPLETIONS } from './druid-query-completions';
import { getHjsonContext } from './hjson-context';
import { type CompletionItem, getCompletionsForPath } from './json-completion-utils';
import { makeDocHtml } from './make-doc-html';

export interface GetHjsonCompletionsOptions {
  textBefore: string;
  charBeforePrefix: string;
  prefix: string;
}

/**
 * Get completions for Hjson Druid queries
 */
export function getHjsonCompletions({
  textBefore,
  charBeforePrefix,
  prefix,
}: GetHjsonCompletionsOptions): Ace.Completion[] {
  // Get the context of where we are in the JSON structure
  const hjsonContext = getHjsonContext(textBefore + charBeforePrefix + prefix);

  // Don't provide completions if we're in a comment
  if (hjsonContext.isEditingComment) {
    return [];
  }

  // Get completions based on the current path and object context
  let pathForCompletions = hjsonContext.path;

  // If we're editing a value, add the current key to the path
  if (!hjsonContext.isEditingKey && hjsonContext.currentKey) {
    pathForCompletions = [...hjsonContext.path, hjsonContext.currentKey];
  }

  const completionItems = getCompletionsForPath(
    DRUID_QUERY_COMPLETIONS,
    pathForCompletions,
    hjsonContext.isEditingKey,
    hjsonContext.currentObject,
  );

  // Filter completions based on whether we're editing a key or value
  const filteredCompletions = filterCompletionsByContext(
    completionItems,
    hjsonContext,
    charBeforePrefix,
  );

  // Convert to Ace completions format
  return filteredCompletions.map(item =>
    convertToAceCompletion(item, charBeforePrefix, hjsonContext.isEditingKey),
  );
}

/**
 * Filter completions based on the current editing context
 */
function filterCompletionsByContext(
  completions: CompletionItem[],
  hjsonContext: { isEditingKey: boolean; currentKey?: string; currentObject: any },
  charBeforePrefix: string,
): CompletionItem[] {
  const quote = charBeforePrefix === '"';

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
 * Convert a CompletionItem to an Ace Completion
 */
function convertToAceCompletion(
  item: CompletionItem,
  _charBeforePrefix: string,
  isEditingKey: boolean,
): Ace.Completion {
  // const quote = charBeforePrefix === '"';

  const completion: Ace.Completion = {
    name: item.value,
    value: item.value,
    score: 5,
    meta: isEditingKey ? 'property' : 'value',
  };

  // Add documentation if available (using caption for now)
  if (item.documentation) {
    (completion as any).docText = item.documentation;
    (completion as any).docHTML = makeDocHtml({
      name: item.value,
      description: item.documentation,
      syntax: 'xx',
    });
  }

  return completion;
}
