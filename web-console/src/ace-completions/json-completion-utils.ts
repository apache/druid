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

export interface CompletionItem {
  value: string;
  label?: string;
  documentation?: string;
  insertText?: string;
  type?: 'property' | 'value' | 'template';
}

export interface CompletionRule {
  path: string | RegExp;
  completions: CompletionItem[];
  condition?: (currentObject: any) => boolean;
}

/**
 * Get completions for a given path and current object context
 */
export function getCompletionsForPath(
  rules: readonly CompletionRule[],
  path: string[],
  currentObject: any,
): CompletionItem[] {
  const pathStr = pathToString(path);
  const completions: CompletionItem[] = [];

  for (const rule of rules) {
    // Check if path matches
    let pathMatches = false;
    if (typeof rule.path === 'string') {
      pathMatches = rule.path === pathStr;
    } else {
      pathMatches = rule.path.test(pathStr);
    }

    if (!pathMatches) continue;

    // Check condition if present
    if (rule.condition && !rule.condition(currentObject)) continue;

    // Add completions
    completions.push(...rule.completions);
  }

  return completions;
}

/**
 * Convert a path array to a string representation
 * e.g., ['aggregations', '0', 'type'] -> '$.aggregations[].type'
 * e.g., ['filter', 'fields', '1', 'type'] -> '$.filter.fields[].type'
 */
function pathToString(path: string[]): string {
  if (path.length === 0) return '$';

  let result = '$';
  for (let i = 0; i < path.length; i++) {
    const segment = path[i];

    if (/^\d+$/.test(segment)) {
      // This is an array index, replace with []
      result += '[]';
    } else {
      result += `.${segment}`;
    }
  }
  return result;
}
