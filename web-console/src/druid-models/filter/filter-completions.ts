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

import type { JsonCompletionRule } from '../../utils';
import { typeIs } from '../../utils';

export const FILTER_COMPLETIONS: JsonCompletionRule[] = [
  // Root level filter
  {
    path: '$',
    isObject: true,
    completions: [{ value: 'type', documentation: 'Type of the filter' }],
  },
  {
    path: '$.type',
    completions: [
      { value: 'selector', documentation: 'Match a specific dimension with a specific value' },
      { value: 'equals', documentation: 'SQL-compatible equality filter for any column type' },
      { value: 'null', documentation: 'Match NULL values' },
      { value: 'columnComparison', documentation: 'Compare dimensions to each other' },
      { value: 'and', documentation: 'Logical AND of multiple filters' },
      { value: 'or', documentation: 'Logical OR of multiple filters' },
      { value: 'not', documentation: 'Logical NOT of a filter' },
      { value: 'in', documentation: 'Match input rows against a set of values' },
      { value: 'bound', documentation: 'Filter on ranges of dimension values' },
      { value: 'range', documentation: 'SQL-compatible range filter for any column type' },
      { value: 'like', documentation: 'SQL LIKE operator with % and _ wildcards' },
      { value: 'regex', documentation: 'Match against regular expressions' },
      { value: 'arrayContainsElement', documentation: 'Check if array contains specific element' },
      { value: 'interval', documentation: 'Filter on time intervals' },
      { value: 'true', documentation: 'Match all values' },
      { value: 'false', documentation: 'Match no values' },
      { value: 'search', documentation: 'Filter on partial string matches' },
      { value: 'expression', documentation: 'Filter using Druid expressions' },
      { value: 'javascript', documentation: 'Filter using JavaScript functions' },
      { value: 'extraction', documentation: 'Filter with extraction functions (deprecated)' },
    ],
  },

  // Selector filter
  {
    path: '$',
    isObject: true,
    condition: typeIs('selector'),
    completions: [
      { value: 'dimension', documentation: 'Input column or virtual column name to filter on' },
      { value: 'value', documentation: 'String value to match (null for NULL values)' },
      { value: 'extractionFn', documentation: 'Extraction function to apply to dimension' },
    ],
  },

  // Equals filter
  {
    path: '$',
    isObject: true,
    condition: typeIs('equals'),
    completions: [
      { value: 'column', documentation: 'Input column or virtual column name to filter on' },
      {
        value: 'matchValueType',
        documentation: 'Type of value to match (STRING, LONG, DOUBLE, etc.)',
      },
      { value: 'matchValue', documentation: 'Value to match (must not be null)' },
    ],
  },
  {
    path: '$.matchValueType',
    condition: typeIs('equals'),
    completions: [
      { value: 'STRING', documentation: 'String value type' },
      { value: 'LONG', documentation: '64-bit integer value type' },
      { value: 'DOUBLE', documentation: '64-bit floating point value type' },
      { value: 'FLOAT', documentation: '32-bit floating point value type' },
      { value: 'ARRAY<STRING>', documentation: 'Array of strings value type' },
      { value: 'ARRAY<LONG>', documentation: 'Array of longs value type' },
      { value: 'ARRAY<DOUBLE>', documentation: 'Array of doubles value type' },
      { value: 'ARRAY<FLOAT>', documentation: 'Array of floats value type' },
    ],
  },

  // Null filter
  {
    path: '$',
    isObject: true,
    condition: typeIs('null'),
    completions: [
      { value: 'column', documentation: 'Input column or virtual column name to filter on' },
    ],
  },

  // Column comparison filter
  {
    path: '$',
    isObject: true,
    condition: typeIs('columnComparison'),
    completions: [{ value: 'dimensions', documentation: 'List of DimensionSpec to compare' }],
  },

  // AND filter
  {
    path: '$',
    isObject: true,
    condition: typeIs('and'),
    completions: [
      { value: 'fields', documentation: 'List of filter objects to combine with AND logic' },
    ],
  },

  // OR filter
  {
    path: '$',
    isObject: true,
    condition: typeIs('or'),
    completions: [
      { value: 'fields', documentation: 'List of filter objects to combine with OR logic' },
    ],
  },

  // NOT filter
  {
    path: '$',
    isObject: true,
    condition: typeIs('not'),
    completions: [{ value: 'field', documentation: 'Filter object to negate' }],
  },

  // In filter
  {
    path: '$',
    isObject: true,
    condition: typeIs('in'),
    completions: [
      { value: 'dimension', documentation: 'Input column or virtual column name to filter on' },
      { value: 'values', documentation: 'List of string values to match' },
      { value: 'extractionFn', documentation: 'Extraction function to apply to dimension' },
    ],
  },

  // Bound filter
  {
    path: '$',
    isObject: true,
    condition: typeIs('bound'),
    completions: [
      { value: 'dimension', documentation: 'Input column or virtual column name to filter on' },
      { value: 'lower', documentation: 'Lower bound string match value' },
      { value: 'upper', documentation: 'Upper bound string match value' },
      {
        value: 'lowerStrict',
        documentation: 'Use strict comparison for lower bound (> instead of >=)',
      },
      {
        value: 'upperStrict',
        documentation: 'Use strict comparison for upper bound (< instead of <=)',
      },
      { value: 'ordering', documentation: 'Sorting order for comparisons' },
      { value: 'extractionFn', documentation: 'Extraction function to apply to dimension' },
    ],
  },
  {
    path: '$.ordering',
    condition: typeIs('bound'),
    completions: [
      { value: 'lexicographic', documentation: 'Default lexicographic ordering' },
      { value: 'alphanumeric', documentation: 'Alphanumeric ordering' },
      { value: 'numeric', documentation: 'Numeric ordering' },
      { value: 'strlen', documentation: 'String length ordering' },
      { value: 'version', documentation: 'Version string ordering' },
    ],
  },

  // Range filter
  {
    path: '$',
    isObject: true,
    condition: typeIs('range'),
    completions: [
      { value: 'column', documentation: 'Input column or virtual column name to filter on' },
      { value: 'matchValueType', documentation: 'Type of bounds to match' },
      { value: 'lower', documentation: 'Lower bound value to match' },
      { value: 'upper', documentation: 'Upper bound value to match' },
      { value: 'lowerOpen', documentation: 'Lower bound is open (> instead of >=)' },
      { value: 'upperOpen', documentation: 'Upper bound is open (< instead of <=)' },
    ],
  },
  {
    path: '$.matchValueType',
    condition: typeIs('range'),
    completions: [
      { value: 'STRING', documentation: 'String value type' },
      { value: 'LONG', documentation: '64-bit integer value type' },
      { value: 'DOUBLE', documentation: '64-bit floating point value type' },
      { value: 'FLOAT', documentation: '32-bit floating point value type' },
      { value: 'ARRAY<STRING>', documentation: 'Array of strings value type' },
      { value: 'ARRAY<LONG>', documentation: 'Array of longs value type' },
      { value: 'ARRAY<DOUBLE>', documentation: 'Array of doubles value type' },
      { value: 'ARRAY<FLOAT>', documentation: 'Array of floats value type' },
    ],
  },

  // Like filter
  {
    path: '$',
    isObject: true,
    condition: typeIs('like'),
    completions: [
      { value: 'dimension', documentation: 'Input column or virtual column name to filter on' },
      { value: 'pattern', documentation: 'LIKE pattern (% for any chars, _ for single char)' },
      { value: 'escape', documentation: 'Escape character for special characters' },
      { value: 'extractionFn', documentation: 'Extraction function to apply to dimension' },
    ],
  },

  // Regex filter
  {
    path: '$',
    isObject: true,
    condition: typeIs('regex'),
    completions: [
      { value: 'dimension', documentation: 'Input column or virtual column name to filter on' },
      { value: 'pattern', documentation: 'Java regular expression pattern' },
      { value: 'extractionFn', documentation: 'Extraction function to apply to dimension' },
    ],
  },

  // Array contains element filter
  {
    path: '$',
    isObject: true,
    condition: typeIs('arrayContainsElement'),
    completions: [
      { value: 'column', documentation: 'Input column or virtual column name to filter on' },
      { value: 'elementMatchValueType', documentation: 'Type of element value to match' },
      { value: 'elementMatchValue', documentation: 'Array element value to match (can be null)' },
    ],
  },
  {
    path: '$.elementMatchValueType',
    condition: typeIs('arrayContainsElement'),
    completions: [
      { value: 'STRING', documentation: 'String value type' },
      { value: 'LONG', documentation: '64-bit integer value type' },
      { value: 'DOUBLE', documentation: '64-bit floating point value type' },
      { value: 'FLOAT', documentation: '32-bit floating point value type' },
      { value: 'ARRAY<STRING>', documentation: 'Array of strings value type' },
      { value: 'ARRAY<LONG>', documentation: 'Array of longs value type' },
      { value: 'ARRAY<DOUBLE>', documentation: 'Array of doubles value type' },
      { value: 'ARRAY<FLOAT>', documentation: 'Array of floats value type' },
    ],
  },

  // Interval filter
  {
    path: '$',
    isObject: true,
    condition: typeIs('interval'),
    completions: [
      { value: 'dimension', documentation: 'Input column or virtual column name to filter on' },
      { value: 'intervals', documentation: 'Array of ISO-8601 interval strings' },
      { value: 'extractionFn', documentation: 'Extraction function to apply to dimension' },
    ],
  },

  // Search filter
  {
    path: '$',
    isObject: true,
    condition: typeIs('search'),
    completions: [
      { value: 'dimension', documentation: 'Input column or virtual column name to filter on' },
      { value: 'query', documentation: 'Search query spec object' },
      { value: 'extractionFn', documentation: 'Extraction function to apply to dimension' },
    ],
  },

  // Search query spec
  {
    path: '$.query',
    isObject: true,
    condition: typeIs('search'),
    completions: [{ value: 'type', documentation: 'Type of search query' }],
  },
  {
    path: '$.query.type',
    condition: typeIs('search'),
    completions: [
      { value: 'contains', documentation: 'Case-sensitive contains search' },
      { value: 'insensitive_contains', documentation: 'Case-insensitive contains search' },
      { value: 'fragment', documentation: 'Fragment search with multiple values' },
    ],
  },
  {
    path: '$.query',
    isObject: true,
    condition: obj => obj.type === 'search' && obj.query?.type === 'contains',
    completions: [
      { value: 'value', documentation: 'String value to search for' },
      { value: 'caseSensitive', documentation: 'Whether search is case-sensitive' },
    ],
  },
  {
    path: '$.query',
    isObject: true,
    condition: obj => obj.type === 'search' && obj.query?.type === 'insensitive_contains',
    completions: [
      { value: 'value', documentation: 'String value to search for (case insensitive)' },
    ],
  },
  {
    path: '$.query',
    isObject: true,
    condition: obj => obj.type === 'search' && obj.query?.type === 'fragment',
    completions: [
      { value: 'values', documentation: 'Array of string values to search for' },
      { value: 'caseSensitive', documentation: 'Whether search is case-sensitive' },
    ],
  },

  // Expression filter
  {
    path: '$',
    isObject: true,
    condition: typeIs('expression'),
    completions: [
      {
        value: 'expression',
        documentation: 'Druid expression string that evaluates to true/false',
      },
    ],
  },

  // JavaScript filter
  {
    path: '$',
    isObject: true,
    condition: typeIs('javascript'),
    completions: [
      { value: 'dimension', documentation: 'Input column or virtual column name to filter on' },
      { value: 'function', documentation: 'JavaScript function that returns true/false' },
      { value: 'extractionFn', documentation: 'Extraction function to apply to dimension' },
    ],
  },

  // Extraction filter (deprecated)
  {
    path: '$',
    isObject: true,
    condition: typeIs('extraction'),
    completions: [
      { value: 'dimension', documentation: 'Input column or virtual column name to filter on' },
      { value: 'value', documentation: 'String value to match (null for NULL values)' },
      { value: 'extractionFn', documentation: 'Extraction function to apply to dimension' },
    ],
  },
];
