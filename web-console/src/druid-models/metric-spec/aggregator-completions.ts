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

export const AGGREGATOR_COMPLETIONS: JsonCompletionRule[] = [
  // Root level properties
  {
    path: '$',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of aggregator' },
      { value: 'name', documentation: 'Output name of the aggregator' },
    ],
  },
  {
    path: '$.type',
    completions: [
      { value: 'count', documentation: 'Count the number of Druid rows' },
      { value: 'longSum', documentation: 'Sum of long values' },
      { value: 'doubleSum', documentation: 'Sum of double values' },
      { value: 'floatSum', documentation: 'Sum of float values' },
      { value: 'longMin', documentation: 'Minimum of long values' },
      { value: 'longMax', documentation: 'Maximum of long values' },
      { value: 'doubleMin', documentation: 'Minimum of double values' },
      { value: 'doubleMax', documentation: 'Maximum of double values' },
      { value: 'floatMin', documentation: 'Minimum of float values' },
      { value: 'floatMax', documentation: 'Maximum of float values' },
      { value: 'longFirst', documentation: 'First long value ordered by time' },
      { value: 'longLast', documentation: 'Last long value ordered by time' },
      { value: 'doubleFirst', documentation: 'First double value ordered by time' },
      { value: 'doubleLast', documentation: 'Last double value ordered by time' },
      { value: 'floatFirst', documentation: 'First float value ordered by time' },
      { value: 'floatLast', documentation: 'Last float value ordered by time' },
      { value: 'stringFirst', documentation: 'First string value ordered by time' },
      { value: 'stringLast', documentation: 'Last string value ordered by time' },
      { value: 'thetaSketch', documentation: 'Theta sketch for approximate distinct counting' },
      { value: 'HLLSketchBuild', documentation: 'HLL sketch builder for approximate distinct counting' },
      { value: 'HLLSketchMerge', documentation: 'HLL sketch merger for approximate distinct counting' },
      { value: 'quantilesDoublesSketch', documentation: 'Quantiles sketch for approximate quantile computation' },
      { value: 'hyperUnique', documentation: 'HyperLogLog for approximate distinct counting (legacy)' },
      { value: 'filtered', documentation: 'Apply a filter then aggregate' },
    ],
  },

  // Common properties for aggregators that use fieldName
  {
    path: '$',
    isObject: true,
    condition: typeIs(
      'longSum', 'doubleSum', 'floatSum',
      'longMin', 'longMax', 'doubleMin', 'doubleMax', 'floatMin', 'floatMax',
      'longFirst', 'longLast', 'doubleFirst', 'doubleLast', 'floatFirst', 'floatLast',
      'stringFirst', 'stringLast', 'thetaSketch', 'HLLSketchBuild', 'HLLSketchMerge',
      'quantilesDoublesSketch', 'hyperUnique'
    ),
    completions: [
      { value: 'fieldName', documentation: 'Name of the input column to aggregate' },
    ],
  },

  // String aggregator specific properties
  {
    path: '$',
    isObject: true,
    condition: typeIs('stringFirst', 'stringLast'),
    completions: [
      { value: 'maxStringBytes', documentation: 'Maximum size of string values to accumulate' },
      { value: 'filterNullValues', documentation: 'Filter out null values' },
    ],
  },

  // First/Last aggregator time column
  {
    path: '$',
    isObject: true,
    condition: typeIs(
      'longFirst', 'longLast', 'doubleFirst', 'doubleLast',
      'floatFirst', 'floatLast', 'stringFirst', 'stringLast'
    ),
    completions: [
      { value: 'timeColumn', documentation: 'Name of time column (defaults to __time)' },
    ],
  },

  // Sketch aggregator properties
  {
    path: '$',
    isObject: true,
    condition: typeIs('thetaSketch'),
    completions: [
      { value: 'size', documentation: 'Size parameter for theta sketch' },
      { value: 'isInputThetaSketch', documentation: 'Input is already a theta sketch' },
    ],
  },
  {
    path: '$',
    isObject: true,
    condition: typeIs('HLLSketchBuild', 'HLLSketchMerge'),
    completions: [
      { value: 'lgK', documentation: 'log2 of K, the number of buckets in the sketch' },
      { value: 'tgtHllType', documentation: 'Target HLL type' },
    ],
  },
  {
    path: '$.tgtHllType',
    condition: typeIs('HLLSketchBuild', 'HLLSketchMerge'),
    completions: [
      { value: 'HLL_4', documentation: 'HLL_4 type (most compact)' },
      { value: 'HLL_6', documentation: 'HLL_6 type (intermediate)' },
      { value: 'HLL_8', documentation: 'HLL_8 type (fastest)' },
    ],
  },
  {
    path: '$',
    isObject: true,
    condition: typeIs('quantilesDoublesSketch'),
    completions: [
      { value: 'k', documentation: 'Size parameter for quantiles sketch' },
    ],
  },
  {
    path: '$',
    isObject: true,
    condition: typeIs('hyperUnique'),
    completions: [
      { value: 'isInputHyperUnique', documentation: 'Input is already a hyperUnique' },
    ],
  },

  // Filtered aggregator
  {
    path: '$',
    isObject: true,
    condition: typeIs('filtered'),
    completions: [
      { value: 'filter', documentation: 'Filter to apply before aggregation' },
      { value: 'aggregator', documentation: 'Aggregator to apply after filtering' },
    ],
  },

  // Nested aggregator for filtered type
  {
    path: '$.aggregator',
    isObject: true,
    completions: [
      { value: 'type', documentation: 'Type of nested aggregator' },
      { value: 'name', documentation: 'Output name of the nested aggregator' },
      { value: 'fieldName', documentation: 'Name of the input column for nested aggregator' },
    ],
  },
  {
    path: '$.aggregator.type',
    completions: [
      { value: 'count', documentation: 'Count the number of Druid rows' },
      { value: 'longSum', documentation: 'Sum of long values' },
      { value: 'doubleSum', documentation: 'Sum of double values' },
      { value: 'floatSum', documentation: 'Sum of float values' },
      { value: 'longMin', documentation: 'Minimum of long values' },
      { value: 'longMax', documentation: 'Maximum of long values' },
      { value: 'doubleMin', documentation: 'Minimum of double values' },
      { value: 'doubleMax', documentation: 'Maximum of double values' },
      { value: 'floatMin', documentation: 'Minimum of float values' },
      { value: 'floatMax', documentation: 'Maximum of float values' },
      { value: 'longFirst', documentation: 'First long value ordered by time' },
      { value: 'longLast', documentation: 'Last long value ordered by time' },
      { value: 'doubleFirst', documentation: 'First double value ordered by time' },
      { value: 'doubleLast', documentation: 'Last double value ordered by time' },
      { value: 'floatFirst', documentation: 'First float value ordered by time' },
      { value: 'floatLast', documentation: 'Last float value ordered by time' },
      { value: 'stringFirst', documentation: 'First string value ordered by time' },
      { value: 'stringLast', documentation: 'Last string value ordered by time' },
    ],
  },
];