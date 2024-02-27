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

export interface NullModeDetection {
  useDefaultValueForNull?: boolean; // Modern expected value: false
  useStrictBooleans?: boolean; // Modern expected value: true
  useThreeValueLogicForNativeFilters?: boolean; // Modern expected value: true
}

const NULL_PROPERTY_DESCRIPTION: { prop: keyof NullModeDetection; newDefault: boolean }[] = [
  {
    prop: 'useDefaultValueForNull',
    newDefault: false,
  },
  {
    prop: 'useStrictBooleans',
    newDefault: true,
  },
  {
    prop: 'useThreeValueLogicForNativeFilters',
    newDefault: true,
  },
];

export const NULL_DETECTION_QUERY = {
  queryType: 'timeseries',
  dataSource: {
    type: 'inline',
    columnNames: ['n', 'v'],
    columnTypes: ['STRING', 'LONG'],
    rows: [
      [null, 2],
      ['A', 3],
    ],
  },
  intervals: '1000/2000',
  granularity: 'all',
  aggregations: [
    {
      type: 'filtered',
      name: 'sum_b', // There is no B so this will produce nothing but whether it gives null or 0 lets us determine the setting for useDefaultValueForNull
      aggregator: {
        type: 'longSum',
        name: '_',
        fieldName: 'v',
      },
      filter: {
        type: 'selector',
        dimension: 'n',
        value: 'B',
      },
    },
    {
      type: 'filtered',
      name: 'sum_not_b', // Whether `!= 'B'` matches the null row lets us determine the setting for useThreeValueLogicForNativeFilters
      aggregator: {
        type: 'longSum',
        name: '_',
        fieldName: 'v',
      },
      filter: {
        type: 'not',
        field: {
          type: 'selector',
          dimension: 'n',
          value: 'B',
        },
      },
    },
  ],
  postAggregations: [
    {
      type: 'expression',
      name: 'two_or_three', // useStrictBooleans will force this expression to 1 (true)
      expression: '2 || 3',
    },
  ],
};

export interface NullDetectionQueryResult {
  sum_b: number | null;
  sum_not_b: number | null;
  two_or_three: number | null;
}

export function nullDetectionQueryResultDecoder(
  result: NullDetectionQueryResult,
): NullModeDetection {
  let useThreeValueLogicForNativeFilters;
  if (result.sum_not_b === 3) useThreeValueLogicForNativeFilters = true;
  if (result.sum_not_b === 5) useThreeValueLogicForNativeFilters = false;

  let useStrictBooleans;
  if (result.two_or_three === 1) useStrictBooleans = true;
  if (result.two_or_three === 2) useStrictBooleans = false;

  let useDefaultValueForNull;
  if (result.sum_b === 0) useDefaultValueForNull = true;
  if (result.sum_b === null) useDefaultValueForNull = false;

  return {
    useThreeValueLogicForNativeFilters,
    useStrictBooleans,
    useDefaultValueForNull,
  };
}

export function summarizeNullModeDetection({
  useThreeValueLogicForNativeFilters,
  useStrictBooleans,
  useDefaultValueForNull,
}: NullModeDetection): string {
  if (
    typeof useThreeValueLogicForNativeFilters === 'undefined' ||
    typeof useStrictBooleans === 'undefined' ||
    typeof useDefaultValueForNull === 'undefined'
  ) {
    return 'Could not determine NULL mode';
  }

  if (useThreeValueLogicForNativeFilters && useStrictBooleans && !useDefaultValueForNull) {
    return 'SQL compliant NULL mode';
  }

  if (!useThreeValueLogicForNativeFilters && !useStrictBooleans && useDefaultValueForNull) {
    return 'Legacy NULL mode';
  }

  return 'Mixed NULL mode';
}

export function explainNullModeDetection(nullMode: NullModeDetection): string[] {
  return NULL_PROPERTY_DESCRIPTION.map(({ prop, newDefault }) => {
    const v = nullMode[prop];
    return `${prop}: ${typeof v === 'undefined' ? 'Not detected' : v}${
      v === newDefault ? ' (SQL compliant)' : ''
    }${v === !newDefault ? ' (Legacy mode)' : ''}`;
  });
}
