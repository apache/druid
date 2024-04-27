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

import React from 'react';

import type { Field } from '../../components';
import { ExternalLink } from '../../components';
import { deepGet, EMPTY_ARRAY, EMPTY_OBJECT } from '../../utils';
import type { IngestionSpec } from '../ingestion-spec/ingestion-spec';
import {
  BASIC_TIME_FORMATS,
  DATE_ONLY_TIME_FORMATS,
  DATETIME_TIME_FORMATS,
  OTHER_TIME_FORMATS,
} from '../time/time';
import type { Transform } from '../transform-spec/transform-spec';

export const NO_SUCH_COLUMN = '!!!_no_such_column_!!!';

export const TIME_COLUMN = '__time';

export const PLACEHOLDER_TIMESTAMP_SPEC: TimestampSpec = {
  column: NO_SUCH_COLUMN,
  missingValue: '1970-01-01T00:00:00Z',
};

export const DETECTION_TIMESTAMP_SPEC: TimestampSpec = {
  column: TIME_COLUMN,
  format: 'millis',
  missingValue: '1970-01-01T00:00:00Z',
};

export const REINDEX_TIMESTAMP_SPEC: TimestampSpec = {
  column: TIME_COLUMN,
  format: 'millis',
};

export const CONSTANT_TIMESTAMP_SPEC: TimestampSpec = {
  column: NO_SUCH_COLUMN,
  missingValue: '2010-01-01T00:00:00Z',
};

export type TimestampSchema = 'none' | 'column' | 'expression';

export function getTimestampSchema(spec: Partial<IngestionSpec>): TimestampSchema {
  const transforms: Transform[] =
    deepGet(spec, 'spec.dataSchema.transformSpec.transforms') || EMPTY_ARRAY;

  const timeTransform = transforms.find(transform => transform.name === TIME_COLUMN);
  if (timeTransform) return 'expression';

  const timestampSpec = deepGet(spec, 'spec.dataSchema.timestampSpec') || EMPTY_OBJECT;
  return timestampSpec.column === NO_SUCH_COLUMN ? 'none' : 'column';
}

export interface TimestampSpec {
  readonly column?: string;
  readonly format?: string;
  readonly missingValue?: string;
}

export function getTimestampSpecColumnFromSpec(spec: Partial<IngestionSpec>): string {
  // For the default https://github.com/apache/druid/blob/master/core/src/main/java/org/apache/druid/data/input/impl/TimestampSpec.java#L44
  return deepGet(spec, 'spec.dataSchema.timestampSpec.column') || 'timestamp';
}

export function getTimestampSpecConstantFromSpec(spec: Partial<IngestionSpec>): string | undefined {
  return deepGet(spec, 'spec.dataSchema.timestampSpec.missingValue');
}

export function getTimestampSpecExpressionFromSpec(
  spec: Partial<IngestionSpec>,
): string | undefined {
  const transforms: Transform[] =
    deepGet(spec, 'spec.dataSchema.transformSpec.transforms') || EMPTY_ARRAY;

  const timeTransform = transforms.find(transform => transform.name === TIME_COLUMN);
  if (!timeTransform) return;
  return timeTransform.expression;
}

export function getTimestampDetailFromSpec(spec: Partial<IngestionSpec>): string {
  const timestampSchema = getTimestampSchema(spec);
  switch (timestampSchema) {
    case 'none':
      return `Constant: ${getTimestampSpecConstantFromSpec(spec)}`;

    case 'column':
      return `Column: ${getTimestampSpecColumnFromSpec(spec)}`;

    case 'expression':
      return `Expression: ${getTimestampSpecExpressionFromSpec(spec)}`;
  }

  return '-';
}

export const TIMESTAMP_SPEC_FIELDS: Field<TimestampSpec>[] = [
  {
    name: 'column',
    type: 'string',
    defaultValue: 'timestamp',
  },
  {
    name: 'format',
    type: 'string',
    defaultValue: 'auto',
    suggestions: [
      ...BASIC_TIME_FORMATS,
      {
        group: 'Date and time formats',
        suggestions: DATETIME_TIME_FORMATS,
      },
      {
        group: 'Date only formats',
        suggestions: DATE_ONLY_TIME_FORMATS,
      },
      {
        group: 'Other time formats',
        suggestions: OTHER_TIME_FORMATS,
      },
    ],
    info: (
      <p>
        Specify your timestamp format by using the suggestions menu or typing in a{' '}
        <ExternalLink href="https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html">
          format string
        </ExternalLink>
        .
      </p>
    ),
  },
  {
    name: 'missingValue',
    type: 'string',
    placeholder: '(optional)',
    info: (
      <p>Specify a static value for cases when the source time column is missing or is null.</p>
    ),
    suggestions: ['2020-01-01T00:00:00Z'],
  },
];

export const CONSTANT_TIMESTAMP_SPEC_FIELDS: Field<TimestampSpec>[] = [
  {
    name: 'missingValue',
    label: 'Placeholder value',
    type: 'string',
    info: <p>The placeholder value that will be used as the timestamp.</p>,
  },
];

export function issueWithTimestampSpec(
  timestampSpec: TimestampSpec | undefined,
): string | undefined {
  if (!timestampSpec) return 'no spec';
  if (!timestampSpec.column && !timestampSpec.missingValue) return 'timestamp spec is blank';
  return;
}
