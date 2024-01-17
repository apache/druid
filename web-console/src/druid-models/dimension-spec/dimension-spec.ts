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

import type { Field } from '../../components';
import { filterMap, typeIsKnown } from '../../utils';
import type { SampleResponse } from '../../utils/sampler';
import { getHeaderNamesFromSampleResponse } from '../../utils/sampler';
import { guessColumnTypeFromSampleResponse } from '../ingestion-spec/ingestion-spec';

export interface DimensionsSpec {
  readonly dimensions?: (string | DimensionSpec)[];
  readonly dimensionExclusions?: string[];
  readonly spatialDimensions?: any[];
  readonly includeAllDimensions?: boolean;
  readonly useSchemaDiscovery?: boolean;
}

export interface DimensionSpec {
  readonly type: string;
  readonly name: string;
  readonly createBitmapIndex?: boolean;
  readonly multiValueHandling?: string;
  readonly castToType?: string;
}

const KNOWN_TYPES = ['auto', 'string', 'long', 'float', 'double', 'json'];
export const DIMENSION_SPEC_FIELDS: Field<DimensionSpec>[] = [
  {
    name: 'name',
    type: 'string',
    required: true,
    placeholder: 'dimension_name',
  },
  {
    name: 'type',
    type: 'string',
    required: true,
    suggestions: KNOWN_TYPES,
  },
  {
    name: 'createBitmapIndex',
    type: 'boolean',
    defined: typeIsKnown(KNOWN_TYPES, 'string'),
    defaultValue: true,
  },
  {
    name: 'multiValueHandling',
    type: 'string',
    defined: typeIsKnown(KNOWN_TYPES, 'string'),
    defaultValue: 'SORTED_ARRAY',
    suggestions: ['SORTED_ARRAY', 'SORTED_SET', 'ARRAY'],
  },
  {
    name: 'castToType',
    type: 'string',
    defined: typeIsKnown(KNOWN_TYPES, 'auto'),
    suggestions: [
      undefined,
      'STRING',
      'LONG',
      'DOUBLE',
      'ARRAY<STRING>',
      'ARRAY<LONG>',
      'ARRAY<DOUBLE>',
    ],
  },
];

export function getDimensionSpecName(dimensionSpec: string | DimensionSpec): string {
  return typeof dimensionSpec === 'string' ? dimensionSpec : dimensionSpec.name;
}

export function getDimensionSpecColumnType(dimensionSpec: string | DimensionSpec): string {
  if (typeof dimensionSpec === 'string') return 'string';
  if (dimensionSpec.type !== 'auto') return dimensionSpec.type;
  return dimensionSpec.castToType ?? 'auto';
}

export function getDimensionSpecUserType(dimensionSpec: string | DimensionSpec): string {
  if (typeof dimensionSpec === 'string') return 'string';
  if (dimensionSpec.type !== 'auto') return dimensionSpec.type;
  return dimensionSpec.castToType ?? 'auto';
}

export function getDimensionSpecClassType(
  dimensionSpec: string | DimensionSpec,
): string | undefined {
  if (typeof dimensionSpec === 'string') return 'string';
  if (dimensionSpec.type !== 'auto') return dimensionSpec.type;
  if (String(dimensionSpec.castToType).startsWith('ARRAY')) return 'array';
  return dimensionSpec.castToType?.toLowerCase();
}

export function inflateDimensionSpec(dimensionSpec: string | DimensionSpec): DimensionSpec {
  return typeof dimensionSpec === 'string'
    ? { name: dimensionSpec, type: 'string' }
    : dimensionSpec;
}

export function getDimensionSpecs(
  sampleResponse: SampleResponse,
  columnTypeHints: Record<string, string>,
  guessNumericStringsAsNumbers: boolean,
  hasRollup: boolean,
): (string | DimensionSpec)[] {
  return filterMap(getHeaderNamesFromSampleResponse(sampleResponse, 'ignore'), h => {
    const columnType =
      columnTypeHints[h] ||
      guessColumnTypeFromSampleResponse(sampleResponse, h, guessNumericStringsAsNumbers);
    if (columnType === 'string') return h;
    if (columnType.startsWith('ARRAY')) {
      return {
        type: 'auto',
        name: h,
        castToType: columnType.toUpperCase(),
      };
    }

    if (hasRollup) return;
    return {
      type: columnType === 'COMPLEX<json>' ? 'json' : columnType,
      name: h,
    };
  });
}
