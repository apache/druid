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

// This is a web console internal made up column type that represents a multi value dimension
const MADE_UP_MV_COLUMN_TYPE = 'mv-string';
function makeMadeUpMvDimensionSpec(name: string): DimensionSpec {
  return {
    type: 'string',
    name,
    multiValueHandling: 'SORTED_ARRAY',
  };
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
    label: 'Multi-value handling',
    type: 'string',
    defined: typeIsKnown(KNOWN_TYPES, 'string'),
    placeholder: 'unset (defaults to SORTED_ARRAY)',
    suggestions: [undefined, 'SORTED_ARRAY', 'SORTED_SET', 'ARRAY'],
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
  switch (dimensionSpec.type) {
    case 'string':
      return typeof dimensionSpec.multiValueHandling === 'string'
        ? MADE_UP_MV_COLUMN_TYPE
        : 'string';

    case 'auto':
      return dimensionSpec.castToType ?? 'auto';

    default:
      return dimensionSpec.type;
  }
}

export function getDimensionSpecUserType(
  dimensionSpec: string | DimensionSpec,
  identifyMv?: boolean,
): string {
  if (typeof dimensionSpec === 'string') return 'string';
  switch (dimensionSpec.type) {
    case 'string':
      return identifyMv && typeof dimensionSpec.multiValueHandling === 'string'
        ? 'string (multi-value)'
        : 'string';

    case 'auto':
      return dimensionSpec.castToType ?? 'auto';

    default:
      return dimensionSpec.type;
  }
}

export function getDimensionSpecClassType(
  dimensionSpec: string | DimensionSpec,
  identifyMv?: boolean,
): string | undefined {
  if (typeof dimensionSpec === 'string') return 'string';
  switch (dimensionSpec.type) {
    case 'string':
      return identifyMv && typeof dimensionSpec.multiValueHandling === 'string'
        ? MADE_UP_MV_COLUMN_TYPE
        : 'string';

    case 'auto':
      if (String(dimensionSpec.castToType).startsWith('ARRAY')) return 'array';
      return dimensionSpec.castToType?.toLowerCase();

    default:
      return dimensionSpec.type;
  }
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
  forceMvdInsteadOfArray: boolean,
  hasRollup: boolean,
): (string | DimensionSpec)[] {
  return filterMap(getHeaderNamesFromSampleResponse(sampleResponse, 'ignore'), h => {
    const columnTypeHint = columnTypeHints[h];
    const guessedColumnType = guessColumnTypeFromSampleResponse(
      sampleResponse,
      h,
      guessNumericStringsAsNumbers,
    );
    let columnType = columnTypeHint || guessedColumnType;

    if (forceMvdInsteadOfArray) {
      if (columnType.startsWith('ARRAY')) {
        columnType = MADE_UP_MV_COLUMN_TYPE;
      }

      if (columnType === MADE_UP_MV_COLUMN_TYPE) {
        return makeMadeUpMvDimensionSpec(h);
      }
    } else {
      // Ignore the type hint if it is MVD and we don't want to force people into them
      if (columnTypeHint === MADE_UP_MV_COLUMN_TYPE) {
        columnType = guessedColumnType;
      }
    }

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
