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
}

const KNOWN_TYPES = ['string', 'long', 'float', 'double', 'json'];
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
];

export function getDimensionSpecName(dimensionSpec: string | DimensionSpec): string {
  return typeof dimensionSpec === 'string' ? dimensionSpec : dimensionSpec.name;
}

export function getDimensionSpecType(dimensionSpec: string | DimensionSpec): string {
  return typeof dimensionSpec === 'string' ? 'string' : dimensionSpec.type;
}

export function inflateDimensionSpec(dimensionSpec: string | DimensionSpec): DimensionSpec {
  return typeof dimensionSpec === 'string'
    ? { name: dimensionSpec, type: 'string' }
    : dimensionSpec;
}

export function getDimensionSpecs(
  sampleResponse: SampleResponse,
  typeHints: Record<string, string>,
  guessNumericStringsAsNumbers: boolean,
  hasRollup: boolean,
): (string | DimensionSpec)[] {
  return filterMap(getHeaderNamesFromSampleResponse(sampleResponse, 'ignore'), h => {
    const dimensionType =
      typeHints[h] ||
      guessColumnTypeFromSampleResponse(sampleResponse, h, guessNumericStringsAsNumbers);
    if (dimensionType === 'string') return h;
    if (hasRollup) return;
    return {
      type: dimensionType === 'COMPLEX<json>' ? 'json' : dimensionType,
      name: h,
    };
  });
}
