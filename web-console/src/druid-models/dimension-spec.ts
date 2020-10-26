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

import { Field } from '../components';
import { filterMap } from '../utils';
import { HeaderAndRows } from '../utils/sampler';

import { getColumnTypeFromHeaderAndRows } from './ingestion-spec';

export interface DimensionsSpec {
  dimensions?: (string | DimensionSpec)[];
  dimensionExclusions?: string[];
  spatialDimensions?: any[];
}

export interface DimensionSpec {
  type: string;
  name: string;
  createBitmapIndex?: boolean;
}

export const DIMENSION_SPEC_FIELDS: Field<DimensionSpec>[] = [
  {
    name: 'name',
    type: 'string',
  },
  {
    name: 'type',
    type: 'string',
    suggestions: ['string', 'long', 'float', 'double'],
  },
  {
    name: 'createBitmapIndex',
    type: 'boolean',
    defaultValue: true,
    defined: (dimensionSpec: DimensionSpec) => dimensionSpec.type === 'string',
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
  headerAndRows: HeaderAndRows,
  typeHints: Record<string, string>,
  hasRollup: boolean,
): (string | DimensionSpec)[] {
  return filterMap(headerAndRows.header, h => {
    if (h === '__time') return;
    const type = typeHints[h] || getColumnTypeFromHeaderAndRows(headerAndRows, h);
    if (type === 'string') return h;
    if (hasRollup) return;
    return {
      type,
      name: h,
    };
  });
}
