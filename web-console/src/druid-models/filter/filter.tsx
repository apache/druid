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

import { ExternalLink, Field } from '../../components';
import { getLink } from '../../links';
import { deepGet, EMPTY_ARRAY, oneOf, typeIs } from '../../utils';
import { IngestionSpec } from '../ingestion-spec/ingestion-spec';

export interface DruidFilter {
  readonly type: string;
  readonly [k: string]: any;
}

export interface DimensionFiltersWithRest {
  dimensionFilters: DruidFilter[];
  restFilter?: DruidFilter;
}

export function splitFilter(filter: DruidFilter | null): DimensionFiltersWithRest {
  const inputAndFilters: DruidFilter[] = filter
    ? filter.type === 'and' && Array.isArray(filter.fields)
      ? filter.fields
      : filter.type !== 'true'
      ? [filter]
      : EMPTY_ARRAY
    : EMPTY_ARRAY;

  const dimensionFilters: DruidFilter[] = inputAndFilters.filter(
    f => typeof getFilterDimension(f) === 'string',
  );
  const restFilters: DruidFilter[] = inputAndFilters.filter(
    f => typeof getFilterDimension(f) !== 'string',
  );

  return {
    dimensionFilters,
    restFilter: restFilters.length
      ? restFilters.length > 1
        ? { type: 'and', filters: restFilters }
        : restFilters[0]
      : undefined,
  };
}

export function joinFilter(
  dimensionFiltersWithRest: DimensionFiltersWithRest,
): DruidFilter | undefined {
  const { dimensionFilters, restFilter } = dimensionFiltersWithRest;
  let newFields = dimensionFilters || EMPTY_ARRAY;
  if (restFilter && restFilter.type) newFields = newFields.concat([restFilter]);

  if (!newFields.length) return;
  if (newFields.length === 1) return newFields[0];
  return { type: 'and', fields: newFields };
}

export function getFilterDimension(filter: DruidFilter): string | undefined {
  if (typeof filter.dimension === 'string') return filter.dimension;
  if (filter.type === 'not' && filter.field) return getFilterDimension(filter.field);
  return;
}

export const KNOWN_FILTER_TYPES = ['selector', 'in', 'interval', 'regex', 'like', 'not'];

export const FILTER_FIELDS: Field<DruidFilter>[] = [
  {
    name: 'type',
    type: 'string',
    required: true,
    suggestions: KNOWN_FILTER_TYPES,
  },
  {
    name: 'dimension',
    type: 'string',
    defined: typeIs('selector', 'in', 'interval', 'regex', 'like'),
    required: true,
  },
  {
    name: 'value',
    type: 'string',
    defined: typeIs('selector'),
    required: true,
  },
  {
    name: 'values',
    type: 'string-array',
    defined: typeIs('in'),
    required: true,
  },
  {
    name: 'intervals',
    type: 'string-array',
    defined: typeIs('interval'),
    required: true,
    placeholder: 'ex: 2020-01-01/2020-06-01',
  },
  {
    name: 'pattern',
    type: 'string',
    defined: typeIs('regex', 'like'),
    required: true,
  },

  {
    name: 'field.type',
    label: 'Sub-filter type',
    type: 'string',
    suggestions: ['selector', 'in', 'interval', 'regex', 'like'],
    defined: typeIs('not'),
    required: true,
  },
  {
    name: 'field.dimension',
    label: 'Sub-filter dimension',
    type: 'string',
    defined: typeIs('not'),
  },
  {
    name: 'field.value',
    label: 'Sub-filter value',
    type: 'string',
    defined: df => df.type === 'not' && deepGet(df, 'field.type') === 'selector',
  },
  {
    name: 'field.values',
    label: 'Sub-filter values',
    type: 'string-array',
    defined: df => df.type === 'not' && deepGet(df, 'field.type') === 'in',
  },
  {
    name: 'field.intervals',
    label: 'Sub-filter intervals',
    type: 'string-array',
    defined: df => df.type === 'not' && deepGet(df, 'field.type') === 'interval',
    placeholder: 'ex: 2020-01-01/2020-06-01',
  },
  {
    name: 'field.pattern',
    label: 'Sub-filter pattern',
    type: 'string',
    defined: df => df.type === 'not' && oneOf(deepGet(df, 'field.type'), 'regex', 'like'),
  },
];

export const FILTERS_FIELDS: Field<IngestionSpec>[] = [
  {
    name: 'spec.dataSchema.transformSpec.filter',
    type: 'json',
    height: '350px',
    placeholder: '{ "type": "true" }',
    info: (
      <>
        <p>
          A Druid{' '}
          <ExternalLink href={`${getLink('DOCS')}/querying/filters.html`}>
            JSON filter expression
          </ExternalLink>{' '}
          to apply to the data.
        </p>
        <p>
          Note that only the value that match the filter will be included. If you want to remove
          some data values you must negate the filter.
        </p>
      </>
    ),
  },
];
