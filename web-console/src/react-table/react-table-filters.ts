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

import type { IconName } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { C, F, SqlExpression } from 'druid-query-toolkit';
import type { Filter } from 'react-table';

import { addOrUpdate, caseInsensitiveContains, filterMap } from '../utils';

export const DEFAULT_TABLE_CLASS_NAME = '-striped -highlight padded-header';

export const STANDARD_TABLE_PAGE_SIZE = 50;
export const STANDARD_TABLE_PAGE_SIZE_OPTIONS = [50, 100, 200];

export const SMALL_TABLE_PAGE_SIZE = 25;
export const SMALL_TABLE_PAGE_SIZE_OPTIONS = [25, 50, 100];

export type FilterMode = '~' | '=' | '!=' | '<' | '<=' | '>' | '>=';

export const FILTER_MODES: FilterMode[] = ['~', '=', '!=', '<', '<=', '>', '>='];
export const FILTER_MODES_NO_COMPARISON: FilterMode[] = ['~', '=', '!='];

export function filterModeToIcon(mode: FilterMode): IconName {
  switch (mode) {
    case '~':
      return IconNames.SEARCH;
    case '=':
      return IconNames.EQUALS;
    case '!=':
      return IconNames.NOT_EQUAL_TO;
    case '<':
      return IconNames.LESS_THAN;
    case '<=':
      return IconNames.LESS_THAN_OR_EQUAL_TO;
    case '>':
      return IconNames.GREATER_THAN;
    case '>=':
      return IconNames.GREATER_THAN_OR_EQUAL_TO;
    default:
      return IconNames.BLANK;
  }
}

export function filterModeToTitle(mode: FilterMode): string {
  switch (mode) {
    case '~':
      return 'Search';
    case '=':
      return 'Equals';
    case '!=':
      return 'Not equals';
    case '<':
      return 'Less than';
    case '<=':
      return 'Less than or equal';
    case '>':
      return 'Greater than';
    case '>=':
      return 'Greater than or equal';
    default:
      return '?';
  }
}

interface FilterModeAndNeedle {
  mode: FilterMode;
  needle: string;
  needleParts: string[];
}

export function parseFilterModeAndNeedle(
  filter: Filter,
  loose = false,
): FilterModeAndNeedle | undefined {
  const m = /^(~|=|!=|<(?!=)|<=|>(?!=)|>=)?(.*)$/.exec(String(filter.value));
  if (!m) return;
  if (!loose && !m[2]) return;
  const mode = (m[1] as FilterMode) || '~';
  const needle = m[2] || '';
  return {
    mode,
    needle,
    needleParts: needle.split('|'),
  };
}

export function combineModeAndNeedle(mode: FilterMode, needle: string, cleanup = false): string {
  if (cleanup && needle === '') return '';
  return `${mode}${needle}`;
}

export function addOrUpdateFilter(filters: readonly Filter[], filter: Filter): Filter[] {
  return addOrUpdate(filters, filter, f => f.id);
}

export function booleanCustomTableFilter(filter: Filter, value: unknown): boolean {
  if (value == null) return false;
  const modeAndNeedles = parseFilterModeAndNeedle(filter);
  if (!modeAndNeedles) return true;
  const { mode, needleParts } = modeAndNeedles;
  const strValue = String(value);
  switch (mode) {
    case '=':
      return needleParts.some(needle => strValue === needle);

    case '!=':
      return needleParts.every(needle => strValue !== needle);

    case '<':
      return needleParts.some(needle => strValue < needle);

    case '<=':
      return needleParts.some(needle => strValue <= needle);

    case '>':
      return needleParts.some(needle => strValue > needle);

    case '>=':
      return needleParts.some(needle => strValue >= needle);

    default:
      return needleParts.some(needle => caseInsensitiveContains(strValue, needle));
  }
}

export function sqlQueryCustomTableFilter(filter: Filter): SqlExpression | undefined {
  const modeAndNeedles = parseFilterModeAndNeedle(filter);
  if (!modeAndNeedles) return;
  const { mode, needleParts } = modeAndNeedles;
  const column = C(filter.id);
  switch (mode) {
    case '=': {
      return SqlExpression.or(...needleParts.map(needle => column.equal(needle)));
    }

    case '!=': {
      return SqlExpression.and(...needleParts.map(needle => column.unequal(needle)));
    }

    case '<':
      return SqlExpression.or(...needleParts.map(needle => column.lessThan(needle)));

    case '<=':
      return SqlExpression.or(...needleParts.map(needle => column.lessThanOrEqual(needle)));

    case '>':
      return SqlExpression.or(...needleParts.map(needle => column.greaterThan(needle)));

    case '>=':
      return SqlExpression.or(...needleParts.map(needle => column.greaterThanOrEqual(needle)));

    default:
      return SqlExpression.or(
        ...needleParts.map(needle => F('LOWER', column).like(`%${needle.toLowerCase()}%`)),
      );
  }
}

export function sqlQueryCustomTableFilters(filters: Filter[]): SqlExpression {
  return SqlExpression.and(...filterMap(filters, sqlQueryCustomTableFilter));
}

export function tableFiltersToString(tableFilters: Filter[]): string {
  return tableFilters
    .map(({ id, value }) => `${id}${value.replace(/[&%]/g, encodeURIComponent)}`)
    .join('&');
}

export function stringToTableFilters(str: string | undefined): Filter[] {
  if (!str) return [];
  // '~' | '=' | '!=' | '<' | '<=' | '>' | '>=';
  return filterMap(str.split('&'), clause => {
    const m = /^(\w+)((?:~|=|!=|<(?!=)|<=|>(?!=)|>=).*)$/.exec(
      clause.replace(/%2[56]/g, decodeURIComponent),
    );
    if (!m) return;
    return { id: m[1], value: m[2] };
  });
}
