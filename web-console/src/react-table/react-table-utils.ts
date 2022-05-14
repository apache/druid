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

import { IconName } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { SqlExpression, SqlFunction, SqlLiteral, SqlRef } from 'druid-query-toolkit';
import { Filter } from 'react-table';

import { addOrUpdate, caseInsensitiveContains } from '../utils';

export const DEFAULT_TABLE_CLASS_NAME = '-striped -highlight padded-header';

export const STANDARD_TABLE_PAGE_SIZE = 50;
export const STANDARD_TABLE_PAGE_SIZE_OPTIONS = [50, 100, 200];

export const SMALL_TABLE_PAGE_SIZE = 25;
export const SMALL_TABLE_PAGE_SIZE_OPTIONS = [25, 50, 100];

export type FilterMode = '~' | '=' | '!=' | '<=' | '>=';

export const FILTER_MODES: FilterMode[] = ['~', '=', '!=', '<=', '>='];
export const FILTER_MODES_NO_COMPARISON: FilterMode[] = ['~', '=', '!='];

export function filterModeToIcon(mode: FilterMode): IconName {
  switch (mode) {
    case '~':
      return IconNames.SEARCH;
    case '=':
      return IconNames.EQUALS;
    case '!=':
      return IconNames.NOT_EQUAL_TO;
    case '<=':
      return IconNames.LESS_THAN_OR_EQUAL_TO;
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
    case '<=':
      return 'Less than or equal';
    case '>=':
      return 'Greater than or equal';
    default:
      return '?';
  }
}

interface FilterModeAndNeedle {
  mode: FilterMode;
  needle: string;
}

export function addFilter(
  filters: readonly Filter[],
  id: string,
  mode: FilterMode,
  needle: string,
): Filter[] {
  return addOrUpdateFilter(filters, { id, value: combineModeAndNeedle(mode, needle) });
}

export function parseFilterModeAndNeedle(
  filter: Filter,
  loose = false,
): FilterModeAndNeedle | undefined {
  const m = String(filter.value).match(/^(~|=|!=|<=|>=)?(.*)$/);
  if (!m) return;
  if (!loose && !m[2]) return;
  const mode = (m[1] as FilterMode) || '~';
  return {
    mode,
    needle: m[2] || '',
  };
}

export function combineModeAndNeedle(mode: FilterMode, needle: string): string {
  return `${mode}${needle}`;
}

export function addOrUpdateFilter(filters: readonly Filter[], filter: Filter): Filter[] {
  return addOrUpdate(filters, filter, f => f.id);
}

export function syncFilterClauseById(
  target: readonly Filter[],
  source: readonly Filter[],
  id: string,
): Filter[] {
  const clause = source.find(filter => filter.id === id);
  return clause ? addOrUpdateFilter(target, clause) : target.filter(filter => filter.id !== id);
}

export function booleanCustomTableFilter(filter: Filter, value: any): boolean {
  if (value == null) return false;
  const modeAndNeedle = parseFilterModeAndNeedle(filter);
  if (!modeAndNeedle) return true;
  const { mode, needle } = modeAndNeedle;
  switch (mode) {
    case '=':
      return String(value) === needle;

    case '!=':
      return String(value) !== needle;

    case '<=':
      return String(value) <= needle;

    case '>=':
      return String(value) >= needle;

    default:
      return caseInsensitiveContains(String(value), needle);
  }
}

export function sqlQueryCustomTableFilter(filter: Filter): SqlExpression | undefined {
  const modeAndNeedle = parseFilterModeAndNeedle(filter);
  if (!modeAndNeedle) return;
  const { mode, needle } = modeAndNeedle;
  const column = SqlRef.columnWithQuotes(filter.id);
  const needleLiteral = SqlLiteral.create(needle);
  switch (mode) {
    case '=':
      return column.equal(needleLiteral);

    case '!=':
      return column.unequal(needleLiteral);

    case '<=':
      return column.lessThanOrEqual(needleLiteral);

    case '>=':
      return column.greaterThanOrEqual(needleLiteral);

    default:
      return SqlFunction.simple('LOWER', [column]).like(
        SqlLiteral.create(`%${needle.toLowerCase()}%`),
      );
  }
}
