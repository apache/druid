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

import { ascending, descending, sort } from 'd3-array';
import type { QueryResult, SqlExpression } from 'druid-query-toolkit';
import { C } from 'druid-query-toolkit';
import type { Filter, SortingRule } from 'react-table';

import { filterMap, formatNumber, isNumberLike, oneOf } from './general';
import { deepSet } from './object-change';

export interface Pagination {
  page: number;
  pageSize: number;
}

export function changePage(pagination: Pagination, page: number): Pagination {
  return deepSet(pagination, 'page', page);
}

export interface ColumnHint {
  displayName?: string;
  group?: string;
  hidden?: boolean;
  expressionForWhere?: SqlExpression;
  formatter?: (x: any) => string;
}

export function getNumericColumnBraces(
  queryResult: QueryResult,
  columnHints: Map<string, ColumnHint> | undefined,
  pagination: Pagination | undefined,
): Record<number, string[]> {
  let rows = queryResult.rows;

  if (pagination) {
    const index = pagination.page * pagination.pageSize;
    rows = rows.slice(index, index + pagination.pageSize);
  }

  const numericColumnBraces: Record<number, string[]> = {};
  if (rows.length) {
    queryResult.header.forEach((column, i) => {
      if (!oneOf(column.nativeType, 'LONG', 'FLOAT', 'DOUBLE')) return;
      const formatter = columnHints?.get(column.name)?.formatter || formatNumber;
      const braces = filterMap(rows, row => (isNumberLike(row[i]) ? formatter(row[i]) : undefined));
      if (braces.length) {
        numericColumnBraces[i] = braces;
      }
    });
  }

  return numericColumnBraces;
}

export interface TableState {
  page: number;
  pageSize: number;
  filtered: Filter[];
  sorted: SortingRule[];
}

export function sortedToOrderByClause(sorted: SortingRule[]): string | undefined {
  if (!sorted.length) return;
  return 'ORDER BY ' + sorted.map(sort => `${C(sort.id)} ${sort.desc ? 'DESC' : 'ASC'}`).join(', ');
}

export function applySorting(xs: any[], sorted: SortingRule[]): any[] {
  const firstSortingRule = sorted[0];
  if (!firstSortingRule) return xs;
  const { id, desc } = firstSortingRule;
  return sort(
    xs,
    desc ? (d1, d2) => descending(d1[id], d2[id]) : (d1, d2) => ascending(d1[id], d2[id]),
  );
}
