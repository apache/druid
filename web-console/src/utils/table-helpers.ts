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

import { QueryResult } from 'druid-query-toolkit';

import { filterMap, formatNumber, oneOf } from './general';
import { deepSet } from './object-change';

export interface Pagination {
  page: number;
  pageSize: number;
}

export function changePage(pagination: Pagination, page: number): Pagination {
  return deepSet(pagination, 'page', page);
}

export function getNumericColumnBraces(
  queryResult: QueryResult,
  pagination?: Pagination,
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
      const brace = filterMap(rows, row =>
        oneOf(typeof row[i], 'number', 'bigint') ? formatNumber(row[i]) : undefined,
      );
      if (rows.length === brace.length) {
        numericColumnBraces[i] = brace;
      }
    });
  }

  return numericColumnBraces;
}
