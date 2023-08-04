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

import { C, F, SqlFunction } from '@druid-toolkit/query';
import type { ExpressionMeta } from '@druid-toolkit/visuals-core';

export function getPossibleAggregateForColumn(column: ExpressionMeta): ExpressionMeta[] {
  switch (column.sqlType) {
    case 'TIMESTAMP':
      return [
        {
          name: `Max ${column.name}`,
          expression: F.max(C(column.name)),
          sqlType: column.sqlType,
        },
        {
          name: `Min ${column.name}`,
          expression: F.min(C(column.name)),
          sqlType: column.sqlType,
        },
      ];

    case 'BIGINT':
    case 'FLOAT':
    case 'DOUBLE':
      return [
        {
          name: `Sum ${column.name}`,
          expression: F.sum(C(column.name)),
          sqlType: column.sqlType,
        },
        {
          name: `Max ${column.name}`,
          expression: F.max(C(column.name)),
          sqlType: column.sqlType,
        },
        {
          name: `Min ${column.name}`,
          expression: F.min(C(column.name)),
          sqlType: column.sqlType,
        },
        {
          name: `Unique ${column.name}`,
          expression: SqlFunction.countDistinct(C(column.name)),
          sqlType: 'BIGINT',
        },
        {
          name: `P98 ${column.name}`,
          expression: F('APPROX_QUANTILE_DS', C(column.name), 0.98),
          sqlType: 'DOUBLE',
        },
      ];

    case 'VARCHAR':
    case 'COMPLEX':
    case 'COMPLEX<hyperUnique>':
      return [
        {
          name: `Unique ${column.name}`,
          expression: SqlFunction.countDistinct(C(column.name)),
          sqlType: 'BIGINT',
        },
      ];

    case 'COMPLEX<HLLSketch>':
      return [
        {
          name: `Unique ${column.name}`,
          expression: F('APPROX_COUNT_DISTINCT_DS_HLL', C(column.name)),
          sqlType: 'BIGINT',
        },
      ];

    case 'COMPLEX<quantilesDoublesSketch>':
      return [
        {
          name: `Median ${column.name}`,
          expression: F('APPROX_QUANTILE_DS', C(column.name), 0.5),
          sqlType: 'DOUBLE',
        },
        {
          name: `P95 ${column.name}`,
          expression: F('APPROX_QUANTILE_DS', C(column.name), 0.95),
          sqlType: 'DOUBLE',
        },
        {
          name: `P98 ${column.name}`,
          expression: F('APPROX_QUANTILE_DS', C(column.name), 0.98),
          sqlType: 'DOUBLE',
        },
      ];

    default:
      return [];
  }
}
