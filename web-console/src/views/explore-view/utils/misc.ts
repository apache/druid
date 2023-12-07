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

import type { SqlExpression, SqlTable } from '@druid-toolkit/query';
import { SqlQuery } from '@druid-toolkit/query';
import type { ExpressionMeta } from '@druid-toolkit/visuals-core';
import type { ParameterDefinition } from '@druid-toolkit/visuals-core/src/models/parameter';

import { nonEmptyArray } from '../../../utils';

export interface Dataset {
  table: SqlTable;
  columns: ExpressionMeta[];
}

export function toggle<T>(xs: readonly T[], x: T, eq?: (a: T, b: T) => boolean): T[] {
  const e = eq || ((a, b) => a === b);
  return xs.find(_ => e(_, x)) ? xs.filter(d => !e(d, x)) : xs.concat([x]);
}

export function getInitQuery(table: SqlExpression, where: SqlExpression): SqlQuery {
  return SqlQuery.from(table).applyIf(String(where) !== 'TRUE', q =>
    q.changeWhereExpression(where),
  );
}

export function normalizeType(paramType: ParameterDefinition['type']): ParameterDefinition['type'] {
  switch (paramType) {
    case 'aggregates':
      return 'aggregate';
    case 'columns':
      return 'column';
    case 'splitCombines':
      return 'splitCombine';
    default:
      return paramType;
  }
}

export function adjustTransferValue(
  value: unknown,
  sourceType: ParameterDefinition['type'],
  targetType: ParameterDefinition['type'],
) {
  const comboType: `${ParameterDefinition['type']}->${ParameterDefinition['type']}` = `${sourceType}->${targetType}`;
  switch (comboType) {
    case 'aggregate->aggregates':
    case 'column->columns':
    case 'splitCombine->splitCombines':
      return [value];

    case 'aggregates->aggregate':
    case 'columns->column':
    case 'splitCombines->splitCombine':
      return nonEmptyArray(value) ? value[0] : undefined;

    default:
      return value;
  }
}
