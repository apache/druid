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

import type { Timezone } from 'chronoshift';
import { Duration } from 'chronoshift';
import { isDate } from 'date-fns';
import type { Column, FilterPattern, SqlExpression } from 'druid-query-toolkit';
import {
  filterPatternsToExpression,
  filterPatternToExpression,
  fitFilterPattern,
  fitFilterPatterns,
  SqlComparison,
  SqlMulti,
  SqlQuery,
} from 'druid-query-toolkit';

import { formatIsoDateRange, prettyFormatIsoDateWithMsIfNeeded } from '../../../utils';

const TIME_RELATIVE_TYPES: Record<string, string> = {
  'maxDataTime/': 'latest',
  'timestamp/ceil': 'current',
  'timestamp/floor': 'previous',
};

export function initPatternForColumn(column: Column): FilterPattern {
  switch (column.sqlType) {
    case 'TIMESTAMP':
      return {
        type: 'timeRelative',
        negated: false,
        column: column.name,
        anchor: 'timestamp',
        rangeDuration: 'P1D',
        startBound: '[',
        endBound: ')',
      };

    case 'BIGINT':
    case 'FLOAT':
    case 'DOUBLE':
      return {
        type: 'numberRange',
        negated: false,
        column: column.name,
        start: 0,
        startBound: '[',
        end: 100,
        endBound: ')',
      };

    default:
      return {
        type: 'values',
        negated: false,
        column: column.name,
        values: [],
      };
  }
}

export function formatPatternWithoutNegation(pattern: FilterPattern, timezone: Timezone): string {
  switch (pattern.type) {
    case 'values':
      return `${pattern.column}: ${pattern.values
        .map(v => {
          if (v === '') return 'empty';
          if (isDate(v)) return prettyFormatIsoDateWithMsIfNeeded(v as Date);
          return String(v);
        })
        .join(', ')}`;

    case 'contains':
      return `${pattern.column} ~ '${pattern.contains}'`;

    case 'regexp':
      return `${pattern.column} ~ /${pattern.regexp}/`;

    case 'timeInterval': {
      return formatIsoDateRange(pattern.start, pattern.end, timezone);
    }

    case 'timeRelative': {
      const type = TIME_RELATIVE_TYPES[`${pattern.anchor}/${pattern.alignType || ''}`];
      return `${pattern.column} in ${type ? `${type} ` : ''}${new Duration(
        pattern.rangeDuration,
      ).getDescription()}`;
    }

    case 'numberRange':
      return `${pattern.column} in ${pattern.startBound}${pattern.start}, ${pattern.end}${pattern.endBound}`;

    case 'mvContains':
      return `${pattern.column} on of ${pattern.values
        .map(v => (v === '' ? 'empty' : String(v)))
        .join(', ')}`;

    case 'custom':
      return String(pattern.expression);
  }
}

export function patternToBoundsQuery(
  source: SqlQuery,
  filterPattern: FilterPattern,
): SqlQuery | undefined {
  if (filterPattern.type !== 'timeRelative') return;
  const ex = filterPatternToExpression(filterPattern);
  if (!(ex instanceof SqlMulti)) return;
  if (ex.numArgs() !== 2) return;
  const [startEx, endEx] = ex.getArgArray();
  if (!(startEx instanceof SqlComparison)) return;
  if (!(endEx instanceof SqlComparison)) return;
  return SqlQuery.from(source)
    .changeSelectExpressions([startEx.lhs.as('start'), (endEx.rhs as SqlExpression).as('end')])
    .changeLimitValue(1); // Todo: make this better
}

export function addOrUpdatePattern(
  patterns: readonly FilterPattern[],
  oldPattern: FilterPattern | undefined,
  newPattern: FilterPattern,
): FilterPattern[] {
  let added = false;
  const newPatterns = patterns.map(pattern => {
    if (pattern === oldPattern) {
      added = true;
      return newPattern;
    } else {
      return pattern;
    }
  });
  if (!added) {
    newPatterns.push(newPattern);
  }
  return newPatterns;
}

export function updateFilterPattern(
  patterns: readonly FilterPattern[],
  newPattern: FilterPattern,
): FilterPattern[] {
  let found = false;
  const newPatterns = patterns.map(pattern => {
    if (!('column' in pattern) || !('column' in newPattern)) return pattern;
    if (pattern.column === newPattern.column) {
      found = true;
      return newPattern;
    } else {
      return pattern;
    }
  });

  if (found) {
    return newPatterns;
  } else {
    return [...newPatterns, newPattern];
  }
}

export function updateFilterClause(filter: SqlExpression, clause: SqlExpression) {
  return filterPatternsToExpression(
    updateFilterPattern(fitFilterPatterns(filter), fitFilterPattern(clause)),
  );
}
