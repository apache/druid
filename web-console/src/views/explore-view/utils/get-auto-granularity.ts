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

import type { SqlExpression } from '@druid-toolkit/query';
import { fitFilterPattern, SqlMulti } from '@druid-toolkit/query';
import { day, Duration, hour } from 'chronoshift';

function getCanonicalDuration(
  expression: SqlExpression,
  timeColumnName: string,
): number | undefined {
  const pattern = fitFilterPattern(expression);
  if ('column' in pattern && pattern.column !== timeColumnName) return undefined;

  switch (pattern.type) {
    case 'timeInterval':
      return pattern.end.valueOf() - pattern.start.valueOf();

    case 'timeRelative':
      return Duration.fromJS(pattern.rangeDuration).getCanonicalLength();

    case 'custom':
      if (pattern.expression instanceof SqlMulti) {
        for (const value of pattern.expression.args.values) {
          const canonicalDuration = getCanonicalDuration(value, timeColumnName);
          if (canonicalDuration !== undefined) return canonicalDuration;
        }
      }

      break;

    default:
      break;
  }

  return undefined;
}

const DEFAULT_GRANULARITY = 'PT1H';

/**
 * Computes the granularity string from a where clause. If the where clause is TRUE, the default
 * granularity is returned (PT1H). Otherwise, the granularity is computed from the time column and the
 * duration of the where clause.
 *
 * @param where the where SQLExpression to read from
 * @param timeColumnName the name of the time column (any other time column will be ignored)
 * @returns the granularity string (default is PT1H)
 */
export function getAutoGranularity(where: SqlExpression, timeColumnName: string): string {
  if (where.toString() === 'TRUE') return DEFAULT_GRANULARITY;

  const canonicalDuration = getCanonicalDuration(where, timeColumnName);

  if (canonicalDuration) {
    if (canonicalDuration > day.canonicalLength * 95) return 'P1W';
    if (canonicalDuration > day.canonicalLength * 8) return 'P1D';
    if (canonicalDuration > hour.canonicalLength * 8) return 'PT1H';
    if (canonicalDuration > hour.canonicalLength * 3) return 'PT5M';

    return 'PT1M';
  }

  console.debug('Unable to determine granularity from where clause', where.toString());

  return DEFAULT_GRANULARITY;
}
