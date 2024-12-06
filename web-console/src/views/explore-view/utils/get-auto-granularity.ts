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

import type { SqlExpression } from 'druid-query-toolkit';
import { fitFilterPatterns } from 'druid-query-toolkit';

import { Duration } from '../../../utils';

function getTimeSpanInExpression(
  expression: SqlExpression,
  timeColumnName: string,
): number | undefined {
  const patterns = fitFilterPatterns(expression);
  for (const pattern of patterns) {
    if (pattern.type === 'timeInterval' && pattern.column === timeColumnName) {
      return pattern.end.valueOf() - pattern.start.valueOf();
    } else if (pattern.type === 'timeRelative' && pattern.column === timeColumnName) {
      return new Duration(pattern.rangeDuration).getCanonicalLength();
    }
  }

  return;
}

export function getAutoGranularity(where: SqlExpression, timeColumnName: string): string {
  const timeSpan = getTimeSpanInExpression(where, timeColumnName);
  if (!timeSpan) return 'P1D';
  return Duration.pickSmallestGranularityThatFits(
    [
      'PT1S',
      'PT5S',
      'PT20S',
      'PT1M',
      'PT5M',
      'PT20M',
      'PT1H',
      'PT3H',
      'PT6H',
      'P1D',
      'P1W',
      'P1M',
      'P3M',
      'P1Y',
    ].map(s => new Duration(s)),
    timeSpan,
    200,
  ).toString();
}
