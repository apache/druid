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

import { Duration } from 'chronoshift';
import type { SqlExpression } from 'druid-query-toolkit';
import { fitFilterPatterns } from 'druid-query-toolkit';

export function getTimeSpanInExpression(
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

export const FINE_GRANULARITY_OPTIONS = [
  'PT1S',
  'PT2S',
  'PT5S',
  'PT15S',
  'PT30S',
  'PT1M',
  'PT2M',
  'PT5M',
  'PT15M',
  'PT30M',
  'PT1H',
  'PT3H',
  'PT6H',
  'P1D',
  'P1W',
  'P1M',
  'P3M',
  'P1Y',
];

const AUTO_GRANULARITY_OPTIONS = FINE_GRANULARITY_OPTIONS.map(s => new Duration(s));

export function getAutoGranularity(
  where: SqlExpression,
  timeColumnName: string,
  maxEntries: number,
): string {
  const timeSpan = getTimeSpanInExpression(where, timeColumnName);
  if (!timeSpan) return 'P1D';
  return pickSmallestGranularityThatFits(AUTO_GRANULARITY_OPTIONS, timeSpan, maxEntries).toString();
}

/**
 * Picks the first granularity that will produce no more than maxEntities to fill the given span
 * @param granularities - granularities to try in sorted from small to large
 * @param span - the span to fit in ms
 * @param maxEntities - the number of entities not to exceed
 */
export function pickSmallestGranularityThatFits(
  granularities: Duration[],
  span: number,
  maxEntities: number,
): Duration {
  for (const granularity of granularities) {
    if (span / granularity.getCanonicalLength() < maxEntities) return granularity;
  }
  return granularities[granularities.length - 1];
}
