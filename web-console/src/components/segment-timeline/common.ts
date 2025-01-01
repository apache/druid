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

import { sum } from 'd3-array';

import type { Duration } from '../../utils';
import { formatBytes, formatInteger } from '../../utils';

export type IntervalStat = 'segments' | 'size' | 'rows';

export const INTERVAL_STATS: IntervalStat[] = ['segments', 'size', 'rows'];

export function getIntervalStatTitle(intervalStat: IntervalStat): string {
  switch (intervalStat) {
    case 'segments':
      return 'Num. segments';

    case 'size':
      return 'Size';

    case 'rows':
      return 'Rows';

    default:
      return intervalStat;
  }
}

export function aggregateSegmentStats(
  xs: readonly Record<IntervalStat, number>[],
): Record<IntervalStat, number> {
  return {
    segments: sum(xs, s => s.segments),
    size: sum(xs, s => s.size),
    rows: sum(xs, s => s.rows),
  };
}

export function formatIntervalStat(stat: IntervalStat, n: number) {
  switch (stat) {
    case 'segments':
    case 'rows':
      return formatInteger(n);

    case 'size':
      return formatBytes(n);

    default:
      return '';
  }
}

export interface IntervalRow extends Record<IntervalStat, number> {
  start: Date;
  end: Date;
  datasource: string;
  realtime: boolean;
  originalTimeSpan: Duration;
}

export interface TrimmedIntervalRow extends IntervalRow {
  shownDays: number;
  normalized: Record<IntervalStat, number>;
}

export interface IntervalBar extends TrimmedIntervalRow {
  offset: Record<IntervalStat, number>;
}

export function formatIsoDateOnly(date: Date): string {
  return date.toISOString().slice(0, 10);
}
