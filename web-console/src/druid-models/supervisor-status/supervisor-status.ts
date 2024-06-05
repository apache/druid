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

import { max, sum } from 'd3-array';

import type { NumberLike } from '../../utils';
import { deepGet, filterMap } from '../../utils';

export type SupervisorOffsetMap = Record<string, NumberLike>;

export interface SupervisorStatus {
  generationTime: string;
  id: string;
  payload: {
    dataSource: string;
    stream: string;
    partitions: number;
    replicas: number;
    durationSeconds: number;
    activeTasks: SupervisorStatusTask[];
    publishingTasks: SupervisorStatusTask[];
    latestOffsets?: SupervisorOffsetMap;
    minimumLag?: SupervisorOffsetMap;
    aggregateLag: number;
    offsetsLastUpdated: string;
    suspended: boolean;
    healthy: boolean;
    state: string;
    detailedState: string;
    recentErrors: SupervisorError[];
  };
}

export interface SupervisorStatusTask {
  id: string;
  startingOffsets: SupervisorOffsetMap;
  startTime: string;
  remainingSeconds: number;
  type: string;
  currentOffsets: SupervisorOffsetMap;
  lag: SupervisorOffsetMap;
}

export interface SupervisorError {
  timestamp: string;
  exceptionClass: string;
  message: string;
  streamException: boolean;
}

export type SupervisorStats = Record<string, Record<string, RowStats>>;

export type RowStatsKey = 'totals' | '1m' | '5m' | '15m';

export interface RowStats {
  movingAverages: {
    buildSegments: {
      '1m': RowStatsCounter;
      '5m': RowStatsCounter;
      '15m': RowStatsCounter;
    };
  };
  totals: {
    buildSegments: RowStatsCounter;
  };
}

export interface RowStatsCounter {
  processed: number;
  processedBytes: number;
  processedWithError: number;
  thrownAway: number;
  unparseable: number;
}

function sumRowStatsCounter(rowStats: RowStatsCounter[]): RowStatsCounter {
  return {
    processed: sum(rowStats, d => d.processed),
    processedBytes: sum(rowStats, d => d.processedBytes),
    processedWithError: sum(rowStats, d => d.processedWithError),
    thrownAway: sum(rowStats, d => d.thrownAway),
    unparseable: sum(rowStats, d => d.unparseable),
  };
}

function maxRowStatsCounter(rowStats: RowStatsCounter[]): RowStatsCounter {
  return {
    processed: max(rowStats, d => d.processed) ?? 0,
    processedBytes: max(rowStats, d => d.processedBytes) ?? 0,
    processedWithError: max(rowStats, d => d.processedWithError) ?? 0,
    thrownAway: max(rowStats, d => d.thrownAway) ?? 0,
    unparseable: max(rowStats, d => d.unparseable) ?? 0,
  };
}

function getRowStatsCounter(rowStats: RowStats, key: RowStatsKey): RowStatsCounter | undefined {
  if (key === 'totals') {
    return deepGet(rowStats, 'totals.buildSegments');
  } else {
    return deepGet(rowStats, `movingAverages.buildSegments.${key}`);
  }
}

export function getTotalSupervisorStats(
  stats: SupervisorStats,
  key: RowStatsKey,
  activeTaskIds: string[] | undefined,
): RowStatsCounter {
  return sumRowStatsCounter(
    Object.values(stats).map(s =>
      maxRowStatsCounter(
        filterMap(Object.entries(s), ([taskId, rs]) =>
          !activeTaskIds || activeTaskIds.includes(taskId)
            ? getRowStatsCounter(rs, key)
            : undefined,
        ),
      ),
    ),
  );
}
