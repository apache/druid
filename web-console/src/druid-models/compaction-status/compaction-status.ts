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

import { formatBytesCompact, pluralIfNeeded } from '../../utils';
import type { CompactionConfig } from '../compaction-config/compaction-config';
import { compactionConfigHasLegacyInputSegmentSizeBytesSet } from '../compaction-config/compaction-config';

function capitalizeFirst(str: string): string {
  return str.slice(0, 1).toUpperCase() + str.slice(1).toLowerCase();
}

/**
 * Tells the console how to treat an interval that was skipped, without needing to
 * know the individual skip reasons.
 *
 * `OUT_OF_SCOPE` intervals were deliberately excluded by the compaction config and
 * `TRANSIENT` ones will be re-evaluated in a later run, so neither counts against
 * the datasource being fully compacted. Unknown categories from a newer server do
 * count, which is the safe default.
 */
export type CompactionSkipCategory = 'OUT_OF_SCOPE' | 'TRANSIENT' | 'DEFERRED' | 'UNSUPPORTED';

const CATEGORIES_MATCHING_CONFIG: CompactionSkipCategory[] = ['OUT_OF_SCOPE', 'TRANSIENT'];

export interface CompactionSkipStatistics {
  reason: string;
  category: CompactionSkipCategory;
  bytes: number;
  segmentCount: number;
  intervalCount: number;
}

export interface CompactionStatus {
  dataSource: string;
  scheduleStatus: string;
  bytesAwaitingCompaction: number;
  bytesCompacted: number;
  bytesSkipped: number;
  segmentCountAwaitingCompaction: number;
  segmentCountCompacted: number;
  segmentCountSkipped: number;
  intervalCountAwaitingCompaction: number;
  intervalCountCompacted: number;
  intervalCountSkipped: number;
  skippedStatsByReason?: CompactionSkipStatistics[];
}

export function zeroCompactionStatus(compactionStatus: CompactionStatus): boolean {
  return (
    !compactionStatus.bytesAwaitingCompaction &&
    !compactionStatus.bytesCompacted &&
    !compactionStatus.bytesSkipped &&
    !compactionStatus.segmentCountAwaitingCompaction &&
    !compactionStatus.segmentCountCompacted &&
    !compactionStatus.segmentCountSkipped &&
    !compactionStatus.intervalCountAwaitingCompaction &&
    !compactionStatus.intervalCountCompacted &&
    !compactionStatus.intervalCountSkipped
  );
}

/**
 * Intervals skipped for a reason that leaves them not matching the compaction
 * config. Excludes the categories that were either never meant to be compacted
 * or will be re-evaluated on their own, so that reported progress does not dip
 * while a successful compaction settles.
 */
export function skippedStatsNotMatchingConfig(
  compactionStatus: CompactionStatus,
): CompactionSkipStatistics[] {
  return (compactionStatus.skippedStatsByReason || []).filter(
    s => !CATEGORIES_MATCHING_CONFIG.includes(s.category),
  );
}

export function skippedStatsOfCategory(
  compactionStatus: CompactionStatus,
  category: CompactionSkipCategory,
): CompactionSkipStatistics[] {
  return (compactionStatus.skippedStatsByReason || []).filter(s => s.category === category);
}

function sumBy(
  stats: CompactionSkipStatistics[],
  field: 'bytes' | 'segmentCount' | 'intervalCount',
): number {
  return stats.reduce((total, s) => total + s[field], 0);
}

/**
 * Bytes that do not match the current compaction config, whether or not the
 * scheduler currently intends to compact them.
 */
export function bytesNotMatchingCompactionConfig(compactionStatus: CompactionStatus): number {
  return (
    compactionStatus.bytesAwaitingCompaction +
    sumBy(skippedStatsNotMatchingConfig(compactionStatus), 'bytes')
  );
}

export function segmentsNotMatchingCompactionConfig(compactionStatus: CompactionStatus): number {
  return (
    compactionStatus.segmentCountAwaitingCompaction +
    sumBy(skippedStatsNotMatchingConfig(compactionStatus), 'segmentCount')
  );
}

export function intervalsNotMatchingCompactionConfig(compactionStatus: CompactionStatus): number {
  return (
    compactionStatus.intervalCountAwaitingCompaction +
    sumBy(skippedStatsNotMatchingConfig(compactionStatus), 'intervalCount')
  );
}

function formatSkipReasons(stats: CompactionSkipStatistics[]): string {
  return stats.map(s => s.reason.toLowerCase().replace(/_/g, ' ')).join(', ');
}

export interface CompactionInfo {
  config?: CompactionConfig;
  status?: CompactionStatus;
}

export function formatCompactionInfo(compaction: CompactionInfo) {
  const { config, status } = compaction;
  if (config) {
    if (status) {
      if (
        status.bytesAwaitingCompaction === 0 &&
        status.segmentCountAwaitingCompaction === 0 &&
        status.intervalCountAwaitingCompaction === 0 &&
        !zeroCompactionStatus(status)
      ) {
        // Intervals skipped for a reason other than being out of scope still do
        // not match the compaction config, so the datasource is not fully compacted
        const notMatching = skippedStatsNotMatchingConfig(status);
        if (notMatching.length) {
          const deferred = skippedStatsOfCategory(status, 'DEFERRED');
          const reported = deferred.length ? deferred : notMatching;
          return `Not fully compacted (${pluralIfNeeded(
            sumBy(reported, 'segmentCount'),
            'segment',
          )} skipped: ${formatSkipReasons(reported)})`;
        } else if (status.segmentCountSkipped) {
          return `Fully compacted (except the last ${config.skipOffsetFromLatest || 'P1D'} of data${
            compactionConfigHasLegacyInputSegmentSizeBytesSet(config)
              ? ` and segments larger than ${formatBytesCompact(config.inputSegmentSizeBytes!)}`
              : ''
          }, ${pluralIfNeeded(status.segmentCountSkipped, 'segment')} skipped)`;
        } else {
          return 'Fully compacted';
        }
      } else {
        return capitalizeFirst(status.scheduleStatus.replace(/_/g, ' '));
      }
    } else {
      return 'Awaiting first run';
    }
  } else {
    return 'Not enabled';
  }
}
