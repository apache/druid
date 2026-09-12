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

import type { CompactionConfig } from '../compaction-config/compaction-config';

import type { CompactionStatus } from './compaction-status';
import {
  bytesNotMatchingCompactionConfig,
  formatCompactionInfo,
  zeroCompactionStatus,
} from './compaction-status';

describe('compaction status', () => {
  const BASIC_CONFIG: CompactionConfig = {
    dataSource: 'tbl',
  };
  const LEGACY_CONFIG: CompactionConfig = {
    dataSource: 'tbl',
    inputSegmentSizeBytes: 1e6,
  };
  const ZERO_STATUS: CompactionStatus = {
    dataSource: 'tbl',
    scheduleStatus: 'RUNNING',
    bytesAwaitingCompaction: 0,
    bytesCompacted: 0,
    bytesSkipped: 0,
    segmentCountAwaitingCompaction: 0,
    segmentCountCompacted: 0,
    segmentCountSkipped: 0,
    intervalCountAwaitingCompaction: 0,
    intervalCountCompacted: 0,
    intervalCountSkipped: 0,
  };

  describe('zeroCompactionStatus', () => {
    it('works with zero', () => {
      expect(zeroCompactionStatus(ZERO_STATUS)).toEqual(true);
    });

    it('works with non-zero', () => {
      expect(
        zeroCompactionStatus({
          dataSource: 'tbl',
          scheduleStatus: 'RUNNING',
          bytesAwaitingCompaction: 1,
          bytesCompacted: 0,
          bytesSkipped: 0,
          segmentCountAwaitingCompaction: 0,
          segmentCountCompacted: 0,
          segmentCountSkipped: 0,
          intervalCountAwaitingCompaction: 0,
          intervalCountCompacted: 0,
          intervalCountSkipped: 0,
        }),
      ).toEqual(false);
    });
  });

  describe('formatCompactionConfigAndStatus', () => {
    it('works with nothing', () => {
      expect(formatCompactionInfo({})).toEqual('Not enabled');
    });

    it('works when there is no status', () => {
      expect(formatCompactionInfo({ config: BASIC_CONFIG })).toEqual('Awaiting first run');
    });

    it('works when here is no config', () => {
      expect(formatCompactionInfo({ status: ZERO_STATUS })).toEqual('Not enabled');
    });

    it('works with config and zero status', () => {
      expect(formatCompactionInfo({ config: BASIC_CONFIG, status: ZERO_STATUS })).toEqual(
        'Running',
      );
    });

    it('works when fully compacted', () => {
      expect(
        formatCompactionInfo({
          config: BASIC_CONFIG,
          status: {
            dataSource: 'tbl',
            scheduleStatus: 'RUNNING',
            bytesAwaitingCompaction: 0,
            bytesCompacted: 100,
            bytesSkipped: 0,
            segmentCountAwaitingCompaction: 0,
            segmentCountCompacted: 10,
            segmentCountSkipped: 0,
            intervalCountAwaitingCompaction: 0,
            intervalCountCompacted: 10,
            intervalCountSkipped: 0,
          },
        }),
      ).toEqual('Fully compacted');
    });

    it('works when fully compacted and some segments skipped', () => {
      expect(
        formatCompactionInfo({
          config: BASIC_CONFIG,
          status: {
            dataSource: 'tbl',
            scheduleStatus: 'RUNNING',
            bytesAwaitingCompaction: 0,
            bytesCompacted: 0,
            bytesSkipped: 3776979,
            segmentCountAwaitingCompaction: 0,
            segmentCountCompacted: 0,
            segmentCountSkipped: 24,
            intervalCountAwaitingCompaction: 0,
            intervalCountCompacted: 0,
            intervalCountSkipped: 24,
          },
        }),
      ).toEqual('Fully compacted (except the last P1D of data, 24 segments skipped)');
    });

    it('works when some segments are deferred by the compaction policy', () => {
      expect(
        formatCompactionInfo({
          config: BASIC_CONFIG,
          status: {
            dataSource: 'tbl',
            scheduleStatus: 'RUNNING',
            bytesAwaitingCompaction: 0,
            bytesCompacted: 100,
            bytesSkipped: 500,
            segmentCountAwaitingCompaction: 0,
            segmentCountCompacted: 10,
            segmentCountSkipped: 999,
            intervalCountAwaitingCompaction: 0,
            intervalCountCompacted: 10,
            intervalCountSkipped: 3,
            skippedStatsByReason: [
              {
                reason: 'REJECTED_BY_SEARCH_POLICY',
                category: 'DEFERRED',
                bytes: 500,
                segmentCount: 999,
                intervalCount: 3,
              },
            ],
          },
        }),
      ).toEqual('Not fully compacted (999 segments skipped: rejected by search policy)');
    });

    it('stays fully compacted when segments are only skipped as out of scope', () => {
      expect(
        formatCompactionInfo({
          config: BASIC_CONFIG,
          status: {
            dataSource: 'tbl',
            scheduleStatus: 'RUNNING',
            bytesAwaitingCompaction: 0,
            bytesCompacted: 0,
            bytesSkipped: 3776979,
            segmentCountAwaitingCompaction: 0,
            segmentCountCompacted: 0,
            segmentCountSkipped: 24,
            intervalCountAwaitingCompaction: 0,
            intervalCountCompacted: 0,
            intervalCountSkipped: 24,
            skippedStatsByReason: [
              {
                reason: 'SKIP_OFFSET',
                category: 'OUT_OF_SCOPE',
                bytes: 3776979,
                segmentCount: 24,
                intervalCount: 24,
              },
            ],
          },
        }),
      ).toEqual('Fully compacted (except the last P1D of data, 24 segments skipped)');
    });

    it('stays fully compacted while a successful compaction settles', () => {
      // TIMELINE_NOT_UPDATED means the compaction task succeeded and the timeline
      // has not refreshed yet, so these segments must not read as needing compaction
      expect(
        formatCompactionInfo({
          config: BASIC_CONFIG,
          status: {
            dataSource: 'tbl',
            scheduleStatus: 'RUNNING',
            bytesAwaitingCompaction: 0,
            bytesCompacted: 100,
            bytesSkipped: 500,
            segmentCountAwaitingCompaction: 0,
            segmentCountCompacted: 10,
            segmentCountSkipped: 5,
            intervalCountAwaitingCompaction: 0,
            intervalCountCompacted: 10,
            intervalCountSkipped: 1,
            skippedStatsByReason: [
              {
                reason: 'TIMELINE_NOT_UPDATED',
                category: 'TRANSIENT',
                bytes: 500,
                segmentCount: 5,
                intervalCount: 1,
              },
            ],
          },
        }),
      ).not.toContain('Not fully compacted');
    });

    it('works when fully compacted and some segments skipped (with legacy config)', () => {
      expect(
        formatCompactionInfo({
          config: LEGACY_CONFIG,
          status: {
            dataSource: 'tbl',
            scheduleStatus: 'RUNNING',
            bytesAwaitingCompaction: 0,
            bytesCompacted: 0,
            bytesSkipped: 3776979,
            segmentCountAwaitingCompaction: 0,
            segmentCountCompacted: 0,
            segmentCountSkipped: 24,
            intervalCountAwaitingCompaction: 0,
            intervalCountCompacted: 0,
            intervalCountSkipped: 24,
          },
        }),
      ).toEqual(
        'Fully compacted (except the last P1D of data and segments larger than 1.00MB, 24 segments skipped)',
      );
    });
  });

  describe('bytesNotMatchingCompactionConfig', () => {
    function statusWith(
      category: 'OUT_OF_SCOPE' | 'TRANSIENT' | 'DEFERRED' | 'UNSUPPORTED',
    ): CompactionStatus {
      return {
        ...ZERO_STATUS,
        bytesAwaitingCompaction: 10,
        bytesSkipped: 500,
        segmentCountSkipped: 5,
        intervalCountSkipped: 1,
        skippedStatsByReason: [
          { reason: 'SOME_REASON', category, bytes: 500, segmentCount: 5, intervalCount: 1 },
        ],
      };
    }

    it('ignores intervals that were never meant to be compacted', () => {
      expect(bytesNotMatchingCompactionConfig(statusWith('OUT_OF_SCOPE'))).toEqual(10);
    });

    it('ignores intervals that will be re-evaluated on their own', () => {
      expect(bytesNotMatchingCompactionConfig(statusWith('TRANSIENT'))).toEqual(10);
    });

    it('counts intervals that the policy or config is holding back', () => {
      expect(bytesNotMatchingCompactionConfig(statusWith('DEFERRED'))).toEqual(510);
    });

    it('counts intervals that cannot be compacted', () => {
      expect(bytesNotMatchingCompactionConfig(statusWith('UNSUPPORTED'))).toEqual(510);
    });

    it('counts an unknown category from a newer server', () => {
      const status = statusWith('DEFERRED');
      status.skippedStatsByReason![0].category = 'SOMETHING_NEW' as never;
      expect(bytesNotMatchingCompactionConfig(status)).toEqual(510);
    });

    it('falls back to awaiting bytes when there is no breakdown', () => {
      expect(
        bytesNotMatchingCompactionConfig({ ...ZERO_STATUS, bytesAwaitingCompaction: 10 }),
      ).toEqual(10);
    });
  });
});
