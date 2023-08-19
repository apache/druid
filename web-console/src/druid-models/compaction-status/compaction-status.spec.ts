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
import { formatCompactionInfo, zeroCompactionStatus } from './compaction-status';

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
});
