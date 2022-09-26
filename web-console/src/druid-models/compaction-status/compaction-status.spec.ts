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

import { CompactionConfig } from '../compaction-config/compaction-config';

import {
  CompactionStatus,
  formatCompactionConfigAndStatus,
  zeroCompactionStatus,
} from './compaction-status';

describe('compaction status', () => {
  const BASIC_CONFIG: CompactionConfig = {};
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

  it('zeroCompactionStatus', () => {
    expect(zeroCompactionStatus(ZERO_STATUS)).toEqual(true);

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

  it('formatCompactionConfigAndStatus', () => {
    expect(formatCompactionConfigAndStatus(undefined, undefined)).toEqual('Not enabled');

    expect(formatCompactionConfigAndStatus(BASIC_CONFIG, undefined)).toEqual('Awaiting first run');

    expect(formatCompactionConfigAndStatus(undefined, ZERO_STATUS)).toEqual('Not enabled');

    expect(formatCompactionConfigAndStatus(BASIC_CONFIG, ZERO_STATUS)).toEqual('Running');

    expect(
      formatCompactionConfigAndStatus(BASIC_CONFIG, {
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
      }),
    ).toEqual('Fully compacted');
  });
});
