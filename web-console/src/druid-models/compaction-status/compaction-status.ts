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

function capitalizeFirst(str: string): string {
  return str.slice(0, 1).toUpperCase() + str.slice(1).toLowerCase();
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

export function formatCompactionConfigAndStatus(
  compactionConfig: CompactionConfig | undefined,
  compactionStatus: CompactionStatus | undefined,
) {
  if (compactionConfig) {
    if (compactionStatus) {
      if (
        compactionStatus.bytesAwaitingCompaction === 0 &&
        !zeroCompactionStatus(compactionStatus)
      ) {
        return 'Fully compacted';
      } else {
        return capitalizeFirst(compactionStatus.scheduleStatus);
      }
    } else {
      return 'Awaiting first run';
    }
  } else {
    return 'Not enabled';
  }
}
