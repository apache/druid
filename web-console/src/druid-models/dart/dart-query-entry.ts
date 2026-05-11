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

export interface DartQueryEntry {
  engine: 'msq-dart';
  sqlQueryId: string;
  dartQueryId: string;
  sql: string;
  authenticator: string;
  identity: string;
  startTime: string;
  state: 'ACCEPTED' | 'RUNNING' | 'CANCELED' | 'SUCCESS' | 'FAILED';
  durationMs?: number;
}

const STATE_RANK: Record<string, number> = {
  RUNNING: 2,
  ACCEPTED: 1,
};

export function compareForDisplay(a: DartQueryEntry, b: DartQueryEntry): number {
  const stateA = STATE_RANK[a.state] ?? 0;
  const stateB = STATE_RANK[b.state] ?? 0;

  // Primary: RUNNING > ACCEPTED > others
  if (stateA !== stateB) {
    return stateB - stateA;
  }

  if (stateA > 0) {
    // RUNNING or ACCEPTED: descending startTime (newest first)
    return b.startTime.localeCompare(a.startTime);
  } else {
    // Finished: descending finish time (startTime + durationMs)
    const finishA = new Date(a.startTime).valueOf() + (a.durationMs ?? 0);
    const finishB = new Date(b.startTime).valueOf() + (b.durationMs ?? 0);
    return finishB - finishA;
  }
}
