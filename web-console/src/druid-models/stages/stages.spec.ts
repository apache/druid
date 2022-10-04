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

import { STAGES } from './stages.mock';

describe('Stages', () => {
  describe('#overallProgress', () => {
    it('works when finished', () => {
      expect(STAGES.overallProgress()).toBeCloseTo(0.987);
    });
  });

  describe('#getByPartitionCountersForStage', () => {
    it('works for input', () => {
      expect(STAGES.getByPartitionCountersForStage(STAGES.stages[2], 'input')).toEqual([
        {
          index: 0,
          input0: {
            bytes: 5458795,
            files: 0,
            frames: 11,
            rows: 19834,
            totalFiles: 0,
          },
        },
        {
          index: 1,
          input0: {
            bytes: 5464500,
            files: 0,
            frames: 11,
            rows: 19833,
            totalFiles: 0,
          },
        },
      ]);
    });

    it('works for output', () => {
      expect(STAGES.getByPartitionCountersForStage(STAGES.stages[2], 'output')).toEqual([
        {
          index: 0,
          shuffle: {
            bytes: 257524,
            files: 0,
            frames: 1,
            rows: 888,
            totalFiles: 0,
          },
        },
        {
          index: 1,
          shuffle: {
            bytes: 289161,
            files: 0,
            frames: 1,
            rows: 993,
            totalFiles: 0,
          },
        },
        {
          index: 2,
          shuffle: {
            bytes: 412116,
            files: 0,
            frames: 1,
            rows: 1418,
            totalFiles: 0,
          },
        },
        {
          index: 3,
          shuffle: {
            bytes: 259346,
            files: 0,
            frames: 1,
            rows: 894,
            totalFiles: 0,
          },
        },
        {
          index: 4,
          shuffle: {
            bytes: 170554,
            files: 0,
            frames: 1,
            rows: 590,
            totalFiles: 0,
          },
        },
        {
          index: 5,
          shuffle: {
            bytes: 182872,
            files: 0,
            frames: 1,
            rows: 633,
            totalFiles: 0,
          },
        },
        {
          index: 6,
          shuffle: {
            bytes: 88525,
            files: 0,
            frames: 1,
            rows: 309,
            totalFiles: 0,
          },
        },
        {
          index: 7,
          shuffle: {
            bytes: 67821,
            files: 0,
            frames: 1,
            rows: 241,
            totalFiles: 0,
          },
        },
        {
          index: 8,
          shuffle: {
            bytes: 65844,
            files: 0,
            frames: 1,
            rows: 236,
            totalFiles: 0,
          },
        },
        {
          index: 9,
          shuffle: {
            bytes: 85875,
            files: 0,
            frames: 1,
            rows: 309,
            totalFiles: 0,
          },
        },
        {
          index: 10,
          shuffle: {
            bytes: 71002,
            files: 0,
            frames: 1,
            rows: 253,
            totalFiles: 0,
          },
        },
        {
          index: 11,
          shuffle: {
            bytes: 72512,
            files: 0,
            frames: 1,
            rows: 260,
            totalFiles: 0,
          },
        },
        {
          index: 12,
          shuffle: {
            bytes: 123204,
            files: 0,
            frames: 1,
            rows: 440,
            totalFiles: 0,
          },
        },
        {
          index: 13,
          shuffle: {
            bytes: 249217,
            files: 0,
            frames: 1,
            rows: 876,
            totalFiles: 0,
          },
        },
        {
          index: 14,
          shuffle: {
            bytes: 399583,
            files: 0,
            frames: 1,
            rows: 1394,
            totalFiles: 0,
          },
        },
        {
          index: 15,
          shuffle: {
            bytes: 256916,
            files: 0,
            frames: 1,
            rows: 892,
            totalFiles: 0,
          },
        },
        {
          index: 16,
          shuffle: {
            bytes: 1039927,
            files: 0,
            frames: 2,
            rows: 3595,
            totalFiles: 0,
          },
        },
        {
          index: 17,
          shuffle: {
            bytes: 1887927,
            files: 0,
            frames: 5,
            rows: 6522,
            totalFiles: 0,
          },
        },
        {
          index: 18,
          shuffle: {
            bytes: 1307287,
            files: 0,
            frames: 3,
            rows: 4525,
            totalFiles: 0,
          },
        },
        {
          index: 19,
          shuffle: {
            bytes: 1247597,
            files: 0,
            frames: 3,
            rows: 4324,
            totalFiles: 0,
          },
        },
        {
          index: 20,
          shuffle: {
            bytes: 1195593,
            files: 0,
            frames: 3,
            rows: 4149,
            totalFiles: 0,
          },
        },
        {
          index: 21,
          shuffle: {
            bytes: 738804,
            files: 0,
            frames: 2,
            rows: 2561,
            totalFiles: 0,
          },
        },
        {
          index: 22,
          shuffle: {
            bytes: 552485,
            files: 0,
            frames: 2,
            rows: 1914,
            totalFiles: 0,
          },
        },
        {
          index: 23,
          shuffle: {
            bytes: 417784,
            files: 0,
            frames: 1,
            rows: 1451,
            totalFiles: 0,
          },
        },
      ]);
    });
  });
});
