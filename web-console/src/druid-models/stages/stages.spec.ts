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
      expect(STAGES.getByPartitionCountersForStage(STAGES.stages[2], 'input'))
        .toMatchInlineSnapshot(`
        Array [
          Object {
            "index": 0,
            "input0": Object {
              "bytes": 10943622,
              "files": 0,
              "frames": 21,
              "rows": 39742,
              "totalFiles": 0,
            },
          },
        ]
      `);
    });

    it('works for output', () => {
      expect(STAGES.getByPartitionCountersForStage(STAGES.stages[2], 'output'))
        .toMatchInlineSnapshot(`
        Array [
          Object {
            "index": 0,
            "shuffle": Object {
              "bytes": 257524,
              "files": 0,
              "frames": 1,
              "rows": 888,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 1,
            "shuffle": Object {
              "bytes": 289731,
              "files": 0,
              "frames": 1,
              "rows": 995,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 2,
            "shuffle": Object {
              "bytes": 412396,
              "files": 0,
              "frames": 1,
              "rows": 1419,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 3,
            "shuffle": Object {
              "bytes": 262388,
              "files": 0,
              "frames": 1,
              "rows": 905,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 4,
            "shuffle": Object {
              "bytes": 170554,
              "files": 0,
              "frames": 1,
              "rows": 590,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 5,
            "shuffle": Object {
              "bytes": 188324,
              "files": 0,
              "frames": 1,
              "rows": 652,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 6,
            "shuffle": Object {
              "bytes": 92275,
              "files": 0,
              "frames": 1,
              "rows": 322,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 7,
            "shuffle": Object {
              "bytes": 69531,
              "files": 0,
              "frames": 1,
              "rows": 247,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 8,
            "shuffle": Object {
              "bytes": 65844,
              "files": 0,
              "frames": 1,
              "rows": 236,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 9,
            "shuffle": Object {
              "bytes": 85875,
              "files": 0,
              "frames": 1,
              "rows": 309,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 10,
            "shuffle": Object {
              "bytes": 71852,
              "files": 0,
              "frames": 1,
              "rows": 256,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 11,
            "shuffle": Object {
              "bytes": 72512,
              "files": 0,
              "frames": 1,
              "rows": 260,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 12,
            "shuffle": Object {
              "bytes": 123204,
              "files": 0,
              "frames": 1,
              "rows": 440,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 13,
            "shuffle": Object {
              "bytes": 249217,
              "files": 0,
              "frames": 1,
              "rows": 876,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 14,
            "shuffle": Object {
              "bytes": 399583,
              "files": 0,
              "frames": 1,
              "rows": 1394,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 15,
            "shuffle": Object {
              "bytes": 256916,
              "files": 0,
              "frames": 1,
              "rows": 892,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 16,
            "shuffle": Object {
              "bytes": 1039927,
              "files": 0,
              "frames": 2,
              "rows": 3595,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 17,
            "shuffle": Object {
              "bytes": 1887893,
              "files": 0,
              "frames": 4,
              "rows": 6522,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 18,
            "shuffle": Object {
              "bytes": 1307287,
              "files": 0,
              "frames": 3,
              "rows": 4525,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 19,
            "shuffle": Object {
              "bytes": 1248166,
              "files": 0,
              "frames": 3,
              "rows": 4326,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 20,
            "shuffle": Object {
              "bytes": 1195593,
              "files": 0,
              "frames": 3,
              "rows": 4149,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 21,
            "shuffle": Object {
              "bytes": 738804,
              "files": 0,
              "frames": 2,
              "rows": 2561,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 22,
            "shuffle": Object {
              "bytes": 552485,
              "files": 0,
              "frames": 2,
              "rows": 1914,
              "totalFiles": 0,
            },
          },
          Object {
            "index": 23,
            "shuffle": Object {
              "bytes": 418062,
              "files": 0,
              "frames": 1,
              "rows": 1452,
              "totalFiles": 0,
            },
          },
        ]
      `);
    });
  });
});
