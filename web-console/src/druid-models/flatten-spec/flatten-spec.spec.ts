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

import { computeFlattenExprsForData } from './flatten-spec';

describe('flatten-spec', () => {
  describe('computeFlattenExprsForData', () => {
    const data = [
      {
        context: { host: 'cla', topic: 'moon', bonus: { 'fo.o': 'bar' } },
        tags: ['a', 'b', 'c'],
        messages: [
          { metric: 'request/time', value: 122 },
          { metric: 'request/time', value: 434 },
          { metric: 'request/time', value: 565 },
        ],
        value: 5,
      },
      {
        context: { 'host': 'piv', '1pic': 'sun' },
        tags: ['a', 'd'],
        messages: [
          { metric: 'request/time', value: 44 },
          { metric: 'request/time', value: 65 },
        ],
        value: 4,
      },
      {
        context: { 'host': 'imp', "d\\o\npi'c'": 'fun' },
        tags: ['x', 'y'],
        messages: [
          { metric: 'request/time', value: 4 },
          { metric: 'request/time', value: 5 },
        ],
        value: 2,
      },
      'Curveball' as any,
    ];

    it('works for path, ignore-arrays', () => {
      expect(computeFlattenExprsForData(data, 'ignore-arrays')).toEqual([
        "$.context.bonus['fo.o']",
        '$.context.host',
        '$.context.topic',
        "$.context['1pic']",
        "$.context['d\\\\o\npi\\'c\\'']",
      ]);
    });
  });
});
