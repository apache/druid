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

import { CSV_SAMPLE, JSON_SAMPLE } from '../../utils/sampler.mock';

import { getDimensionSpecs } from './dimension-spec';

describe('dimension-spec', () => {
  describe('getDimensionSpecs', () => {
    it('works for empty', () => {
      expect(getDimensionSpecs({ header: ['header'], rows: [] }, {}, false, true)).toEqual([
        'header',
      ]);
    });

    it('works with json', () => {
      expect(getDimensionSpecs(JSON_SAMPLE, {}, false, false)).toEqual([
        'timestamp',
        'user',
        {
          name: 'followers',
          type: 'long',
        },
        {
          name: 'spend',
          type: 'double',
        },
        'id',
        'tags',
        'nums',
      ]);

      expect(getDimensionSpecs(JSON_SAMPLE, {}, false, true)).toEqual([
        'timestamp',
        'user',
        'id',
        'tags',
        'nums',
      ]);
    });

    it('works with csv', () => {
      expect(getDimensionSpecs(CSV_SAMPLE, {}, true, false)).toEqual([
        'timestamp',
        'user',
        {
          name: 'followers',
          type: 'long',
        },
        {
          name: 'spend',
          type: 'double',
        },
        {
          name: 'id',
          type: 'long',
        },
        'tags',
        'nums',
      ]);

      expect(getDimensionSpecs(CSV_SAMPLE, {}, true, true)).toEqual([
        'timestamp',
        'user',
        'tags',
        'nums',
      ]);
    });
  });
});
