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

import * as JSONBig from 'json-bigint-native';

import { deepDelete, deepExtend, deepGet, deepSet, makePath, parsePath } from './object-change';

describe('object-change', () => {
  describe('parsePath', () => {
    it('works', () => {
      expect(parsePath('hello.wow.0')).toEqual(['hello', 'wow', '0']);
      expect(parsePath('hello.{wow.moon}.0')).toEqual(['hello', 'wow.moon', '0']);
      expect(parsePath('hello.#.0.[append]')).toEqual(['hello', '#', '0', '[append]']);
    });
  });

  describe('makePath', () => {
    it('works', () => {
      expect(makePath(['hello', 'wow', '0'])).toEqual('hello.wow.0');
      expect(makePath(['hello', 'wow.moon', '0'])).toEqual('hello.{wow.moon}.0');
    });
  });

  describe('deepGet', () => {
    const thing = {
      hello: {
        'consumer.props': 'lol',
        wow: ['a', { test: 'moon' }],
      },
      zetrix: null,
    };

    it('works', () => {
      expect(deepGet(thing, 'hello.wow.0')).toEqual('a');
      expect(deepGet(thing, 'hello.wow.4')).toEqual(undefined);
      expect(deepGet(thing, 'hello.{consumer.props}')).toEqual('lol');
    });
  });

  describe('deepSet', () => {
    const thing = {
      hello: {
        wow: ['a', { test: 'moon' }],
      },
      zetrix: null,
    };

    it('works to set an existing thing', () => {
      expect(deepSet(thing, 'hello.wow.0', 5)).toEqual({
        hello: {
          wow: [
            5,
            {
              test: 'moon',
            },
          ],
        },
        zetrix: null,
      });
    });

    it('works to set a non-existing thing', () => {
      expect(deepSet(thing, 'lets.do.this.now', 5)).toEqual({
        hello: {
          wow: [
            'a',
            {
              test: 'moon',
            },
          ],
        },
        lets: {
          do: {
            this: {
              now: 5,
            },
          },
        },
        zetrix: null,
      });
    });

    it('works to set an existing array', () => {
      expect(deepSet(thing, 'hello.wow.[append]', 5)).toEqual({
        hello: {
          wow: [
            'a',
            {
              test: 'moon',
            },
            5,
          ],
        },
        zetrix: null,
      });
    });
  });

  describe('deepDelete', () => {
    const thing = {
      hello: {
        moon: 1,
        wow: ['a', { test: 'moon' }],
      },
      zetrix: null,
    };

    it('works to delete an existing thing', () => {
      expect(deepDelete(thing, 'hello.wow')).toEqual({
        hello: { moon: 1 },
        zetrix: null,
      });
    });

    it('works is harmless to delete a non-existing thing', () => {
      expect(deepDelete(thing, 'hello.there.lol.why')).toEqual(thing);
    });

    it('removes things completely', () => {
      expect(deepDelete(deepDelete(thing, 'hello.wow'), 'hello.moon')).toEqual({
        zetrix: null,
      });
    });

    it('works with arrays', () => {
      expect(JSON.parse(JSONBig.stringify(deepDelete(thing, 'hello.wow.0')))).toEqual({
        hello: {
          moon: 1,
          wow: [
            {
              test: 'moon',
            },
          ],
        },
        zetrix: null,
      });
    });
  });

  describe('deepExtend', () => {
    it('works', () => {
      const obj1 = {
        money: 1,
        bag: 2,
        nice: {
          a: 1,
          b: [],
          c: { an: 123, ice: 321, bag: 1 },
        },
        swag: {
          diamond: ['collar'],
        },
        pockets: { ice: 3 },
        f: ['bag'],
      };

      const obj2 = {
        bag: 3,
        nice: null,
        pockets: { need: 1, an: 2 },
        swag: {
          diamond: ['collar', 'molar'],
        },
      };

      expect(deepExtend(obj1, obj2)).toEqual({
        money: 1,
        bag: 3,
        nice: null,
        swag: {
          diamond: ['collar', 'molar'],
        },
        pockets: { need: 1, an: 2, ice: 3 },
        f: ['bag'],
      });
    });
  });
});
