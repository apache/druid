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

import { Api } from './api';

describe('Api', () => {
  it('escapes stuff', () => {
    expect(Api.encodePath('wikipedia')).toEqual('wikipedia');
    expect(Api.encodePath(`wi%ki?pe#dia&'[]`)).toEqual('wi%25ki%3Fpe%23dia%26%27%5B%5D');
  });

  describe(`with BigInt`, () => {
    it('works as expected', () => {
      const res = (Api.getDefaultConfig().transformResponse as any)[0](`{"x":9223372036854775799}`);
      expect(typeof res).toEqual('object');
      expect(typeof res.x).toEqual('bigint');
      expect(String(res.x)).toEqual('9223372036854775799');
    });
  });

  describe(`without BigInt`, () => {
    const originalBigInt = (global as any).BigInt;

    beforeAll(() => {
      (global as any).BigInt = null;
    });

    afterAll(() => {
      (global as any).BigInt = originalBigInt;
    });

    it('works as expected', () => {
      const res = (Api.getDefaultConfig().transformResponse as any)[0](`{"x":9223372036854775799}`);
      expect(typeof res).toEqual('object');
      expect(typeof res.x).toEqual('number');
      expect(String(res.x)).toEqual('9223372036854776000');
    });
  });
});
