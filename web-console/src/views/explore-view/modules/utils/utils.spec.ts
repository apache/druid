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

import { SqlExpression } from '@druid-toolkit/query';

import { shiftTimeInWhere } from './utils';

describe('shiftTimeInWhere', () => {
  it('works with TIME_IN_INTERVAL', () => {
    expect(
      shiftTimeInWhere(
        SqlExpression.parse(`TIME_IN_INTERVAL("__time", '2016-06-27/2016-06-28')`),
        'P1D',
      ).toString(),
    ).toEqual(`TIME_IN_INTERVAL(TIME_SHIFT("__time", 'P1D', 1), '2016-06-27/2016-06-28')`);
  });

  it('works with relative time', () => {
    expect(
      shiftTimeInWhere(
        SqlExpression.parse(
          `(TIME_SHIFT(MAX_DATA_TIME(), 'PT1H', -1) <= "__time" AND "__time" < MAX_DATA_TIME())`,
        ),
        'PT1H',
      ).toString(),
    ).toEqual(
      `(TIME_SHIFT(TIME_SHIFT(MAX_DATA_TIME(), 'PT1H', -1), 'PT1H', -1) <= "__time" AND "__time" < TIME_SHIFT(MAX_DATA_TIME(), 'PT1H', -1))`,
    );
  });

  it('works with relative time (specific timestamps)', () => {
    expect(
      shiftTimeInWhere(
        SqlExpression.parse(
          `TIMESTAMP '2016-06-27 20:31:02.498' <= "__time" AND "__time" < TIMESTAMP '2016-06-27 21:31:02.498'`,
        ),
        'PT1H',
      ).toString(),
    ).toEqual(
      `TIME_SHIFT(TIMESTAMP '2016-06-27 20:31:02.498', 'PT1H', -1) <= "__time" AND "__time" < TIME_SHIFT(TIMESTAMP '2016-06-27 21:31:02.498', 'PT1H', -1)`,
    );
  });
});
