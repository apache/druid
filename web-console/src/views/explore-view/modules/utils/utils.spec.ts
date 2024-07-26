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

import { getWhereForCompares, shiftTimeInExpression } from './utils';

describe('getWhereForCompares', () => {
  it('works', () => {
    expect(
      getWhereForCompares(
        SqlExpression.parse(
          `TIME_IN_INTERVAL("__time", '2016-06-27/2016-06-28') AND "country" = 'United States'`,
        ),
        ['PT1H', 'P1D'],
      ).toString(),
    ).toEqual(
      `(TIME_IN_INTERVAL("__time", '2016-06-27/2016-06-28') OR (TIME_SHIFT(TIMESTAMP '2016-06-27', 'PT1H', -1) <= "__time" AND "__time" < TIME_SHIFT(TIMESTAMP '2016-06-28', 'PT1H', -1)) OR (TIME_SHIFT(TIMESTAMP '2016-06-27', 'P1D', -1) <= "__time" AND "__time" < TIME_SHIFT(TIMESTAMP '2016-06-28', 'P1D', -1))) AND "country" = 'United States'`,
    );
  });
});

describe('shiftTimeInExpression', () => {
  it('works with TIME_IN_INTERVAL (date)', () => {
    expect(
      shiftTimeInExpression(
        SqlExpression.parse(`TIME_IN_INTERVAL("__time", '2016-06-27/2016-06-28')`),
        'P1D',
      ).toString(),
    ).toEqual(
      `TIME_SHIFT(TIMESTAMP '2016-06-27', 'P1D', -1) <= "__time" AND "__time" < TIME_SHIFT(TIMESTAMP '2016-06-28', 'P1D', -1)`,
    );
  });

  it('works with TIME_IN_INTERVAL (date and time)', () => {
    expect(
      shiftTimeInExpression(
        SqlExpression.parse(
          `TIME_IN_INTERVAL("__time", '2016-06-27T12:34:56/2016-06-28T12:34:56')`,
        ),
        'P1D',
      ).toString(),
    ).toEqual(
      `TIME_SHIFT(TIMESTAMP '2016-06-27 12:34:56', 'P1D', -1) <= "__time" AND "__time" < TIME_SHIFT(TIMESTAMP '2016-06-28 12:34:56', 'P1D', -1)`,
    );
  });

  it('works with TIME_IN_INTERVAL (date and time, zulu)', () => {
    expect(
      shiftTimeInExpression(
        SqlExpression.parse(
          `TIME_IN_INTERVAL("__time", '2016-06-27T12:34:56Z/2016-06-28T12:34:56Z')`,
        ),
        'P1D',
      ).toString(),
    ).toEqual(
      `TIME_SHIFT(TIME_PARSE('2016-06-27 12:34:56', NULL, 'Etc/UTC'), 'P1D', -1) <= "__time" AND "__time" < TIME_SHIFT(TIME_PARSE('2016-06-28 12:34:56', NULL, 'Etc/UTC'), 'P1D', -1)`,
    );
  });

  it('works with relative time', () => {
    expect(
      shiftTimeInExpression(
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
      shiftTimeInExpression(
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
