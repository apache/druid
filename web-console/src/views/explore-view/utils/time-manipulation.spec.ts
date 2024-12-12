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

import { SqlExpression } from 'druid-query-toolkit';

import { decomposeTimeInInterval, shiftBackAndExpandTimeInExpression } from './time-manipulation';

describe('decomposeTimeInInterval', () => {
  it('works with TIME_IN_INTERVAL (date)', () => {
    expect(
      decomposeTimeInInterval(
        SqlExpression.parse(`TIME_IN_INTERVAL("__time", '2016-06-27/2016-06-28')`),
      ).toString(),
    ).toEqual(`TIMESTAMP '2016-06-27' <= "__time" AND "__time" < TIMESTAMP '2016-06-28'`);
  });

  it('works with TIME_IN_INTERVAL (date and time)', () => {
    expect(
      decomposeTimeInInterval(
        SqlExpression.parse(
          `TIME_IN_INTERVAL("__time", '2016-06-27T12:34:56/2016-06-28T12:34:56')`,
        ),
      ).toString(),
    ).toEqual(
      `TIMESTAMP '2016-06-27 12:34:56' <= "__time" AND "__time" < TIMESTAMP '2016-06-28 12:34:56'`,
    );
  });

  it('works with TIME_IN_INTERVAL (date and time, zulu)', () => {
    expect(
      decomposeTimeInInterval(
        SqlExpression.parse(
          `TIME_IN_INTERVAL("__time", '2016-06-27T12:34:56Z/2016-06-28T12:34:56Z')`,
        ),
      ).toString(),
    ).toEqual(
      `TIME_PARSE('2016-06-27 12:34:56', NULL, 'Etc/UTC') <= "__time" AND "__time" < TIME_PARSE('2016-06-28 12:34:56', NULL, 'Etc/UTC')`,
    );
  });
});

describe('shiftBackAndExpandTimeInExpression', () => {
  describe('no expand', () => {
    it('works with TIME_IN_INTERVAL (date)', () => {
      expect(
        shiftBackAndExpandTimeInExpression(
          SqlExpression.parse(`TIME_IN_INTERVAL("__time", '2016-06-27/2016-06-28')`),
          'P1D',
          undefined,
        ).toString(),
      ).toEqual(
        `TIME_SHIFT(TIMESTAMP '2016-06-27', 'P1D', -1) <= "__time" AND "__time" < TIME_SHIFT(TIMESTAMP '2016-06-28', 'P1D', -1)`,
      );
    });

    it('works with TIME_IN_INTERVAL (date and time)', () => {
      expect(
        shiftBackAndExpandTimeInExpression(
          SqlExpression.parse(
            `TIME_IN_INTERVAL("__time", '2016-06-27T12:34:56/2016-06-28T12:34:56')`,
          ),
          'P1D',
          undefined,
        ).toString(),
      ).toEqual(
        `TIME_SHIFT(TIMESTAMP '2016-06-27 12:34:56', 'P1D', -1) <= "__time" AND "__time" < TIME_SHIFT(TIMESTAMP '2016-06-28 12:34:56', 'P1D', -1)`,
      );
    });

    it('works with TIME_IN_INTERVAL (date and time, zulu)', () => {
      expect(
        shiftBackAndExpandTimeInExpression(
          SqlExpression.parse(
            `TIME_IN_INTERVAL("__time", '2016-06-27T12:34:56Z/2016-06-28T12:34:56Z')`,
          ),
          'P1D',
          undefined,
        ).toString(),
      ).toEqual(
        `TIME_SHIFT(TIME_PARSE('2016-06-27 12:34:56', NULL, 'Etc/UTC'), 'P1D', -1) <= "__time" AND "__time" < TIME_SHIFT(TIME_PARSE('2016-06-28 12:34:56', NULL, 'Etc/UTC'), 'P1D', -1)`,
      );
    });

    it('works with relative time', () => {
      expect(
        shiftBackAndExpandTimeInExpression(
          SqlExpression.parse(
            `(TIME_SHIFT(MAX_DATA_TIME(), 'PT1H', -1) <= "__time" AND "__time" < MAX_DATA_TIME())`,
          ),
          'PT1H',
          undefined,
        ).toString(),
      ).toEqual(
        `(TIME_SHIFT(TIME_SHIFT(MAX_DATA_TIME(), 'PT1H', -1), 'PT1H', -1) <= "__time" AND "__time" < TIME_SHIFT(MAX_DATA_TIME(), 'PT1H', -1))`,
      );
    });

    it('works with specific timestamps', () => {
      expect(
        shiftBackAndExpandTimeInExpression(
          SqlExpression.parse(
            `TIMESTAMP '2016-06-27 20:31:02.498' <= "__time" AND "__time" < TIMESTAMP '2016-06-27 21:31:02.498'`,
          ),
          'PT1H',
          undefined,
        ).toString(),
      ).toEqual(
        `TIME_SHIFT(TIMESTAMP '2016-06-27 20:31:02.498', 'PT1H', -1) <= "__time" AND "__time" < TIME_SHIFT(TIMESTAMP '2016-06-27 21:31:02.498', 'PT1H', -1)`,
      );
    });

    it('works with specific timestamps (between)', () => {
      expect(
        shiftBackAndExpandTimeInExpression(
          SqlExpression.parse(
            `"__time" BETWEEN TIMESTAMP '2016-06-27 20:31:02.498' AND TIMESTAMP '2016-06-27 21:31:02.498'`,
          ),
          'PT1H',
          undefined,
        ).toString(),
      ).toEqual(
        `"__time" BETWEEN TIME_SHIFT(TIMESTAMP '2016-06-27 20:31:02.498', 'PT1H', -1) AND TIME_SHIFT(TIMESTAMP '2016-06-27 21:31:02.498', 'PT1H', -1)`,
      );
    });
  });

  describe('expand hour', () => {
    it('works with TIME_IN_INTERVAL (date)', () => {
      expect(
        shiftBackAndExpandTimeInExpression(
          SqlExpression.parse(`TIME_IN_INTERVAL("__time", '2016-06-27/2016-06-28')`),
          'P1D',
          'PT1H',
        ).toString(),
      ).toEqual(
        `TIME_FLOOR(TIME_SHIFT(TIMESTAMP '2016-06-27', 'P1D', -1), 'PT1H') <= "__time" AND "__time" < TIME_CEIL(TIME_SHIFT(TIMESTAMP '2016-06-28', 'P1D', -1), 'PT1H')`,
      );
    });

    it('works with TIME_IN_INTERVAL (date and time)', () => {
      expect(
        shiftBackAndExpandTimeInExpression(
          SqlExpression.parse(
            `TIME_IN_INTERVAL("__time", '2016-06-27T12:34:56/2016-06-28T12:34:56')`,
          ),
          'P1D',
          'PT1H',
        ).toString(),
      ).toEqual(
        `TIME_FLOOR(TIME_SHIFT(TIMESTAMP '2016-06-27 12:34:56', 'P1D', -1), 'PT1H') <= "__time" AND "__time" < TIME_CEIL(TIME_SHIFT(TIMESTAMP '2016-06-28 12:34:56', 'P1D', -1), 'PT1H')`,
      );
    });

    it('works with TIME_IN_INTERVAL (date and time, zulu)', () => {
      expect(
        shiftBackAndExpandTimeInExpression(
          SqlExpression.parse(
            `TIME_IN_INTERVAL("__time", '2016-06-27T12:34:56Z/2016-06-28T12:34:56Z')`,
          ),
          'P1D',
          'PT1H',
        ).toString(),
      ).toEqual(
        `TIME_FLOOR(TIME_SHIFT(TIME_PARSE('2016-06-27 12:34:56', NULL, 'Etc/UTC'), 'P1D', -1), 'PT1H') <= "__time" AND "__time" < TIME_CEIL(TIME_SHIFT(TIME_PARSE('2016-06-28 12:34:56', NULL, 'Etc/UTC'), 'P1D', -1), 'PT1H')`,
      );
    });

    it('works with relative time', () => {
      expect(
        shiftBackAndExpandTimeInExpression(
          SqlExpression.parse(
            `(TIME_SHIFT(MAX_DATA_TIME(), 'PT1H', -1) <= "__time" AND "__time" < MAX_DATA_TIME())`,
          ),
          'PT1H',
          'PT1H',
        ).toString(),
      ).toEqual(
        `(TIME_FLOOR(TIME_SHIFT(TIME_SHIFT(MAX_DATA_TIME(), 'PT1H', -1), 'PT1H', -1), 'PT1H') <= "__time" AND "__time" < TIME_CEIL(TIME_SHIFT(MAX_DATA_TIME(), 'PT1H', -1), 'PT1H'))`,
      );
    });

    it('works with specific timestamps', () => {
      expect(
        shiftBackAndExpandTimeInExpression(
          SqlExpression.parse(
            `TIMESTAMP '2016-06-27 20:31:02.498' <= "__time" AND "__time" < TIMESTAMP '2016-06-27 21:31:02.498'`,
          ),
          'PT1H',
          'PT1H',
        ).toString(),
      ).toEqual(
        `TIME_FLOOR(TIME_SHIFT(TIMESTAMP '2016-06-27 20:31:02.498', 'PT1H', -1), 'PT1H') <= "__time" AND "__time" < TIME_CEIL(TIME_SHIFT(TIMESTAMP '2016-06-27 21:31:02.498', 'PT1H', -1), 'PT1H')`,
      );
    });

    it('works with specific timestamps (between)', () => {
      expect(
        shiftBackAndExpandTimeInExpression(
          SqlExpression.parse(
            `"__time" BETWEEN TIMESTAMP '2016-06-27 20:31:02.498' AND TIMESTAMP '2016-06-27 21:31:02.498'`,
          ),
          'PT1H',
          'PT1H',
        ).toString(),
      ).toEqual(
        `"__time" BETWEEN TIME_FLOOR(TIME_SHIFT(TIMESTAMP '2016-06-27 20:31:02.498', 'PT1H', -1), 'PT1H') AND TIME_CEIL(TIME_SHIFT(TIMESTAMP '2016-06-27 21:31:02.498', 'PT1H', -1), 'PT1H')`,
      );
    });
  });
});
