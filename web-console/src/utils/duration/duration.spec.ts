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

import { Duration } from './duration';

describe('Duration', () => {
  const TZ_LA = 'America/Los_Angeles';
  const TZ_JUNEAU = 'America/Juneau';

  describe('errors', () => {
    it('throws error if invalid duration', () => {
      expect(() => new Duration('')).toThrow("Can not parse duration ''");

      expect(() => new Duration('P00')).toThrow("Can not parse duration 'P00'");

      expect(() => new Duration('P')).toThrow('Duration can not be empty');

      expect(() => new Duration('P0YT0H')).toThrow('Duration can not be empty');

      expect(() => new Duration('P0W').shift(new Date(), TZ_LA)).toThrow(
        'Duration can not have empty weeks',
      );

      expect(() => new Duration('P0Y0MT0H0M0S').shift(new Date(), TZ_LA)).toThrow(
        'Duration can not be empty',
      );
    });

    it('throws error if fromJS is not given a string', () => {
      expect(() => new Duration(new Date() as any)).toThrow('Duration can not be empty');
    });
  });

  describe('#toString', () => {
    it('gives back the correct string', () => {
      let durationStr: string;

      durationStr = 'P3Y';
      expect(new Duration(durationStr).toString()).toEqual(durationStr);

      durationStr = 'P2W';
      expect(new Duration(durationStr).toString()).toEqual(durationStr);

      durationStr = 'PT5H';
      expect(new Duration(durationStr).toString()).toEqual(durationStr);

      durationStr = 'P3DT15H';
      expect(new Duration(durationStr).toString()).toEqual(durationStr);
    });

    it('eliminates 0', () => {
      expect(new Duration('P0DT15H').toString()).toEqual('PT15H');
    });
  });

  describe('fromCanonicalLength', () => {
    it('handles zero', () => {
      expect(() => {
        Duration.fromCanonicalLength(0);
      }).toThrow('length must be positive');
    });

    it('works 1', () => {
      expect(Duration.fromCanonicalLength(86400000).toString()).toEqual('P1D');
    });

    it('works 2', () => {
      const len =
        new Date('2018-03-01T00:00:00Z').valueOf() - new Date('2016-02-22T00:00:00Z').valueOf();
      expect(Duration.fromCanonicalLength(len).toString()).toEqual('P2Y8D');
    });

    it('works 3', () => {
      const len =
        new Date('2018-09-15T00:00:00Z').valueOf() - new Date('2018-09-04T00:00:00Z').valueOf();
      expect(Duration.fromCanonicalLength(len).toString()).toEqual('P11D');
    });

    it('works with months', () => {
      expect(Duration.fromCanonicalLength(2592000000).toString()).toEqual('P1M');
      expect(Duration.fromCanonicalLength(2678400000).toString()).toEqual('P1M1D');
    });

    it('works without months', () => {
      expect(Duration.fromCanonicalLength(2592000000, true).toString()).toEqual('P30D');
      expect(Duration.fromCanonicalLength(2678400000, true).toString()).toEqual('P31D');
    });
  });

  describe('construct from span', () => {
    it('parses days over DST', () => {
      expect(
        Duration.fromRange(
          new Date('2012-10-29T00:00:00-07:00'),
          new Date('2012-11-05T00:00:00-08:00'),
          TZ_LA,
        ).toString(),
      ).toEqual('P7D');

      expect(
        Duration.fromRange(
          new Date('2012-10-29T00:00:00-07:00'),
          new Date('2012-11-12T00:00:00-08:00'),
          TZ_LA,
        ).toString(),
      ).toEqual('P14D');
    });

    it('parses complex case', () => {
      expect(
        Duration.fromRange(
          new Date('2012-10-29T00:00:00-07:00'),
          new Date(new Date('2012-11-05T00:00:00-08:00').valueOf() - 1000),
          TZ_LA,
        ).toString(),
      ).toEqual('P6DT24H59M59S');

      expect(
        Duration.fromRange(
          new Date('2012-01-01T00:00:00-08:00'),
          new Date('2013-03-04T04:05:06-08:00'),
          TZ_LA,
        ).toString(),
      ).toEqual('P1Y2M3DT4H5M6S');
    });
  });

  describe('#isFloorable', () => {
    const floorable = 'P1Y P5Y P10Y P100Y P1M P2M P3M P4M P1D'.split(' ');
    for (const v of floorable) {
      it(`works on floorable ${v}`, () => {
        expect(new Duration(v).isFloorable()).toEqual(true);
      });
    }

    const unfloorable = 'P1Y1M P5M P2D P3D'.split(' ');
    for (const v of unfloorable) {
      it(`works on not floorable ${v}`, () => {
        expect(new Duration(v).isFloorable()).toEqual(false);
      });
    }
  });

  describe('#floor', () => {
    it('throws error if complex duration', () => {
      expect(() => new Duration('P1Y2D').floor(new Date(), TZ_LA)).toThrow(
        'Can not floor on a complex duration',
      );

      expect(() => new Duration('P3DT15H').floor(new Date(), TZ_LA)).toThrow(
        'Can not floor on a complex duration',
      );

      expect(() => new Duration('PT5H').floor(new Date(), TZ_LA)).toThrow(
        'Can not floor on a hour duration that does not divide into 24',
      );
    });

    it('works for year', () => {
      const p1y = new Duration('P1Y');
      expect(p1y.floor(new Date('2013-09-29T01:02:03.456-07:00'), TZ_LA)).toEqual(
        new Date('2013-01-01T00:00:00.000-08:00'),
      );
    });

    it('works for PT2M', () => {
      const pt2h = new Duration('PT2M');
      expect(pt2h.floor(new Date('2013-09-29T03:03:03.456-07:00'), TZ_LA)).toEqual(
        new Date('2013-09-29T03:02:00.000-07:00'),
      );
    });

    it('works for P2H', () => {
      const pt2h = new Duration('PT2H');
      expect(pt2h.floor(new Date('2013-09-29T03:02:03.456-07:00'), TZ_LA)).toEqual(
        new Date('2013-09-29T02:00:00.000-07:00'),
      );
    });

    it('works for PT12H', () => {
      const pt12h = new Duration('PT12H');
      expect(pt12h.floor(new Date('2015-09-12T13:05:00-08:00'), TZ_JUNEAU)).toEqual(
        new Date('2015-09-12T12:00:00-08:00'),
      );
    });

    it('works for P1W', () => {
      const p1w = new Duration('P1W');

      expect(p1w.floor(new Date('2013-09-29T01:02:03.456-07:00'), TZ_LA)).toEqual(
        new Date('2013-09-23T07:00:00.000Z'),
      );

      expect(p1w.floor(new Date('2013-10-03T01:02:03.456-07:00'), TZ_LA)).toEqual(
        new Date('2013-09-30T00:00:00.000-07:00'),
      );
    });

    it('works for P3M', () => {
      const p3m = new Duration('P3M');
      expect(p3m.floor(new Date('2013-09-29T03:02:03.456-07:00'), TZ_LA)).toEqual(
        new Date('2013-07-01T00:00:00.000-07:00'),
      );

      expect(p3m.floor(new Date('2013-02-29T03:02:03.456-07:00'), TZ_LA)).toEqual(
        new Date('2013-01-01T00:00:00.000-08:00'),
      );
    });

    it('works for P4Y', () => {
      const p4y = new Duration('P4Y');
      expect(p4y.floor(new Date('2013-09-29T03:02:03.456-07:00'), TZ_LA)).toEqual(
        new Date('2012-01-01T00:00:00.000-08:00'),
      );
    });
  });

  describe('#shift', () => {
    it('works for weeks', () => {
      let p1w = new Duration('P1W');
      expect(p1w.shift(new Date('2012-10-29T00:00:00-07:00'), TZ_LA)).toEqual(
        new Date('2012-11-05T00:00:00-08:00'),
      );

      p1w = new Duration('P1W');
      expect(p1w.shift(new Date('2012-10-29T00:00:00-07:00'), TZ_LA, 2)).toEqual(
        new Date('2012-11-12T00:00:00-08:00'),
      );

      const p2w = new Duration('P2W');
      expect(p2w.shift(new Date('2012-10-29T05:16:17-07:00'), TZ_LA)).toEqual(
        new Date('2012-11-12T05:16:17-08:00'),
      );
    });

    it('works for general complex case', () => {
      const pComplex = new Duration('P1Y2M3DT4H5M6S');
      expect(pComplex.shift(new Date('2012-01-01T00:00:00-08:00'), TZ_LA)).toEqual(
        new Date('2013-03-04T04:05:06-08:00'),
      );
    });
  });

  describe('#materialize', () => {
    it('works for weeks', () => {
      const p1w = new Duration('P1W');

      expect(
        p1w.materialize(
          new Date('2012-10-29T00:00:00-07:00'),
          new Date('2012-12-01T00:00:00-08:00'),
          TZ_LA,
        ),
      ).toEqual([
        new Date('2012-10-29T07:00:00.000Z'),
        new Date('2012-11-05T08:00:00.000Z'),
        new Date('2012-11-12T08:00:00.000Z'),
        new Date('2012-11-19T08:00:00.000Z'),
        new Date('2012-11-26T08:00:00.000Z'),
      ]);

      expect(
        p1w.materialize(
          new Date('2012-10-29T00:00:00-07:00'),
          new Date('2012-12-01T00:00:00-08:00'),
          TZ_LA,
          2,
        ),
      ).toEqual([
        new Date('2012-10-29T07:00:00.000Z'),
        new Date('2012-11-12T08:00:00.000Z'),
        new Date('2012-11-26T08:00:00.000Z'),
      ]);
    });
  });

  describe('#isAligned', () => {
    it('works for weeks', () => {
      const p1w = new Duration('P1W');
      expect(p1w.isAligned(new Date('2012-10-29T00:00:00-07:00'), TZ_LA)).toEqual(true);
      expect(p1w.isAligned(new Date('2012-10-29T00:00:00-07:00'), 'Etc/UTC')).toEqual(false);
    });
  });

  describe('#dividesBy', () => {
    const divisible = 'P5Y/P1Y P1D/P1D P1M/P1D P1W/P1D P1D/PT6H PT3H/PT1H'.split(' ');
    for (const v of divisible) {
      it(`works for ${v} (true)`, () => {
        const p = v.split('/');
        expect(new Duration(p[0]).dividesBy(new Duration(p[1]))).toEqual(true);
      });
    }

    const undivisible = 'P1D/P1M PT5H/PT1H'.split(' ');
    for (const v of undivisible) {
      it(`works for ${v} (false)`, () => {
        const p = v.split('/');
        expect(new Duration(p[0]).dividesBy(new Duration(p[1]))).toEqual(false);
      });
    }
  });

  describe('#getCanonicalLength', () => {
    it('gives back the correct canonical length', () => {
      let durationStr: string;

      durationStr = 'P3Y';
      expect(new Duration(durationStr).getCanonicalLength()).toEqual(94608000000);

      durationStr = 'P2W';
      expect(new Duration(durationStr).getCanonicalLength()).toEqual(1209600000);

      durationStr = 'PT5H';
      expect(new Duration(durationStr).getCanonicalLength()).toEqual(18000000);

      durationStr = 'P3DT15H';
      expect(new Duration(durationStr).getCanonicalLength()).toEqual(313200000);
    });
  });

  describe('#add()', () => {
    it('works with a simple duration', () => {
      const d1 = new Duration('P1D');
      const d2 = new Duration('P1D');

      expect(d1.add(d2).toString()).toEqual('P2D');
    });

    it('works with heterogeneous spans', () => {
      const d1 = new Duration('P1D');
      const d2 = new Duration('P1Y');

      expect(d1.add(d2).toString()).toEqual('P1Y1D');
    });

    it('works with weeks', () => {
      let d1 = new Duration('P1W');
      let d2 = new Duration('P2W');
      expect(d1.add(d2).toString()).toEqual('P3W');

      d1 = new Duration('P6D');
      d2 = new Duration('P1D');
      expect(d1.add(d2).toString()).toEqual('P1W');
    });
  });

  describe('#subtract()', () => {
    it('works with a simple duration', () => {
      const d1 = new Duration('P1DT2H');
      const d2 = new Duration('PT1H');

      expect(d1.subtract(d2).toString()).toEqual('P1DT1H');
    });

    it('works with a less simple duration', () => {
      const d1 = new Duration('P1D');
      const d2 = new Duration('PT1H');

      expect(d1.subtract(d2).toString()).toEqual('PT23H');
    });

    it('works with weeks', () => {
      const d1 = new Duration('P1W');
      const d2 = new Duration('P1D');

      expect(d1.subtract(d2).toString()).toEqual('P6D');
    });

    it('throws an error if result is going to be negative', () => {
      const d1 = new Duration('P1D');
      const d2 = new Duration('P2D');

      expect(() => d1.subtract(d2)).toThrow();
    });
  });

  describe('#multiply()', () => {
    it('works with a simple duration', () => {
      const d = new Duration('P1D');
      expect(d.multiply(5).toString()).toEqual('P5D');
    });

    it('works with a less simple duration', () => {
      const d = new Duration('P1DT2H');
      expect(d.multiply(2).toString()).toEqual('P2DT4H');
    });

    it('works with weeks', () => {
      const d = new Duration('P1W');
      expect(d.multiply(5).toString()).toEqual('P5W');
    });

    it('throws an error if result is going to be negative', () => {
      const d = new Duration('P1D');
      expect(() => d.multiply(-1)).toThrow('Multiplier must be positive non-zero');
    });

    it('gets description properly', () => {
      const d = new Duration('P2D');
      expect(d.multiply(2).getDescription(true)).toEqual('4 Days');
    });
  });

  describe('#getDescription()', () => {
    it('gives back the correct description', () => {
      let durationStr: string;

      durationStr = 'P1D';
      expect(new Duration(durationStr).getDescription()).toEqual('day');

      durationStr = 'P1DT2H';
      expect(new Duration(durationStr).getDescription()).toEqual('1 day, 2 hours');

      durationStr = 'P3Y';
      expect(new Duration(durationStr).getDescription()).toEqual('3 years');

      durationStr = 'P2W';
      expect(new Duration(durationStr).getDescription()).toEqual('2 weeks');

      durationStr = 'PT5H';
      expect(new Duration(durationStr).getDescription()).toEqual('5 hours');

      durationStr = 'P3DT15H';
      expect(new Duration(durationStr).getDescription()).toEqual('3 days, 15 hours');

      durationStr = 'P3DT15H';
      expect(new Duration(durationStr).getDescription(true)).toEqual('3 Days, 15 Hours');
    });
  });

  describe('#getSingleSpan()', () => {
    it('gives back the correct span', () => {
      let durationStr: string;

      durationStr = 'P1D';
      expect(new Duration(durationStr).getSingleSpan()).toEqual('day');

      durationStr = 'P3Y';
      expect(new Duration(durationStr).getSingleSpan()).toEqual('year');

      durationStr = 'P2W';
      expect(new Duration(durationStr).getSingleSpan()).toEqual('week');

      durationStr = 'PT5H';
      expect(new Duration(durationStr).getSingleSpan()).toEqual('hour');

      durationStr = 'P3DT15H';
      expect(new Duration(durationStr).getSingleSpan()).toBeUndefined();

      durationStr = 'P3DT15H';
      expect(new Duration(durationStr).getSingleSpan()).toBeUndefined();
    });
  });

  describe('#getSingleSpanValue()', () => {
    it('gives back the correct span value', () => {
      let durationStr: string;

      durationStr = 'P1D';
      expect(new Duration(durationStr).getSingleSpanValue()).toEqual(1);

      durationStr = 'P3Y';
      expect(new Duration(durationStr).getSingleSpanValue()).toEqual(3);

      durationStr = 'P2W';
      expect(new Duration(durationStr).getSingleSpanValue()).toEqual(2);

      durationStr = 'PT5H';
      expect(new Duration(durationStr).getSingleSpanValue()).toEqual(5);

      durationStr = 'P3DT15H';
      expect(new Duration(durationStr).getSingleSpanValue()).toBeUndefined();

      durationStr = 'P3DT15H';
      expect(new Duration(durationStr).getSingleSpanValue()).toBeUndefined();
    });
  });

  describe('#limitToDays', () => {
    it('works', () => {
      expect(new Duration('P6D').limitToDays().toString()).toEqual('P6D');

      expect(new Duration('P1M').limitToDays().toString()).toEqual('P30D');

      expect(new Duration('P1Y').limitToDays().toString()).toEqual('P365D');

      expect(new Duration('P1Y2M').limitToDays().toString()).toEqual('P425D');
    });
  });
});
