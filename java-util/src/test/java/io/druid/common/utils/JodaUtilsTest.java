/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.common.utils;

import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.JodaUtils;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 */
public class JodaUtilsTest
{
  @Test
  public void testUmbrellaIntervalsSimple()
  {
    List<Interval> intervals = Arrays.asList(
        Intervals.of("2011-03-03/2011-03-04"),
        Intervals.of("2011-01-01/2011-01-02"),
        Intervals.of("2011-02-01/2011-02-05"),
        Intervals.of("2011-02-03/2011-02-08"),
        Intervals.of("2011-01-01/2011-01-03"),
        Intervals.of("2011-03-01/2011-03-02"),
        Intervals.of("2011-03-05/2011-03-06"),
        Intervals.of("2011-02-01/2011-02-02")
    );

    Assert.assertEquals(
        Intervals.of("2011-01-01/2011-03-06"),
        JodaUtils.umbrellaInterval(intervals)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUmbrellaIntervalsNull()
  {
    JodaUtils.umbrellaInterval(Collections.emptyList());
  }

  @Test
  public void testCondenseIntervalsSimple()
  {
    List<Interval> intervals = Arrays.asList(
        Intervals.of("2011-01-01/2011-01-02"),
        Intervals.of("2011-01-02/2011-01-03"),
        Intervals.of("2011-02-01/2011-02-05"),
        Intervals.of("2011-02-01/2011-02-02"),
        Intervals.of("2011-02-03/2011-02-08"),
        Intervals.of("2011-03-01/2011-03-02"),
        Intervals.of("2011-03-03/2011-03-04"),
        Intervals.of("2011-03-05/2011-03-06")
    );

    Assert.assertEquals(
        Arrays.asList(
            Intervals.of("2011-01-01/2011-01-03"),
            Intervals.of("2011-02-01/2011-02-08"),
            Intervals.of("2011-03-01/2011-03-02"),
            Intervals.of("2011-03-03/2011-03-04"),
            Intervals.of("2011-03-05/2011-03-06")
        ),
        JodaUtils.condenseIntervals(intervals)
    );
  }

  @Test
  public void testCondenseIntervalsMixedUp()
  {
    List<Interval> intervals = Arrays.asList(
        Intervals.of("2011-01-01/2011-01-02"),
        Intervals.of("2011-01-02/2011-01-03"),
        Intervals.of("2011-02-01/2011-02-05"),
        Intervals.of("2011-02-01/2011-02-02"),
        Intervals.of("2011-02-03/2011-02-08"),
        Intervals.of("2011-03-01/2011-03-02"),
        Intervals.of("2011-03-03/2011-03-04"),
        Intervals.of("2011-03-05/2011-03-06"),
        Intervals.of("2011-04-01/2011-04-05"),
        Intervals.of("2011-04-02/2011-04-03"),
        Intervals.of("2011-05-01/2011-05-05"),
        Intervals.of("2011-05-02/2011-05-07")
    );

    for (int i = 0; i < 20; ++i) {
      Collections.shuffle(intervals);
      Assert.assertEquals(
          Arrays.asList(
              Intervals.of("2011-01-01/2011-01-03"),
              Intervals.of("2011-02-01/2011-02-08"),
              Intervals.of("2011-03-01/2011-03-02"),
              Intervals.of("2011-03-03/2011-03-04"),
              Intervals.of("2011-03-05/2011-03-06"),
              Intervals.of("2011-04-01/2011-04-05"),
              Intervals.of("2011-05-01/2011-05-07")
          ),
          JodaUtils.condenseIntervals(intervals)
      );
    }
  }

  @Test
  public void testMinMaxInterval()
  {
    Assert.assertEquals(Long.MAX_VALUE, Intervals.ETERNITY.toDuration().getMillis());
  }

  @Test
  public void testMinMaxDuration()
  {
    final Duration duration = Intervals.ETERNITY.toDuration();
    Assert.assertEquals(Long.MAX_VALUE, duration.getMillis());
    Assert.assertEquals("PT9223372036854775.807S", duration.toString());
  }

  // new Period(Long.MAX_VALUE) throws ArithmeticException
  @Test(expected = ArithmeticException.class)
  public void testMinMaxPeriod()
  {
    final Period period = Intervals.ETERNITY.toDuration().toPeriod();
    Assert.assertEquals(Long.MAX_VALUE, period.getMinutes());
  }

}
