/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.coordinator.rules;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RulesEligibleIntervalTest
{
  private final DateTime referenceTime;

  @Parameterized.Parameters
  public static Object[] getReferenceTimestamps()
  {
    final DateTime now = DateTimes.nowUtc();
    return new Object[]{
        now,
        now.minusYears(5),
        now.plusYears(10),
        DateTimes.utc(0),
        DateTimes.utc(JodaUtils.MIN_INSTANT + 1),
        DateTimes.utc(JodaUtils.MIN_INSTANT),
        DateTimes.utc(JodaUtils.MAX_INSTANT - 1),
        DateTimes.utc(JodaUtils.MAX_INSTANT)
    };
  }

  public RulesEligibleIntervalTest(final DateTime referenceTimestamp)
  {
    this.referenceTime = referenceTimestamp;
  }

  @Test
  public void testPeriodLoadEligibleInterval()
  {
    final Period period = new Period("P50Y");
    final PeriodLoadRule loadPT1H = new PeriodLoadRule(
        period,
        true,
        null,
        null
    );
    Assert.assertEquals(
        new Interval(this.referenceTime.minus(period), DateTimes.MAX),
        loadPT1H.getEligibleInterval(this.referenceTime)
    );
  }

  @Test
  public void testPeriodLoadExcludingFutureEligibleInterval()
  {
    final Period period = new Period("PT1H");
    final PeriodLoadRule rule = new PeriodLoadRule(
        period,
        false,
        null,
        null
    );
    Assert.assertEquals(new Interval(period, this.referenceTime), rule.getEligibleInterval(this.referenceTime));
  }

  @Test
  public void testIntervalLoadEligibleInterval()
  {
    final Interval interval = Intervals.of("2000/3000");
    final IntervalLoadRule rule = new IntervalLoadRule(
        interval,
        null,
        null
    );
    Assert.assertEquals(interval, rule.getEligibleInterval(this.referenceTime));
  }

  @Test
  public void testForeverLoadEligibleInterval()
  {
    final ForeverLoadRule rule = new ForeverLoadRule(ImmutableMap.of(), false);
    Assert.assertEquals(Intervals.ETERNITY, rule.getEligibleInterval(this.referenceTime));
  }

  @Test
  public void testPeriodDropEligibleInterval()
  {
    final Period period = new Period("P5000Y");
    final PeriodDropRule rule = new PeriodDropRule(
        period,
        true
    );
    Assert.assertEquals(
        new Interval(this.referenceTime.minus(period), DateTimes.utc(JodaUtils.MAX_INSTANT)),
        rule.getEligibleInterval(this.referenceTime)
    );
  }

  @Test
  public void testPeriodDropExcludingFutureEligibleInterval()
  {
    final Period period = new Period("P50Y");
    final PeriodDropRule rule = new PeriodDropRule(
        period,
        false
    );
    Assert.assertEquals(
        new Interval(this.referenceTime.minus(period), this.referenceTime),
        rule.getEligibleInterval(this.referenceTime)
    );
  }

  @Test
  public void testPeriodDropBeforeEligibleInterval()
  {
    final Period period = new Period("P50Y");
    final PeriodDropBeforeRule rule = new PeriodDropBeforeRule(period);

    if (this.referenceTime.minus(period).isBefore(DateTimes.MIN)) {
      Assert.assertEquals(
          new Interval(DateTimes.utc(Long.MIN_VALUE), this.referenceTime.minus(period)),
          rule.getEligibleInterval(this.referenceTime)
      );
    } else {
      Assert.assertEquals(
          new Interval(DateTimes.MIN, this.referenceTime.minus(period)),
          rule.getEligibleInterval(this.referenceTime)
      );
    }
  }

  @Test
  public void testForeverDropEligibleInterval()
  {
    final ForeverDropRule rule = new ForeverDropRule();
    Assert.assertEquals(Intervals.ETERNITY, rule.getEligibleInterval(this.referenceTime));
  }

  @Test
  public void testPeriodBroadcastEligibleInterval()
  {
    final Period period = new Period("P15Y");
    final PeriodBroadcastDistributionRule rule = new PeriodBroadcastDistributionRule(period, true);
    Assert.assertEquals(
        new Interval(referenceTime.minus(period), DateTimes.MAX),
        rule.getEligibleInterval(referenceTime)
    );
  }

  @Test
  public void testPeriodBroadcastExcludingFutureEligibleInterval()
  {
    final Period period = new Period("P15Y");
    final PeriodBroadcastDistributionRule rule = new PeriodBroadcastDistributionRule(period, false);
    Assert.assertEquals(new Interval(period, this.referenceTime), rule.getEligibleInterval(this.referenceTime));
  }

  @Test
  public void testForeverBroadcastEligibleInterval()
  {
    final ForeverBroadcastDistributionRule rule = new ForeverBroadcastDistributionRule();
    Assert.assertEquals(Intervals.ETERNITY, rule.getEligibleInterval(this.referenceTime));
  }

  @Test
  public void testIntervalBroadcastEligibleInterval()
  {
    final Interval interval = Intervals.of("1993/2070");
    final IntervalBroadcastDistributionRule rule = new IntervalBroadcastDistributionRule(interval);
    Assert.assertEquals(interval, rule.getEligibleInterval(this.referenceTime));
  }
}
