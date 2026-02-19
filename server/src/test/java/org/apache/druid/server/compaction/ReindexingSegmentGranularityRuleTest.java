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

package org.apache.druid.server.compaction;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReindexingSegmentGranularityRuleTest
{
  private static final DateTime REFERENCE_TIME = DateTimes.of("2025-12-19T12:00:00Z");
  private static final Period PERIOD_7_DAYS = Period.days(7);

  private final ReindexingSegmentGranularityRule rule = new ReindexingSegmentGranularityRule(
      "test-rule",
      "Test segment granularity rule",
      PERIOD_7_DAYS,
      Granularities.HOUR
  );

  @Test
  public void test_appliesTo_intervalFullyBeforeThreshold_returnsFull()
  {
    // Threshold is 2025-12-12T12:00:00Z (7 days before reference time)
    // Interval ends at 2025-12-10, which is fully before threshold
    Interval interval = Intervals.of("2025-12-09T00:00:00Z/2025-12-10T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalEndsAtThreshold_returnsFull()
  {
    // Threshold is 2025-12-12T12:00:00Z (7 days before reference time)
    // Interval ends exactly at threshold - should be FULL (boundary case)
    Interval interval = Intervals.of("2025-12-11T12:00:00Z/2025-12-12T12:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalSpansThreshold_returnsPartial()
  {
    // Threshold is 2025-12-12T12:00:00Z (7 days before reference time)
    // Interval starts before threshold and ends after - PARTIAL
    Interval interval = Intervals.of("2025-12-11T00:00:00Z/2025-12-13T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.PARTIAL, result);
  }

  @Test
  public void test_appliesTo_intervalStartsAfterThreshold_returnsNone()
  {
    // Threshold is 2025-12-12T12:00:00Z (7 days before reference time)
    // Interval starts after threshold - NONE
    Interval interval = Intervals.of("2025-12-18T00:00:00Z/2025-12-19T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.NONE, result);
  }

  @Test
  public void test_getGranularity_returnsConfiguredValue()
  {
    Granularity granularity = rule.getSegmentGranularity();

    Assertions.assertNotNull(granularity);
    Assertions.assertEquals(Granularities.HOUR, granularity);
  }

  @Test
  public void test_getId_returnsConfiguredId()
  {
    Assertions.assertEquals("test-rule", rule.getId());
  }

  @Test
  public void test_getDescription_returnsConfiguredDescription()
  {
    Assertions.assertEquals("Test segment granularity rule", rule.getDescription());
  }

  @Test
  public void test_getOlderThan_returnsConfiguredPeriod()
  {
    Assertions.assertEquals(PERIOD_7_DAYS, rule.getOlderThan());
  }

  @Test
  public void test_constructor_nullId_throwsNullPointerException()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new ReindexingSegmentGranularityRule(null, "description", PERIOD_7_DAYS, Granularities.HOUR)
    );
  }

  @Test
  public void test_constructor_nullPeriod_throwsNullPointerException()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new ReindexingSegmentGranularityRule("test-id", "description", null, Granularities.HOUR)
    );
  }

  @Test
  public void test_constructor_zeroPeriod_succeeds()
  {
    // P0D is valid - indicates rules that apply immediately to all data
    Period zeroPeriod = Period.days(0);
    ReindexingSegmentGranularityRule rule = new ReindexingSegmentGranularityRule(
        "test-id",
        "description",
        zeroPeriod,
        Granularities.HOUR
    );
    Assertions.assertEquals(zeroPeriod, rule.getOlderThan());
  }

  @Test
  public void test_constructor_negativePeriod_throwsIllegalArgumentException()
  {
    Period negativePeriod = Period.days(-7);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new ReindexingSegmentGranularityRule("test-id", "description", negativePeriod, Granularities.HOUR)
    );
  }

  @Test
  public void test_constructor_nullGranularity_throwsNullPointerException()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new ReindexingSegmentGranularityRule("test-id", "description", PERIOD_7_DAYS, null)
    );
  }

  @Test
  public void test_constructor_supportedGranularities_allSucceed()
  {
    Granularity[] supportedGranularities = {
        Granularities.MINUTE,
        Granularities.FIFTEEN_MINUTE,
        Granularities.HOUR,
        Granularities.DAY,
        Granularities.MONTH,
        Granularities.QUARTER,
        Granularities.YEAR
    };

    for (Granularity granularity : supportedGranularities) {
      ReindexingSegmentGranularityRule rule = new ReindexingSegmentGranularityRule(
          "test-id",
          "description",
          PERIOD_7_DAYS,
          granularity
      );
      Assertions.assertEquals(granularity, rule.getSegmentGranularity());
    }
  }

  @Test
  public void test_constructor_unsupportedGranularities_allThrowDruidException()
  {
    Granularity[] unsupportedGranularities = {
        Granularities.THIRTY_MINUTE,
        Granularities.SIX_HOUR,
        Granularities.EIGHT_HOUR,
        Granularities.WEEK,
        new PeriodGranularity(Period.days(3), null, DateTimeZone.UTC),  // Custom period
        new PeriodGranularity(Period.days(1), null, DateTimes.inferTzFromString("America/Los_Angeles"))  // With timezone
    };

    for (Granularity granularity : unsupportedGranularities) {
      DruidException exception = Assertions.assertThrows(
          DruidException.class,
          () -> new ReindexingSegmentGranularityRule("test-id", "description", PERIOD_7_DAYS, granularity)
      );
      Assertions.assertTrue(
          exception.getMessage().contains("Unsupported segment granularity"),
          "Expected exception message to contain 'Unsupported segment granularity' but got: " + exception.getMessage()
      );
    }
  }
}
