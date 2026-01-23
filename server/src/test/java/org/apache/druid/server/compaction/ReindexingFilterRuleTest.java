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

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class ReindexingFilterRuleTest
{
  private static final DateTime REFERENCE_TIME = DateTimes.of("2025-12-19T12:00:00Z");
  private static final Period PERIOD_30_DAYS = Period.days(30);

  private final DimFilter testFilter = new SelectorDimFilter("isRobot", "true", null);
  private final ReindexingFilterRule rule = new ReindexingFilterRule(
      "test-filter-rule",
      "Remove robot traffic",
      PERIOD_30_DAYS,
      testFilter
  );

  @Test
  public void test_appliesTo_intervalFullyBeforeThreshold_returnsFull()
  {
    // Threshold is 2025-11-19T12:00:00Z (30 days before reference time)
    // Interval ends at 2025-11-15, which is fully before threshold
    Interval interval = Intervals.of("2025-11-14T00:00:00Z/2025-11-15T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(ReindexingRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalEndsAtThreshold_returnsFull()
  {
    // Threshold is 2025-11-19T12:00:00Z (30 days before reference time)
    // Interval ends exactly at threshold - should be FULL (boundary case)
    Interval interval = Intervals.of("2025-11-18T12:00:00Z/2025-11-19T12:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(ReindexingRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalSpansThreshold_returnsPartial()
  {
    // Threshold is 2025-11-19T12:00:00Z (30 days before reference time)
    // Interval starts before threshold and ends after - PARTIAL
    Interval interval = Intervals.of("2025-11-18T00:00:00Z/2025-11-20T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(ReindexingRule.AppliesToMode.PARTIAL, result);
  }

  @Test
  public void test_appliesTo_intervalStartsAfterThreshold_returnsNone()
  {
    // Threshold is 2025-11-19T12:00:00Z (30 days before reference time)
    // Interval starts after threshold - NONE
    Interval interval = Intervals.of("2025-12-15T00:00:00Z/2025-12-16T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(ReindexingRule.AppliesToMode.NONE, result);
  }

  @Test
  public void test_getFilter_returnsConfiguredFilter()
  {
    DimFilter filter = rule.getFilter();

    Assert.assertNotNull(filter);
    Assert.assertEquals(testFilter, filter);
  }

  @Test
  public void test_getId_returnsConfiguredId()
  {
    Assert.assertEquals("test-filter-rule", rule.getId());
  }

  @Test
  public void test_getDescription_returnsConfiguredDescription()
  {
    Assert.assertEquals("Remove robot traffic", rule.getDescription());
  }

  @Test
  public void test_getPeriod_returnsConfiguredPeriod()
  {
    Assert.assertEquals(PERIOD_30_DAYS, rule.getPeriod());
  }

  @Test
  public void test_constructor_nullId_throwsNullPointerException()
  {
    Assert.assertThrows(
        NullPointerException.class,
        () -> new ReindexingFilterRule(null, "description", PERIOD_30_DAYS, testFilter)
    );
  }

  @Test
  public void test_constructor_nullPeriod_throwsNullPointerException()
  {
    Assert.assertThrows(
        NullPointerException.class,
        () -> new ReindexingFilterRule("test-id", "description", null, testFilter)
    );
  }

  @Test
  public void test_constructor_zeroPeriod_throwsIllegalArgumentException()
  {
    Period zeroPeriod = Period.days(0);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new ReindexingFilterRule("test-id", "description", zeroPeriod, testFilter)
    );
  }

  @Test
  public void test_constructor_negativePeriod_throwsIllegalArgumentException()
  {
    Period negativePeriod = Period.days(-30);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new ReindexingFilterRule("test-id", "description", negativePeriod, testFilter)
    );
  }

  @Test
  public void test_constructor_nullFilter_throwsNullPointerException()
  {
    Assert.assertThrows(
        NullPointerException.class,
        () -> new ReindexingFilterRule("test-id", "description", PERIOD_30_DAYS, null)
    );
  }

  // ========== Tests for variable-length periods (months/years) ==========

  @Test
  public void test_constructor_periodWithMonths_succeeds()
  {
    // P6M should work - months are valid even though they're variable length
    Period period = Period.months(6);
    ReindexingFilterRule rule = new ReindexingFilterRule(
        "test-id",
        "6 month rule",
        period,
        testFilter
    );

    Assert.assertEquals(period, rule.getPeriod());
  }

  @Test
  public void test_constructor_periodWithYears_succeeds()
  {
    // P1Y should work - years are valid even though they're variable length
    Period period = Period.years(1);
    ReindexingFilterRule rule = new ReindexingFilterRule(
        "test-id",
        "1 year rule",
        period,
        testFilter
    );

    Assert.assertEquals(period, rule.getPeriod());
  }

  @Test
  public void test_constructor_periodWithMixedMonthsAndDays_succeeds()
  {
    // P6M15D should work - mixed months and days
    Period period = Period.months(6).plusDays(15);
    ReindexingFilterRule rule = new ReindexingFilterRule(
        "test-id",
        "6 months 15 days rule",
        period,
        testFilter
    );

    Assert.assertEquals(period, rule.getPeriod());
  }

  @Test
  public void test_constructor_periodWithYearsMonthsDays_succeeds()
  {
    // P1Y3M10D should work - complex period with years, months, and days
    Period period = Period.years(1).plusMonths(3).plusDays(10);
    ReindexingFilterRule rule = new ReindexingFilterRule(
        "test-id",
        "1 year 3 months 10 days rule",
        period,
        testFilter
    );

    Assert.assertEquals(period, rule.getPeriod());
  }

  @Test
  public void test_constructor_zeroMonthsPeriod_throwsIllegalArgumentException()
  {
    // P0M should fail - all components are zero/non-positive
    Period zeroPeriod = Period.months(0);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new ReindexingFilterRule("test-id", "description", zeroPeriod, testFilter)
    );
  }

  @Test
  public void test_constructor_negativeMonthsPeriod_throwsIllegalArgumentException()
  {
    // P-6M should fail - negative months
    Period negativePeriod = Period.months(-6);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new ReindexingFilterRule("test-id", "description", negativePeriod, testFilter)
    );
  }

  @Test
  public void test_appliesTo_periodWithMonths_calculatesThresholdCorrectly()
  {
    // Test that month-based periods correctly calculate threshold using calendar arithmetic
    // Reference time: 2025-12-19T12:00:00Z
    // Period: P6M (6 months)
    // Expected threshold: 2025-06-19T12:00:00Z (6 months before reference)

    Period sixMonths = Period.months(6);
    ReindexingFilterRule monthRule = new ReindexingFilterRule(
        "test-month-rule",
        "6 months rule",
        sixMonths,
        testFilter
    );

    // Interval ending before 6-month threshold - should be FULL
    Interval beforeThreshold = Intervals.of("2025-06-01T00:00:00Z/2025-06-15T00:00:00Z");
    Assert.assertEquals(
        ReindexingRule.AppliesToMode.FULL,
        monthRule.appliesTo(beforeThreshold, REFERENCE_TIME)
    );

    // Interval spanning the 6-month threshold - should be PARTIAL
    Interval spanningThreshold = Intervals.of("2025-06-15T00:00:00Z/2025-07-15T00:00:00Z");
    Assert.assertEquals(
        ReindexingRule.AppliesToMode.PARTIAL,
        monthRule.appliesTo(spanningThreshold, REFERENCE_TIME)
    );

    // Interval starting after 6-month threshold - should be NONE
    Interval afterThreshold = Intervals.of("2025-07-01T00:00:00Z/2025-07-15T00:00:00Z");
    Assert.assertEquals(
        ReindexingRule.AppliesToMode.NONE,
        monthRule.appliesTo(afterThreshold, REFERENCE_TIME)
    );
  }

  @Test
  public void test_appliesTo_periodWithYears_calculatesThresholdCorrectly()
  {
    // Test that year-based periods correctly calculate threshold using calendar arithmetic
    // Reference time: 2025-12-19T12:00:00Z
    // Period: P1Y (1 year)
    // Expected threshold: 2024-12-19T12:00:00Z (1 year before reference)

    Period oneYear = Period.years(1);
    ReindexingFilterRule yearRule = new ReindexingFilterRule(
        "test-year-rule",
        "1 year rule",
        oneYear,
        testFilter
    );

    // Interval ending before 1-year threshold - should be FULL
    Interval beforeThreshold = Intervals.of("2024-11-01T00:00:00Z/2024-12-01T00:00:00Z");
    Assert.assertEquals(
        ReindexingRule.AppliesToMode.FULL,
        yearRule.appliesTo(beforeThreshold, REFERENCE_TIME)
    );

    // Interval spanning the 1-year threshold - should be PARTIAL
    Interval spanningThreshold = Intervals.of("2024-12-01T00:00:00Z/2025-01-01T00:00:00Z");
    Assert.assertEquals(
        ReindexingRule.AppliesToMode.PARTIAL,
        yearRule.appliesTo(spanningThreshold, REFERENCE_TIME)
    );

    // Interval starting after 1-year threshold - should be NONE
    Interval afterThreshold = Intervals.of("2025-01-01T00:00:00Z/2025-02-01T00:00:00Z");
    Assert.assertEquals(
        ReindexingRule.AppliesToMode.NONE,
        yearRule.appliesTo(afterThreshold, REFERENCE_TIME)
    );
  }
}
