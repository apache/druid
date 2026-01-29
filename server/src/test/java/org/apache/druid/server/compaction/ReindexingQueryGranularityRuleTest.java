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
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class ReindexingQueryGranularityRuleTest
{
  private static final DateTime REFERENCE_TIME = DateTimes.of("2025-12-19T12:00:00Z");
  private static final Period PERIOD_7_DAYS = Period.days(7);

  private final ReindexingQueryGranularityRule rule = new ReindexingQueryGranularityRule(
      "test-rule",
      "Test query granularity rule",
      PERIOD_7_DAYS,
      Granularities.HOUR,
      true
  );

  @Test
  public void test_appliesTo_intervalFullyBeforeThreshold_returnsFull()
  {
    // Threshold is 2025-12-12T12:00:00Z (7 days before reference time)
    // Interval ends at 2025-12-10, which is fully before threshold
    Interval interval = Intervals.of("2025-12-09T00:00:00Z/2025-12-10T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(ReindexingRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalEndsAtThreshold_returnsFull()
  {
    // Threshold is 2025-12-12T12:00:00Z (7 days before reference time)
    // Interval ends exactly at threshold - should be FULL (boundary case)
    Interval interval = Intervals.of("2025-12-11T12:00:00Z/2025-12-12T12:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(ReindexingRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalSpansThreshold_returnsPartial()
  {
    // Threshold is 2025-12-12T12:00:00Z (7 days before reference time)
    // Interval starts before threshold and ends after - PARTIAL
    Interval interval = Intervals.of("2025-12-11T00:00:00Z/2025-12-13T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(ReindexingRule.AppliesToMode.PARTIAL, result);
  }

  @Test
  public void test_appliesTo_intervalStartsAfterThreshold_returnsNone()
  {
    // Threshold is 2025-12-12T12:00:00Z (7 days before reference time)
    // Interval starts after threshold - NONE
    Interval interval = Intervals.of("2025-12-18T00:00:00Z/2025-12-19T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(ReindexingRule.AppliesToMode.NONE, result);
  }

  @Test
  public void test_getQueryGranularity_returnsConfiguredValue()
  {
    Granularity granularity = rule.getQueryGranularity();

    Assert.assertNotNull(granularity);
    Assert.assertEquals(Granularities.HOUR, granularity);
  }

  @Test
  public void test_getRollup_returnsConfiguredValue()
  {
    Boolean rollup = rule.getRollup();

    Assert.assertNotNull(rollup);
    Assert.assertEquals(true, rollup);
  }

  @Test
  public void test_getRollup_returnsFalse_whenConfiguredFalse()
  {
    ReindexingQueryGranularityRule ruleWithoutRollup = new ReindexingQueryGranularityRule(
        "test-rule-no-rollup",
        "Test query granularity rule without rollup",
        PERIOD_7_DAYS,
        Granularities.HOUR,
        false
    );

    Boolean rollup = ruleWithoutRollup.getRollup();

    Assert.assertNotNull(rollup);
    Assert.assertEquals(false, rollup);
  }

  @Test
  public void test_getId_returnsConfiguredId()
  {
    Assert.assertEquals("test-rule", rule.getId());
  }

  @Test
  public void test_getDescription_returnsConfiguredDescription()
  {
    Assert.assertEquals("Test query granularity rule", rule.getDescription());
  }

  @Test
  public void test_getOlderThan_returnsConfiguredPeriod()
  {
    Assert.assertEquals(PERIOD_7_DAYS, rule.getOlderThan());
  }

  @Test
  public void test_constructor_nullId_throwsNullPointerException()
  {
    Assert.assertThrows(
        NullPointerException.class,
        () -> new ReindexingQueryGranularityRule(null, "description", PERIOD_7_DAYS, Granularities.HOUR, true)
    );
  }

  @Test
  public void test_constructor_nullPeriod_throwsNullPointerException()
  {
    Assert.assertThrows(
        NullPointerException.class,
        () -> new ReindexingQueryGranularityRule("test-id", "description", null, Granularities.HOUR, true)
    );
  }

  @Test
  public void test_constructor_zeroPeriod_throwsIllegalArgumentException()
  {
    Period zeroPeriod = Period.days(0);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new ReindexingQueryGranularityRule("test-id", "description", zeroPeriod, Granularities.HOUR, true)
    );
  }

  @Test
  public void test_constructor_negativePeriod_throwsIllegalArgumentException()
  {
    Period negativePeriod = Period.days(-7);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new ReindexingQueryGranularityRule("test-id", "description", negativePeriod, Granularities.HOUR, true)
    );
  }

  @Test
  public void test_constructor_nullQueryGranularity_throwsNullPointerException()
  {
    Assert.assertThrows(
        NullPointerException.class,
        () -> new ReindexingQueryGranularityRule("test-id", "description", PERIOD_7_DAYS, null, true)
    );
  }

  @Test
  public void test_constructor_nullRollup_throwsNullPointerException()
  {
    Assert.assertThrows(
        NullPointerException.class,
        () -> new ReindexingQueryGranularityRule("test-id", "description", PERIOD_7_DAYS, Granularities.HOUR, null)
    );
  }
}
