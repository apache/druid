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

import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class ReindexingGranularityRuleTest
{
  private static final DateTime REFERENCE_TIME = new DateTime("2025-12-19T12:00:00Z");
  private static final Period PERIOD_7_DAYS = Period.days(7);

  private final ReindexingGranularityRule rule = new ReindexingGranularityRule(
      "test-rule",
      "Test granularity rule",
      PERIOD_7_DAYS,
      new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null)
  );

  @Test
  public void test_isAdditive_returnsFalse()
  {
    // Granularity rules are not additive - only one granularity can apply
    Assert.assertFalse(rule.isAdditive());
  }

  @Test
  public void test_appliesTo_intervalFullyBeforeThreshold_returnsFull()
  {
    // Threshold is 2025-12-12T12:00:00Z (7 days before reference time)
    // Interval ends at 2025-12-10, which is fully before threshold
    Interval interval = new Interval("2025-12-09T00:00:00Z/2025-12-10T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(ReindexingRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalEndsAtThreshold_returnsFull()
  {
    // Threshold is 2025-12-12T12:00:00Z (7 days before reference time)
    // Interval ends exactly at threshold - should be FULL (boundary case)
    Interval interval = new Interval("2025-12-11T12:00:00Z/2025-12-12T12:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(ReindexingRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalSpansThreshold_returnsPartial()
  {
    // Threshold is 2025-12-12T12:00:00Z (7 days before reference time)
    // Interval starts before threshold and ends after - PARTIAL
    Interval interval = new Interval("2025-12-11T00:00:00Z/2025-12-13T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(ReindexingRule.AppliesToMode.PARTIAL, result);
  }

  @Test
  public void test_appliesTo_intervalStartsAfterThreshold_returnsNone()
  {
    // Threshold is 2025-12-12T12:00:00Z (7 days before reference time)
    // Interval starts after threshold - NONE
    Interval interval = new Interval("2025-12-18T00:00:00Z/2025-12-19T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(ReindexingRule.AppliesToMode.NONE, result);
  }

  @Test
  public void test_getGranularityConfig_returnsConfiguredValue()
  {
    UserCompactionTaskGranularityConfig config = rule.getGranularityConfig();

    Assert.assertNotNull(config);
    Assert.assertEquals(Granularities.HOUR, config.getSegmentGranularity());
  }

  @Test
  public void test_getId_returnsConfiguredId()
  {
    Assert.assertEquals("test-rule", rule.getId());
  }

  @Test
  public void test_getDescription_returnsConfiguredDescription()
  {
    Assert.assertEquals("Test granularity rule", rule.getDescription());
  }

  @Test
  public void test_getPeriod_returnsConfiguredPeriod()
  {
    Assert.assertEquals(PERIOD_7_DAYS, rule.getPeriod());
  }

  @Test
  public void test_constructor_nullId_throwsNullPointerException()
  {
    UserCompactionTaskGranularityConfig config = new UserCompactionTaskGranularityConfig(
        Granularities.HOUR,
        null,
        null
    );
    Assert.assertThrows(
        NullPointerException.class,
        () -> new ReindexingGranularityRule(null, "description", PERIOD_7_DAYS, config)
    );
  }

  @Test
  public void test_constructor_nullPeriod_throwsNullPointerException()
  {
    UserCompactionTaskGranularityConfig config = new UserCompactionTaskGranularityConfig(
        Granularities.HOUR,
        null,
        null
    );
    Assert.assertThrows(
        NullPointerException.class,
        () -> new ReindexingGranularityRule("test-id", "description", null, config)
    );
  }

  @Test
  public void test_constructor_zeroPeriod_throwsIllegalArgumentException()
  {
    UserCompactionTaskGranularityConfig config = new UserCompactionTaskGranularityConfig(
        Granularities.HOUR,
        null,
        null
    );
    Period zeroPeriod = Period.days(0);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new ReindexingGranularityRule("test-id", "description", zeroPeriod, config)
    );
  }

  @Test
  public void test_constructor_negativePeriod_throwsIllegalArgumentException()
  {
    UserCompactionTaskGranularityConfig config = new UserCompactionTaskGranularityConfig(
        Granularities.HOUR,
        null,
        null
    );
    Period negativePeriod = Period.days(-7);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new ReindexingGranularityRule("test-id", "description", negativePeriod, config)
    );
  }

  @Test
  public void test_constructor_nullGranularityConfig_throwsNullPointerException()
  {
    Assert.assertThrows(
        NullPointerException.class,
        () -> new ReindexingGranularityRule("test-id", "description", PERIOD_7_DAYS, null)
    );
  }
}
