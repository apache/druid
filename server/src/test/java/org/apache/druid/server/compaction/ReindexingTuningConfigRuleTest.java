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

import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class ReindexingTuningConfigRuleTest
{
  private static final DateTime REFERENCE_TIME = new DateTime("2025-12-19T12:00:00Z");
  private static final Period PERIOD_21_DAYS = Period.days(21);

  private final ReindexingTuningConfigRule rule = new ReindexingTuningConfigRule(
      "test-tuning-rule",
      "Custom tuning config",
      PERIOD_21_DAYS,
      createTestTuningConfig()

  );

  @Test
  public void test_isAdditive_returnsFalse()
  {
    // Tuning config rules are not additive - only one tuning config can apply
    Assert.assertFalse(rule.isAdditive());
  }

  @Test
  public void test_appliesTo_intervalFullyBeforeThreshold_returnsFull()
  {
    // Threshold is 2025-11-28T12:00:00Z (21 days before reference time)
    // Interval ends at 2025-11-25, which is fully before threshold
    Interval interval = new Interval("2025-11-24T00:00:00Z/2025-11-25T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(ReindexingRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalEndsAtThreshold_returnsFull()
  {
    // Threshold is 2025-11-28T12:00:00Z (21 days before reference time)
    // Interval ends exactly at threshold - should be FULL (boundary case)
    Interval interval = new Interval("2025-11-27T12:00:00Z/2025-11-28T12:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(ReindexingRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalSpansThreshold_returnsPartial()
  {
    // Threshold is 2025-11-28T12:00:00Z (21 days before reference time)
    // Interval starts before threshold and ends after - PARTIAL
    Interval interval = new Interval("2025-11-27T00:00:00Z/2025-11-29T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(ReindexingRule.AppliesToMode.PARTIAL, result);
  }

  @Test
  public void test_appliesTo_intervalStartsAfterThreshold_returnsNone()
  {
    // Threshold is 2025-11-28T12:00:00Z (21 days before reference time)
    // Interval starts after threshold - NONE
    Interval interval = new Interval("2025-12-15T00:00:00Z/2025-12-16T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(ReindexingRule.AppliesToMode.NONE, result);
  }

  @Test
  public void test_getTuningConfig_returnsConfiguredValue()
  {
    UserCompactionTaskQueryTuningConfig config = rule.getTuningConfig();

    Assert.assertNotNull(config);
    Assert.assertNotNull(config.getPartitionsSpec());
  }

  @Test
  public void test_getId_returnsConfiguredId()
  {
    Assert.assertEquals("test-tuning-rule", rule.getId());
  }

  @Test
  public void test_getDescription_returnsConfiguredDescription()
  {
    Assert.assertEquals("Custom tuning config", rule.getDescription());
  }

  @Test
  public void test_getPeriod_returnsConfiguredPeriod()
  {
    Assert.assertEquals(PERIOD_21_DAYS, rule.getPeriod());
  }

  @Test
  public void test_constructor_nullId_throwsNullPointerException()
  {
    Assert.assertThrows(
        NullPointerException.class,
        () -> new ReindexingTuningConfigRule(null, "description", PERIOD_21_DAYS, createTestTuningConfig())
    );
  }

  @Test
  public void test_constructor_nullPeriod_throwsNullPointerException()
  {
    Assert.assertThrows(
        NullPointerException.class,
        () -> new ReindexingTuningConfigRule("test-id", "description", null, createTestTuningConfig())
    );
  }

  @Test
  public void test_constructor_zeroPeriod_throwsIllegalArgumentException()
  {
    Period zeroPeriod = Period.days(0);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new ReindexingTuningConfigRule("test-id", "description", zeroPeriod, createTestTuningConfig())
    );
  }

  @Test
  public void test_constructor_negativePeriod_throwsIllegalArgumentException()
  {
    Period negativePeriod = Period.days(-21);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new ReindexingTuningConfigRule("test-id", "description", negativePeriod, createTestTuningConfig())
    );
  }

  @Test
  public void test_constructor_nullTuningConfig_throwsNullPointerException()
  {
    Assert.assertThrows(
        NullPointerException.class,
        () -> new ReindexingTuningConfigRule("test-id", "description", PERIOD_21_DAYS, null)
    );
  }

  private UserCompactionTaskQueryTuningConfig createTestTuningConfig()
  {
    return new UserCompactionTaskQueryTuningConfig(
        null,
        null,
        null,
        null,
        null,
        new DynamicPartitionsSpec(5000000, null),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }
}
