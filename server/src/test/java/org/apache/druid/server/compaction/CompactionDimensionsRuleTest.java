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

import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class CompactionDimensionsRuleTest
{
  private static final DateTime REFERENCE_TIME = new DateTime("2025-12-19T12:00:00Z");
  private static final Period PERIOD_14_DAYS = Period.days(14);

  private final CompactionDimensionsRule rule = new CompactionDimensionsRule(
      "test-dimensions-rule",
      "Custom dimensions config",
      PERIOD_14_DAYS,
      new UserCompactionTaskDimensionsConfig(null)
  );

  @Test
  public void test_isAdditive_returnsFalse()
  {
    // Dimensions rules are not additive - only one dimensions spec can apply
    Assert.assertFalse(rule.isAdditive());
  }

  @Test
  public void test_appliesTo_intervalFullyBeforeThreshold_returnsFull()
  {
    // Threshold is 2025-12-05T12:00:00Z (14 days before reference time)
    // Interval ends at 2025-12-03, which is fully before threshold
    Interval interval = new Interval("2025-12-02T00:00:00Z/2025-12-03T00:00:00Z");

    CompactionRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(CompactionRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalEndsAtThreshold_returnsFull()
  {
    // Threshold is 2025-12-05T12:00:00Z (14 days before reference time)
    // Interval ends exactly at threshold - should be FULL (boundary case)
    Interval interval = new Interval("2025-12-04T12:00:00Z/2025-12-05T12:00:00Z");

    CompactionRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(CompactionRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalSpansThreshold_returnsPartial()
  {
    // Threshold is 2025-12-05T12:00:00Z (14 days before reference time)
    // Interval starts before threshold and ends after - PARTIAL
    Interval interval = new Interval("2025-12-04T00:00:00Z/2025-12-06T00:00:00Z");

    CompactionRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(CompactionRule.AppliesToMode.PARTIAL, result);
  }

  @Test
  public void test_appliesTo_intervalStartsAfterThreshold_returnsNone()
  {
    // Threshold is 2025-12-05T12:00:00Z (14 days before reference time)
    // Interval starts after threshold - NONE
    Interval interval = new Interval("2025-12-18T00:00:00Z/2025-12-19T00:00:00Z");

    CompactionRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(CompactionRule.AppliesToMode.NONE, result);
  }

  @Test
  public void test_getDimensionsSpec_returnsConfiguredValue()
  {
    UserCompactionTaskDimensionsConfig spec = rule.getDimensionsSpec();

    Assert.assertNotNull(spec);
  }

  @Test
  public void test_getId_returnsConfiguredId()
  {
    Assert.assertEquals("test-dimensions-rule", rule.getId());
  }

  @Test
  public void test_getDescription_returnsConfiguredDescription()
  {
    Assert.assertEquals("Custom dimensions config", rule.getDescription());
  }

  @Test
  public void test_getPeriod_returnsConfiguredPeriod()
  {
    Assert.assertEquals(PERIOD_14_DAYS, rule.getPeriod());
  }

  @Test
  public void test_constructor_nullId_throwsNullPointerException()
  {
    UserCompactionTaskDimensionsConfig config = new UserCompactionTaskDimensionsConfig(null);
    Assert.assertThrows(
        NullPointerException.class,
        () -> new CompactionDimensionsRule(null, "description", PERIOD_14_DAYS, config)
    );
  }

  @Test
  public void test_constructor_nullPeriod_throwsNullPointerException()
  {
    UserCompactionTaskDimensionsConfig config = new UserCompactionTaskDimensionsConfig(null);
    Assert.assertThrows(
        NullPointerException.class,
        () -> new CompactionDimensionsRule("test-id", "description", null, config)
    );
  }

  @Test
  public void test_constructor_zeroPeriod_throwsIllegalArgumentException()
  {
    UserCompactionTaskDimensionsConfig config = new UserCompactionTaskDimensionsConfig(null);
    Period zeroPeriod = Period.days(0);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new CompactionDimensionsRule("test-id", "description", zeroPeriod, config)
    );
  }

  @Test
  public void test_constructor_negativePeriod_throwsIllegalArgumentException()
  {
    UserCompactionTaskDimensionsConfig config = new UserCompactionTaskDimensionsConfig(null);
    Period negativePeriod = Period.days(-14);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new CompactionDimensionsRule("test-id", "description", negativePeriod, config)
    );
  }

  @Test
  public void test_constructor_nullDimensionsSpec_throwsNullPointerException()
  {
    Assert.assertThrows(
        NullPointerException.class,
        () -> new CompactionDimensionsRule("test-id", "description", PERIOD_14_DAYS, null)
    );
  }
}
