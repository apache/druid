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
import org.apache.druid.server.coordinator.UserCompactionTaskIOConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReindexingIOConfigRuleTest
{
  private static final DateTime REFERENCE_TIME = DateTimes.of("2025-12-19T12:00:00Z");
  private static final Period PERIOD_60_DAYS = Period.days(60);

  private final ReindexingIOConfigRule rule = new ReindexingIOConfigRule(
      "test-ioconfig-rule",
      "Custom IO config",
      PERIOD_60_DAYS,
      new UserCompactionTaskIOConfig(null)
  );

  @Test
  public void test_appliesTo_intervalFullyBeforeThreshold_returnsFull()
  {
    // Threshold is 2025-10-20T12:00:00Z (60 days before reference time)
    // Interval ends at 2025-10-15, which is fully before threshold
    Interval interval = Intervals.of("2025-10-14T00:00:00Z/2025-10-15T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalEndsAtThreshold_returnsFull()
  {
    // Threshold is 2025-10-20T12:00:00Z (60 days before reference time)
    // Interval ends exactly at threshold - should be FULL (boundary case)
    Interval interval = Intervals.of("2025-10-19T12:00:00Z/2025-10-20T12:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalSpansThreshold_returnsPartial()
  {
    // Threshold is 2025-10-20T12:00:00Z (60 days before reference time)
    // Interval starts before threshold and ends after - PARTIAL
    Interval interval = Intervals.of("2025-10-19T00:00:00Z/2025-10-21T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.PARTIAL, result);
  }

  @Test
  public void test_appliesTo_intervalStartsAfterThreshold_returnsNone()
  {
    // Threshold is 2025-10-20T12:00:00Z (60 days before reference time)
    // Interval starts after threshold - NONE
    Interval interval = Intervals.of("2025-12-15T00:00:00Z/2025-12-16T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.NONE, result);
  }

  @Test
  public void test_getIoConfig_returnsConfiguredValue()
  {
    UserCompactionTaskIOConfig config = rule.getIoConfig();

    Assertions.assertNotNull(config);
  }

  @Test
  public void test_getId_returnsConfiguredId()
  {
    Assertions.assertEquals("test-ioconfig-rule", rule.getId());
  }

  @Test
  public void test_getDescription_returnsConfiguredDescription()
  {
    Assertions.assertEquals("Custom IO config", rule.getDescription());
  }

  @Test
  public void test_getOlderThan_returnsConfiguredPeriod()
  {
    Assertions.assertEquals(PERIOD_60_DAYS, rule.getOlderThan());
  }

  @Test
  public void test_constructor_nullId_throwsNullPointerException()
  {
    UserCompactionTaskIOConfig config = new UserCompactionTaskIOConfig(null);
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new ReindexingIOConfigRule(null, "description", PERIOD_60_DAYS, config)
    );
  }

  @Test
  public void test_constructor_nullPeriod_throwsNullPointerException()
  {
    UserCompactionTaskIOConfig config = new UserCompactionTaskIOConfig(null);
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new ReindexingIOConfigRule("test-id", "description", null, config)
    );
  }

  @Test
  public void test_constructor_zeroPeriod_succeeds()
  {
    // P0D is valid - indicates rules that apply immediately to all data
    UserCompactionTaskIOConfig config = new UserCompactionTaskIOConfig(null);
    Period zeroPeriod = Period.days(0);
    ReindexingIOConfigRule rule = new ReindexingIOConfigRule(
        "test-id",
        "description",
        zeroPeriod,
        config
    );
    Assertions.assertEquals(zeroPeriod, rule.getOlderThan());
  }

  @Test
  public void test_constructor_negativePeriod_throwsIllegalArgumentException()
  {
    UserCompactionTaskIOConfig config = new UserCompactionTaskIOConfig(null);
    Period negativePeriod = Period.days(-60);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new ReindexingIOConfigRule("test-id", "description", negativePeriod, config)
    );
  }

  @Test
  public void test_constructor_nullIOConfig_throwsDruidException()
  {
    Assertions.assertThrows(
        DruidException.class,
        () -> new ReindexingIOConfigRule("test-id", "description", PERIOD_60_DAYS, null)
    );
  }
}
