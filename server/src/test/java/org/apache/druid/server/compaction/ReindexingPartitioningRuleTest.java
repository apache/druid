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
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
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

public class ReindexingPartitioningRuleTest
{
  private static final DateTime REFERENCE_TIME = DateTimes.of("2025-12-19T12:00:00Z");
  private static final Period PERIOD_7_DAYS = Period.days(7);

  private final ReindexingPartitioningRule rule = new ReindexingPartitioningRule(
      "test-rule",
      "Test partitioning rule",
      PERIOD_7_DAYS,
      Granularities.HOUR,
      new DynamicPartitionsSpec(5000000, null),
      null
  );

  @Test
  public void test_appliesTo_intervalFullyBeforeThreshold_returnsFull()
  {
    Interval interval = Intervals.of("2025-12-09T00:00:00Z/2025-12-10T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalEndsAtThreshold_returnsFull()
  {
    Interval interval = Intervals.of("2025-12-11T12:00:00Z/2025-12-12T12:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalSpansThreshold_returnsPartial()
  {
    Interval interval = Intervals.of("2025-12-11T00:00:00Z/2025-12-13T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.PARTIAL, result);
  }

  @Test
  public void test_appliesTo_intervalStartsAfterThreshold_returnsNone()
  {
    Interval interval = Intervals.of("2025-12-18T00:00:00Z/2025-12-19T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.NONE, result);
  }

  @Test
  public void test_getSegmentGranularity_returnsConfiguredValue()
  {
    Assertions.assertEquals(Granularities.HOUR, rule.getSegmentGranularity());
  }

  @Test
  public void test_getPartitionsSpec_returnsConfiguredValue()
  {
    Assertions.assertNotNull(rule.getPartitionsSpec());
    Assertions.assertInstanceOf(DynamicPartitionsSpec.class, rule.getPartitionsSpec());
  }

  @Test
  public void test_getVirtualColumns_returnsNull_whenNotSet()
  {
    Assertions.assertNull(rule.getVirtualColumns());
  }

  @Test
  public void test_getId_returnsConfiguredId()
  {
    Assertions.assertEquals("test-rule", rule.getId());
  }

  @Test
  public void test_getDescription_returnsConfiguredDescription()
  {
    Assertions.assertEquals("Test partitioning rule", rule.getDescription());
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
        () -> new ReindexingPartitioningRule(null, "description", PERIOD_7_DAYS, Granularities.HOUR,
            new DynamicPartitionsSpec(5000000, null), null)
    );
  }

  @Test
  public void test_constructor_nullPeriod_throwsNullPointerException()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new ReindexingPartitioningRule("test-id", "description", null, Granularities.HOUR,
            new DynamicPartitionsSpec(5000000, null), null)
    );
  }

  @Test
  public void test_constructor_zeroPeriod_succeeds()
  {
    Period zeroPeriod = Period.days(0);
    ReindexingPartitioningRule rule = new ReindexingPartitioningRule(
        "test-id",
        "description",
        zeroPeriod,
        Granularities.HOUR,
        new DynamicPartitionsSpec(5000000, null),
        null
    );
    Assertions.assertEquals(zeroPeriod, rule.getOlderThan());
  }

  @Test
  public void test_constructor_negativePeriod_throwsIllegalArgumentException()
  {
    Period negativePeriod = Period.days(-7);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new ReindexingPartitioningRule("test-id", "description", negativePeriod, Granularities.HOUR,
            new DynamicPartitionsSpec(5000000, null), null)
    );
  }

  @Test
  public void test_constructor_nullPartitionsSpec_throwsDruidException()
  {
    Assertions.assertThrows(
        DruidException.class,
        () -> new ReindexingPartitioningRule("test-id", "description", PERIOD_7_DAYS, Granularities.HOUR,
            null, null)
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
      ReindexingPartitioningRule rule = new ReindexingPartitioningRule(
          "test-id",
          "description",
          PERIOD_7_DAYS,
          granularity,
          new DynamicPartitionsSpec(5000000, null),
          null
      );
      Assertions.assertEquals(granularity, rule.getSegmentGranularity());
    }
  }

  @Test
  public void test_syntheticRule_createsRuleWithExpectedDefaults()
  {
    ReindexingPartitioningRule synthetic = ReindexingPartitioningRule.syntheticRule(
        Granularities.DAY,
        new DynamicPartitionsSpec(5000000, null),
        null
    );

    Assertions.assertEquals(ReindexingPartitioningRule.SYNTHETIC_RULE_ID, synthetic.getId());
    Assertions.assertNull(synthetic.getDescription());
    Assertions.assertEquals(Period.ZERO, synthetic.getOlderThan());
    Assertions.assertEquals(Granularities.DAY, synthetic.getSegmentGranularity());
    Assertions.assertNotNull(synthetic.getPartitionsSpec());
    Assertions.assertNull(synthetic.getVirtualColumns());
  }

  @Test
  public void test_equals_sameObject_returnsTrue()
  {
    Assertions.assertEquals(rule, rule);
  }

  @Test
  public void test_equals_null_returnsFalse()
  {
    Assertions.assertNotEquals(null, rule);
  }

  @Test
  public void test_equals_differentClass_returnsFalse()
  {
    Assertions.assertNotEquals("not a rule", rule);
  }

  @Test
  public void test_equals_equalObjects_returnsTrue()
  {
    ReindexingPartitioningRule other = new ReindexingPartitioningRule(
        "test-rule",
        "Test partitioning rule",
        PERIOD_7_DAYS,
        Granularities.HOUR,
        new DynamicPartitionsSpec(5000000, null),
        null
    );

    Assertions.assertEquals(rule, other);
    Assertions.assertEquals(rule.hashCode(), other.hashCode());
  }

  @Test
  public void test_equals_differentId_returnsFalse()
  {
    ReindexingPartitioningRule other = new ReindexingPartitioningRule(
        "different-id",
        "Test partitioning rule",
        PERIOD_7_DAYS,
        Granularities.HOUR,
        new DynamicPartitionsSpec(5000000, null),
        null
    );

    Assertions.assertNotEquals(rule, other);
  }

  @Test
  public void test_equals_differentGranularity_returnsFalse()
  {
    ReindexingPartitioningRule other = new ReindexingPartitioningRule(
        "test-rule",
        "Test partitioning rule",
        PERIOD_7_DAYS,
        Granularities.DAY,
        new DynamicPartitionsSpec(5000000, null),
        null
    );

    Assertions.assertNotEquals(rule, other);
  }

  @Test
  public void test_constructor_unsupportedGranularities_allThrowDruidException()
  {
    Granularity[] unsupportedGranularities = {
        Granularities.THIRTY_MINUTE,
        Granularities.SIX_HOUR,
        Granularities.EIGHT_HOUR,
        Granularities.WEEK,
        new PeriodGranularity(Period.days(3), null, DateTimeZone.UTC),
        new PeriodGranularity(Period.days(1), null, DateTimes.inferTzFromString("America/Los_Angeles"))
    };

    for (Granularity granularity : unsupportedGranularities) {
      DruidException exception = Assertions.assertThrows(
          DruidException.class,
          () -> new ReindexingPartitioningRule("test-id", "description", PERIOD_7_DAYS, granularity,
              new DynamicPartitionsSpec(5000000, null), null)
      );
      Assertions.assertTrue(
          exception.getMessage().contains("Unsupported segment granularity"),
          "Expected exception message to contain 'Unsupported segment granularity' but got: " + exception.getMessage()
      );
    }
  }
}
