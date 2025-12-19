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

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class CompactionMetricsRuleTest
{
  private static final DateTime REFERENCE_TIME = new DateTime("2025-12-19T12:00:00Z");
  private static final Period PERIOD_90_DAYS = Period.days(90);

  private final AggregatorFactory[] testMetrics = new AggregatorFactory[]{
      new CountAggregatorFactory("count"),
      new LongSumAggregatorFactory("total_value", "value")
  };

  private final CompactionMetricsRule rule = new CompactionMetricsRule(
      "test-metrics-rule",
      "Aggregate metrics for old data",
      PERIOD_90_DAYS,
      testMetrics
  );

  @Test
  public void test_isAdditive_returnsFalse()
  {
    // Metrics rules are not additive - only one metrics spec can apply
    Assert.assertFalse(rule.isAdditive());
  }

  @Test
  public void test_appliesTo_intervalFullyBeforeThreshold_returnsFull()
  {
    // Threshold is 2025-09-20T12:00:00Z (90 days before reference time)
    // Interval ends at 2025-09-15, which is fully before threshold
    Interval interval = new Interval("2025-09-14T00:00:00Z/2025-09-15T00:00:00Z");

    CompactionRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(CompactionRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalEndsAtThreshold_returnsFull()
  {
    // Threshold is 2025-09-20T12:00:00Z (90 days before reference time)
    // Interval ends exactly at threshold - should be FULL (boundary case)
    Interval interval = new Interval("2025-09-19T12:00:00Z/2025-09-20T12:00:00Z");

    CompactionRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(CompactionRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalSpansThreshold_returnsPartial()
  {
    // Threshold is 2025-09-20T12:00:00Z (90 days before reference time)
    // Interval starts before threshold and ends after - PARTIAL
    Interval interval = new Interval("2025-09-19T00:00:00Z/2025-09-21T00:00:00Z");

    CompactionRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(CompactionRule.AppliesToMode.PARTIAL, result);
  }

  @Test
  public void test_appliesTo_intervalStartsAfterThreshold_returnsNone()
  {
    // Threshold is 2025-09-20T12:00:00Z (90 days before reference time)
    // Interval starts after threshold - NONE
    Interval interval = new Interval("2025-12-01T00:00:00Z/2025-12-02T00:00:00Z");

    CompactionRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assert.assertEquals(CompactionRule.AppliesToMode.NONE, result);
  }

  @Test
  public void test_getMetricsSpec_returnsConfiguredMetrics()
  {
    AggregatorFactory[] metrics = rule.getMetricsSpec();

    Assert.assertNotNull(metrics);
    Assert.assertEquals(2, metrics.length);
    Assert.assertEquals("count", metrics[0].getName());
    Assert.assertEquals("total_value", metrics[1].getName());
  }

  @Test
  public void test_getId_returnsConfiguredId()
  {
    Assert.assertEquals("test-metrics-rule", rule.getId());
  }

  @Test
  public void test_getDescription_returnsConfiguredDescription()
  {
    Assert.assertEquals("Aggregate metrics for old data", rule.getDescription());
  }

  @Test
  public void test_getPeriod_returnsConfiguredPeriod()
  {
    Assert.assertEquals(PERIOD_90_DAYS, rule.getPeriod());
  }

  @Test
  public void test_constructor_nullId_throwsNullPointerException()
  {
    Assert.assertThrows(
        NullPointerException.class,
        () -> new CompactionMetricsRule(null, "description", PERIOD_90_DAYS, testMetrics)
    );
  }

  @Test
  public void test_constructor_nullPeriod_throwsNullPointerException()
  {
    Assert.assertThrows(
        NullPointerException.class,
        () -> new CompactionMetricsRule("test-id", "description", null, testMetrics)
    );
  }

  @Test
  public void test_constructor_zeroPeriod_throwsIllegalArgumentException()
  {
    Period zeroPeriod = Period.days(0);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new CompactionMetricsRule("test-id", "description", zeroPeriod, testMetrics)
    );
  }

  @Test
  public void test_constructor_negativePeriod_throwsIllegalArgumentException()
  {
    Period negativePeriod = Period.days(-90);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new CompactionMetricsRule("test-id", "description", negativePeriod, testMetrics)
    );
  }

  @Test
  public void test_constructor_nullMetricsSpec_throwsNullPointerException()
  {
    Assert.assertThrows(
        NullPointerException.class,
        () -> new CompactionMetricsRule("test-id", "description", PERIOD_90_DAYS, null)
    );
  }
}
