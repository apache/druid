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

import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class ReindexingDataSchemaRuleTest
{
  private static final DateTime REFERENCE_TIME = DateTimes.of("2025-12-19T12:00:00Z");
  private static final Period PERIOD_14_DAYS = Period.days(14);

  private final UserCompactionTaskDimensionsConfig dimensionsSpec = new UserCompactionTaskDimensionsConfig(List.of(new LongDimensionSchema("dim1")));
  private final AggregatorFactory[] metricsSpec = new AggregatorFactory[]{
      new CountAggregatorFactory("count"),
      new LongSumAggregatorFactory("sum_metric", "metric")
  };
  private final Granularity queryGranularity = Granularities.MINUTE;
  private final Boolean rollup = true;

  private final ReindexingDataSchemaRule rule = new ReindexingDataSchemaRule(
      "test-schema-rule",
      "Custom data schema",
      PERIOD_14_DAYS,
      dimensionsSpec,
      metricsSpec,
      queryGranularity,
      rollup,
      Collections.emptyList()
  );

  @Test
  public void test_appliesTo_intervalFullyBeforeThreshold_returnsFull()
  {
    // Threshold is 2025-12-05T12:00:00Z (14 days before reference time)
    // Interval ends at 2025-12-03, which is fully before threshold
    Interval interval = Intervals.of("2025-12-02T00:00:00Z/2025-12-03T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalEndsAtThreshold_returnsFull()
  {
    // Threshold is 2025-12-05T12:00:00Z (14 days before reference time)
    // Interval ends exactly at threshold - should be FULL (boundary case)
    Interval interval = Intervals.of("2025-12-04T12:00:00Z/2025-12-05T12:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.FULL, result);
  }

  @Test
  public void test_appliesTo_intervalSpansThreshold_returnsPartial()
  {
    // Threshold is 2025-12-05T12:00:00Z (14 days before reference time)
    // Interval starts before threshold and ends after - PARTIAL
    Interval interval = Intervals.of("2025-12-04T00:00:00Z/2025-12-06T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.PARTIAL, result);
  }

  @Test
  public void test_appliesTo_intervalStartsAfterThreshold_returnsNone()
  {
    // Threshold is 2025-12-05T12:00:00Z (14 days before reference time)
    // Interval starts after threshold - NONE
    Interval interval = Intervals.of("2025-12-15T00:00:00Z/2025-12-16T00:00:00Z");

    ReindexingRule.AppliesToMode result = rule.appliesTo(interval, REFERENCE_TIME);

    Assertions.assertEquals(ReindexingRule.AppliesToMode.NONE, result);
  }

  @Test
  public void test_getDimensionsSpec_returnsConfiguredValue()
  {
    UserCompactionTaskDimensionsConfig dimensions = rule.getDimensionsSpec();

    Assertions.assertNotNull(dimensions);
    Assertions.assertEquals(dimensionsSpec, dimensions);
  }

  @Test
  public void test_getMetricsSpec_returnsConfiguredValue()
  {
    AggregatorFactory[] metrics = rule.getMetricsSpec();

    Assertions.assertNotNull(metrics);
    Assertions.assertEquals(2, metrics.length);
    Assertions.assertEquals("count", metrics[0].getName());
    Assertions.assertEquals("sum_metric", metrics[1].getName());
  }

  @Test
  public void test_getQueryGranularity_returnsConfiguredValue()
  {
    Granularity granularity = rule.getQueryGranularity();

    Assertions.assertNotNull(granularity);
    Assertions.assertEquals(queryGranularity, granularity);
  }

  @Test
  public void test_getRollup_returnsConfiguredValue()
  {
    Boolean rollupValue = rule.getRollup();

    Assertions.assertNotNull(rollupValue);
    Assertions.assertEquals(true, rollupValue);
  }

  @Test
  public void test_getProjections_returnsConfiguredValue()
  {
    Assertions.assertNotNull(rule.getProjections());
    Assertions.assertEquals(Collections.emptyList(), rule.getProjections());
  }

  @Test
  public void test_getId_returnsConfiguredId()
  {
    Assertions.assertEquals("test-schema-rule", rule.getId());
  }

  @Test
  public void test_getDescription_returnsConfiguredDescription()
  {
    Assertions.assertEquals("Custom data schema", rule.getDescription());
  }

  @Test
  public void test_getOlderThan_returnsConfiguredPeriod()
  {
    Assertions.assertEquals(PERIOD_14_DAYS, rule.getOlderThan());
  }

  @Test
  public void test_constructor_nullId_throwsNullPointerException()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new ReindexingDataSchemaRule(
            null,
            "description",
            PERIOD_14_DAYS,
            dimensionsSpec,
            metricsSpec,
            queryGranularity,
            rollup,
            Collections.emptyList()
        )
    );
  }

  @Test
  public void test_constructor_nullPeriod_throwsNullPointerException()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new ReindexingDataSchemaRule(
            "test-id",
            "description",
            null,
            dimensionsSpec,
            metricsSpec,
            queryGranularity,
            rollup,
            Collections.emptyList()
        )
    );
  }

  @Test
  public void test_constructor_zeroPeriod_succeeds()
  {
    // P0D is valid - indicates rules that apply immediately to all data
    Period zeroPeriod = Period.days(0);
    ReindexingDataSchemaRule rule = new ReindexingDataSchemaRule(
        "test-id",
        "description",
        zeroPeriod,
        dimensionsSpec,
        metricsSpec,
        queryGranularity,
        rollup,
        Collections.emptyList()
    );
    Assertions.assertEquals(zeroPeriod, rule.getOlderThan());
  }

  @Test
  public void test_constructor_negativePeriod_throwsIllegalArgumentException()
  {
    Period negativePeriod = Period.days(-14);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new ReindexingDataSchemaRule(
            "test-id",
            "description",
            negativePeriod,
            dimensionsSpec,
            metricsSpec,
            queryGranularity,
            rollup,
            Collections.emptyList()
        )
    );
  }

  @Test
  public void test_constructor_nullDimensionsSpec_succeeds()
  {
    // Null dimensions spec is allowed
    ReindexingDataSchemaRule rule = new ReindexingDataSchemaRule(
        "test-id",
        "description",
        PERIOD_14_DAYS,
        null,
        metricsSpec,
        queryGranularity,
        rollup,
        Collections.emptyList()
    );
    Assertions.assertNull(rule.getDimensionsSpec());
  }

  @Test
  public void test_constructor_nullMetricsSpec_succeeds()
  {
    // Null metrics spec is allowed
    ReindexingDataSchemaRule rule = new ReindexingDataSchemaRule(
        "test-id",
        "description",
        PERIOD_14_DAYS,
        dimensionsSpec,
        null,
        queryGranularity,
        rollup,
        Collections.emptyList()
    );
    Assertions.assertNull(rule.getMetricsSpec());
  }

  @Test
  public void test_constructor_nullQueryGranularity_succeeds()
  {
    // Null query granularity is allowed
    ReindexingDataSchemaRule rule = new ReindexingDataSchemaRule(
        "test-id",
        "description",
        PERIOD_14_DAYS,
        dimensionsSpec,
        metricsSpec,
        null,
        rollup,
        Collections.emptyList()
    );
    Assertions.assertNull(rule.getQueryGranularity());
  }

  @Test
  public void test_constructor_nullRollup_succeeds()
  {
    // Null rollup is allowed
    ReindexingDataSchemaRule rule = new ReindexingDataSchemaRule(
        "test-id",
        "description",
        PERIOD_14_DAYS,
        dimensionsSpec,
        metricsSpec,
        queryGranularity,
        null,
        Collections.emptyList()
    );
    Assertions.assertNull(rule.getRollup());
  }

  @Test
  public void test_constructor_nullProjections_succeeds()
  {
    // Null projections is allowed
    ReindexingDataSchemaRule rule = new ReindexingDataSchemaRule(
        "test-id",
        "description",
        PERIOD_14_DAYS,
        dimensionsSpec,
        metricsSpec,
        queryGranularity,
        rollup,
        null
    );
    Assertions.assertNull(rule.getProjections());
  }

  @Test
  public void test_constructor_emptyMetricsSpec_succeeds()
  {
    // Empty metrics array is allowed
    AggregatorFactory[] emptyMetrics = new AggregatorFactory[0];
    ReindexingDataSchemaRule rule = new ReindexingDataSchemaRule(
        "test-id",
        "description",
        PERIOD_14_DAYS,
        dimensionsSpec,
        emptyMetrics,
        queryGranularity,
        rollup,
        Collections.emptyList()
    );
    Assertions.assertNotNull(rule.getMetricsSpec());
    Assertions.assertEquals(0, rule.getMetricsSpec().length);
  }
}
