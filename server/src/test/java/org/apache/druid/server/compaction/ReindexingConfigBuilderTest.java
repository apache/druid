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

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskIOConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class ReindexingConfigBuilderTest
{
  private static final Interval TEST_INTERVAL = Intervals.of("2024-11-01/2024-11-02");
  private static final DateTime REFERENCE_TIME = DateTimes.of("2025-01-15");

  @Test
  public void test_applyTo_allRulesPresent_appliesAllConfigsAndReturnsCorrectCount()
  {
    ReindexingRuleProvider provider = createFullyPopulatedProvider();
    InlineSchemaDataSourceCompactionConfig.Builder builder =
        InlineSchemaDataSourceCompactionConfig.builder()
            .forDataSource("test_datasource");

    ReindexingConfigBuilder configBuilder = new ReindexingConfigBuilder(
        provider,
        TEST_INTERVAL,
        REFERENCE_TIME
    );

    int count = configBuilder.applyTo(builder);

    Assert.assertEquals(9, count); // 5 non-additive + 2 projection rules + 2 filter rules

    InlineSchemaDataSourceCompactionConfig config = builder.build();

    Assert.assertNotNull(config.getGranularitySpec());
    Assert.assertEquals(Granularities.DAY, config.getGranularitySpec().getSegmentGranularity());

    Assert.assertNotNull(config.getTuningConfig());
    Assert.assertNotNull(config.getMetricsSpec());
    Assert.assertEquals(1, config.getMetricsSpec().length);
    Assert.assertEquals("count", config.getMetricsSpec()[0].getName());

    Assert.assertNotNull(config.getDimensionsSpec());
    Assert.assertNotNull(config.getIoConfig());

    Assert.assertNotNull(config.getProjections());
    Assert.assertEquals(3, config.getProjections().size()); // 2 from rule1 + 1 from rule2

    Assert.assertNotNull(config.getTransformSpec());
    DimFilter appliedFilter = config.getTransformSpec().getFilter();
    Assert.assertTrue(appliedFilter instanceof NotDimFilter);

    NotDimFilter notFilter = (NotDimFilter) appliedFilter;
    Assert.assertTrue(notFilter.getField() instanceof OrDimFilter);

    OrDimFilter orFilter = (OrDimFilter) notFilter.getField();
    Assert.assertEquals(2, orFilter.getFields().size()); // 2 filters combined
  }

  @Test
  public void test_applyTo_noRulesPresent_appliesNothingAndReturnsZero()
  {
    ReindexingRuleProvider provider = InlineReindexingRuleProvider.builder().build();
    InlineSchemaDataSourceCompactionConfig.Builder builder =
        InlineSchemaDataSourceCompactionConfig.builder()
            .forDataSource("test_datasource");

    ReindexingConfigBuilder configBuilder = new ReindexingConfigBuilder(
        provider,
        TEST_INTERVAL,
        REFERENCE_TIME
    );

    int count = configBuilder.applyTo(builder);

    Assert.assertEquals(0, count);

    InlineSchemaDataSourceCompactionConfig config = builder.build();

    Assert.assertNull(config.getGranularitySpec());
    Assert.assertNull(config.getTuningConfig());
    Assert.assertNull(config.getMetricsSpec());
    Assert.assertNull(config.getDimensionsSpec());
    Assert.assertNull(config.getIoConfig());
    Assert.assertNull(config.getProjections());
    Assert.assertNull(config.getTransformSpec());
  }

  private ReindexingRuleProvider createFullyPopulatedProvider()
  {
    ReindexingGranularityRule granularityRule = new ReindexingGranularityRule(
        "gran-30d",
        null,
        Period.days(30),
        new UserCompactionTaskGranularityConfig(Granularities.DAY, null, false)
    );

    ReindexingTuningConfigRule tuningConfigRule = new ReindexingTuningConfigRule(
        "tuning-30d",
        null,
        Period.days(30),
        new UserCompactionTaskQueryTuningConfig(null, null, null, null, null, null,
                                                null, null, null, null, null, null,
                                                null, null, null, null, null, null, null)
    );

    ReindexingMetricsRule metricsRule = new ReindexingMetricsRule(
        "metrics-30d",
        null,
        Period.days(30),
        new AggregatorFactory[]{new CountAggregatorFactory("count")}
    );

    ReindexingDimensionsRule dimensionsRule = new ReindexingDimensionsRule(
        "dims-30d",
        null,
        Period.days(30),
        new UserCompactionTaskDimensionsConfig(null)
    );

    ReindexingIOConfigRule ioConfigRule = new ReindexingIOConfigRule(
        "io-30d",
        null,
        Period.days(30),
        new UserCompactionTaskIOConfig(null)
    );

    // Two projection rules (additive)
    ReindexingProjectionRule projectionRule1 = new ReindexingProjectionRule(
        "proj-30d",
        null,
        Period.days(30),
        ImmutableList.of(
            new AggregateProjectionSpec("proj1", null, null, null,
                                       new AggregatorFactory[]{new CountAggregatorFactory("count1")}),
            new AggregateProjectionSpec("proj2", null, null, null,
                                       new AggregatorFactory[]{new CountAggregatorFactory("count2")})
        )
    );

    ReindexingProjectionRule projectionRule2 = new ReindexingProjectionRule(
        "proj-60d",
        null,
        Period.days(60),
        ImmutableList.of(
            new AggregateProjectionSpec("proj3", null, null, null,
                                       new AggregatorFactory[]{new CountAggregatorFactory("count3")})
        )
    );

    // Two filter rules (additive)
    ReindexingFilterRule filterRule1 = new ReindexingFilterRule(
        "filter-30d",
        null,
        Period.days(30),
        new SelectorDimFilter("country", "US", null)
    );

    ReindexingFilterRule filterRule2 = new ReindexingFilterRule(
        "filter-60d",
        null,
        Period.days(60),
        new SelectorDimFilter("device", "mobile", null)
    );

    return InlineReindexingRuleProvider.builder()
        .granularityRules(ImmutableList.of(granularityRule))
        .tuningConfigRules(ImmutableList.of(tuningConfigRule))
        .metricsRules(ImmutableList.of(metricsRule))
        .dimensionsRules(ImmutableList.of(dimensionsRule))
        .ioConfigRules(ImmutableList.of(ioConfigRule))
        .projectionRules(ImmutableList.of(projectionRule1, projectionRule2))
        .filterRules(ImmutableList.of(filterRule1, filterRule2))
        .build();
  }
}