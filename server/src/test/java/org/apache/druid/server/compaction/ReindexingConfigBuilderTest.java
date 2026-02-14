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
  public void test_applyTo_handlesSynteticSegmentGranularityInsertion()
  {
    ReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
        .dataSchemaRules(
            ImmutableList.of(
                new ReindexingDataSchemaRule(
                    "schema-30d",
                    null,
                    Period.days(30),
                    new UserCompactionTaskDimensionsConfig(null),
                    new AggregatorFactory[]{new CountAggregatorFactory("count")},
                    Granularities.HOUR,
                    true,
                    ImmutableList.of()
                )
            )
        ).build();

    InlineSchemaDataSourceCompactionConfig.Builder builder =
        InlineSchemaDataSourceCompactionConfig.builder()
                                              .forDataSource("test_datasource");

    ReindexingConfigBuilder configBuilder = new ReindexingConfigBuilder(
        provider,
        Granularities.DAY,
        TEST_INTERVAL,
        REFERENCE_TIME
    );

    int count = configBuilder.applyTo(builder);

    Assert.assertEquals(1, count);

    InlineSchemaDataSourceCompactionConfig config = builder.build();
    Assert.assertNotNull(config.getGranularitySpec());
    Assert.assertNotNull(config.getGranularitySpec().getSegmentGranularity());
    Assert.assertEquals(Granularities.DAY, config.getGranularitySpec().getSegmentGranularity());
    Assert.assertNotNull(config.getGranularitySpec().getQueryGranularity());
    Assert.assertEquals(Granularities.HOUR, config.getGranularitySpec().getQueryGranularity());
    Assert.assertNotNull(config.getGranularitySpec().isRollup());
    Assert.assertTrue(config.getGranularitySpec().isRollup());

    // Test applyToWithDetails() on a fresh builder
    InlineSchemaDataSourceCompactionConfig.Builder builderForDetails =
        InlineSchemaDataSourceCompactionConfig.builder()
            .forDataSource("test_datasource");

    ReindexingConfigBuilder.BuildResult buildResult = configBuilder.applyToWithDetails(builderForDetails);

    // Verify count matches
    Assert.assertEquals(count, buildResult.getRuleCount());

    // Verify applied rules - should only contain the data schema rule, not the synthetic segment granularity
    Assert.assertNotNull(buildResult.getAppliedRules());
    Assert.assertEquals(1, buildResult.getAppliedRules().size());
    Assert.assertTrue(buildResult.getAppliedRules().get(0) instanceof ReindexingDataSchemaRule);

    // Verify config matches
    InlineSchemaDataSourceCompactionConfig configFromDetails = builderForDetails.build();
    Assert.assertEquals(config.getGranularitySpec(), configFromDetails.getGranularitySpec());
  }

  @Test
  public void test_applyTo_allRulesPresent_appliesAllConfigsAndReturnsCorrectCount()
  {
    ReindexingRuleProvider provider = createFullyPopulatedProvider();
    InlineSchemaDataSourceCompactionConfig.Builder builder =
        InlineSchemaDataSourceCompactionConfig.builder()
            .forDataSource("test_datasource");

    ReindexingConfigBuilder configBuilder = new ReindexingConfigBuilder(
        provider,
        Granularities.DAY,
        TEST_INTERVAL,
        REFERENCE_TIME
    );

    int count = configBuilder.applyTo(builder);

    Assert.assertEquals(6, count);

    InlineSchemaDataSourceCompactionConfig config = builder.build();

    Assert.assertNotNull(config.getGranularitySpec().getSegmentGranularity());
    Assert.assertEquals(Granularities.DAY, config.getGranularitySpec().getSegmentGranularity());

    Assert.assertNotNull(config.getGranularitySpec().getQueryGranularity());
    Assert.assertEquals(Granularities.HOUR, config.getGranularitySpec().getQueryGranularity());
    Assert.assertTrue(config.getGranularitySpec().isRollup());

    Assert.assertNotNull(config.getTuningConfig());
    Assert.assertNotNull(config.getMetricsSpec());
    Assert.assertEquals(1, config.getMetricsSpec().length);
    Assert.assertEquals("count", config.getMetricsSpec()[0].getName());

    Assert.assertNotNull(config.getDimensionsSpec());
    Assert.assertNotNull(config.getIoConfig());

    Assert.assertNotNull(config.getProjections());
    Assert.assertEquals(1, config.getProjections().size()); // only 1 as we match the 2nd dataSchemaRule

    Assert.assertNotNull(config.getTransformSpec());
    DimFilter appliedFilter = config.getTransformSpec().getFilter();
    Assert.assertTrue(appliedFilter instanceof NotDimFilter);

    NotDimFilter notFilter = (NotDimFilter) appliedFilter;
    Assert.assertTrue(notFilter.getField() instanceof OrDimFilter);

    OrDimFilter orFilter = (OrDimFilter) notFilter.getField();
    Assert.assertEquals(2, orFilter.getFields().size()); // 2 filters combined

    // Now test applyToWithDetails() on a fresh builder
    InlineSchemaDataSourceCompactionConfig.Builder builderForDetails =
        InlineSchemaDataSourceCompactionConfig.builder()
            .forDataSource("test_datasource");

    ReindexingConfigBuilder.BuildResult buildResult = configBuilder.applyToWithDetails(builderForDetails);

    // Verify BuildResult count matches applyTo() count
    Assert.assertEquals(count, buildResult.getRuleCount());

    // Verify applied rules list
    Assert.assertNotNull(buildResult.getAppliedRules());
    Assert.assertEquals(6, buildResult.getAppliedRules().size());

    // Verify rule types in order: tuning, io, dataSchema, 2 deletion rules, segment granularity
    Assert.assertTrue(buildResult.getAppliedRules().get(0) instanceof ReindexingTuningConfigRule);
    Assert.assertTrue(buildResult.getAppliedRules().get(1) instanceof ReindexingIOConfigRule);
    Assert.assertTrue(buildResult.getAppliedRules().get(2) instanceof ReindexingDataSchemaRule);
    Assert.assertTrue(buildResult.getAppliedRules().get(3) instanceof ReindexingDeletionRule);
    Assert.assertTrue(buildResult.getAppliedRules().get(4) instanceof ReindexingDeletionRule);
    Assert.assertTrue(buildResult.getAppliedRules().get(5) instanceof ReindexingSegmentGranularityRule);

    // Verify the config produced by applyToWithDetails() matches the original
    InlineSchemaDataSourceCompactionConfig configFromDetails = builderForDetails.build();
    Assert.assertEquals(config.getGranularitySpec(), configFromDetails.getGranularitySpec());
    Assert.assertEquals(config.getTuningConfig(), configFromDetails.getTuningConfig());
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
        Granularities.DAY,
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
    ReindexingSegmentGranularityRule segmentGranularityRule = new ReindexingSegmentGranularityRule(
        "gran-30d",
        null,
        Period.days(30),
        Granularities.DAY
    );

    ReindexingTuningConfigRule tuningConfigRule = new ReindexingTuningConfigRule(
        "tuning-30d",
        null,
        Period.days(30),
        new UserCompactionTaskQueryTuningConfig(null, null, null, null, null, null,
                                                null, null, null, null, null, null,
                                                null, null, null, null, null, null, null)
    );

    ReindexingDeletionRule filterRule1 = new ReindexingDeletionRule(
        "filter-30d",
        null,
        Period.days(30),
        new SelectorDimFilter("country", "US", null),
        null
    );

    ReindexingDeletionRule filterRule2 = new ReindexingDeletionRule(
        "filter-60d",
        null,
        Period.days(60),
        new SelectorDimFilter("device", "mobile", null),
        null
    );

    ReindexingIOConfigRule ioConfigRule = new ReindexingIOConfigRule(
        "io-30d",
        null,
        Period.days(30),
        new UserCompactionTaskIOConfig(null)
    );

    ReindexingDataSchemaRule dataSchemaRule1 = new ReindexingDataSchemaRule(
        "schema-30d",
        null,
        Period.days(30),
        new UserCompactionTaskDimensionsConfig(null),
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        Granularities.HOUR,
        true,
        ImmutableList.of(
            new AggregateProjectionSpec("proj1", null, null, null,
                                        new AggregatorFactory[]{new CountAggregatorFactory("count1")}),
            new AggregateProjectionSpec("proj2", null, null, null,
                                        new AggregatorFactory[]{new CountAggregatorFactory("count2")})
        )
    );

    ReindexingDataSchemaRule dataSchemaRule2 = new ReindexingDataSchemaRule(
        "schema-60d",
        null,
        Period.days(60),
        new UserCompactionTaskDimensionsConfig(null),
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        Granularities.HOUR,
        true,
        ImmutableList.of(
            new AggregateProjectionSpec("proj3", null, null, null,
                                        new AggregatorFactory[]{new CountAggregatorFactory("count3")})
        )
    );

    return InlineReindexingRuleProvider.builder()
        .segmentGranularityRules(ImmutableList.of(segmentGranularityRule))
        .tuningConfigRules(ImmutableList.of(tuningConfigRule))
        .ioConfigRules(ImmutableList.of(ioConfigRule))
        .deletionRules(ImmutableList.of(filterRule1, filterRule2))
        .dataSchemaRules(ImmutableList.of(dataSchemaRule1, dataSchemaRule2))
        .build();
  }
}
