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

package org.apache.druid.indexing.compact;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.server.compaction.InlineReindexingRuleProvider;
import org.apache.druid.server.compaction.IntervalPartitioningInfo;
import org.apache.druid.server.compaction.ReindexingDataSchemaRule;
import org.apache.druid.server.compaction.ReindexingDeletionRule;
import org.apache.druid.server.compaction.ReindexingIndexSpecRule;
import org.apache.druid.server.compaction.ReindexingPartitioningRule;
import org.apache.druid.server.compaction.ReindexingRule;
import org.apache.druid.server.compaction.ReindexingRuleProvider;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

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

    // Create synthetic timeline with default granularity (no source rule since it's default)
    ImmutableList<IntervalPartitioningInfo> syntheticTimeline = ImmutableList.of(
        new IntervalPartitioningInfo(
            TEST_INTERVAL,
            ReindexingPartitioningRule.syntheticRule(
                Granularities.DAY,
                new DynamicPartitionsSpec(5000000, null),
                null
            ),
            true
        )
    );

    ReindexingConfigBuilder configBuilder = new ReindexingConfigBuilder(
        provider,
        TEST_INTERVAL,
        REFERENCE_TIME,
        syntheticTimeline,
        null
    );

    int count = configBuilder.applyTo(builder);

    Assertions.assertEquals(1, count);

    InlineSchemaDataSourceCompactionConfig config = builder.build();
    Assertions.assertNotNull(config.getGranularitySpec());
    Assertions.assertNotNull(config.getGranularitySpec().getSegmentGranularity());
    Assertions.assertEquals(Granularities.DAY, config.getGranularitySpec().getSegmentGranularity());
    Assertions.assertNotNull(config.getGranularitySpec().getQueryGranularity());
    Assertions.assertEquals(Granularities.HOUR, config.getGranularitySpec().getQueryGranularity());
    Assertions.assertNotNull(config.getGranularitySpec().isRollup());
    Assertions.assertTrue(config.getGranularitySpec().isRollup());

    // Test applyToWithDetails() on a fresh builder
    InlineSchemaDataSourceCompactionConfig.Builder builderForDetails =
        InlineSchemaDataSourceCompactionConfig.builder()
            .forDataSource("test_datasource");

    ReindexingConfigBuilder.BuildResult buildResult = configBuilder.applyToWithDetails(builderForDetails);

    // Verify count matches
    Assertions.assertEquals(count, buildResult.getRuleCount());

    // Verify applied rules - should only contain the data schema rule, not the synthetic segment granularity
    Assertions.assertNotNull(buildResult.getAppliedRules());
    Assertions.assertEquals(1, buildResult.getAppliedRules().size());
    Assertions.assertTrue(buildResult.getAppliedRules().get(0) instanceof ReindexingDataSchemaRule);

    // Verify config matches
    InlineSchemaDataSourceCompactionConfig configFromDetails = builderForDetails.build();
    Assertions.assertEquals(config.getGranularitySpec(), configFromDetails.getGranularitySpec());
  }

  @Test
  public void test_applyTo_allRulesPresent_appliesAllConfigsAndReturnsCorrectCount()
  {
    ReindexingRuleProvider provider = createFullyPopulatedProvider();
    InlineSchemaDataSourceCompactionConfig.Builder builder =
        InlineSchemaDataSourceCompactionConfig.builder()
            .forDataSource("test_datasource");

    // Create the partitioning rule used in the provider
    ReindexingPartitioningRule partitioningRule = new ReindexingPartitioningRule(
        "gran-30d",
        null,
        Period.days(30),
        Granularities.DAY,
        new DynamicPartitionsSpec(5000000, null),
        null
    );

    // Create synthetic timeline with granularity from the rule
    ImmutableList<IntervalPartitioningInfo> syntheticTimeline = ImmutableList.of(
        new IntervalPartitioningInfo(TEST_INTERVAL, partitioningRule)
    );

    ReindexingConfigBuilder configBuilder = new ReindexingConfigBuilder(
        provider,
        TEST_INTERVAL,
        REFERENCE_TIME,
        syntheticTimeline,
        null
    );

    int count = configBuilder.applyTo(builder);

    Assertions.assertEquals(5, count);

    InlineSchemaDataSourceCompactionConfig config = builder.build();

    Assertions.assertNotNull(config.getGranularitySpec().getSegmentGranularity());
    Assertions.assertEquals(Granularities.DAY, config.getGranularitySpec().getSegmentGranularity());

    Assertions.assertNotNull(config.getGranularitySpec().getQueryGranularity());
    Assertions.assertEquals(Granularities.HOUR, config.getGranularitySpec().getQueryGranularity());
    Assertions.assertTrue(config.getGranularitySpec().isRollup());

    Assertions.assertNotNull(config.getTuningConfig());
    Assertions.assertNotNull(config.getMetricsSpec());
    Assertions.assertEquals(1, config.getMetricsSpec().length);
    Assertions.assertEquals("count", config.getMetricsSpec()[0].getName());

    Assertions.assertNotNull(config.getDimensionsSpec());

    Assertions.assertNotNull(config.getProjections());
    Assertions.assertEquals(1, config.getProjections().size()); // only 1 as we match the 2nd dataSchemaRule

    Assertions.assertNotNull(config.getTransformSpec());
    DimFilter appliedFilter = config.getTransformSpec().getFilter();
    Assertions.assertTrue(appliedFilter instanceof NotDimFilter);

    NotDimFilter notFilter = (NotDimFilter) appliedFilter;
    Assertions.assertTrue(notFilter.getField() instanceof OrDimFilter);

    OrDimFilter orFilter = (OrDimFilter) notFilter.getField();
    Assertions.assertEquals(2, orFilter.getFields().size()); // 2 filters combined

    // Now test applyToWithDetails() on a fresh builder
    InlineSchemaDataSourceCompactionConfig.Builder builderForDetails =
        InlineSchemaDataSourceCompactionConfig.builder()
            .forDataSource("test_datasource");

    ReindexingConfigBuilder.BuildResult buildResult = configBuilder.applyToWithDetails(builderForDetails);

    // Verify BuildResult count matches applyTo() count
    Assertions.assertEquals(count, buildResult.getRuleCount());

    // Verify applied rules list
    Assertions.assertNotNull(buildResult.getAppliedRules());
    Assertions.assertEquals(5, buildResult.getAppliedRules().size());

    // Verify rule types (order-independent)
    Assertions.assertEquals(1, buildResult.getAppliedRules().stream().filter(r -> r instanceof ReindexingPartitioningRule).count());
    Assertions.assertEquals(1, buildResult.getAppliedRules().stream().filter(r -> r instanceof ReindexingIndexSpecRule).count());
    Assertions.assertEquals(1, buildResult.getAppliedRules().stream().filter(r -> r instanceof ReindexingDataSchemaRule).count());
    Assertions.assertEquals(2, buildResult.getAppliedRules().stream().filter(r -> r instanceof ReindexingDeletionRule).count());

    // Verify rule IDs (order-independent)
    Set<String> appliedRuleIds = buildResult.getAppliedRules().stream()
        .map(ReindexingRule::getId)
        .collect(Collectors.toSet());
    Assertions.assertEquals(
        Set.of("gran-30d", "indexSpec-30d", "schema-60d", "filter-30d", "filter-60d"),
        appliedRuleIds
    );

    // Verify the config produced by applyToWithDetails() matches the original
    InlineSchemaDataSourceCompactionConfig configFromDetails = builderForDetails.build();
    Assertions.assertEquals(config.getGranularitySpec(), configFromDetails.getGranularitySpec());
    Assertions.assertEquals(config.getTuningConfig(), configFromDetails.getTuningConfig());
  }

  @Test
  public void test_applyTo_noRulesPresent_appliesNothingAndReturnsZero()
  {
    ReindexingRuleProvider provider = InlineReindexingRuleProvider.builder().build();
    InlineSchemaDataSourceCompactionConfig.Builder builder =
        InlineSchemaDataSourceCompactionConfig.builder()
            .forDataSource("test_datasource");

    // Create synthetic timeline with default granularity (no source rule)
    ImmutableList<IntervalPartitioningInfo> syntheticTimeline = ImmutableList.of(
        new IntervalPartitioningInfo(
            TEST_INTERVAL,
            ReindexingPartitioningRule.syntheticRule(
                Granularities.DAY,
                new DynamicPartitionsSpec(5000000, null),
                null
            ),
            true
        )
    );

    ReindexingConfigBuilder configBuilder = new ReindexingConfigBuilder(
        provider,
        TEST_INTERVAL,
        REFERENCE_TIME,
        syntheticTimeline,
        null
    );

    int count = configBuilder.applyTo(builder);

    Assertions.assertEquals(0, count);

    InlineSchemaDataSourceCompactionConfig config = builder.build();

    Assertions.assertNotNull(config.getTuningConfig());
    Assertions.assertNull(config.getMetricsSpec());
    Assertions.assertNull(config.getDimensionsSpec());
    Assertions.assertNull(config.getIoConfig());
    Assertions.assertNull(config.getProjections());
    Assertions.assertNull(config.getTransformSpec());
  }

  @Test
  public void test_applyTo_singleDeletionRule_usesDirectFilter()
  {
    ReindexingDeletionRule singleRule = new ReindexingDeletionRule(
        "filter-single",
        null,
        Period.days(30),
        new SelectorDimFilter("country", "US", null),
        null
    );

    ReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
        .deletionRules(ImmutableList.of(singleRule))
        .build();

    InlineSchemaDataSourceCompactionConfig.Builder builder =
        InlineSchemaDataSourceCompactionConfig.builder()
            .forDataSource("test_datasource");

    ImmutableList<IntervalPartitioningInfo> syntheticTimeline = ImmutableList.of(
        new IntervalPartitioningInfo(
            TEST_INTERVAL,
            ReindexingPartitioningRule.syntheticRule(
                Granularities.DAY,
                new DynamicPartitionsSpec(5000000, null),
                null
            ),
            true
        )
    );

    ReindexingConfigBuilder configBuilder = new ReindexingConfigBuilder(
        provider,
        TEST_INTERVAL,
        REFERENCE_TIME,
        syntheticTimeline,
        null
    );

    int count = configBuilder.applyTo(builder);

    Assertions.assertEquals(1, count);

    InlineSchemaDataSourceCompactionConfig config = builder.build();
    Assertions.assertNotNull(config.getTransformSpec());
    DimFilter appliedFilter = config.getTransformSpec().getFilter();
    // Single rule: filter should be NOT(directFilter), not NOT(OR(directFilter))
    Assertions.assertInstanceOf(NotDimFilter.class, appliedFilter);
    NotDimFilter notFilter = (NotDimFilter) appliedFilter;
    // Inner filter should be the SelectorDimFilter directly, not an OrDimFilter
    Assertions.assertInstanceOf(SelectorDimFilter.class, notFilter.getField());
  }

  @Test
  public void test_applyTo_deletionRuleWithVirtualColumns_mergesIntoTransformSpec()
  {
    VirtualColumns vcs = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn(
                "extractedField",
                "json_value(metadata, '$.category')",
                ColumnType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
    );

    ReindexingDeletionRule ruleWithVCs = new ReindexingDeletionRule(
        "filter-vc",
        null,
        Period.days(30),
        new SelectorDimFilter("extractedField", "unwantedValue", null),
        vcs
    );

    ReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
        .deletionRules(ImmutableList.of(ruleWithVCs))
        .build();

    InlineSchemaDataSourceCompactionConfig.Builder builder =
        InlineSchemaDataSourceCompactionConfig.builder()
            .forDataSource("test_datasource");

    ImmutableList<IntervalPartitioningInfo> syntheticTimeline = ImmutableList.of(
        new IntervalPartitioningInfo(
            TEST_INTERVAL,
            ReindexingPartitioningRule.syntheticRule(
                Granularities.DAY,
                new DynamicPartitionsSpec(5000000, null),
                null
            ),
            true
        )
    );

    ReindexingConfigBuilder configBuilder = new ReindexingConfigBuilder(
        provider,
        TEST_INTERVAL,
        REFERENCE_TIME,
        syntheticTimeline,
        null
    );

    int count = configBuilder.applyTo(builder);

    Assertions.assertEquals(1, count);

    InlineSchemaDataSourceCompactionConfig config = builder.build();
    Assertions.assertNotNull(config.getTransformSpec());
    Assertions.assertNotNull(config.getTransformSpec().getFilter());
    // VCs should be present in the transform spec
    VirtualColumns resultVCs = config.getTransformSpec().getVirtualColumns();
    Assertions.assertNotNull(resultVCs);
    Assertions.assertEquals(1, resultVCs.getVirtualColumns().length);
    Assertions.assertEquals("extractedField", resultVCs.getVirtualColumns()[0].getOutputName());
  }

  @Test
  public void test_applyTo_withBaseTuningConfig_overlaysOnBase()
  {
    ReindexingRuleProvider provider = InlineReindexingRuleProvider.builder().build();

    UserCompactionTaskQueryTuningConfig baseTuning = UserCompactionTaskQueryTuningConfig.builder()
        .maxRowsInMemory(100_000)
        .build();

    InlineSchemaDataSourceCompactionConfig.Builder builder =
        InlineSchemaDataSourceCompactionConfig.builder()
            .forDataSource("test_datasource");

    ImmutableList<IntervalPartitioningInfo> syntheticTimeline = ImmutableList.of(
        new IntervalPartitioningInfo(
            TEST_INTERVAL,
            ReindexingPartitioningRule.syntheticRule(
                Granularities.DAY,
                new DynamicPartitionsSpec(5000000, null),
                null
            ),
            true
        )
    );

    ReindexingConfigBuilder configBuilder = new ReindexingConfigBuilder(
        provider,
        TEST_INTERVAL,
        REFERENCE_TIME,
        syntheticTimeline,
        baseTuning
    );

    int count = configBuilder.applyTo(builder);

    Assertions.assertEquals(0, count);

    InlineSchemaDataSourceCompactionConfig config = builder.build();
    Assertions.assertNotNull(config.getTuningConfig());
    // maxRowsInMemory from base should be preserved
    Assertions.assertEquals(100_000, config.getTuningConfig().getMaxRowsInMemory());
  }

  @Test
  public void test_applyTo_partitioningAndDeletionVCsCollide_throwsException()
  {
    VirtualColumns partitioningVCs = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn(
                "shared_vc",
                "col1 + 1",
                ColumnType.LONG,
                TestExprMacroTable.INSTANCE
            )
        )
    );

    ReindexingPartitioningRule partitioningRule = new ReindexingPartitioningRule(
        "part-rule",
        null,
        Period.days(30),
        Granularities.DAY,
        new DynamicPartitionsSpec(5000000, null),
        partitioningVCs
    );

    VirtualColumns deletionVCs = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn(
                "shared_vc",
                "col2 + 2",
                ColumnType.LONG,
                TestExprMacroTable.INSTANCE
            )
        )
    );

    ReindexingDeletionRule deletionRule = new ReindexingDeletionRule(
        "del-rule",
        null,
        Period.days(30),
        new SelectorDimFilter("shared_vc", "value", null),
        deletionVCs
    );

    ReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
        .partitioningRules(ImmutableList.of(partitioningRule))
        .deletionRules(ImmutableList.of(deletionRule))
        .build();

    InlineSchemaDataSourceCompactionConfig.Builder builder =
        InlineSchemaDataSourceCompactionConfig.builder()
            .forDataSource("test_datasource");

    ImmutableList<IntervalPartitioningInfo> syntheticTimeline = ImmutableList.of(
        new IntervalPartitioningInfo(TEST_INTERVAL, partitioningRule)
    );

    ReindexingConfigBuilder configBuilder = new ReindexingConfigBuilder(
        provider,
        TEST_INTERVAL,
        REFERENCE_TIME,
        syntheticTimeline,
        null
    );

    Assertions.assertThrows(
        DruidException.class,
        () -> configBuilder.applyTo(builder)
    );
  }

  @Test
  public void test_applyTo_partitioningAndDeletionVCsNoCollision_mergesSuccessfully()
  {
    VirtualColumns partitioningVCs = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn(
                "partition_vc",
                "col1 + 1",
                ColumnType.LONG,
                TestExprMacroTable.INSTANCE
            )
        )
    );

    ReindexingPartitioningRule partitioningRule = new ReindexingPartitioningRule(
        "part-rule",
        null,
        Period.days(30),
        Granularities.DAY,
        new DynamicPartitionsSpec(5000000, null),
        partitioningVCs
    );

    VirtualColumns deletionVCs = VirtualColumns.create(
        ImmutableList.of(
            new ExpressionVirtualColumn(
                "deletion_vc",
                "col2 + 2",
                ColumnType.LONG,
                TestExprMacroTable.INSTANCE
            )
        )
    );

    ReindexingDeletionRule deletionRule = new ReindexingDeletionRule(
        "del-rule",
        null,
        Period.days(30),
        new SelectorDimFilter("deletion_vc", "value", null),
        deletionVCs
    );

    ReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
        .partitioningRules(ImmutableList.of(partitioningRule))
        .deletionRules(ImmutableList.of(deletionRule))
        .build();

    InlineSchemaDataSourceCompactionConfig.Builder builder =
        InlineSchemaDataSourceCompactionConfig.builder()
            .forDataSource("test_datasource");

    ImmutableList<IntervalPartitioningInfo> syntheticTimeline = ImmutableList.of(
        new IntervalPartitioningInfo(TEST_INTERVAL, partitioningRule)
    );

    ReindexingConfigBuilder configBuilder = new ReindexingConfigBuilder(
        provider,
        TEST_INTERVAL,
        REFERENCE_TIME,
        syntheticTimeline,
        null
    );

    int count = configBuilder.applyTo(builder);

    // partitioningRule + deletionRule = 2
    Assertions.assertEquals(2, count);

    InlineSchemaDataSourceCompactionConfig config = builder.build();
    Assertions.assertNotNull(config.getTransformSpec());
    VirtualColumns resultVCs = config.getTransformSpec().getVirtualColumns();
    Assertions.assertNotNull(resultVCs);
    // Should have both VCs merged
    Assertions.assertEquals(2, resultVCs.getVirtualColumns().length);
    Set<String> vcNames = Set.of(
        resultVCs.getVirtualColumns()[0].getOutputName(),
        resultVCs.getVirtualColumns()[1].getOutputName()
    );
    Assertions.assertTrue(vcNames.contains("partition_vc"));
    Assertions.assertTrue(vcNames.contains("deletion_vc"));
  }

  private ReindexingRuleProvider createFullyPopulatedProvider()
  {
    ReindexingPartitioningRule partitioningRule = new ReindexingPartitioningRule(
        "gran-30d",
        null,
        Period.days(30),
        Granularities.DAY,
        new DynamicPartitionsSpec(5000000, null),
        null
    );

    ReindexingIndexSpecRule indexSpecRule = new ReindexingIndexSpecRule(
        "indexSpec-30d",
        null,
        Period.days(30),
        IndexSpec.getDefault()
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
        .partitioningRules(ImmutableList.of(partitioningRule))
        .indexSpecRules(ImmutableList.of(indexSpecRule))
        .deletionRules(ImmutableList.of(filterRule1, filterRule2))
        .dataSchemaRules(ImmutableList.of(dataSchemaRule1, dataSchemaRule2))
        .build();
  }
}
