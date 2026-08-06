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

import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.compaction.InlineReindexingRuleProvider;
import org.apache.druid.server.compaction.ReindexingDataSchemaRule;
import org.apache.druid.server.compaction.ReindexingDeletionRule;
import org.apache.druid.server.compaction.ReindexingPartitioningRule;
import org.apache.druid.server.compaction.ReindexingRule;
import org.apache.druid.server.compaction.ReindexingRuleProvider;
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link ReindexingPlanner}. The planner requires a non-null, non-empty
 * {@link SegmentTimeline} — callers short-circuit on missing data before invoking it — so every
 * test passes a constructed timeline that covers the rule range under test.
 */
public class ReindexingPlannerTest extends InitializedNullHandlingTest
{
  private static final DateTime REFERENCE_TIME = DateTimes.of("2025-02-01T00:00:00Z");
  // A timeline that covers everything the rules in these tests will look at.
  private static final SegmentTimeline WIDE_TIMELINE = createTimeline(
      DateTimes.of("2024-01-01T00:00:00Z"),
      DateTimes.of("2025-01-31T23:00:00Z")
  );

  @Test
  public void test_plan_comprehensive()
  {
    final ReindexingPartitioningRule partitioning7d = new ReindexingPartitioningRule(
        "seg-gran-7d",
        null,
        Period.days(7),
        Granularities.HOUR,
        new DynamicPartitionsSpec(5000000, null),
        null
    );
    final ReindexingPartitioningRule partitioning30d = new ReindexingPartitioningRule(
        "seg-gran-30d",
        null,
        Period.days(30),
        Granularities.DAY,
        new DynamicPartitionsSpec(5000000, null),
        null
    );
    final ReindexingDataSchemaRule dataSchema15d = new ReindexingDataSchemaRule(
        "data-schema-15d",
        null,
        Period.days(15),
        new UserCompactionTaskDimensionsConfig(null),
        new AggregatorFactory[]{new CountAggregatorFactory("count")},
        Granularities.MINUTE,
        true,
        null
    );
    final ReindexingDeletionRule deletion10d = new ReindexingDeletionRule(
        "deletion-10d",
        null,
        Period.days(10),
        new EqualityFilter("country", ColumnType.STRING, "US", null),
        null
    );
    final ReindexingDeletionRule deletion20d = new ReindexingDeletionRule(
        "deletion-20d",
        null,
        Period.days(20),
        new EqualityFilter("device", ColumnType.STRING, "mobile", null),
        null
    );

    final ReindexingRuleProvider provider = InlineReindexingRuleProvider
        .builder()
        .partitioningRules(List.of(partitioning7d, partitioning30d))
        .dataSchemaRules(List.of(dataSchema15d))
        .deletionRules(List.of(deletion10d, deletion20d))
        .build();

    final CascadingReindexingTemplate template = newTemplate(provider, null);
    final ReindexingPlan plan = new ReindexingPlanner(template).plan(REFERENCE_TIME, WIDE_TIMELINE);

    Assertions.assertEquals("testDS", plan.getDataSource());
    Assertions.assertEquals(REFERENCE_TIME, plan.getReferenceTime());
    Assertions.assertNull(plan.getValidationError());
    Assertions.assertNull(plan.getSkipOffset());
    Assertions.assertTrue(plan.getIntervals().size() >= 2, "Expected at least 2 intervals");

    boolean foundPartitioning = false;
    boolean foundDataSchema = false;
    boolean foundDeletion = false;

    for (ReindexingPlan.PlannedInterval planned : plan.getIntervals()) {
      Assertions.assertNotNull(planned.getInterval());
      if (planned.getRuleCount() == 0) {
        continue; // SKIPPED_NO_DATA or no rules apply — outside the comprehensive coverage check.
      }
      Assertions.assertNotNull(planned.getConfig());
      Assertions.assertEquals(planned.getRuleCount(), planned.getAppliedRules().size());
      Assertions.assertEquals(
          ReindexingPlan.IntervalDisposition.INCLUDED,
          planned.getDisposition()
      );

      for (ReindexingRule rule : planned.getAppliedRules()) {
        if (rule instanceof ReindexingPartitioningRule) {
          foundPartitioning = true;
        } else if (rule instanceof ReindexingDataSchemaRule) {
          foundDataSchema = true;
        } else if (rule instanceof ReindexingDeletionRule) {
          foundDeletion = true;
        }
      }
    }

    Assertions.assertTrue(foundPartitioning, "Plan should contain a segmentGranularity rule");
    Assertions.assertTrue(foundDataSchema, "Plan should contain a dataSchema rule");
    Assertions.assertTrue(foundDeletion, "Plan should contain a deletion rule");
  }

  @Test
  public void test_plan_propagatesTemplateTuningConfig()
  {
    final ReindexingRuleProvider provider = InlineReindexingRuleProvider
        .builder()
        .partitioningRules(List.of(
            new ReindexingPartitioningRule(
                "seg-7d",
                null,
                Period.days(7),
                Granularities.HOUR,
                new DynamicPartitionsSpec(5000000, null),
                null
            )
        ))
        .build();

    final UserCompactionTaskQueryTuningConfig templateTuningConfig = UserCompactionTaskQueryTuningConfig.builder()
        .maxRowsInMemory(12345)
        .maxNumConcurrentSubTasks(7)
        .maxRetry(9)
        .build();

    final CascadingReindexingTemplate template = newTemplate(provider, templateTuningConfig);
    final ReindexingPlan plan = new ReindexingPlanner(template).plan(REFERENCE_TIME, WIDE_TIMELINE);

    boolean sawConfig = false;
    for (ReindexingPlan.PlannedInterval planned : plan.getIntervals()) {
      if (planned.getConfig() == null) {
        continue;
      }
      sawConfig = true;
      final UserCompactionTaskQueryTuningConfig effective = planned.getConfig().getTuningConfig();
      Assertions.assertNotNull(effective);
      Assertions.assertEquals(Integer.valueOf(12345), effective.getMaxRowsInMemory());
      Assertions.assertEquals(Integer.valueOf(7), effective.getMaxNumConcurrentSubTasks());
      Assertions.assertEquals(Integer.valueOf(9), effective.getMaxRetry());
    }
    Assertions.assertTrue(sawConfig, "Expected at least one interval with a non-null config");
  }

  @Test
  public void test_plan_skipOffsetFromNow_skipsBeyondBoundary()
  {
    final DateTime referenceTime = DateTimes.of("2025-01-29T00:00:00Z");
    final Period skipOffset = Period.days(10);

    final ReindexingRuleProvider provider = InlineReindexingRuleProvider
        .builder()
        .partitioningRules(List.of(
            new ReindexingPartitioningRule("seg-3d", null, Period.days(3), Granularities.HOUR, new DynamicPartitionsSpec(5000000, null), null),
            new ReindexingPartitioningRule("seg-30d", null, Period.days(30), Granularities.DAY, new DynamicPartitionsSpec(5000000, null), null)
        ))
        .build();

    final CascadingReindexingTemplate template = new CascadingReindexingTemplate(
        "testDS",
        null,
        null,
        provider,
        null,
        null,
        skipOffset,
        Granularities.DAY,
        new DynamicPartitionsSpec(5000000, null),
        null,
        null
    );

    final SegmentTimeline timeline = createTimeline(
        DateTimes.of("2024-01-01T00:00:00Z"),
        DateTimes.of("2025-01-28T23:00:00Z")
    );
    final ReindexingPlan plan = new ReindexingPlanner(template).plan(referenceTime, timeline);

    Assertions.assertNotNull(plan.getSkipOffset());
    Assertions.assertEquals(ReindexingPlan.SkipOffsetResolution.Type.FROM_NOW, plan.getSkipOffset().getType());
    Assertions.assertEquals(skipOffset, plan.getSkipOffset().getPeriod());
    Assertions.assertEquals(referenceTime.minus(skipOffset), plan.getSkipOffset().getEffectiveEndTime());

    for (ReindexingPlan.PlannedInterval planned : plan.getIntervals()) {
      if (planned.getOriginalInterval().getEnd().isAfter(plan.getSkipOffset().getEffectiveEndTime())) {
        Assertions.assertTrue(
            planned.getDisposition() == ReindexingPlan.IntervalDisposition.TRUNCATED
            || planned.getDisposition() == ReindexingPlan.IntervalDisposition.SKIPPED_BEYOND_BOUNDARY,
            "Interval beyond skip-offset boundary should be TRUNCATED or SKIPPED_BEYOND_BOUNDARY"
        );
      }
    }
  }

  @Test
  public void test_plan_skipOffsetFromLatest_anchorsOnData()
  {
    final DateTime referenceTime = DateTimes.of("2025-01-29T00:00:00Z");
    final DateTime latestData = DateTimes.of("2025-01-28T12:00:00Z");

    final ReindexingRuleProvider provider = InlineReindexingRuleProvider
        .builder()
        .partitioningRules(List.of(
            new ReindexingPartitioningRule(
                "seg-7d",
                null,
                Period.days(7),
                Granularities.DAY,
                new DynamicPartitionsSpec(5000000, null),
                null
            )
        ))
        .build();

    final CascadingReindexingTemplate template = new CascadingReindexingTemplate(
        "testDS",
        null,
        null,
        provider,
        null,
        Period.hours(6),
        null,
        Granularities.DAY,
        new DynamicPartitionsSpec(5000000, null),
        null,
        null
    );

    final SegmentTimeline timeline = createTimeline(
        DateTimes.of("2024-12-01T00:00:00Z"),
        latestData
    );

    final ReindexingPlan plan = new ReindexingPlanner(template).plan(referenceTime, timeline);

    Assertions.assertNotNull(plan.getSkipOffset());
    Assertions.assertEquals(ReindexingPlan.SkipOffsetResolution.Type.FROM_LATEST, plan.getSkipOffset().getType());
    Assertions.assertEquals(latestData.minus(Period.hours(6)), plan.getSkipOffset().getEffectiveEndTime());
  }

  @Test
  public void test_plan_validationError_invalidGranularityTimeline()
  {
    final DateTime referenceTime = DateTimes.of("2025-01-29T16:15:00Z");

    final ReindexingRuleProvider provider = InlineReindexingRuleProvider
        .builder()
        .partitioningRules(List.of(
            new ReindexingPartitioningRule("hour-rule", null, Period.days(30), Granularities.HOUR, new DynamicPartitionsSpec(5000000, null), null),
            new ReindexingPartitioningRule("day-rule", null, Period.days(90), Granularities.DAY, new DynamicPartitionsSpec(5000000, null), null)
        ))
        .dataSchemaRules(List.of(
            new ReindexingDataSchemaRule(
                "metrics-7d",
                null,
                Period.days(7),
                null,
                new AggregatorFactory[]{new CountAggregatorFactory("count")},
                null,
                null,
                null
            )
        ))
        .build();

    final CascadingReindexingTemplate template = new CascadingReindexingTemplate(
        "testDS",
        null,
        null,
        provider,
        null,
        null,
        null,
        Granularities.MONTH,
        new DynamicPartitionsSpec(5000000, null),
        null,
        null
    );

    final ReindexingPlan plan = new ReindexingPlanner(template).plan(referenceTime, WIDE_TIMELINE);

    Assertions.assertNotNull(plan.getValidationError());
    Assertions.assertEquals(
        ReindexingPlan.PlanValidationError.Type.INVALID_GRANULARITY_TIMELINE,
        plan.getValidationError().getType()
    );
    Assertions.assertTrue(plan.getValidationError().getMessage().contains("Invalid segment granularity timeline"));
    Assertions.assertNotNull(plan.getValidationError().getOlderInterval());
    Assertions.assertNotNull(plan.getValidationError().getOlderGranularity());
    Assertions.assertNotNull(plan.getValidationError().getNewerInterval());
    Assertions.assertNotNull(plan.getValidationError().getNewerGranularity());
    Assertions.assertTrue(plan.getIntervals().isEmpty(), "Intervals should be empty on validation error");
  }

  @Test
  public void test_plan_ruleProviderNotReady_returnsEmptyPlan()
  {
    final ReindexingRuleProvider notReadyProvider = EasyMock.createMock(ReindexingRuleProvider.class);
    EasyMock.expect(notReadyProvider.isReady()).andReturn(false).anyTimes();
    EasyMock.expect(notReadyProvider.getType()).andReturn("mock").anyTimes();
    EasyMock.replay(notReadyProvider);

    final CascadingReindexingTemplate template = newTemplate(notReadyProvider, null);
    final ReindexingPlan plan = new ReindexingPlanner(template).plan(REFERENCE_TIME, WIDE_TIMELINE);

    Assertions.assertEquals("testDS", plan.getDataSource());
    Assertions.assertEquals(REFERENCE_TIME, plan.getReferenceTime());
    Assertions.assertTrue(plan.getIntervals().isEmpty());
    Assertions.assertNull(plan.getValidationError());
    Assertions.assertNull(plan.getSkipOffset());
  }

  @Test
  public void test_plan_intervalsOutsideDataRangeAreSkipped()
  {
    final ReindexingRuleProvider provider = InlineReindexingRuleProvider
        .builder()
        .partitioningRules(List.of(
            new ReindexingPartitioningRule(
                "seg-7d",
                null,
                Period.days(7),
                Granularities.DAY,
                new DynamicPartitionsSpec(5000000, null),
                null
            ),
            new ReindexingPartitioningRule(
                "seg-90d",
                null,
                Period.days(90),
                Granularities.DAY,
                new DynamicPartitionsSpec(5000000, null),
                null
            )
        ))
        .build();

    final CascadingReindexingTemplate template = newTemplate(provider, null);

    // Timeline covers only a thin slice — most rule intervals will have no data overlap.
    final SegmentTimeline timeline = createTimeline(
        DateTimes.of("2024-12-01T00:00:00Z"),
        DateTimes.of("2024-12-15T00:00:00Z")
    );

    final ReindexingPlan plan = new ReindexingPlanner(template).plan(REFERENCE_TIME, timeline);

    boolean sawSkippedNoData = false;
    for (ReindexingPlan.PlannedInterval planned : plan.getIntervals()) {
      if (planned.getDisposition() == ReindexingPlan.IntervalDisposition.SKIPPED_NO_DATA) {
        sawSkippedNoData = true;
        Assertions.assertNull(planned.getConfig());
        Assertions.assertEquals(0, planned.getRuleCount());
        Assertions.assertFalse(planned.isJobEligible());
      }
    }
    Assertions.assertTrue(sawSkippedNoData, "Expected at least one interval with disposition SKIPPED_NO_DATA");
  }

  private static CascadingReindexingTemplate newTemplate(
      ReindexingRuleProvider provider,
      UserCompactionTaskQueryTuningConfig tuningConfig
  )
  {
    return new CascadingReindexingTemplate(
        "testDS",
        null,
        null,
        provider,
        null,
        null,
        null,
        Granularities.DAY,
        new DynamicPartitionsSpec(5000000, null),
        null,
        tuningConfig
    );
  }

  private static SegmentTimeline createTimeline(DateTime start, DateTime end)
  {
    final DataSegment segment = DataSegment.builder()
        .dataSource("testDS")
        .interval(new Interval(start, end))
        .version("v1")
        .size(1000)
        .build();
    return SegmentTimeline.forSegments(Collections.singletonList(segment));
  }
}
