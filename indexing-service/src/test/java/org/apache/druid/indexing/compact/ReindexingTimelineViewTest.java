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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.guice.SupervisorModule;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.compaction.ReindexingPartitioningRule;
import org.apache.druid.server.compaction.ReindexingRule;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class ReindexingTimelineViewTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  @BeforeEach
  public void setUp()
  {
    OBJECT_MAPPER.registerModules(new SupervisorModule().getJacksonModules());
  }

  @Test
  public void test_serde() throws Exception
  {
    final DateTime referenceTime = DateTimes.of("2025-01-29T16:15:00Z");

    final ReindexingTimelineView.SkipOffsetInfo skipOffset = new ReindexingTimelineView.SkipOffsetInfo(
        "skipOffsetFromLatest",
        Period.days(1),
        DateTimes.of("2025-01-28T16:15:00Z")
    );
    final ReindexingTimelineView.ValidationError validationError = new ReindexingTimelineView.ValidationError(
        "INVALID_GRANULARITY_TIMELINE",
        "Granularity must decrease over time",
        "2024-12-01/2025-01-01",
        "MONTH",
        "2025-01-01/2025-01-29",
        "DAY"
    );

    final ReindexingTimelineView.IntervalConfig skippedInterval = new ReindexingTimelineView.IntervalConfig(
        Intervals.of("2024-12-01/2025-01-01"),
        0,
        null,
        Collections.emptyList()
    );
    final ReindexingTimelineView.IntervalConfig activeInterval = new ReindexingTimelineView.IntervalConfig(
        Intervals.of("2025-01-01/2025-01-29"),
        1,
        InlineSchemaDataSourceCompactionConfig.builder().forDataSource("testDS").build(),
        List.of(new ReindexingPartitioningRule(
            "day-rule",
            null,
            Period.days(7),
            Granularities.DAY,
            new DynamicPartitionsSpec(null, null),
            null
        ))
    );

    final ReindexingTimelineView original = new ReindexingTimelineView(
        "testDS",
        referenceTime,
        skipOffset,
        List.of(skippedInterval, activeInterval),
        validationError
    );

    final String json = OBJECT_MAPPER.writeValueAsString(original);
    final ReindexingTimelineView deserialized = OBJECT_MAPPER.readValue(json, ReindexingTimelineView.class);

    Assertions.assertEquals(original, deserialized);
  }

  @Test
  public void test_fromPlan_projectsIntervalsByDisposition()
  {
    final DateTime referenceTime = DateTimes.of("2025-01-29T00:00:00Z");

    final ReindexingPartitioningRule partitioningRule = new ReindexingPartitioningRule(
        "day-rule",
        null,
        Period.days(7),
        Granularities.DAY,
        new DynamicPartitionsSpec(null, null),
        null
    );

    final Interval includedRange = Intervals.of("2024-12-01/2025-01-01");
    final Interval skippedBoundary = Intervals.of("2025-01-01/2025-01-29");
    final Interval skippedNoData = Intervals.of("2024-01-01/2024-12-01");

    final ReindexingPlan plan = new ReindexingPlan(
        "testDS",
        referenceTime,
        null,
        List.of(
            new ReindexingPlan.PlannedInterval(
                skippedNoData,
                skippedNoData,
                ReindexingPlan.IntervalDisposition.SKIPPED_NO_DATA,
                partitioningRule,
                false,
                Granularities.DAY,
                0,
                null,
                Collections.emptyList()
            ),
            new ReindexingPlan.PlannedInterval(
                includedRange,
                includedRange,
                ReindexingPlan.IntervalDisposition.INCLUDED,
                partitioningRule,
                false,
                Granularities.DAY,
                1,
                InlineSchemaDataSourceCompactionConfig.builder().forDataSource("testDS").build(),
                List.of((ReindexingRule) partitioningRule)
            ),
            new ReindexingPlan.PlannedInterval(
                skippedBoundary,
                skippedBoundary,
                ReindexingPlan.IntervalDisposition.SKIPPED_BEYOND_BOUNDARY,
                partitioningRule,
                false,
                Granularities.DAY,
                0,
                null,
                Collections.emptyList()
            )
        ),
        null
    );

    final ReindexingTimelineView view = ReindexingTimelineView.fromPlan(plan);

    // SKIPPED_NO_DATA should be omitted; INCLUDED and SKIPPED_BEYOND_BOUNDARY should be present.
    Assertions.assertEquals(2, view.getIntervals().size());
    Assertions.assertEquals(includedRange, view.getIntervals().get(0).getInterval());
    Assertions.assertEquals(1, view.getIntervals().get(0).getRuleCount());
    Assertions.assertNotNull(view.getIntervals().get(0).getConfig());

    Assertions.assertEquals(skippedBoundary, view.getIntervals().get(1).getInterval());
    Assertions.assertEquals(0, view.getIntervals().get(1).getRuleCount());
    Assertions.assertNull(view.getIntervals().get(1).getConfig());
  }
}
