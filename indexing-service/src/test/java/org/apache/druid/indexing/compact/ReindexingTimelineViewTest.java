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
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.joda.time.DateTime;
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
        "fromLatest",
        Period.days(1),
        true,
        DateTimes.of("2025-01-28T16:15:00Z"),
        null
    );

    final ReindexingTimelineView.ValidationError validationError = new ReindexingTimelineView.ValidationError(
        "GRANULARITY_ORDER",
        "Granularity must decrease over time",
        "2024-12-01/2025-01-01",
        "MONTH",
        "2025-01-01/2025-01-29",
        "DAY"
    );

    // Skipped interval: ruleCount=0, no config, no appliedRules
    final ReindexingTimelineView.IntervalConfig skippedInterval = new ReindexingTimelineView.IntervalConfig(
        Intervals.of("2024-12-01/2025-01-01"),
        0,
        null,
        Collections.emptyList()
    );

    // Active interval: ruleCount=1, has config and appliedRules
    final ReindexingTimelineView.IntervalConfig activeInterval = new ReindexingTimelineView.IntervalConfig(
        Intervals.of("2025-01-01/2025-01-29"),
        1,
        InlineSchemaDataSourceCompactionConfig.builder().forDataSource("testDS").build(),
        List.of(
            new ReindexingPartitioningRule(
                "day-rule",
                null,
                Period.days(7),
                Granularities.DAY,
                new DynamicPartitionsSpec(null, null),
                null
            )
        )
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
}
