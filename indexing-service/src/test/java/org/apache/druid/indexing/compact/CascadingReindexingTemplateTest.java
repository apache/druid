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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.guice.SupervisorModule;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.server.compaction.InlineReindexingRuleProvider;
import org.apache.druid.server.compaction.ReindexingMetricsRule;
import org.apache.druid.server.compaction.ReindexingRule;
import org.apache.druid.server.compaction.ReindexingRuleProvider;
import org.apache.druid.server.compaction.ReindexingSegmentGranularityRule;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CascadingReindexingTemplateTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    OBJECT_MAPPER.registerModules(new SupervisorModule().getJacksonModules());
  }

  @Test
  public void test_serde() throws Exception
  {
    final CascadingReindexingTemplate template = new CascadingReindexingTemplate(
        "testDataSource",
        50,
        1000000L,
        InlineReindexingRuleProvider.builder()
            .segmentGranularityRules(List.of(
                new ReindexingSegmentGranularityRule(
                    "hourRule",
                    null,
                    Period.days(7),
                    Granularities.HOUR
                ),
                new ReindexingSegmentGranularityRule(
                    "dayRule",
                    null,
                    Period.days(30),
                    Granularities.DAY
                )
            ))
            .build(),
        CompactionEngine.NATIVE,
        ImmutableMap.of("context_key", "context_value"),
        null,
        null,
        Granularities.DAY
    );

    final String json = OBJECT_MAPPER.writeValueAsString(template);
    final CascadingReindexingTemplate fromJson = OBJECT_MAPPER.readValue(json, CascadingReindexingTemplate.class);

    Assert.assertEquals(template.getDataSource(), fromJson.getDataSource());
    Assert.assertEquals(template.getTaskPriority(), fromJson.getTaskPriority());
    Assert.assertEquals(template.getInputSegmentSizeBytes(), fromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(template.getEngine(), fromJson.getEngine());
    Assert.assertEquals(template.getTaskContext(), fromJson.getTaskContext());
    Assert.assertEquals(template.getType(), fromJson.getType());
  }

  @Test
  public void test_serde_asDataSourceCompactionConfig() throws Exception
  {
    final CascadingReindexingTemplate template = new CascadingReindexingTemplate(
        "testDataSource",
        30,
        500000L,
        InlineReindexingRuleProvider.builder()
            .segmentGranularityRules(List.of(
                new ReindexingSegmentGranularityRule(
                    "rule1",
                    null,
                    Period.days(7),
                    Granularities.HOUR
                )
            ))
            .build(),
        CompactionEngine.MSQ,
        ImmutableMap.of("key", "value"),
        null,
        null,
        Granularities.HOUR
    );

    // Serialize and deserialize as DataSourceCompactionConfig interface
    final String json = OBJECT_MAPPER.writeValueAsString(template);
    final DataSourceCompactionConfig fromJson = OBJECT_MAPPER.readValue(json, DataSourceCompactionConfig.class);

    Assert.assertTrue(fromJson instanceof CascadingReindexingTemplate);
    final CascadingReindexingTemplate cascadingFromJson = (CascadingReindexingTemplate) fromJson;

    Assert.assertEquals("testDataSource", cascadingFromJson.getDataSource());
    Assert.assertEquals(30, cascadingFromJson.getTaskPriority());
    Assert.assertEquals(500000L, cascadingFromJson.getInputSegmentSizeBytes());
    Assert.assertEquals(CompactionEngine.MSQ, cascadingFromJson.getEngine());
    Assert.assertEquals(ImmutableMap.of("key", "value"), cascadingFromJson.getTaskContext());
    Assert.assertEquals(CascadingReindexingTemplate.TYPE, cascadingFromJson.getType());
  }

  @Test
  public void test_createCompactionJobs_ruleProviderNotReady()
  {
    final ReindexingRuleProvider notReadyProvider = EasyMock.createMock(ReindexingRuleProvider.class);
    EasyMock.expect(notReadyProvider.isReady()).andReturn(false);
    EasyMock.expect(notReadyProvider.getType()).andReturn("mock-provider");
    EasyMock.replay(notReadyProvider);

    final CascadingReindexingTemplate template = new CascadingReindexingTemplate(
        "testDataSource",
        null,
        null,
        notReadyProvider,
        null,
        null,
        null,
        null,
        Granularities.DAY
    );

    // Call createCompactionJobs - should return empty list without processing
    final List<CompactionJob> jobs = template.createCompactionJobs(null, null);

    Assert.assertTrue(jobs.isEmpty());
    EasyMock.verify(notReadyProvider);
  }

  @Test
  public void test_constructor_setBothSkipOffsetStrategiesThrowsException()
  {
    final ReindexingRuleProvider mockProvider = EasyMock.createMock(ReindexingRuleProvider.class);
    EasyMock.replay(mockProvider);

    IllegalArgumentException exception = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new CascadingReindexingTemplate(
            "testDataSource",
            null,
            null,
            mockProvider,
            null,
            null,
            Period.days(7),  // skipOffsetFromLatest
            Period.days(3),   // skipOffsetFromNow
            Granularities.DAY
        )
    );

    Assert.assertEquals("Cannot set both skipOffsetFromNow and skipOffsetFromLatest", exception.getMessage());
    EasyMock.verify(mockProvider);
  }

  @Test
  public void test_createCompactionJobs_simple()
  {
    DateTime referenceTime = DateTimes.of("2024-01-15T00:00:00Z");
    SegmentTimeline timeline = createTestTimeline(referenceTime.minusDays(90), referenceTime.minusDays(10));
    ReindexingRuleProvider mockProvider = createMockProvider(List.of(Period.days(7), Period.days(30)));
    CompactionJobParams mockParams = createMockParams(referenceTime, timeline);
    DruidInputSource mockSource = createMockSource();

    TestCascadingReindexingTemplate template = new TestCascadingReindexingTemplate(
        "testDS", null, null, mockProvider, null, null, null, null
    );

    template.createCompactionJobs(mockSource, mockParams);
    List<Interval> processedIntervals = template.getProcessedIntervals();

    Assert.assertEquals(2, processedIntervals.size());
    // Intervals are now in chronological order (oldest first)
    Assert.assertEquals(referenceTime.minusDays(90), processedIntervals.get(0).getStart());
    Assert.assertEquals(referenceTime.minusDays(30), processedIntervals.get(0).getEnd());
    Assert.assertEquals(referenceTime.minusDays(30), processedIntervals.get(1).getStart());
    Assert.assertEquals(referenceTime.minusDays(10), processedIntervals.get(1).getEnd());

    EasyMock.verify(mockProvider, mockParams, mockSource);
  }

  @Test
  public void test_createCompactionJobs_withSkipOffsetFromLatest_skipAllOfTime()
  {
    DateTime referenceTime = DateTimes.of("2024-01-15T00:00:00Z");
    SegmentTimeline timeline = createTestTimeline(referenceTime.minusDays(90), referenceTime.minusDays(10));
    ReindexingRuleProvider mockProvider = createMockProvider(List.of(Period.days(7), Period.days(30)));
    CompactionJobParams mockParams = createMockParams(referenceTime, timeline);
    DruidInputSource mockSource = createMockSource();

    TestCascadingReindexingTemplate template = new TestCascadingReindexingTemplate(
        "testDS", null, null, mockProvider, null, null, Period.days(100), null
    );

    List<CompactionJob> jobs = template.createCompactionJobs(mockSource, mockParams);

    Assert.assertTrue(jobs.isEmpty());
    Assert.assertTrue(template.getProcessedIntervals().isEmpty());

    EasyMock.verify(mockProvider, mockParams, mockSource);
  }

  @Test
  public void test_createCompactionJobs_withSkipOffsetFromLatest_trimsIntervalEnd()
  {
    DateTime referenceTime = DateTimes.of("2024-01-15T00:00:00Z");
    SegmentTimeline timeline = createTestTimeline(referenceTime.minusDays(90), referenceTime.minusDays(10));
    ReindexingRuleProvider mockProvider = createMockProvider(List.of(Period.days(7), Period.days(30)));
    CompactionJobParams mockParams = createMockParams(referenceTime, timeline);
    DruidInputSource mockSource = createMockSource();

    TestCascadingReindexingTemplate template = new TestCascadingReindexingTemplate(
        "testDS", null, null, mockProvider, null, null, Period.days(5), null
    );

    template.createCompactionJobs(mockSource, mockParams);
    List<Interval> processedIntervals = template.getProcessedIntervals();

    Assert.assertEquals(2, processedIntervals.size());
    // Intervals are now in chronological order (oldest first)
    Assert.assertEquals(referenceTime.minusDays(90), processedIntervals.get(0).getStart());
    Assert.assertEquals(referenceTime.minusDays(30), processedIntervals.get(0).getEnd());
    Assert.assertEquals(referenceTime.minusDays(30), processedIntervals.get(1).getStart());
    Assert.assertEquals(referenceTime.minusDays(15), processedIntervals.get(1).getEnd());

    EasyMock.verify(mockProvider, mockParams, mockSource);
  }

  @Test
  public void test_createCompactionJobs_withSkipOffsetFromLatest_eliminatesInterval()
  {
    DateTime referenceTime = DateTimes.of("2024-01-15T00:00:00Z");
    SegmentTimeline timeline = createTestTimeline(referenceTime.minusDays(90), referenceTime.minusDays(10));
    ReindexingRuleProvider mockProvider = createMockProvider(List.of(Period.days(7), Period.days(30)));
    CompactionJobParams mockParams = createMockParams(referenceTime, timeline);
    DruidInputSource mockSource = createMockSource();

    TestCascadingReindexingTemplate template = new TestCascadingReindexingTemplate(
        "testDS", null, null, mockProvider, null, null, Period.days(30), null
    );

    template.createCompactionJobs(mockSource, mockParams);
    List<Interval> processedIntervals = template.getProcessedIntervals();

    Assert.assertEquals(1, processedIntervals.size());
    Assert.assertEquals(referenceTime.minusDays(90), processedIntervals.get(0).getStart());
    Assert.assertEquals(referenceTime.minusDays(40), processedIntervals.get(0).getEnd());

    EasyMock.verify(mockProvider, mockParams, mockSource);
  }

  @Test
  public void test_createCompactionJobs_withSkipOffsetFromNow_skipAllOfTime()
  {
    DateTime referenceTime = DateTimes.of("2024-01-15T00:00:00Z");
    SegmentTimeline timeline = createTestTimeline(referenceTime.minusDays(90), referenceTime.minusDays(10));
    ReindexingRuleProvider mockProvider = createMockProvider(List.of(Period.days(7), Period.days(30)));
    CompactionJobParams mockParams = createMockParams(referenceTime, timeline);
    DruidInputSource mockSource = createMockSource();

    TestCascadingReindexingTemplate template = new TestCascadingReindexingTemplate(
        "testDS", null, null, mockProvider, null, null, null, Period.days(100)
    );

    List<CompactionJob> jobs = template.createCompactionJobs(mockSource, mockParams);

    Assert.assertTrue(jobs.isEmpty());
    Assert.assertTrue(template.getProcessedIntervals().isEmpty());

    EasyMock.verify(mockProvider, mockParams, mockSource);
  }

  @Test
  public void test_createCompactionJobs_withSkipOffsetFromNow_trimsIntervalEnd()
  {
    DateTime referenceTime = DateTimes.of("2024-01-15T00:00:00Z");
    SegmentTimeline timeline = createTestTimeline(referenceTime.minusDays(90), referenceTime.minusDays(10));
    ReindexingRuleProvider mockProvider = createMockProvider(List.of(Period.days(7), Period.days(30)));
    CompactionJobParams mockParams = createMockParams(referenceTime, timeline);
    DruidInputSource mockSource = createMockSource();

    TestCascadingReindexingTemplate template = new TestCascadingReindexingTemplate(
        "testDS", null, null, mockProvider, null, null, null, Period.days(20)
    );

    template.createCompactionJobs(mockSource, mockParams);
    List<Interval> processedIntervals = template.getProcessedIntervals();

    Assert.assertEquals(2, processedIntervals.size());
    // Intervals are now in chronological order (oldest first)
    Assert.assertEquals(referenceTime.minusDays(90), processedIntervals.get(0).getStart());
    Assert.assertEquals(referenceTime.minusDays(30), processedIntervals.get(0).getEnd());
    Assert.assertEquals(referenceTime.minusDays(30), processedIntervals.get(1).getStart());
    Assert.assertEquals(referenceTime.minusDays(20), processedIntervals.get(1).getEnd());

    EasyMock.verify(mockProvider, mockParams, mockSource);
  }

  @Test
  public void test_createCompactionJobs_withSkipOffsetFromNow_eliminatesInterval()
  {
    DateTime referenceTime = DateTimes.of("2024-01-15T00:00:00Z");
    SegmentTimeline timeline = createTestTimeline(referenceTime.minusDays(90), referenceTime.minusDays(10));
    ReindexingRuleProvider mockProvider = createMockProvider(List.of(Period.days(7), Period.days(30)));
    CompactionJobParams mockParams = createMockParams(referenceTime, timeline);
    DruidInputSource mockSource = createMockSource();

    TestCascadingReindexingTemplate template = new TestCascadingReindexingTemplate(
        "testDS", null, null, mockProvider, null, null, null, Period.days(40)
    );

    template.createCompactionJobs(mockSource, mockParams);
    List<Interval> processedIntervals = template.getProcessedIntervals();

    Assert.assertEquals(1, processedIntervals.size());
    Assert.assertEquals(referenceTime.minusDays(90), processedIntervals.get(0).getStart());
    Assert.assertEquals(referenceTime.minusDays(40), processedIntervals.get(0).getEnd());

    EasyMock.verify(mockProvider, mockParams, mockSource);
  }

  @Test
  public void test_generateAlignedSearchIntervals_withGranularityAlignment()
  {
    // Reference time: 2025-01-29T16:15
    DateTime referenceTime = DateTimes.of("2025-01-29T16:15:00Z");

    // Create rules with different periods and granularities
    // P7D with HOUR granularity -> raw end: 2025-01-22T16:15 -> aligned: 2025-01-22T16:00
    // P1M with DAY granularity -> raw end: 2024-12-29T16:15 -> aligned: 2024-12-29T00:00
    // P3M with MONTH granularity -> raw end: 2024-10-29T16:15 -> aligned: 2024-10-01T00:00
    ReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
        .segmentGranularityRules(List.of(
            new ReindexingSegmentGranularityRule("hour-rule", null, Period.days(7), Granularities.HOUR),
            new ReindexingSegmentGranularityRule("day-rule", null, Period.months(1), Granularities.DAY),
            new ReindexingSegmentGranularityRule("month-rule", null, Period.months(3), Granularities.MONTH)
        ))
        .build();

    CascadingReindexingTemplate template = new CascadingReindexingTemplate(
        "testDS",
        null,
        null,
        provider,
        null,
        null,
        null,
        null,
        Granularities.DAY
    );

    List<Interval> intervals = template.generateAlignedSearchIntervals(referenceTime);

    // Expected 3 intervals, ordered from oldest to newest:
    // 1. [-inf, 2024-10-01T00:00) - P3M rule with MONTH granularity
    // 2. [2024-10-01T00:00, 2024-12-29T00:00) - P1M rule with DAY granularity
    // 3. [2024-12-29T00:00, 2025-01-22T16:00) - P7D rule with HOUR granularity

    Assert.assertEquals(3, intervals.size());

    // Interval 1: oldest
    Assert.assertEquals(DateTimes.MIN, intervals.get(0).getStart());
    Assert.assertEquals(DateTimes.of("2024-10-01T00:00:00Z"), intervals.get(0).getEnd());

    // Interval 2: middle
    Assert.assertEquals(DateTimes.of("2024-10-01T00:00:00Z"), intervals.get(1).getStart());
    Assert.assertEquals(DateTimes.of("2024-12-29T00:00:00Z"), intervals.get(1).getEnd());

    // Interval 3: most recent
    Assert.assertEquals(DateTimes.of("2024-12-29T00:00:00Z"), intervals.get(2).getStart());
    Assert.assertEquals(DateTimes.of("2025-01-22T16:00:00Z"), intervals.get(2).getEnd());
  }

  @Test
  public void test_generateAlignedSearchIntervals_withNonSegmentGranularityRuleSplits()
  {
    // Reference time: 2025-01-29T16:15
    DateTime referenceTime = DateTimes.of("2025-01-29T16:15:00Z");

    // Segment granularity rules create base timeline:
    // [-inf, 2024-10-01T00:00) MONTH
    // [2024-10-01T00:00, 2024-12-29T00:00) DAY
    // [2024-12-29T00:00, 2025-01-22T16:00) HOUR
    //
    // Non-segment-gran rules with thresholds:
    // P8D  -> 2025-01-21T16:15 -> falls in HOUR interval -> aligned: 2025-01-21T16:00
    // P14D -> 2025-01-15T16:15 -> falls in HOUR interval -> aligned: 2025-01-15T16:00
    // P45D -> 2024-12-15T16:15 -> falls in DAY interval  -> aligned: 2024-12-15T00:00
    // P100D -> 2024-10-21T16:15 -> falls in MONTH interval -> aligned: 2024-10-01T00:00 (at boundary, no split)

    ReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
        .segmentGranularityRules(List.of(
            new ReindexingSegmentGranularityRule("hour-rule", null, Period.days(7), Granularities.HOUR),
            new ReindexingSegmentGranularityRule("day-rule", null, Period.months(1), Granularities.DAY),
            new ReindexingSegmentGranularityRule("month-rule", null, Period.months(3), Granularities.MONTH)
        ))
        .metricsRules(List.of(
            new ReindexingMetricsRule(
                "metrics-8d", null, Period.days(8),
                new AggregatorFactory[0]
            ),
            new ReindexingMetricsRule(
                "metrics-14d", null, Period.days(14),
                new AggregatorFactory[0]
            ),
            new ReindexingMetricsRule(
                "metrics-45d", null, Period.days(45),
                new AggregatorFactory[0]
            ),
            new ReindexingMetricsRule(
                "metrics-100d", null, Period.days(100),
                new AggregatorFactory[0]
            )
        ))
        .build();

    CascadingReindexingTemplate template = new CascadingReindexingTemplate(
        "testDS",
        null,
        null,
        provider,
        null,
        null,
        null,
        null,
        Granularities.DAY
    );

    List<Interval> intervals = template.generateAlignedSearchIntervals(referenceTime);

    // Expected 7 intervals after splitting:
    // 1. [-inf, 2024-10-01T00:00)                      - no split in MONTH interval
    // 2. [2024-10-01T00:00, 2024-10-21T00:00)         - split by P100D (falls in DAY interval!)
    // 3. [2024-10-21T00:00, 2024-12-15T00:00)         - between P100D and P45D
    // 4. [2024-12-15T00:00, 2024-12-29T00:00)         - split by P45D
    // 5. [2024-12-29T00:00, 2025-01-15T16:00)         - split by P14D
    // 6. [2025-01-15T16:00, 2025-01-21T16:00)         - between P14D and P8D
    // 7. [2025-01-21T16:00, 2025-01-22T16:00)         - split by P8D

    Assert.assertEquals(7, intervals.size());

    // Interval 1: no split in MONTH interval
    Assert.assertEquals(DateTimes.MIN, intervals.get(0).getStart());
    Assert.assertEquals(DateTimes.of("2024-10-01T00:00:00Z"), intervals.get(0).getEnd());

    // Interval 2: split by P100D (2024-10-21 falls in DAY granularity interval)
    Assert.assertEquals(DateTimes.of("2024-10-01T00:00:00Z"), intervals.get(1).getStart());
    Assert.assertEquals(DateTimes.of("2024-10-21T00:00:00Z"), intervals.get(1).getEnd());

    // Interval 3: between P100D and P45D
    Assert.assertEquals(DateTimes.of("2024-10-21T00:00:00Z"), intervals.get(2).getStart());
    Assert.assertEquals(DateTimes.of("2024-12-15T00:00:00Z"), intervals.get(2).getEnd());

    // Interval 4: split by P45D
    Assert.assertEquals(DateTimes.of("2024-12-15T00:00:00Z"), intervals.get(3).getStart());
    Assert.assertEquals(DateTimes.of("2024-12-29T00:00:00Z"), intervals.get(3).getEnd());

    // Interval 5: split by P14D
    Assert.assertEquals(DateTimes.of("2024-12-29T00:00:00Z"), intervals.get(4).getStart());
    Assert.assertEquals(DateTimes.of("2025-01-15T16:00:00Z"), intervals.get(4).getEnd());

    // Interval 6: between P14D and P8D
    Assert.assertEquals(DateTimes.of("2025-01-15T16:00:00Z"), intervals.get(5).getStart());
    Assert.assertEquals(DateTimes.of("2025-01-21T16:00:00Z"), intervals.get(5).getEnd());

    // Interval 7: split by P8D
    Assert.assertEquals(DateTimes.of("2025-01-21T16:00:00Z"), intervals.get(6).getStart());
    Assert.assertEquals(DateTimes.of("2025-01-22T16:00:00Z"), intervals.get(6).getEnd());
  }

  @Test
  public void test_generateAlignedSearchIntervals_withNoSegmentGranularityRules()
  {
    // Reference time: 2025-01-29T16:15
    DateTime referenceTime = DateTimes.of("2025-01-29T16:15:00Z");

    // No segment granularity rules - only metrics rules
    // P8D  -> 2025-01-21T16:15 (smallest/most recent)
    // P14D -> 2025-01-15T16:15
    // P45D -> 2024-12-15T16:15
    ReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
        .metricsRules(List.of(
            new ReindexingMetricsRule("metrics-8d", null, Period.days(8), new AggregatorFactory[0]),
            new ReindexingMetricsRule("metrics-14d", null, Period.days(14), new AggregatorFactory[0]),
            new ReindexingMetricsRule("metrics-45d", null, Period.days(45), new AggregatorFactory[0])
        ))
        .build();

    CascadingReindexingTemplate template = new CascadingReindexingTemplate(
        "testDS",
        null,
        null,
        provider,
        null,
        null,
        null,
        null,
        Granularities.DAY  // default granularity
    );

    List<Interval> intervals = template.generateAlignedSearchIntervals(referenceTime);

    // Expected: One base interval [-inf, aligned(smallest period)) then split by other rules
    // Smallest period is P8D -> threshold: 2025-01-21T16:15
    // Aligned to DAY: 2025-01-21T00:00
    // Then split by P14D (2025-01-15T00:00) and P45D (2024-12-15T00:00)
    //
    // Result: 4 intervals
    // 1. [-inf, 2024-12-15T00:00)
    // 2. [2024-12-15T00:00, 2025-01-15T00:00)
    // 3. [2025-01-15T00:00, 2025-01-21T00:00)
    // 4. [2025-01-21T00:00, 2025-01-21T00:00) - WAIT, this would be zero-length!

    // Actually, P14D and P45D both fall within the base interval [-inf, 2025-01-21T00:00)
    // So they will split it:
    // 1. [-inf, 2024-12-15T00:00)                - before P45D
    // 2. [2024-12-15T00:00, 2025-01-15T00:00)   - between P45D and P14D
    // 3. [2025-01-15T00:00, 2025-01-21T00:00)   - after P14D, before base end

    Assert.assertEquals(3, intervals.size());

    Assert.assertEquals(DateTimes.MIN, intervals.get(0).getStart());
    Assert.assertEquals(DateTimes.of("2024-12-15T00:00:00Z"), intervals.get(0).getEnd());

    Assert.assertEquals(DateTimes.of("2024-12-15T00:00:00Z"), intervals.get(1).getStart());
    Assert.assertEquals(DateTimes.of("2025-01-15T00:00:00Z"), intervals.get(1).getEnd());

    Assert.assertEquals(DateTimes.of("2025-01-15T00:00:00Z"), intervals.get(2).getStart());
    Assert.assertEquals(DateTimes.of("2025-01-21T00:00:00Z"), intervals.get(2).getEnd());
  }

  @Test
  public void test_generateAlignedSearchIntervals_prependIntervalForShortNonSegmentGranRules()
  {
    // Reference time: 2025-01-29T16:15
    DateTime referenceTime = DateTimes.of("2025-01-29T16:15:00Z");

    // Segment granularity rules:
    // P1M -> 2024-12-29T16:15 (most recent segment gran threshold)
    // P3M -> 2024-10-29T16:15
    //
    // Non-segment-gran rules (more recent than P1M!):
    // P7D  -> 2025-01-22T16:15
    // P14D -> 2025-01-15T16:15
    // P21D -> 2025-01-08T16:15

    ReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
        .segmentGranularityRules(List.of(
            new ReindexingSegmentGranularityRule("month-rule", null, Period.months(3), Granularities.MONTH),
            new ReindexingSegmentGranularityRule("day-rule", null, Period.months(1), Granularities.DAY)
        ))
        .metricsRules(List.of(
            new ReindexingMetricsRule("metrics-7d", null, Period.days(7), new AggregatorFactory[0]),
            new ReindexingMetricsRule("metrics-14d", null, Period.days(14), new AggregatorFactory[0]),
            new ReindexingMetricsRule("metrics-21d", null, Period.days(21), new AggregatorFactory[0])
        ))
        .build();

    CascadingReindexingTemplate template = new CascadingReindexingTemplate(
        "testDS",
        null,
        null,
        provider,
        null,
        null,
        null,
        null,
        Granularities.HOUR  // default for prepended interval
    );

    List<Interval> intervals = template.generateAlignedSearchIntervals(referenceTime);

    // Expected base timeline:
    // 1. [-inf, 2024-10-01T00:00)              - P3M with MONTH gran
    // 2. [2024-10-01T00:00, 2024-12-29T00:00)  - P1M with DAY gran
    // 3. [2024-12-29T00:00, 2025-01-22T16:00)  - PREPENDED with HOUR gran (for P7D)
    //
    // Then split by P14D (2025-01-15T16:00) and P21D (2025-01-08T16:00) which fall in interval 3:
    // 3a. [2024-12-29T00:00, 2025-01-08T16:00)
    // 3b. [2025-01-08T16:00, 2025-01-15T16:00)
    // 3c. [2025-01-15T16:00, 2025-01-22T16:00)

    Assert.assertEquals(5, intervals.size());

    // Interval 1: oldest
    Assert.assertEquals(DateTimes.MIN, intervals.get(0).getStart());
    Assert.assertEquals(DateTimes.of("2024-10-01T00:00:00Z"), intervals.get(0).getEnd());

    // Interval 2: middle
    Assert.assertEquals(DateTimes.of("2024-10-01T00:00:00Z"), intervals.get(1).getStart());
    Assert.assertEquals(DateTimes.of("2024-12-29T00:00:00Z"), intervals.get(1).getEnd());

    // Interval 3a: prepended interval, split by P21D
    Assert.assertEquals(DateTimes.of("2024-12-29T00:00:00Z"), intervals.get(2).getStart());
    Assert.assertEquals(DateTimes.of("2025-01-08T16:00:00Z"), intervals.get(2).getEnd());

    // Interval 3b: between P21D and P14D
    Assert.assertEquals(DateTimes.of("2025-01-08T16:00:00Z"), intervals.get(3).getStart());
    Assert.assertEquals(DateTimes.of("2025-01-15T16:00:00Z"), intervals.get(3).getEnd());

    // Interval 3c: after P14D
    Assert.assertEquals(DateTimes.of("2025-01-15T16:00:00Z"), intervals.get(4).getStart());
    Assert.assertEquals(DateTimes.of("2025-01-22T16:00:00Z"), intervals.get(4).getEnd());
  }

  private static class TestCascadingReindexingTemplate extends CascadingReindexingTemplate
  {
    // Capture intervals that were processed for assertions
    private final List<Interval> processedIntervals = new ArrayList<>();

    public TestCascadingReindexingTemplate(
        String dataSource,
        Integer taskPriority,
        Long inputSegmentSizeBytes,
        ReindexingRuleProvider ruleProvider,
        CompactionEngine engine,
        Map<String, Object> taskContext,
        Period skipOffsetFromLatest,
        Period skipOffsetFromNow
    )
    {
      super(dataSource, taskPriority, inputSegmentSizeBytes, ruleProvider,
            engine, taskContext, skipOffsetFromLatest, skipOffsetFromNow, Granularities.DAY);
    }

    public List<Interval> getProcessedIntervals()
    {
      return processedIntervals;
    }

    @Override
    protected CompactionJobTemplate createJobTemplateForInterval(
        InlineSchemaDataSourceCompactionConfig config
    )
    {
      return new CompactionJobTemplate() {
        @Override
        public String getType()
        {
          return "test";
        }

        @Override
        @Nullable
        public Granularity getSegmentGranularity()
        {
          return null;
        }

        @Override
        public List<CompactionJob> createCompactionJobs(
            DruidInputSource source,
            CompactionJobParams params
        )
        {
          // Record the interval that was processed
          processedIntervals.add(source.getInterval());

          // Return a single mock job
          return List.of();
        }
      };
    }
  }

  private SegmentTimeline createTestTimeline(DateTime start, DateTime end)
  {
    DataSegment segment = DataSegment.builder()
        .dataSource("testDS")
        .interval(new Interval(start, end))
        .version("v1")
        .size(1000)
        .build();
    return SegmentTimeline.forSegments(Collections.singletonList(segment));
  }

  private ReindexingRuleProvider createMockProvider(List<Period> periods)
  {
    // Create segment granularity rules for each period
    List<ReindexingSegmentGranularityRule> segmentGranularityRules = new ArrayList<>();
    for (int i = 0; i < periods.size(); i++) {
      segmentGranularityRules.add(new ReindexingSegmentGranularityRule(
          "segment-gran-rule-" + i,
          null,
          periods.get(i),
          Granularities.HOUR
      ));
    }

    ReindexingRuleProvider mockProvider = EasyMock.createMock(ReindexingRuleProvider.class);
    EasyMock.expect(mockProvider.isReady()).andReturn(true);
    EasyMock.expect(mockProvider.getSegmentGranularityRules()).andReturn(segmentGranularityRules).anyTimes();
    // Return a fresh stream on each call to avoid "stream has already been operated upon or closed" errors
    EasyMock.expect(mockProvider.streamAllRules()).andAnswer(() -> segmentGranularityRules.stream().map(r -> (ReindexingRule) r)).anyTimes();
    EasyMock.expect(mockProvider.getSegmentGranularityRule(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(segmentGranularityRules.get(0)).anyTimes();
    EasyMock.expect(mockProvider.getQueryGranularityRule(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.expect(mockProvider.getMetricsRule(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.expect(mockProvider.getDimensionsRule(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.expect(mockProvider.getIOConfigRule(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.expect(mockProvider.getProjectionRule(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.expect(mockProvider.getTuningConfigRule(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.expect(mockProvider.getDeletionRules(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(Collections.emptyList()).anyTimes();
    EasyMock.replay(mockProvider);
    return mockProvider;
  }

  private CompactionJobParams createMockParams(DateTime referenceTime, SegmentTimeline timeline)
  {
    CompactionJobParams mockParams = EasyMock.createMock(CompactionJobParams.class);
    EasyMock.expect(mockParams.getScheduleStartTime()).andReturn(referenceTime).anyTimes();
    EasyMock.expect(mockParams.getTimeline("testDS")).andReturn(timeline);
    EasyMock.replay(mockParams);
    return mockParams;
  }

  private DruidInputSource createMockSource()
  {
    final Interval[] capturedInterval = new Interval[1];

    DruidInputSource mockSource = EasyMock.createMock(DruidInputSource.class);
    EasyMock.expect(mockSource.withInterval(EasyMock.anyObject(Interval.class)))
        .andAnswer(() -> {
          capturedInterval[0] = (Interval) EasyMock.getCurrentArguments()[0];
          return mockSource;
        })
        .anyTimes();
    EasyMock.expect(mockSource.getInterval())
        .andAnswer(() -> capturedInterval[0])
        .anyTimes();
    EasyMock.replay(mockSource);
    return mockSource;
  }
}
