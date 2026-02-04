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

  /**
   * TEST: Basic timeline construction with multiple segment granularity rules
   * <p>
   * REFERENCE TIME: 2025-01-29T16:15:00Z
   * <p>
   * INPUT RULES:
   * <ul>
   *   <li>Segment Granularity Rules: P7D→HOUR, P1M→DAY, P3M→MONTH</li>
   *   <li>Other Rules: None</li>
   *   <li>Default Segment Granularity: DAY</li>
   * </ul>
   * <p>
   * PROCESSING:
   * <ol>
   *   <li>Synthetic Rules: None created</li>
   *   <li>Initial Timeline:
   *     <ul>
   *       <li>P3M → MONTH: Raw 2024-10-29T16:15 → Aligned 2024-10-01T00:00</li>
   *       <li>P1M → DAY: Raw 2024-12-29T16:15 → Aligned 2024-12-29T00:00</li>
   *       <li>P7D → HOUR: Raw 2025-01-22T16:15 → Aligned 2025-01-22T16:00</li>
   *     </ul>
   *   </li>
   *   <li>Timeline Splits: None (no non-segment-gran rules)</li>
   * </ol>
   * <p>
   * EXPECTED OUTPUT: 3 intervals
   * <ol>
   *   <li>[-∞, 2024-10-01T00:00:00) - MONTH</li>
   *   <li>[2024-10-01T00:00:00, 2024-12-29T00:00:00) - DAY</li>
   *   <li>[2024-12-29T00:00:00, 2025-01-22T16:00:00) - HOUR</li>
   * </ol>
   */
  @Test
  public void test_generateAlignedSearchIntervals_withGranularityAlignment()
  {
    DateTime referenceTime = DateTimes.of("2025-01-29T16:15:00Z");

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

    Assert.assertEquals(3, intervals.size());

    Assert.assertEquals(DateTimes.MIN, intervals.get(0).getStart());
    Assert.assertEquals(DateTimes.of("2024-10-01T00:00:00Z"), intervals.get(0).getEnd());

    Assert.assertEquals(DateTimes.of("2024-10-01T00:00:00Z"), intervals.get(1).getStart());
    Assert.assertEquals(DateTimes.of("2024-12-29T00:00:00Z"), intervals.get(1).getEnd());

    Assert.assertEquals(DateTimes.of("2024-12-29T00:00:00Z"), intervals.get(2).getStart());
    Assert.assertEquals(DateTimes.of("2025-01-22T16:00:00Z"), intervals.get(2).getEnd());
  }

  /**
   * TEST: Timeline splitting by non-segment-granularity rules (metrics rules)
   * <p>
   * REFERENCE TIME: 2025-01-29T16:15:00Z
   * <p>
   * INPUT RULES:
   * <ul>
   *   <li>Segment Granularity Rules: P7D→HOUR, P1M→DAY, P3M→MONTH</li>
   *   <li>Other Rules: P8D-metrics, P14D-metrics, P45D-metrics, P100D-metrics</li>
   *   <li>Default Segment Granularity: DAY</li>
   * </ul>
   * <p>
   * PROCESSING:
   * <ol>
   *   <li>Synthetic Rules: None (smallest segment gran rule P7D is finer than all metrics rules)</li>
   *   <li>Initial Timeline: [-∞, 2024-10-01) MONTH, [2024-10-01, 2024-12-29) DAY, [2024-12-29, 2025-01-22T16:00) HOUR</li>
   *   <li>Timeline Splits:
   *     <ul>
   *       <li>P100D → Raw 2024-10-21T16:15 → Falls in DAY interval → Aligned 2024-10-21T00:00 → CREATES SPLIT</li>
   *       <li>P45D  → Raw 2024-12-15T16:15 → Falls in DAY interval → Aligned 2024-12-15T00:00 → CREATES SPLIT</li>
   *       <li>P14D  → Raw 2025-01-15T16:15 → Falls in HOUR interval → Aligned 2025-01-15T16:00 → CREATES SPLIT</li>
   *       <li>P8D   → Raw 2025-01-21T16:15 → Falls in HOUR interval → Aligned 2025-01-21T16:00 → CREATES SPLIT</li>
   *     </ul>
   *   </li>
   * </ol>
   * <p>
   * EXPECTED OUTPUT: 7 intervals
   * <ol>
   *   <li>[-∞, 2024-10-01T00:00:00) - MONTH</li>
   *   <li>[2024-10-01T00:00:00, 2024-10-21T00:00:00) - DAY</li>
   *   <li>[2024-10-21T00:00:00, 2024-12-15T00:00:00) - DAY</li>
   *   <li>[2024-12-15T00:00:00, 2024-12-29T00:00:00) - DAY</li>
   *   <li>[2024-12-29T00:00:00, 2025-01-15T16:00:00) - HOUR</li>
   *   <li>[2025-01-15T16:00:00, 2025-01-21T16:00:00) - HOUR</li>
   *   <li>[2025-01-21T16:00:00, 2025-01-22T16:00:00) - HOUR</li>
   * </ol>
   */
  @Test
  public void test_generateAlignedSearchIntervals_withNonSegmentGranularityRuleSplits()
  {
    DateTime referenceTime = DateTimes.of("2025-01-29T16:15:00Z");

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

    Assert.assertEquals(7, intervals.size());

    Assert.assertEquals(DateTimes.MIN, intervals.get(0).getStart());
    Assert.assertEquals(DateTimes.of("2024-10-01T00:00:00Z"), intervals.get(0).getEnd());

    Assert.assertEquals(DateTimes.of("2024-10-01T00:00:00Z"), intervals.get(1).getStart());
    Assert.assertEquals(DateTimes.of("2024-10-21T00:00:00Z"), intervals.get(1).getEnd());

    Assert.assertEquals(DateTimes.of("2024-10-21T00:00:00Z"), intervals.get(2).getStart());
    Assert.assertEquals(DateTimes.of("2024-12-15T00:00:00Z"), intervals.get(2).getEnd());

    Assert.assertEquals(DateTimes.of("2024-12-15T00:00:00Z"), intervals.get(3).getStart());
    Assert.assertEquals(DateTimes.of("2024-12-29T00:00:00Z"), intervals.get(3).getEnd());

    Assert.assertEquals(DateTimes.of("2024-12-29T00:00:00Z"), intervals.get(4).getStart());
    Assert.assertEquals(DateTimes.of("2025-01-15T16:00:00Z"), intervals.get(4).getEnd());

    Assert.assertEquals(DateTimes.of("2025-01-15T16:00:00Z"), intervals.get(5).getStart());
    Assert.assertEquals(DateTimes.of("2025-01-21T16:00:00Z"), intervals.get(5).getEnd());

    Assert.assertEquals(DateTimes.of("2025-01-21T16:00:00Z"), intervals.get(6).getStart());
    Assert.assertEquals(DateTimes.of("2025-01-22T16:00:00Z"), intervals.get(6).getEnd());
  }

  /**
   * TEST: Timeline construction when NO segment granularity rules exist (Case A: default usage)
   * <p>
   * REFERENCE TIME: 2025-01-29T16:15:00Z
   * <p>
   * INPUT RULES:
   * <ul>
   *   <li>Segment Granularity Rules: None</li>
   *   <li>Other Rules: P8D-metrics, P14D-metrics, P45D-metrics</li>
   *   <li>Default Segment Granularity: DAY</li>
   * </ul>
   * <p>
   * PROCESSING:
   * <ol>
   *   <li>Synthetic Rules: Created P8D→DAY (Case A: no segment gran rules exist, use smallest rule period with default gran)</li>
   *   <li>Initial Timeline: [-∞, 2025-01-21T00:00) - DAY (from synthetic P8D rule)</li>
   *   <li>Timeline Splits:
   *     <ul>
   *       <li>P45D → Raw 2024-12-15T16:15 → Falls in DAY interval → Aligned 2024-12-15T00:00 → CREATES SPLIT</li>
   *       <li>P14D → Raw 2025-01-15T16:15 → Falls in DAY interval → Aligned 2025-01-15T00:00 → CREATES SPLIT</li>
   *       <li>P8D is now a segment gran rule (not processed as split)</li>
   *     </ul>
   *   </li>
   * </ol>
   * <p>
   * EXPECTED OUTPUT: 3 intervals
   * <ol>
   *   <li>[-∞, 2024-12-15T00:00:00) - DAY</li>
   *   <li>[2024-12-15T00:00:00, 2025-01-15T00:00:00) - DAY</li>
   *   <li>[2025-01-15T00:00:00, 2025-01-21T00:00:00) - DAY</li>
   * </ol>
   */
  @Test
  public void test_generateAlignedSearchIntervals_withNoSegmentGranularityRules()
  {
    DateTime referenceTime = DateTimes.of("2025-01-29T16:15:00Z");

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
        Granularities.DAY
    );

    List<Interval> intervals = template.generateAlignedSearchIntervals(referenceTime);

    Assert.assertEquals(3, intervals.size());

    Assert.assertEquals(DateTimes.MIN, intervals.get(0).getStart());
    Assert.assertEquals(DateTimes.of("2024-12-15T00:00:00Z"), intervals.get(0).getEnd());

    Assert.assertEquals(DateTimes.of("2024-12-15T00:00:00Z"), intervals.get(1).getStart());
    Assert.assertEquals(DateTimes.of("2025-01-15T00:00:00Z"), intervals.get(1).getEnd());

    Assert.assertEquals(DateTimes.of("2025-01-15T00:00:00Z"), intervals.get(2).getStart());
    Assert.assertEquals(DateTimes.of("2025-01-21T00:00:00Z"), intervals.get(2).getEnd());
  }

  /**
   * TEST: Synthetic segment gran rule creation when rules are finer than smallest segment gran rule (Case B)
   * <p>
   * REFERENCE TIME: 2025-01-29T16:15:00Z
   * <p>
   * INPUT RULES:
   * <ul>
   *   <li>Segment Granularity Rules: P1M→DAY, P3M→MONTH</li>
   *   <li>Other Rules: P7D-metrics, P14D-metrics, P21D-metrics (all finer than P1M!)</li>
   *   <li>Default Segment Granularity: HOUR</li>
   * </ul>
   * <p>
   * PROCESSING:
   * <ol>
   *   <li>Synthetic Rules: Created P7D→HOUR (Case B: P7D/P14D/P21D are finer than smallest segment gran rule P1M, use finest with default gran)</li>
   *   <li>Initial Timeline:
   *     <ul>
   *       <li>P3M → MONTH: Raw 2024-10-29T16:15 → Aligned 2024-10-01T00:00</li>
   *       <li>P1M → DAY: Raw 2024-12-29T16:15 → Aligned 2024-12-29T00:00</li>
   *       <li>P7D → HOUR (synthetic): Raw 2025-01-22T16:15 → Aligned 2025-01-22T16:00 (PREPENDED interval!)</li>
   *     </ul>
   *   </li>
   *   <li>Timeline Splits:
   *     <ul>
   *       <li>P21D → Raw 2025-01-08T16:15 → Falls in HOUR interval → Aligned 2025-01-08T16:00 → CREATES SPLIT</li>
   *       <li>P14D → Raw 2025-01-15T16:15 → Falls in HOUR interval → Aligned 2025-01-15T16:00 → CREATES SPLIT</li>
   *       <li>P7D is now a segment gran rule (not processed as split)</li>
   *     </ul>
   *   </li>
   * </ol>
   * <p>
   * EXPECTED OUTPUT: 5 intervals
   * <ol>
   *   <li>[-∞, 2024-10-01T00:00:00) - MONTH</li>
   *   <li>[2024-10-01T00:00:00, 2024-12-29T00:00:00) - DAY</li>
   *   <li>[2024-12-29T00:00:00, 2025-01-08T16:00:00) - HOUR (prepended)</li>
   *   <li>[2025-01-08T16:00:00, 2025-01-15T16:00:00) - HOUR (prepended)</li>
   *   <li>[2025-01-15T16:00:00, 2025-01-22T16:00:00) - HOUR (prepended)</li>
   * </ol>
   */
  @Test
  public void test_generateAlignedSearchIntervals_prependIntervalForShortNonSegmentGranRules()
  {
    DateTime referenceTime = DateTimes.of("2025-01-29T16:15:00Z");

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
        Granularities.HOUR
    );

    List<Interval> intervals = template.generateAlignedSearchIntervals(referenceTime);

    Assert.assertEquals(5, intervals.size());

    Assert.assertEquals(DateTimes.MIN, intervals.get(0).getStart());
    Assert.assertEquals(DateTimes.of("2024-10-01T00:00:00Z"), intervals.get(0).getEnd());

    Assert.assertEquals(DateTimes.of("2024-10-01T00:00:00Z"), intervals.get(1).getStart());
    Assert.assertEquals(DateTimes.of("2024-12-29T00:00:00Z"), intervals.get(1).getEnd());

    Assert.assertEquals(DateTimes.of("2024-12-29T00:00:00Z"), intervals.get(2).getStart());
    Assert.assertEquals(DateTimes.of("2025-01-08T16:00:00Z"), intervals.get(2).getEnd());

    Assert.assertEquals(DateTimes.of("2025-01-08T16:00:00Z"), intervals.get(3).getStart());
    Assert.assertEquals(DateTimes.of("2025-01-15T16:00:00Z"), intervals.get(3).getEnd());

    Assert.assertEquals(DateTimes.of("2025-01-15T16:00:00Z"), intervals.get(4).getStart());
    Assert.assertEquals(DateTimes.of("2025-01-22T16:00:00Z"), intervals.get(4).getEnd());
  }

  /**
   * TEST: Comprehensive example demonstrating Case B, multiple segment gran rules, and timeline splits
   * <p>
   * REFERENCE TIME: 2024-02-04T22:12:04.873Z (realistic messy timestamp)
   * <p>
   * INPUT RULES:
   * <ul>
   *   <li>Segment Granularity Rules: P1Y→YEAR, P1M→MONTH, P7D→DAY</li>
   *   <li>Other Rules: P1D-metrics, P14D-metrics, P45D-metrics (P1D is finer than P7D!)</li>
   *   <li>Default Segment Granularity: HOUR</li>
   * </ul>
   * <p>
   * PROCESSING:
   * <ol>
   *   <li>Synthetic Rules: Created P1D→HOUR (Case B: P1D is finer than smallest segment gran rule P7D)</li>
   *   <li>Initial Timeline:
   *     <ul>
   *       <li>P1Y → YEAR: Raw 2023-02-04T22:12:04.873 → Aligned 2023-01-01T00:00:00</li>
   *       <li>P1M → MONTH: Raw 2024-01-04T22:12:04.873 → Aligned 2024-01-01T00:00:00</li>
   *       <li>P7D → DAY: Raw 2024-01-28T22:12:04.873 → Aligned 2024-01-28T00:00:00</li>
   *       <li>P1D → HOUR (synthetic): Raw 2024-02-03T22:12:04.873 → Aligned 2024-02-03T22:00:00 (PREPENDED!)</li>
   *     </ul>
   *   </li>
   *   <li>Timeline Splits:
   *     <ul>
   *       <li>P45D → Raw 2023-12-21T22:12:04.873 → Falls in MONTH interval → Aligned 2023-12-01T00:00:00 → CREATES SPLIT</li>
   *       <li>P14D → Raw 2024-01-21T22:12:04.873 → Falls in DAY interval → Aligned 2024-01-21T00:00:00 → CREATES SPLIT</li>
   *       <li>P1D is now a segment gran rule (not processed as split)</li>
   *     </ul>
   *   </li>
   * </ol>
   * <p>
   * EXPECTED OUTPUT: 6 intervals
   * <ol>
   *   <li>[-∞, 2023-01-01T00:00:00) - YEAR</li>
   *   <li>[2023-01-01T00:00:00, 2023-12-01T00:00:00) - MONTH</li>
   *   <li>[2023-12-01T00:00:00, 2024-01-01T00:00:00) - MONTH</li>
   *   <li>[2024-01-01T00:00:00, 2024-01-21T00:00:00) - DAY</li>
   *   <li>[2024-01-21T00:00:00, 2024-01-28T00:00:00) - DAY</li>
   *   <li>[2024-01-28T00:00:00, 2024-02-03T22:00:00) - HOUR (prepended, note non-midnight end)</li>
   * </ol>
   */
  @Test
  public void test_generateAlignedSearchIntervals()
  {
    DateTime referenceTime = DateTimes.of("2024-02-04T22:12:04.873Z");

    ReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
                                                                  .segmentGranularityRules(List.of(
                                                                      new ReindexingSegmentGranularityRule("month-rule", null, Period.years(1), Granularities.YEAR),
                                                                      new ReindexingSegmentGranularityRule("day-rule", null, Period.months(1), Granularities.MONTH),
                                                                      new ReindexingSegmentGranularityRule("day-rule", null, Period.days(7), Granularities.DAY)
                                                                  ))
                                                                  .metricsRules(List.of(
                                                                      new ReindexingMetricsRule("metrics-7d", null, Period.days(1), new AggregatorFactory[0]),
                                                                      new ReindexingMetricsRule("metrics-14d", null, Period.days(14), new AggregatorFactory[0]),
                                                                      new ReindexingMetricsRule("metrics-21d", null, Period.days(45), new AggregatorFactory[0])
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
        Granularities.HOUR
    );

    List<Interval> intervals = template.generateAlignedSearchIntervals(referenceTime);

    Assert.assertEquals(6, intervals.size());

    Assert.assertEquals(DateTimes.MIN, intervals.get(0).getStart());
    Assert.assertEquals(DateTimes.of("2023-01-01T00:00:00Z"), intervals.get(0).getEnd());

    Assert.assertEquals(DateTimes.of("2023-01-01T00:00:00Z"), intervals.get(1).getStart());
    Assert.assertEquals(DateTimes.of("2023-12-01T00:00:00Z"), intervals.get(1).getEnd());

    Assert.assertEquals(DateTimes.of("2023-12-01T00:00:00Z"), intervals.get(2).getStart());
    Assert.assertEquals(DateTimes.of("2024-01-01T00:00:00Z"), intervals.get(2).getEnd());

    Assert.assertEquals(DateTimes.of("2024-01-01T00:00:00Z"), intervals.get(3).getStart());
    Assert.assertEquals(DateTimes.of("2024-01-21T00:00:00Z"), intervals.get(3).getEnd());

    Assert.assertEquals(DateTimes.of("2024-01-21T00:00:00Z"), intervals.get(4).getStart());
    Assert.assertEquals(DateTimes.of("2024-01-28T00:00:00Z"), intervals.get(4).getEnd());

    Assert.assertEquals(DateTimes.of("2024-01-28T00:00:00"), intervals.get(5).getStart());
    Assert.assertEquals(DateTimes.of("2024-02-03T22:00:00"), intervals.get(5).getEnd());
  }

  /**
   * TEST: No rules at all - should throw IAE
   * <p>
   * REFERENCE TIME: 2025-01-29T16:15:00Z
   * <p>
   * INPUT RULES:
   * <ul>
   *   <li>Segment Granularity Rules: None</li>
   *   <li>Other Rules: None</li>
   *   <li>Default Segment Granularity: DAY</li>
   * </ul>
   * <p>
   * EXPECTED: IllegalArgumentException with message "requires at least one reindexing rule"
   */
  @Test
  public void test_generateAlignedSearchIntervals_noRulesThrowsException()
  {
    DateTime referenceTime = DateTimes.of("2025-01-29T16:15:00Z");

    ReindexingRuleProvider provider = InlineReindexingRuleProvider.builder().build();

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

    IllegalArgumentException exception = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> template.generateAlignedSearchIntervals(referenceTime)
    );

    Assert.assertTrue(
        exception.getMessage().contains("requires at least one reindexing rule")
    );
  }

  /**
   * TEST: Split point aligns exactly to existing boundary (boundary snapping, no split created)
   * <p>
   * REFERENCE TIME: 2025-02-01T00:00:00Z (carefully chosen for alignment)
   * <p>
   * INPUT RULES:
   * <ul>
   *   <li>Segment Granularity Rules: P1M→MONTH</li>
   *   <li>Other Rules: P1M-metrics (same period as segment gran rule!)</li>
   *   <li>Default Segment Granularity: DAY</li>
   * </ul>
   * <p>
   * PROCESSING:
   * <ol>
   *   <li>Synthetic Rules: None</li>
   *   <li>Initial Timeline: [-∞, 2025-01-01T00:00:00) - MONTH</li>
   *   <li>Timeline Splits:
   *     <ul>
   *       <li>P1M metrics → Raw 2025-01-01T00:00:00 → Aligned to MONTH: 2025-01-01T00:00:00</li>
   *       <li>This aligns EXACTLY to the existing boundary → no split created</li>
   *     </ul>
   *   </li>
   * </ol>
   * <p>
   * EXPECTED OUTPUT: 1 interval (no split despite having a non-segment-gran rule)
   * <ol>
   *   <li>[-∞, 2025-01-01T00:00:00) - MONTH</li>
   * </ol>
   */
  @Test
  public void test_generateAlignedSearchIntervals_splitPointSnapsToExistingBoundary()
  {
    DateTime referenceTime = DateTimes.of("2025-02-01T00:00:00Z");

    ReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
        .segmentGranularityRules(List.of(
            new ReindexingSegmentGranularityRule("month-rule", null, Period.months(1), Granularities.MONTH)
        ))
        .metricsRules(List.of(
            new ReindexingMetricsRule("metrics-1m", null, Period.months(1), new AggregatorFactory[0])
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

    Assert.assertEquals(1, intervals.size());

    Assert.assertEquals(DateTimes.MIN, intervals.get(0).getStart());
    Assert.assertEquals(DateTimes.of("2025-01-01T00:00:00Z"), intervals.get(0).getEnd());
  }

  /**
   * TEST: Prepending that aligns back to last segment gran rule interval end (no prepend actually created)
   * <p>
   * REFERENCE TIME: 2025-01-01T01:00:00Z
   * <p>
   * INPUT RULES:
   * <ul>
   *   <li>Segment Granularity Rules: P1D→DAY</li>
   *   <li>Other Rules: PT12H-metrics (finer than P1D, but aligns back to same interval end as the P1D rule so no prepend is done)</li>
   *   <li>Default Segment Granularity: DAY</li>
   * </ul>
   * <p>
   * PROCESSING:
   * <ol>
   *   <li>Initial Timeline: [-∞, 2024-12-31T00:00:00) - DAY</li>
   *   <li>Check for prepending:
   *     <ul>
   *       <li>PT12H threshold: 2024-12-31T13:00:00</li>
   *       <li>Align to DAY (default gran): 2024-12-31T00:00:00</li>
   *       <li>This EQUALS the most recent segment gran rule end (2024-12-31T00:00:00) → NO PREPEND</li>
   *     </ul>
   *   </li>
   * </ol>
   * <p>
   * EXPECTED OUTPUT: 1 interval (no split prepend despite having a finer non-segment-gran rule)
   * <ol>
   *   <li>[-∞, 2024-12-31T00:00:00) - DAY</li>
   * </ol>
   */
  @Test
  public void test_generateAlignedSearchIntervals_prependAlignmentDoesNotExtendTimeline()
  {
    DateTime referenceTime = DateTimes.of("2025-01-01T01:00:00Z");

    ReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
        .segmentGranularityRules(List.of(
            new ReindexingSegmentGranularityRule("day-rule", null, Period.days(1), Granularities.DAY)
        ))
        .metricsRules(List.of(
            new ReindexingMetricsRule("metrics-12h", null, Period.hours(12), new AggregatorFactory[0])
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

    Assert.assertEquals(1, intervals.size());

    Assert.assertEquals(DateTimes.MIN, intervals.get(0).getStart());
    Assert.assertEquals(DateTimes.of("2024-12-31T00:00:00Z"), intervals.get(0).getEnd());
  }

  /**
   * TEST: Multiple split points align to same timestamp (distinct() filtering removes duplicates)
   * <p>
   * REFERENCE TIME: 2025-01-15T00:00:00Z
   * <p>
   * INPUT RULES:
   * <ul>
   *   <li>Segment Granularity Rules: P1M→DAY</li>
   *   <li>Other Rules: P23D+6h-metrics, P23D+18h-metrics (both align to same DAY boundary in DAY interval)</li>
   *   <li>Default Segment Granularity: DAY</li>
   * </ul>
   * <p>
   * PROCESSING:
   * <ol>
   *   <li>Synthetic Rules: None</li>
   *   <li>Initial Timeline: [-∞, 2024-12-15T00:00:00) - DAY</li>
   *   <li>Timeline Splits:
   *     <ul>
   *       <li>P33D+6h → Raw: 2024-12-12T18:00:00 → Falls in DAY interval → Align to DAY: 2024-12-12T00:00:00</li>
   *       <li>P33D+18h → Raw: 2024-12-12T06:00:00 → Falls in DAY interval → Align to DAY: 2024-12-12T00:00:00</li>
   *       <li>Both create split point 2024-12-12T00:00:00 → distinct() removes duplicate!</li>
   *       <li>Only ONE split created at 2024-12-12T00:00:00</li>
   *     </ul>
   *   </li>
   * </ol>
   * <p>
   * EXPECTED OUTPUT: 2 intervals (not 3, because duplicate split point was filtered)
   * <ol>
   *   <li>[-∞, 2024-12-12T00:00:00) - DAY</li>
   *   <li>[2024-12-12T00:00:00, 2024-12-15T00:00:00) - DAY</li>
   * </ol>
   */
  @Test
  public void test_generateAlignedSearchIntervals_duplicateSplitPointsFiltered()
  {
    DateTime referenceTime = DateTimes.of("2025-01-15T00:00:00Z");

    ReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
        .segmentGranularityRules(List.of(
            new ReindexingSegmentGranularityRule("month-rule", null, Period.months(1), Granularities.DAY)
        ))
        .metricsRules(List.of(
            new ReindexingMetricsRule("metrics-33d-6h", null, Period.hours(33 * 24 + 6), new AggregatorFactory[0]),
            new ReindexingMetricsRule("metrics-33d-18h", null, Period.hours(33 * 24 + 18), new AggregatorFactory[0])
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

    Assert.assertEquals(2, intervals.size());

    Assert.assertEquals(DateTimes.MIN, intervals.get(0).getStart());
    Assert.assertEquals(DateTimes.of("2024-12-12T00:00:00Z"), intervals.get(0).getEnd());

    Assert.assertEquals(DateTimes.of("2024-12-12T00:00:00Z"), intervals.get(1).getStart());
    Assert.assertEquals(DateTimes.of("2024-12-15T00:00:00Z"), intervals.get(1).getEnd());
  }

  /**
   * TEST: Single rule only (minimal valid case)
   * <p>
   * REFERENCE TIME: 2025-01-29T16:15:00Z
   * <p>
   * INPUT RULES:
   * <ul>
   *   <li>Segment Granularity Rules: P1M→MONTH</li>
   *   <li>Other Rules: None</li>
   *   <li>Default Segment Granularity: DAY</li>
   * </ul>
   * <p>
   * PROCESSING:
   * <ol>
   *   <li>Synthetic Rules: None</li>
   *   <li>Initial Timeline: [-∞, 2024-12-01T00:00:00) - MONTH</li>
   *   <li>Timeline Splits: None (no non-segment-gran rules)</li>
   * </ol>
   * <p>
   * EXPECTED OUTPUT: 1 interval
   * <ol>
   *   <li>[-∞, 2024-12-01T00:00:00) - MONTH</li>
   * </ol>
   */
  @Test
  public void test_generateAlignedSearchIntervals_singleRuleOnly()
  {
    DateTime referenceTime = DateTimes.of("2025-01-29T16:15:00Z");

    ReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
        .segmentGranularityRules(List.of(
            new ReindexingSegmentGranularityRule("month-rule", null, Period.months(1), Granularities.MONTH)
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

    Assert.assertEquals(1, intervals.size());

    Assert.assertEquals(DateTimes.MIN, intervals.get(0).getStart());
    Assert.assertEquals(DateTimes.of("2024-12-01T00:00:00Z"), intervals.get(0).getEnd());
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
