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
import org.apache.druid.server.compaction.InlineReindexingRuleProvider;
import org.apache.druid.server.compaction.ReindexingRuleProvider;
import org.apache.druid.server.compaction.ReindexingSegmentGranularityRule;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;
import org.easymock.EasyMock;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
        null
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
        null
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
        null
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
            Period.days(3)   // skipOffsetFromNow
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
    ReindexingRuleProvider mockProvider = createMockProvider(referenceTime, List.of(Period.days(7), Period.days(30)));
    CompactionJobParams mockParams = createMockParams(referenceTime, timeline);
    DruidInputSource mockSource = createMockSource();

    TestCascadingReindexingTemplate template = new TestCascadingReindexingTemplate(
        "testDS", null, null, mockProvider, null, null, null, null
    );

    template.createCompactionJobs(mockSource, mockParams);
    List<Interval> processedIntervals = template.getProcessedIntervals();

    Assert.assertEquals(2, processedIntervals.size());
    Assert.assertEquals(referenceTime.minusDays(30), processedIntervals.get(0).getStart());
    Assert.assertEquals(referenceTime.minusDays(10), processedIntervals.get(0).getEnd());
    Assert.assertEquals(referenceTime.minusDays(90), processedIntervals.get(1).getStart());
    Assert.assertEquals(referenceTime.minusDays(30), processedIntervals.get(1).getEnd());

    EasyMock.verify(mockProvider, mockParams, mockSource);
  }

  @Test
  public void test_createCompactionJobs_withSkipOffsetFromLatest_skipAllOfTime()
  {
    DateTime referenceTime = DateTimes.of("2024-01-15T00:00:00Z");
    SegmentTimeline timeline = createTestTimeline(referenceTime.minusDays(90), referenceTime.minusDays(10));
    ReindexingRuleProvider mockProvider = createMockProvider(referenceTime, List.of(Period.days(7), Period.days(30)));
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
    ReindexingRuleProvider mockProvider = createMockProvider(referenceTime, List.of(Period.days(7), Period.days(30)));
    CompactionJobParams mockParams = createMockParams(referenceTime, timeline);
    DruidInputSource mockSource = createMockSource();

    TestCascadingReindexingTemplate template = new TestCascadingReindexingTemplate(
        "testDS", null, null, mockProvider, null, null, Period.days(5), null
    );

    template.createCompactionJobs(mockSource, mockParams);
    List<Interval> processedIntervals = template.getProcessedIntervals();

    Assert.assertEquals(2, processedIntervals.size());
    Assert.assertEquals(referenceTime.minusDays(30), processedIntervals.get(0).getStart());
    Assert.assertEquals(referenceTime.minusDays(15), processedIntervals.get(0).getEnd());
    Assert.assertEquals(referenceTime.minusDays(90), processedIntervals.get(1).getStart());
    Assert.assertEquals(referenceTime.minusDays(30), processedIntervals.get(1).getEnd());

    EasyMock.verify(mockProvider, mockParams, mockSource);
  }

  @Test
  public void test_createCompactionJobs_withSkipOffsetFromLatest_eliminatesInterval()
  {
    DateTime referenceTime = DateTimes.of("2024-01-15T00:00:00Z");
    SegmentTimeline timeline = createTestTimeline(referenceTime.minusDays(90), referenceTime.minusDays(10));
    ReindexingRuleProvider mockProvider = createMockProvider(referenceTime, List.of(Period.days(7), Period.days(30)));
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
    ReindexingRuleProvider mockProvider = createMockProvider(referenceTime, List.of(Period.days(7), Period.days(30)));
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
    ReindexingRuleProvider mockProvider = createMockProvider(referenceTime, List.of(Period.days(7), Period.days(30)));
    CompactionJobParams mockParams = createMockParams(referenceTime, timeline);
    DruidInputSource mockSource = createMockSource();

    TestCascadingReindexingTemplate template = new TestCascadingReindexingTemplate(
        "testDS", null, null, mockProvider, null, null, null, Period.days(20)
    );

    template.createCompactionJobs(mockSource, mockParams);
    List<Interval> processedIntervals = template.getProcessedIntervals();

    Assert.assertEquals(2, processedIntervals.size());
    Assert.assertEquals(referenceTime.minusDays(30), processedIntervals.get(0).getStart());
    Assert.assertEquals(referenceTime.minusDays(20), processedIntervals.get(0).getEnd());
    Assert.assertEquals(referenceTime.minusDays(90), processedIntervals.get(1).getStart());
    Assert.assertEquals(referenceTime.minusDays(30), processedIntervals.get(1).getEnd());

    EasyMock.verify(mockProvider, mockParams, mockSource);
  }

  @Test
  public void test_createCompactionJobs_withSkipOffsetFromNow_eliminatesInterval()
  {
    DateTime referenceTime = DateTimes.of("2024-01-15T00:00:00Z");
    SegmentTimeline timeline = createTestTimeline(referenceTime.minusDays(90), referenceTime.minusDays(10));
    ReindexingRuleProvider mockProvider = createMockProvider(referenceTime, List.of(Period.days(7), Period.days(30)));
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
            engine, taskContext, skipOffsetFromLatest, skipOffsetFromNow);
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
        public @Nullable Granularity getSegmentGranularity()
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

  private ReindexingRuleProvider createMockProvider(DateTime referenceTime, List<Period> periods)
  {
    ReindexingSegmentGranularityRule mockGranularityRule = new ReindexingSegmentGranularityRule(
        "test-rule",
        null,
        Period.days(1),
        Granularities.HOUR
    );

    ReindexingRuleProvider mockProvider = EasyMock.createMock(ReindexingRuleProvider.class);
    EasyMock.expect(mockProvider.isReady()).andReturn(true);
    EasyMock.expect(mockProvider.getCondensedAndSortedPeriods(referenceTime)).andReturn(periods);
    EasyMock.expect(mockProvider.getSegmentGranularityRule(EasyMock.anyObject(), EasyMock.anyObject())).andReturn(mockGranularityRule).anyTimes();
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
