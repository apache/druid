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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.compaction.InlineReindexingRuleProvider;
import org.apache.druid.server.compaction.ReindexingGranularityRule;
import org.apache.druid.server.compaction.ReindexingRuleProvider;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

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
            .granularityRules(List.of(
                new ReindexingGranularityRule(
                    "hourRule",
                    null,
                    Period.days(7),
                    new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null)
                ),
                new ReindexingGranularityRule(
                    "dayRule",
                    null,
                    Period.days(30),
                    new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null)
                )
            ))
            .build(),
        CompactionEngine.NATIVE,
        ImmutableMap.of("context_key", "context_value")
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
            .granularityRules(List.of(
                new ReindexingGranularityRule(
                    "rule1",
                    null,
                    Period.days(7),
                    new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null)
                )
            ))
            .build(),
        CompactionEngine.MSQ,
        ImmutableMap.of("key", "value")
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
        null
    );

    // Call createCompactionJobs - should return empty list without processing
    final List<CompactionJob> jobs = template.createCompactionJobs(null, null);

    Assert.assertTrue(jobs.isEmpty());
    EasyMock.verify(notReadyProvider);
  }

  @Test
  public void test_generateIntervalsFromPeriods_noGranularityRules()
  {
    // Test with multiple periods but no granularity rules - should use raw period calculations
    final DateTime referenceTime = new DateTime("2025-01-16T12:00:00Z");
    final List<Period> periods = List.of(Period.days(7), Period.days(30), Period.days(90));
    final List<ReindexingGranularityRule> granularityRules = List.of();

    final List<Interval> intervals = CascadingReindexingTemplate.generateIntervalsFromPeriods(
        periods,
        referenceTime,
        granularityRules
    );

    // Verify we get 3 intervals
    Assert.assertEquals(3, intervals.size());

    // Interval 0: [now-30d, now-7d)
    Assert.assertEquals(new DateTime("2024-12-17T12:00:00Z"), intervals.get(0).getStart());
    Assert.assertEquals(new DateTime("2025-01-09T12:00:00Z"), intervals.get(0).getEnd());

    // Interval 1: [now-90d, now-30d)
    Assert.assertEquals(new DateTime("2024-10-18T12:00:00Z"), intervals.get(1).getStart());
    Assert.assertEquals(new DateTime("2024-12-17T12:00:00Z"), intervals.get(1).getEnd());

    // Interval 2: [MIN, now-90d)
    Assert.assertEquals(DateTimes.MIN, intervals.get(2).getStart());
    Assert.assertEquals(new DateTime("2024-10-18T12:00:00Z"), intervals.get(2).getEnd());

    // Verify no gaps between intervals
    assertNoGapsBetweenIntervals(intervals);
  }

  @Test
  public void test_generateIntervalsFromPeriods_singleGranularityRule()
  {
    // Test with one granularity rule - should NOT adjust because we need both adjacent rules
    final DateTime referenceTime = new DateTime("2025-01-16T12:00:00Z");
    final List<Period> periods = List.of(Period.days(7), Period.days(30));

    // Only P7D has a granularity rule
    final List<ReindexingGranularityRule> granularityRules = List.of(
        new ReindexingGranularityRule(
            "hourRule",
            null,
            Period.days(7),
            new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null)
        )
    );

    final List<Interval> intervals = CascadingReindexingTemplate.generateIntervalsFromPeriods(
        periods,
        referenceTime,
        granularityRules
    );

    // Verify we get 2 intervals
    Assert.assertEquals(2, intervals.size());

    // No adjustment should happen because P30D doesn't have a granularity rule
    // Interval 0: [now-30d, now-7d) - raw calculation
    Assert.assertEquals(new DateTime("2024-12-17T12:00:00Z"), intervals.get(0).getStart());
    Assert.assertEquals(new DateTime("2025-01-09T12:00:00Z"), intervals.get(0).getEnd());

    // Interval 1: [MIN, now-30d)
    Assert.assertEquals(DateTimes.MIN, intervals.get(1).getStart());
    Assert.assertEquals(new DateTime("2024-12-17T12:00:00Z"), intervals.get(1).getEnd());

    // Verify no gaps
    assertNoGapsBetweenIntervals(intervals);
  }

  @Test
  public void test_generateIntervalsFromPeriods_multipleGranularityRules_verifyNoBoundaryShift()
  {
    // Test when both rules have granularities but calculated boundary is already aligned
    // Reference time chosen so that now-30d falls exactly on a day boundary
    final DateTime referenceTime = new DateTime("2025-01-16T00:00:00Z"); // Midnight
    final List<Period> periods = List.of(Period.days(7), Period.days(30));

    final List<ReindexingGranularityRule> granularityRules = List.of(
        new ReindexingGranularityRule(
            "hourRule",
            null,
            Period.days(7),
            new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null)
        ),
        new ReindexingGranularityRule(
            "dayRule",
            null,
            Period.days(30),
            new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null)
        )
    );

    final List<Interval> intervals = CascadingReindexingTemplate.generateIntervalsFromPeriods(
        periods,
        referenceTime,
        granularityRules
    );

    Assert.assertEquals(2, intervals.size());

    // calculatedStartTime = 2024-12-17T00:00:00Z (already at day boundary)
    // beforeRuleEffectiveEnd = DAY.bucketStart(2024-12-17T00:00:00Z) = 2024-12-17T00:00:00Z
    // possibleStartTime = HOUR.bucketStart(2024-12-17T00:00:00Z) = 2024-12-17T00:00:00Z
    // possibleStartTime.isBefore(beforeRuleEffectiveEnd) = false, so no increment
    Assert.assertEquals(new DateTime("2024-12-17T00:00:00Z"), intervals.get(0).getStart());
    Assert.assertEquals(new DateTime("2025-01-09T00:00:00Z"), intervals.get(0).getEnd());

    Assert.assertEquals(DateTimes.MIN, intervals.get(1).getStart());
    Assert.assertEquals(new DateTime("2024-12-17T00:00:00Z"), intervals.get(1).getEnd());

    assertNoGapsBetweenIntervals(intervals);
  }

  @Test
  public void test_generateIntervalsFromPeriods_multipleGranularityRules_verifyBoundaryShift()
  {
    // Test when both rules have granularities and boundary needs adjustment
    // Use coarser granularity (DAY) for the current rule and finer (HOUR) for the before rule
    final DateTime referenceTime = new DateTime("2025-01-16T12:30:00Z"); // Mid-day
    final List<Period> periods = List.of(Period.days(7), Period.days(30));

    final List<ReindexingGranularityRule> granularityRules = List.of(
        new ReindexingGranularityRule(
            "dayRule",
            null,
            Period.days(7),
            new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null)
        ),
        new ReindexingGranularityRule(
            "hourRule",
            null,
            Period.days(30),
            new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null)
        )
    );

    final List<Interval> intervals = CascadingReindexingTemplate.generateIntervalsFromPeriods(
        periods,
        referenceTime,
        granularityRules
    );

    Assert.assertEquals(2, intervals.size());

    // calculatedStartTime = 2024-12-17T12:30:00Z (mid-day)
    // beforeRuleEffectiveEnd = HOUR.bucketStart(2024-12-17T12:30:00Z) = 2024-12-17T12:00:00Z
    // possibleStartTime = DAY.bucketStart(2024-12-17T12:00:00Z) = 2024-12-17T00:00:00Z
    // possibleStartTime.isBefore(beforeRuleEffectiveEnd) = true (00:00 < 12:00)
    // start = DAY.increment(2024-12-17T00:00:00Z) = 2024-12-18T00:00:00Z
    Assert.assertEquals(new DateTime("2024-12-18T00:00:00Z"), intervals.get(0).getStart());
    Assert.assertEquals(new DateTime("2025-01-09T12:30:00Z"), intervals.get(0).getEnd());

    Assert.assertEquals(DateTimes.MIN, intervals.get(1).getStart());
    Assert.assertEquals(new DateTime("2024-12-18T00:00:00Z"), intervals.get(1).getEnd());

    assertNoGapsBetweenIntervals(intervals);
  }

  @Test
  public void test_generateIntervalsFromPeriods_edgeCases_emptyAndSinglePeriod()
  {
    final DateTime referenceTime = new DateTime("2025-01-16T12:00:00Z");

    // Empty periods list should return empty intervals
    final List<Interval> emptyIntervals = CascadingReindexingTemplate.generateIntervalsFromPeriods(
        List.of(),
        referenceTime,
        List.of()
    );
    Assert.assertEquals(0, emptyIntervals.size());

    // Single period should create one interval ending at MIN
    final List<Interval> singleInterval = CascadingReindexingTemplate.generateIntervalsFromPeriods(
        List.of(Period.days(7)),
        referenceTime,
        List.of()
    );
    Assert.assertEquals(1, singleInterval.size());
    Assert.assertEquals(DateTimes.MIN, singleInterval.get(0).getStart());
    Assert.assertEquals(new DateTime("2025-01-09T12:00:00Z"), singleInterval.get(0).getEnd());
  }

  @Test
  public void test_generateIntervalsFromPeriods_currentRuleNull_beforeRuleExists_withPropagation()
  {
    // Test with 3 periods where only the 2nd and 3rd periods have granularity rules
    final DateTime referenceTime = new DateTime("2025-01-16T14:30:00Z");
    final List<Period> periods = List.of(Period.days(7), Period.days(30), Period.days(90));

    // Only P30D and P90D have granularity rules (P7D does not)
    final List<ReindexingGranularityRule> granularityRules = List.of(
        new ReindexingGranularityRule(
            "dayRule",
            null,
            Period.days(30),
            new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null)
        ),
        new ReindexingGranularityRule(
            "monthRule",
            null,
            Period.days(90),
            new UserCompactionTaskGranularityConfig(Granularities.MONTH, null, null)
        )
    );

    final List<Interval> intervals = CascadingReindexingTemplate.generateIntervalsFromPeriods(
        periods,
        referenceTime,
        granularityRules
    );

    Assert.assertEquals(3, intervals.size());

    // Interval 0: [adjusted, now-7d)
    // currentRule=null (P7D has no rule), beforeRule=dayRule (P30D)
    // Should use raw calculated time since currentRule is null
    Assert.assertEquals(new DateTime("2024-12-17T14:30:00Z"), intervals.get(0).getStart());
    Assert.assertEquals(new DateTime("2025-01-09T14:30:00Z"), intervals.get(0).getEnd());

    // Interval 1: [adjusted, now-30d)
    // currentRule=dayRule (P30D), beforeRule=monthRule (P90D)
    // calculatedStartTime = 2024-10-18T14:30:00Z
    // beforeRuleEffectiveEnd = MONTH.bucketStart(2024-10-18T14:30:00Z) = 2024-10-01T00:00:00Z
    // possibleStartTime = DAY.bucketStart(2024-10-01T00:00:00Z) = 2024-10-01T00:00:00Z
    // possibleStartTime.isBefore(beforeRuleEffectiveEnd) = false, so no increment
    // IMPORTANT: end should be the previousAdjustedBoundary from interval 0 (raw calc since no adjustment)
    Assert.assertEquals(new DateTime("2024-10-01T00:00:00Z"), intervals.get(1).getStart());
    Assert.assertEquals(new DateTime("2024-12-17T14:30:00Z"), intervals.get(1).getEnd());

    // Interval 2: [MIN, adjusted)
    // end should be the adjusted start from interval 1
    Assert.assertEquals(DateTimes.MIN, intervals.get(2).getStart());
    Assert.assertEquals(new DateTime("2024-10-01T00:00:00Z"), intervals.get(2).getEnd());

    assertNoGapsBetweenIntervals(intervals);
  }

  /**
   * Asserts that there are no gaps between consecutive intervals.
   * Each interval's start should equal the previous interval's end.
   */
  private void assertNoGapsBetweenIntervals(List<Interval> intervals)
  {
    for (int i = 0; i < intervals.size() - 1; i++) {
      Assert.assertEquals(
          "Gap detected between interval " + i + " and " + (i + 1),
          intervals.get(i).getStart(),
          intervals.get(i + 1).getEnd()
      );
    }
  }

}
