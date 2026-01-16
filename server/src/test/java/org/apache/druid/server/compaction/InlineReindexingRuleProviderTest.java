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

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class InlineReindexingRuleProviderTest
{
  private static final DateTime REFERENCE_TIME = DateTimes.of("2025-12-19T12:00:00Z");

  // Test intervals
  private static final Interval INTERVAL_100_DAYS_OLD = Intervals.of(
      "2025-09-01T00:00:00Z/2025-09-02T00:00:00Z"
  ); // Ends 109 days before reference time

  private static final Interval INTERVAL_50_DAYS_OLD = Intervals.of(
      "2025-10-20T00:00:00Z/2025-10-21T00:00:00Z"
  ); // Ends 59 days before reference time

  private static final Interval INTERVAL_20_DAYS_OLD = Intervals.of(
      "2025-11-20T00:00:00Z/2025-11-21T00:00:00Z"
  ); // Ends 28 days before reference time

  private static final Interval INTERVAL_5_DAYS_OLD = Intervals.of(
      "2025-12-13T00:00:00Z/2025-12-14T00:00:00Z"
  ); // Ends 5 days before reference time

  @Test
  public void test_getFilterRules_noRulesMatch_returnsEmpty()
  {
    ReindexingFilterRule rule30d = new ReindexingFilterRule(
        "filter-30d",
        null,
        Period.days(30),
        new SelectorDimFilter("dim", "val", null)
    );

    InlineReindexingRuleProvider provider = InlineReindexingRuleProvider.builder().filterRules(List.of(rule30d)).build();

    // Interval is only 5 days old, rule requires 30 days
    List<ReindexingFilterRule> result = provider.getFilterRules(INTERVAL_5_DAYS_OLD, REFERENCE_TIME);

    Assert.assertTrue("Should return empty when no rules match", result.isEmpty());
  }

  @Test
  public void test_getFilterRules_oneRuleMatchesFull_returnsThatRule()
  {
    ReindexingFilterRule rule30d = new ReindexingFilterRule(
        "filter-30d",
        null,
        Period.days(30),
        new SelectorDimFilter("dim", "val", null)
    );

    InlineReindexingRuleProvider provider = InlineReindexingRuleProvider.builder().filterRules(List.of(rule30d)).build();

    // Interval is 50 days old, rule requires 30 days - FULL match
    List<ReindexingFilterRule> result = provider.getFilterRules(INTERVAL_50_DAYS_OLD, REFERENCE_TIME);

    Assert.assertEquals(1, result.size());
    Assert.assertEquals("filter-30d", result.get(0).getId());
  }

  @Test
  public void test_getFilterRules_multipleAdditiveRulesMatchFull_returnsAll()
  {
    // Filter rules are additive - should return all matching rules
    ReindexingFilterRule rule30d = new ReindexingFilterRule(
        "filter-30d",
        null,
        Period.days(30),
        new SelectorDimFilter("dim1", "val1", null)
    );

    ReindexingFilterRule rule60d = new ReindexingFilterRule(
        "filter-60d",
        null,
        Period.days(60),
        new SelectorDimFilter("dim2", "val2", null)
    );

    ReindexingFilterRule rule90d = new ReindexingFilterRule(
        "filter-90d",
        null,
        Period.days(90),
        new SelectorDimFilter("dim3", "val3", null)
    );


    InlineReindexingRuleProvider provider = InlineReindexingRuleProvider.builder().filterRules(List.of(rule30d, rule60d, rule90d)).build();

    // Interval is 100 days old - all three rules match
    List<ReindexingFilterRule> result = provider.getFilterRules(INTERVAL_100_DAYS_OLD, REFERENCE_TIME);

    Assert.assertEquals("Should return all matching additive rules", 3, result.size());
    Assert.assertTrue(result.stream().anyMatch(r -> r.getId().equals("filter-30d")));
    Assert.assertTrue(result.stream().anyMatch(r -> r.getId().equals("filter-60d")));
    Assert.assertTrue(result.stream().anyMatch(r -> r.getId().equals("filter-90d")));
  }

  @Test
  public void test_getGranularityRules_multipleNonAdditiveRulesMatchFull_returnsOldestThreshold()
  {
    // Granularity rules are NOT additive - should return only the one with oldest threshold
    ReindexingGranularityRule rule30d = new ReindexingGranularityRule(
        "gran-30d",
        null,
        Period.days(30),
        new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null)
    );

    ReindexingGranularityRule rule60d = new ReindexingGranularityRule(
        "gran-60d",
        null,
        Period.days(60),
        new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null)
    );

    ReindexingGranularityRule rule90d = new ReindexingGranularityRule(
        "gran-90d",
        null,
        Period.days(90),
        new UserCompactionTaskGranularityConfig(Granularities.MONTH, null, null)
    );

    InlineReindexingRuleProvider provider = InlineReindexingRuleProvider.builder().granularityRules(List.of(rule30d, rule60d, rule90d)).build();

    // Interval is 100 days old - all three rules match FULL
    // Should return rule90d because it has the oldest threshold (now - 90d)
    List<ReindexingGranularityRule> result = provider.getGranularityRules(INTERVAL_100_DAYS_OLD, REFERENCE_TIME);

    Assert.assertEquals("Should return only one non-additive rule", 1, result.size());
    Assert.assertEquals("gran-90d", result.get(0).getId());
    Assert.assertEquals(Granularities.MONTH, result.get(0).getGranularityConfig().getSegmentGranularity());
  }

  @Test
  public void test_getGranularityRules_partialMatchNotReturned()
  {
    ReindexingGranularityRule rule30d = new ReindexingGranularityRule(
        "gran-30d",
        null,
        Period.days(30),
        new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null)
    );

    InlineReindexingRuleProvider provider = InlineReindexingRuleProvider.builder().granularityRules(List.of(rule30d)).build();

    // Interval is 20 days old, but rule requires 30 days
    // The interval likely has PARTIAL or NONE match, not FULL
    List<ReindexingGranularityRule> result = provider.getGranularityRules(INTERVAL_20_DAYS_OLD, REFERENCE_TIME);

    Assert.assertTrue("Should not return rules with PARTIAL match", result.isEmpty());
  }

  @Test
  public void test_getCondensedAndSortedPeriods_returnsDistinctSortedPeriods()
  {
    ReindexingFilterRule filter30d = new ReindexingFilterRule(
        "f1", null, Period.days(30), new SelectorDimFilter("d", "v", null)
    );
    ReindexingFilterRule filter60d = new ReindexingFilterRule(
        "f2", null, Period.days(60), new SelectorDimFilter("d", "v", null)
    );
    ReindexingGranularityRule gran30d = new ReindexingGranularityRule(
        "g1", null, Period.days(30), new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null)
    );
    ReindexingGranularityRule gran90d = new ReindexingGranularityRule(
        "g2", null, Period.days(90), new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null)
    );

    InlineReindexingRuleProvider provider = InlineReindexingRuleProvider.builder().filterRules(List.of(filter30d, filter60d)).granularityRules(List.of(gran30d, gran90d)).build();

    List<Period> periods = provider.getCondensedAndSortedPeriods(REFERENCE_TIME);

    // Should have 3 distinct periods: P30D (appears twice), P60D, P90D
    Assert.assertEquals(3, periods.size());
    // Should be sorted by duration (ascending)
    Assert.assertEquals(Period.days(30), periods.get(0));
    Assert.assertEquals(Period.days(60), periods.get(1));
    Assert.assertEquals(Period.days(90), periods.get(2));
  }

  @Test
  public void test_getCondensedAndSortedPeriods_withEmptyRules_returnsEmpty()
  {
    InlineReindexingRuleProvider provider = InlineReindexingRuleProvider.builder().filterRules(Collections.emptyList()).build();

    List<Period> periods = provider.getCondensedAndSortedPeriods(REFERENCE_TIME);

    Assert.assertTrue(periods.isEmpty());
  }

  @Test
  public void test_getProjectionRules_multipleAdditiveRulesMatchFull_returnsAll()
  {
    // Projection rules are additive
    ReindexingProjectionRule proj30d = new ReindexingProjectionRule(
        "proj-30d", null, Period.days(30), Collections.emptyList()
    );
    ReindexingProjectionRule proj60d = new ReindexingProjectionRule(
        "proj-60d", null, Period.days(60), Collections.emptyList()
    );

    InlineReindexingRuleProvider provider = InlineReindexingRuleProvider.builder().projectionRules(List.of(proj30d, proj60d)).build();

    // Interval is 100 days old - both rules match
    List<ReindexingProjectionRule> result = provider.getProjectionRules(INTERVAL_100_DAYS_OLD, REFERENCE_TIME);

    Assert.assertEquals("Should return all matching additive projection rules", 2, result.size());
  }

  @Test
  public void test_getApplicableRules_mixOfFullPartialNone_onlyReturnsFull()
  {
    // Create rules that will have different AppliesToMode results
    ReindexingGranularityRule rule10d = new ReindexingGranularityRule(
        "gran-10d",
        null,
        Period.days(10),
        new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null)
    ); // Will match FULL for 20-day-old interval

    ReindexingGranularityRule rule25d = new ReindexingGranularityRule(
        "gran-25d",
        null,
        Period.days(25),
        new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null)
    ); // Will match FULL for 20-day-old interval

    ReindexingGranularityRule rule50d = new ReindexingGranularityRule(
        "gran-50d",
        null,
        Period.days(50),
        new UserCompactionTaskGranularityConfig(Granularities.MONTH, null, null)
    ); // Will be NONE for 20-day-old interval

    InlineReindexingRuleProvider provider = InlineReindexingRuleProvider.builder().granularityRules(List.of(rule10d, rule25d, rule50d)).build();

    // Interval is 20 days old (ends at 2025-11-21, reference is 2025-12-19)
    // rule10d: threshold = now - 10d = 2025-12-09, interval ends 2025-11-21 < 2025-12-09 -> FULL
    // rule25d: threshold = now - 25d = 2025-11-24, interval ends 2025-11-21 < 2025-11-24 -> FULL
    // rule50d: threshold = now - 50d = 2025-10-30, interval ends 2025-11-21 > 2025-10-30 -> NONE
    // When multiple rules match, select the one with oldest threshold (smallest millis) = rule25d
    List<ReindexingGranularityRule> result = provider.getGranularityRules(INTERVAL_20_DAYS_OLD, REFERENCE_TIME);

    // Should return rule25d (has oldest threshold among FULL matches)
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("gran-25d", result.get(0).getId());
  }

  @Test
  public void test_constructor_nullListsDefaultToEmpty()
  {
    InlineReindexingRuleProvider provider = new InlineReindexingRuleProvider(
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );

    Assert.assertNotNull(provider.getFilterRules());
    Assert.assertTrue(provider.getFilterRules().isEmpty());
    Assert.assertNotNull(provider.getMetricsRules());
    Assert.assertTrue(provider.getMetricsRules().isEmpty());
    Assert.assertNotNull(provider.getGranularityRules());
    Assert.assertTrue(provider.getGranularityRules().isEmpty());
  }

  @Test
  public void test_getType_returnsInline()
  {
    InlineReindexingRuleProvider provider = InlineReindexingRuleProvider.builder().build();

    Assert.assertEquals("inline", provider.getType());
  }
}
