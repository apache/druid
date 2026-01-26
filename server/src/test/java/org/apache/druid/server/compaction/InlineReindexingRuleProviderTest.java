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
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskIOConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class InlineReindexingRuleProviderTest
{
  private static final DateTime REFERENCE_TIME = DateTimes.of("2025-12-19T12:00:00Z");

  private static final Interval INTERVAL_100_DAYS_OLD = Intervals.of(
      "2025-09-01T00:00:00Z/2025-09-02T00:00:00Z"
  ); // Ends 109 days before reference time

  private static final Interval INTERVAL_50_DAYS_OLD = Intervals.of(
      "2025-10-20T00:00:00Z/2025-10-21T00:00:00Z"
  ); // Ends 59 days before reference time

  private static final Interval INTERVAL_20_DAYS_OLD = Intervals.of(
      "2025-11-20T00:00:00Z/2025-11-21T00:00:00Z"
  ); // Ends 28 days before reference time

  @Test
  public void test_constructor_nullListsDefaultToEmpty()
  {
    InlineReindexingRuleProvider provider = new InlineReindexingRuleProvider(null, null, null, null,
                                                                             null, null, null);

    Assert.assertNotNull(provider.getFilterRules());
    Assert.assertTrue(provider.getFilterRules().isEmpty());
    Assert.assertNotNull(provider.getMetricsRules());
    Assert.assertTrue(provider.getMetricsRules().isEmpty());
    Assert.assertNotNull(provider.getDimensionsRules());
    Assert.assertTrue(provider.getDimensionsRules().isEmpty());
    Assert.assertNotNull(provider.getIOConfigRules());
    Assert.assertTrue(provider.getIOConfigRules().isEmpty());
    Assert.assertNotNull(provider.getProjectionRules());
    Assert.assertTrue(provider.getProjectionRules().isEmpty());
    Assert.assertNotNull(provider.getGranularityRules());
    Assert.assertTrue(provider.getGranularityRules().isEmpty());
    Assert.assertNotNull(provider.getTuningConfigRules());
    Assert.assertTrue(provider.getTuningConfigRules().isEmpty());
  }

  @Test
  public void test_getCondensedAndSortedPeriods_returnsDistinctSortedPeriods()
  {
    ReindexingFilterRule filter30d = createFilterRule("f1", Period.days(30));
    ReindexingFilterRule filter60d = createFilterRule("f2", Period.days(60));
    ReindexingGranularityRule gran30d = createGranularityRule("g1", Period.days(30)); // Duplicate P30D
    ReindexingGranularityRule gran90d = createGranularityRule("g2", Period.days(90));

    InlineReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
        .filterRules(ImmutableList.of(filter30d, filter60d))
        .granularityRules(ImmutableList.of(gran30d, gran90d))
        .build();

    List<Period> periods = provider.getCondensedAndSortedPeriods(REFERENCE_TIME);

    Assert.assertEquals(3, periods.size());

    Assert.assertEquals(Period.days(30), periods.get(0));
    Assert.assertEquals(Period.days(60), periods.get(1));
    Assert.assertEquals(Period.days(90), periods.get(2));
  }

  @Test
  public void test_getCondensedAndSortedPeriods_withEmptyRules_returnsEmpty()
  {
    InlineReindexingRuleProvider provider = InlineReindexingRuleProvider.builder().build();

    List<Period> periods = provider.getCondensedAndSortedPeriods(REFERENCE_TIME);

    Assert.assertTrue(periods.isEmpty());
  }

  @Test
  public void test_additiveRules_allScenarios()
  {
    ReindexingFilterRule rule30d = createFilterRule("filter-30d", Period.days(30));
    ReindexingFilterRule rule60d = createFilterRule("filter-60d", Period.days(60));
    ReindexingFilterRule rule90d = createFilterRule("filter-90d", Period.days(90));

    InlineReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
        .filterRules(ImmutableList.of(rule30d, rule60d, rule90d))
        .build();

    List<ReindexingFilterRule> noMatch = provider.getFilterRules(INTERVAL_20_DAYS_OLD, REFERENCE_TIME);
    Assert.assertTrue("No rules should match interval that's too recent", noMatch.isEmpty());

    List<ReindexingFilterRule> oneMatch = provider.getFilterRules(INTERVAL_50_DAYS_OLD, REFERENCE_TIME);
    Assert.assertEquals("Only rule30d should match", 1, oneMatch.size());
    Assert.assertEquals("filter-30d", oneMatch.get(0).getId());

    List<ReindexingFilterRule> multiMatch = provider.getFilterRules(INTERVAL_100_DAYS_OLD, REFERENCE_TIME);
    Assert.assertEquals("All 3 additive rules should be returned", 3, multiMatch.size());
    Assert.assertTrue(multiMatch.stream().anyMatch(r -> r.getId().equals("filter-30d")));
    Assert.assertTrue(multiMatch.stream().anyMatch(r -> r.getId().equals("filter-60d")));
    Assert.assertTrue(multiMatch.stream().anyMatch(r -> r.getId().equals("filter-90d")));
  }

  @Test
  public void test_nonAdditiveRules_allScenarios()
  {
    ReindexingGranularityRule rule30d = createGranularityRule("gran-30d", Period.days(30));
    ReindexingGranularityRule rule60d = createGranularityRule("gran-60d", Period.days(60));
    ReindexingGranularityRule rule90d = createGranularityRule("gran-90d", Period.days(90));

    InlineReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
        .granularityRules(ImmutableList.of(rule30d, rule60d, rule90d))
        .build();

    Assert.assertNull(provider.getGranularityRule(INTERVAL_20_DAYS_OLD, REFERENCE_TIME));

    ReindexingGranularityRule oneMatch = provider.getGranularityRule(INTERVAL_50_DAYS_OLD, REFERENCE_TIME);
    Assert.assertEquals("gran-30d", oneMatch.getId());

    ReindexingGranularityRule multiMatch = provider.getGranularityRule(INTERVAL_100_DAYS_OLD, REFERENCE_TIME);
    Assert.assertEquals("Should return rule with oldest threshold (P90D)", "gran-90d", multiMatch.getId());
  }

  @Test
  public void test_allRuleTypesWireCorrectly_withInterval()
  {
    ReindexingFilterRule filterRule = createFilterRule("filter", Period.days(30));
    ReindexingMetricsRule metricsRule = createMetricsRule("metrics", Period.days(30));
    ReindexingDimensionsRule dimensionsRule = createDimensionsRule("dimensions", Period.days(30));
    ReindexingIOConfigRule ioConfigRule = createIOConfigRule("ioconfig", Period.days(30));
    ReindexingProjectionRule projectionRule = createProjectionRule("projection", Period.days(30));
    ReindexingGranularityRule granularityRule = createGranularityRule("granularity", Period.days(30));
    ReindexingTuningConfigRule tuningConfigRule = createTuningConfigRule("tuning", Period.days(30));

    InlineReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
        .filterRules(ImmutableList.of(filterRule))
        .metricsRules(ImmutableList.of(metricsRule))
        .dimensionsRules(ImmutableList.of(dimensionsRule))
        .ioConfigRules(ImmutableList.of(ioConfigRule))
        .projectionRules(ImmutableList.of(projectionRule))
        .granularityRules(ImmutableList.of(granularityRule))
        .tuningConfigRules(ImmutableList.of(tuningConfigRule))
        .build();

    Assert.assertEquals(1, provider.getFilterRules(INTERVAL_100_DAYS_OLD, REFERENCE_TIME).size());
    Assert.assertEquals("filter", provider.getFilterRules(INTERVAL_100_DAYS_OLD, REFERENCE_TIME).get(0).getId());

    Assert.assertEquals("metrics", provider.getMetricsRule(INTERVAL_100_DAYS_OLD, REFERENCE_TIME).getId());

    Assert.assertEquals("dimensions", provider.getDimensionsRule(INTERVAL_100_DAYS_OLD, REFERENCE_TIME).getId());

    Assert.assertEquals("ioconfig", provider.getIOConfigRule(INTERVAL_100_DAYS_OLD, REFERENCE_TIME).getId());

    Assert.assertEquals(1, provider.getProjectionRules(INTERVAL_100_DAYS_OLD, REFERENCE_TIME).size());
    Assert.assertEquals("projection", provider.getProjectionRules(INTERVAL_100_DAYS_OLD, REFERENCE_TIME).get(0).getId());

    Assert.assertEquals("granularity", provider.getGranularityRule(INTERVAL_100_DAYS_OLD, REFERENCE_TIME).getId());

    Assert.assertEquals("tuning", provider.getTuningConfigRule(INTERVAL_100_DAYS_OLD, REFERENCE_TIME).getId());
  }

  @Test
  public void test_getType_returnsInline()
  {
    InlineReindexingRuleProvider provider = InlineReindexingRuleProvider.builder().build();
    Assert.assertEquals("inline", provider.getType());
  }

  private ReindexingFilterRule createFilterRule(String id, Period period)
  {
    return new ReindexingFilterRule(id, null, period, new SelectorDimFilter("dim", "val", null), null);
  }

  private ReindexingMetricsRule createMetricsRule(String id, Period period)
  {
    return new ReindexingMetricsRule(
        id,
        null,
        period,
        new AggregatorFactory[]{new CountAggregatorFactory("count")}
    );
  }

  private ReindexingDimensionsRule createDimensionsRule(String id, Period period)
  {
    return new ReindexingDimensionsRule(id, null, period, new UserCompactionTaskDimensionsConfig(null));
  }

  private ReindexingIOConfigRule createIOConfigRule(String id, Period period)
  {
    return new ReindexingIOConfigRule(id, null, period, new UserCompactionTaskIOConfig(null));
  }

  private ReindexingProjectionRule createProjectionRule(String id, Period period)
  {
    AggregateProjectionSpec projectionSpec = new AggregateProjectionSpec(
        "test_projection",
        null,
        null,
        null,
        new AggregatorFactory[]{new CountAggregatorFactory("count")}
    );
    return new ReindexingProjectionRule(id, null, period, ImmutableList.of(projectionSpec));
  }

  private ReindexingGranularityRule createGranularityRule(String id, Period period)
  {
    return new ReindexingGranularityRule(
        id,
        null,
        period,
        new UserCompactionTaskGranularityConfig(Granularities.DAY, null, false)
    );
  }

  private ReindexingTuningConfigRule createTuningConfigRule(String id, Period period)
  {
    return new ReindexingTuningConfigRule(
        id,
        null,
        period,
        new UserCompactionTaskQueryTuningConfig(null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null, null, null, null
        )
    );
  }
}
