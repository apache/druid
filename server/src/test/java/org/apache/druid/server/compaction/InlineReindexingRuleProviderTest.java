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
import org.apache.druid.server.coordinator.UserCompactionTaskIOConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.function.BiFunction;

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
                                                                             null, null, null, null
    );

    Assert.assertNotNull(provider.getDeletionRules());
    Assert.assertTrue(provider.getDeletionRules().isEmpty());
    Assert.assertNotNull(provider.getMetricsRules());
    Assert.assertTrue(provider.getMetricsRules().isEmpty());
    Assert.assertNotNull(provider.getDimensionsRules());
    Assert.assertTrue(provider.getDimensionsRules().isEmpty());
    Assert.assertNotNull(provider.getIOConfigRules());
    Assert.assertTrue(provider.getIOConfigRules().isEmpty());
    Assert.assertNotNull(provider.getProjectionRules());
    Assert.assertTrue(provider.getProjectionRules().isEmpty());
    Assert.assertNotNull(provider.getSegmentGranularityRules());
    Assert.assertTrue(provider.getSegmentGranularityRules().isEmpty());
    Assert.assertNotNull(provider.getQueryGranularityRules());
    Assert.assertTrue(provider.getQueryGranularityRules().isEmpty());
    Assert.assertNotNull(provider.getTuningConfigRules());
    Assert.assertTrue(provider.getTuningConfigRules().isEmpty());
  }

  @Test
  public void test_reindexingRules_validateAdditivity()
  {
    ReindexingDeletionRule rule30d = createFilterRule("filter-30d", Period.days(30));
    ReindexingDeletionRule rule60d = createFilterRule("filter-60d", Period.days(60));
    ReindexingDeletionRule rule90d = createFilterRule("filter-90d", Period.days(90));

    InlineReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
        .deletionRules(ImmutableList.of(rule30d, rule60d, rule90d))
        .build();

    List<ReindexingDeletionRule> noMatch = provider.getDeletionRules(INTERVAL_20_DAYS_OLD, REFERENCE_TIME);
    Assert.assertTrue("No rules should match interval that's too recent", noMatch.isEmpty());

    List<ReindexingDeletionRule> oneMatch = provider.getDeletionRules(INTERVAL_50_DAYS_OLD, REFERENCE_TIME);
    Assert.assertEquals("Only rule30d should match", 1, oneMatch.size());
    Assert.assertEquals("filter-30d", oneMatch.get(0).getId());

    List<ReindexingDeletionRule> multiMatch = provider.getDeletionRules(INTERVAL_100_DAYS_OLD, REFERENCE_TIME);
    Assert.assertEquals("All 3 additive rules should be returned", 3, multiMatch.size());
    Assert.assertTrue(multiMatch.stream().anyMatch(r -> r.getId().equals("filter-30d")));
    Assert.assertTrue(multiMatch.stream().anyMatch(r -> r.getId().equals("filter-60d")));
    Assert.assertTrue(multiMatch.stream().anyMatch(r -> r.getId().equals("filter-90d")));
  }

  @Test
  public void test_allNonAdditiveRules_validateNonAdditivity()
  {
    // Test metrics rules
    testNonAdditivity(
        "metrics",
        this::createMetricsRule,
        InlineReindexingRuleProvider.Builder::metricsRules,
        InlineReindexingRuleProvider::getMetricsRule
    );

    // Test dimensions rules
    testNonAdditivity(
        "dimensions",
        this::createDimensionsRule,
        InlineReindexingRuleProvider.Builder::dimensionsRules,
        InlineReindexingRuleProvider::getDimensionsRule
    );

    // Test IOConfig rules
    testNonAdditivity(
        "ioConfig",
        this::createIOConfigRule,
        InlineReindexingRuleProvider.Builder::ioConfigRules,
        InlineReindexingRuleProvider::getIOConfigRule
    );

    // Test projection rules
    testNonAdditivity(
        "projection",
        this::createProjectionRule,
        InlineReindexingRuleProvider.Builder::projectionRules,
        InlineReindexingRuleProvider::getProjectionRule
    );

    // Test segment granularity rules
    testNonAdditivity(
        "segmentGranularity",
        this::createSegmentGranularityRule,
        InlineReindexingRuleProvider.Builder::segmentGranularityRules,
        InlineReindexingRuleProvider::getSegmentGranularityRule
    );

    // Test query granularity rules
    testNonAdditivity(
        "queryGranularity",
        this::createQueryGranularityRule,
        InlineReindexingRuleProvider.Builder::queryGranularityRules,
        InlineReindexingRuleProvider::getQueryGranularityRule
    );

    // Test tuning config rules
    testNonAdditivity(
        "tuningConfig",
        this::createTuningConfigRule,
        InlineReindexingRuleProvider.Builder::tuningConfigRules,
        InlineReindexingRuleProvider::getTuningConfigRule
    );
  }

  @Test
  public void test_allRuleTypesWireCorrectly_withInterval()
  {
    ReindexingDeletionRule filterRule = createFilterRule("filter", Period.days(30));
    ReindexingMetricsRule metricsRule = createMetricsRule("metrics", Period.days(30));
    ReindexingDimensionsRule dimensionsRule = createDimensionsRule("dimensions", Period.days(30));
    ReindexingIOConfigRule ioConfigRule = createIOConfigRule("ioconfig", Period.days(30));
    ReindexingProjectionRule projectionRule = createProjectionRule("projection", Period.days(30));
    ReindexingSegmentGranularityRule segmentGranularityRule = createSegmentGranularityRule("segmentGranularity", Period.days(30));
    ReindexingQueryGranularityRule queryGranularityRule = createQueryGranularityRule("queryGranularity", Period.days(30));
    ReindexingTuningConfigRule tuningConfigRule = createTuningConfigRule("tuning", Period.days(30));

    InlineReindexingRuleProvider provider = InlineReindexingRuleProvider.builder()
        .deletionRules(ImmutableList.of(filterRule))
        .metricsRules(ImmutableList.of(metricsRule))
        .dimensionsRules(ImmutableList.of(dimensionsRule))
        .ioConfigRules(ImmutableList.of(ioConfigRule))
        .projectionRules(ImmutableList.of(projectionRule))
        .segmentGranularityRules(ImmutableList.of(segmentGranularityRule))
        .queryGranularityRules(ImmutableList.of(queryGranularityRule))
        .tuningConfigRules(ImmutableList.of(tuningConfigRule))
        .build();

    Assert.assertEquals(1, provider.getDeletionRules(INTERVAL_100_DAYS_OLD, REFERENCE_TIME).size());
    Assert.assertEquals("filter", provider.getDeletionRules(INTERVAL_100_DAYS_OLD, REFERENCE_TIME).get(0).getId());

    Assert.assertEquals("metrics", provider.getMetricsRule(INTERVAL_100_DAYS_OLD, REFERENCE_TIME).getId());

    Assert.assertEquals("dimensions", provider.getDimensionsRule(INTERVAL_100_DAYS_OLD, REFERENCE_TIME).getId());

    Assert.assertEquals("ioconfig", provider.getIOConfigRule(INTERVAL_100_DAYS_OLD, REFERENCE_TIME).getId());

    Assert.assertEquals("projection", provider.getProjectionRule(INTERVAL_100_DAYS_OLD, REFERENCE_TIME).getId());

    Assert.assertEquals("segmentGranularity", provider.getSegmentGranularityRule(INTERVAL_100_DAYS_OLD, REFERENCE_TIME).getId());

    Assert.assertEquals("queryGranularity", provider.getQueryGranularityRule(INTERVAL_100_DAYS_OLD, REFERENCE_TIME).getId());

    Assert.assertEquals("tuning", provider.getTuningConfigRule(INTERVAL_100_DAYS_OLD, REFERENCE_TIME).getId());
  }

  /**
   * Generic test helper for validating non-additive rule behavior.
   * <p>
   * Tests that when multiple rules match an interval, only the rule with the oldest threshold
   * (largest period) is returned.
   *
   * @param ruleTypeName descriptive name for error messages
   * @param ruleFactory function to create a rule instance
   * @param builderSetter function to set rules on the builder
   * @param ruleGetter function to retrieve the applicable rule from the provider
   */
  private <T extends ReindexingRule> void testNonAdditivity(
      String ruleTypeName,
      BiFunction<String, Period, T> ruleFactory,
      BiFunction<InlineReindexingRuleProvider.Builder, List<T>, InlineReindexingRuleProvider.Builder> builderSetter,
      TriFunction<InlineReindexingRuleProvider, Interval, DateTime, T> ruleGetter
  )
  {
    T rule30d = ruleFactory.apply(ruleTypeName + "-30d", Period.days(30));
    T rule60d = ruleFactory.apply(ruleTypeName + "-60d", Period.days(60));
    T rule90d = ruleFactory.apply(ruleTypeName + "-90d", Period.days(90));

    InlineReindexingRuleProvider.Builder builder = InlineReindexingRuleProvider.builder();
    builderSetter.apply(builder, ImmutableList.of(rule30d, rule60d, rule90d));
    InlineReindexingRuleProvider provider = builder.build();

    Assert.assertNull(
        ruleTypeName + ": No rule should match interval that's too recent",
        ruleGetter.apply(provider, INTERVAL_20_DAYS_OLD, REFERENCE_TIME)
    );

    T oneMatch = ruleGetter.apply(provider, INTERVAL_50_DAYS_OLD, REFERENCE_TIME);
    Assert.assertEquals(
        ruleTypeName + ": Only 30d rule should match",
        ruleTypeName + "-30d",
        oneMatch.getId()
    );

    T multiMatch = ruleGetter.apply(provider, INTERVAL_100_DAYS_OLD, REFERENCE_TIME);
    Assert.assertEquals(
        ruleTypeName + ": Should return rule with oldest threshold (P90D)",
        ruleTypeName + "-90d",
        multiMatch.getId()
    );
  }

  @FunctionalInterface
  private interface TriFunction<T, U, V, R>
  {
    R apply(T t, U u, V v);
  }

  @Test
  public void test_getType_returnsInline()
  {
    InlineReindexingRuleProvider provider = InlineReindexingRuleProvider.builder().build();
    Assert.assertEquals("inline", provider.getType());
  }

  private ReindexingDeletionRule createFilterRule(String id, Period period)
  {
    return new ReindexingDeletionRule(id, null, period, new SelectorDimFilter("dim", "val", null), null);
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

  private ReindexingSegmentGranularityRule createSegmentGranularityRule(String id, Period period)
  {
    return new ReindexingSegmentGranularityRule(
        id,
        null,
        period,
        Granularities.DAY
    );
  }

  private ReindexingQueryGranularityRule createQueryGranularityRule(String id, Period period)
  {
    return new ReindexingQueryGranularityRule(
        id,
        null,
        period,
        Granularities.DAY,
        true
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
