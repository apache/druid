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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ComposingReindexingRuleProviderTest
{
  private static final DateTime REFERENCE_TIME = DateTimes.of("2025-12-19T12:00:00Z");

  @Test
  public void test_constructor_nullProviders_throwsNullPointerException()
  {
    Assert.assertThrows(
        NullPointerException.class,
        () -> new ComposingReindexingRuleProvider(null)
    );
  }

  @Test
  public void test_constructor_nullProviderInList_throwsNullPointerException()
  {
    List<ReindexingRuleProvider> providers = new ArrayList<>();
    providers.add(InlineReindexingRuleProvider.builder().build());
    providers.add(null); // Null provider

    NullPointerException exception = Assert.assertThrows(
        NullPointerException.class,
        () -> new ComposingReindexingRuleProvider(providers)
    );

    Assert.assertTrue(exception.getMessage().contains("providers list contains null element"));
  }

  @Test
  public void test_constructor_emptyProviderList_succeeds()
  {
    ComposingReindexingRuleProvider composing = new ComposingReindexingRuleProvider(
        Collections.emptyList()
    );

    Assert.assertEquals("composing", composing.getType());
    Assert.assertTrue(composing.isReady());
    Assert.assertTrue(composing.getDeletionRules().isEmpty());
  }


  @Test
  public void test_isReady_allProvidersReady_returnsTrue()
  {
    ReindexingRuleProvider provider1 = InlineReindexingRuleProvider.builder().build();
    ReindexingRuleProvider provider2 = InlineReindexingRuleProvider.builder().build();

    ComposingReindexingRuleProvider composing = new ComposingReindexingRuleProvider(
        ImmutableList.of(provider1, provider2)
    );

    Assert.assertTrue(composing.isReady());
  }

  @Test
  public void test_isReady_someProvidersNotReady_returnsFalse()
  {
    ReindexingRuleProvider readyProvider = InlineReindexingRuleProvider.builder().build();
    ReindexingRuleProvider notReadyProvider = createNotReadyProvider();

    ComposingReindexingRuleProvider composing = new ComposingReindexingRuleProvider(
        ImmutableList.of(readyProvider, notReadyProvider)
    );

    Assert.assertFalse(composing.isReady());
  }

  @Test
  public void test_isReady_emptyProviderList_returnsTrue()
  {
    ComposingReindexingRuleProvider composing = new ComposingReindexingRuleProvider(
        Collections.emptyList()
    );

    Assert.assertTrue(composing.isReady());
  }

  @Test
  public void test_getDeletionRules_compositingBehavior()
  {
    testComposingBehaviorForRuleType(
        rules -> InlineReindexingRuleProvider.builder().deletionRules(rules).build(),
        ComposingReindexingRuleProvider::getDeletionRules,
        createFilterRule("rule1", Period.days(7)),
        createFilterRule("rule2", Period.days(30)),
        ReindexingDeletionRule::getId
    );
  }

  @Test
  public void test_getDeletionRulesWithInterval_compositingBehavior()
  {
    testComposingBehaviorForAdditiveRuleTypeWithInterval(
        rules -> InlineReindexingRuleProvider.builder().deletionRules(rules).build(),
        (provider, it) -> provider.getDeletionRules(it.interval, it.time),
        createFilterRule("rule1", Period.days(7)),
        createFilterRule("rule2", Period.days(30)),
        ReindexingDeletionRule::getId
    );
  }

  @Test
  public void test_getGranularityRules_compositingBehavior()
  {
    testComposingBehaviorForRuleType(
        rules -> InlineReindexingRuleProvider.builder().granularityRules(rules).build(),
        ComposingReindexingRuleProvider::getGranularityRules,
        createGranularityRule("rule1", Period.days(7)),
        createGranularityRule("rule2", Period.days(30)),
        ReindexingGranularityRule::getId
    );
  }

  @Test
  public void test_getCondensedAndSortedPeriods_mergesFromAllProviders()
  {
    ReindexingDeletionRule rule1 = createFilterRule("rule1", Period.days(7));
    ReindexingDeletionRule rule2 = createFilterRule("rule2", Period.months(1));
    ReindexingDeletionRule rule3 = createFilterRule("rule3", Period.days(7)); // Duplicate period

    ReindexingRuleProvider provider1 = InlineReindexingRuleProvider.builder()
                                                                   .deletionRules(ImmutableList.of(rule1)).build();
    ReindexingRuleProvider provider2 = InlineReindexingRuleProvider.builder()
                                                                   .deletionRules(ImmutableList.of(rule2, rule3)).build();

    ComposingReindexingRuleProvider composing = new ComposingReindexingRuleProvider(
        ImmutableList.of(provider1, provider2)
    );

    List<Period> result = composing.getCondensedAndSortedPeriods(REFERENCE_TIME);

    Assert.assertEquals(2, result.size());
    Assert.assertEquals(Period.days(7), result.get(0));
    Assert.assertEquals(Period.months(1), result.get(1));
  }


  @Test
  public void test_getMetricsRules_compositingBehavior()
  {
    testComposingBehaviorForRuleType(
        rules -> InlineReindexingRuleProvider.builder().metricsRules(rules).build(),
        ComposingReindexingRuleProvider::getMetricsRules,
        createMetricsRule("rule1", Period.days(7)),
        createMetricsRule("rule2", Period.days(30)),
        ReindexingMetricsRule::getId
    );
  }

  @Test
  public void test_getMetricsRuleWithInterval_compositingBehavior()
  {
    testComposingBehaviorForNonAdditiveRuleTypeWithInterval(
        rules -> InlineReindexingRuleProvider.builder().metricsRules(rules).build(),
        (provider, it) -> provider.getMetricsRule(it.interval, it.time),
        createMetricsRule("rule1", Period.days(7)),
        createMetricsRule("rule2", Period.days(30)),
        ReindexingMetricsRule::getId
    );
  }

  @Test
  public void test_getDimensionsRules_compositingBehavior()
  {
    testComposingBehaviorForRuleType(
        rules -> InlineReindexingRuleProvider.builder().dimensionsRules(rules).build(),
        ComposingReindexingRuleProvider::getDimensionsRules,
        createDimensionsRule("rule1", Period.days(7)),
        createDimensionsRule("rule2", Period.days(30)),
        ReindexingDimensionsRule::getId
    );
  }

  @Test
  public void test_getDimensionsRuleWithInterval_compositingBehavior()
  {
    testComposingBehaviorForNonAdditiveRuleTypeWithInterval(
        rules -> InlineReindexingRuleProvider.builder().dimensionsRules(rules).build(),
        (provider, it) -> provider.getDimensionsRule(it.interval, it.time),
        createDimensionsRule("rule1", Period.days(7)),
        createDimensionsRule("rule2", Period.days(30)),
        ReindexingDimensionsRule::getId
    );
  }

  @Test
  public void test_getIOConfigRules_compositingBehavior()
  {
    testComposingBehaviorForRuleType(
        rules -> InlineReindexingRuleProvider.builder().ioConfigRules(rules).build(),
        ComposingReindexingRuleProvider::getIOConfigRules,
        createIOConfigRule("rule1", Period.days(7)),
        createIOConfigRule("rule2", Period.days(30)),
        ReindexingIOConfigRule::getId
    );
  }

  @Test
  public void test_getIOConfigRuleWithInterval_compositingBehavior()
  {
    testComposingBehaviorForNonAdditiveRuleTypeWithInterval(
        rules -> InlineReindexingRuleProvider.builder().ioConfigRules(rules).build(),
        (provider, it) -> provider.getIOConfigRule(it.interval, it.time),
        createIOConfigRule("rule1", Period.days(7)),
        createIOConfigRule("rule2", Period.days(30)),
        ReindexingIOConfigRule::getId
    );
  }

  @Test
  public void test_getProjectionRules_compositingBehavior()
  {
    testComposingBehaviorForRuleType(
        rules -> InlineReindexingRuleProvider.builder().projectionRules(rules).build(),
        ComposingReindexingRuleProvider::getProjectionRules,
        createProjectionRule("rule1", Period.days(7)),
        createProjectionRule("rule2", Period.days(30)),
        ReindexingProjectionRule::getId
    );
  }

  @Test
  public void test_getProjectionRuleWithInterval_compositingBehavior()
  {
    testComposingBehaviorForNonAdditiveRuleTypeWithInterval(
        rules -> InlineReindexingRuleProvider.builder().projectionRules(rules).build(),
        (provider, it) -> provider.getProjectionRule(it.interval, it.time),
        createProjectionRule("rule1", Period.days(7)),
        createProjectionRule("rule2", Period.days(30)),
        ReindexingProjectionRule::getId
    );
  }

  @Test
  public void test_getTuningConfigRules_compositingBehavior()
  {
    testComposingBehaviorForRuleType(
        rules -> InlineReindexingRuleProvider.builder().tuningConfigRules(rules).build(),
        ComposingReindexingRuleProvider::getTuningConfigRules,
        createTuningConfigRule("rule1", Period.days(7)),
        createTuningConfigRule("rule2", Period.days(30)),
        ReindexingTuningConfigRule::getId
    );
  }

  @Test
  public void test_getTuningConfigRuleWithInterval_compositingBehavior()
  {
    testComposingBehaviorForNonAdditiveRuleTypeWithInterval(
        rules -> InlineReindexingRuleProvider.builder().tuningConfigRules(rules).build(),
        (provider, it) -> provider.getTuningConfigRule(it.interval, it.time),
        createTuningConfigRule("rule1", Period.days(7)),
        createTuningConfigRule("rule2", Period.days(30)),
        ReindexingTuningConfigRule::getId
    );
  }

  @Test
  public void test_getGranularityRuleWithInterval_compositingBehavior()
  {
    testComposingBehaviorForNonAdditiveRuleTypeWithInterval(
        rules -> InlineReindexingRuleProvider.builder().granularityRules(rules).build(),
        (provider, it) -> provider.getGranularityRule(it.interval, it.time),
        createGranularityRule("rule1", Period.days(7)),
        createGranularityRule("rule2", Period.days(30)),
        ReindexingGranularityRule::getId
    );
  }


  @Test
  public void test_equals_sameProviders_returnsTrue()
  {
    ReindexingRuleProvider provider1 = InlineReindexingRuleProvider.builder().build();
    ReindexingRuleProvider provider2 = InlineReindexingRuleProvider.builder()
        .deletionRules(ImmutableList.of(createFilterRule("rule1", Period.days(30))))
        .build();

    ComposingReindexingRuleProvider composing1 = new ComposingReindexingRuleProvider(
        ImmutableList.of(provider1, provider2)
    );
    ComposingReindexingRuleProvider composing2 = new ComposingReindexingRuleProvider(
        ImmutableList.of(provider1, provider2)
    );

    Assert.assertEquals(composing1, composing2);
    Assert.assertEquals(composing1.hashCode(), composing2.hashCode());
  }

  @Test
  public void test_equals_differentProviders_returnsFalse()
  {
    ReindexingRuleProvider provider1 = InlineReindexingRuleProvider.builder().build();
    ReindexingRuleProvider provider2 = InlineReindexingRuleProvider.builder()
        .deletionRules(ImmutableList.of(createFilterRule("rule1", Period.days(30))))
        .build();

    ComposingReindexingRuleProvider composing1 = new ComposingReindexingRuleProvider(
        ImmutableList.of(provider1)
    );
    ComposingReindexingRuleProvider composing2 = new ComposingReindexingRuleProvider(
        ImmutableList.of(provider1, provider2)
    );

    Assert.assertNotEquals(composing1, composing2);
  }



  /**
   * Helper class to pass interval + time together
   */
  private static class IntervalAndTime
  {
    final Interval interval;
    final DateTime time;

    IntervalAndTime(Interval interval, DateTime time)
    {
      this.interval = interval;
      this.time = time;
    }
  }

  /**
   * Tests composing behavior for getXxxRules() - all three scenarios:
   * 1. First provider has rules → returns first provider's rules
   * 2. First provider empty → falls through to second provider
   * 3. Both providers empty → returns empty list
   */
  private <T> void testComposingBehaviorForRuleType(
      Function<List<T>, ReindexingRuleProvider> providerFactory,
      Function<ComposingReindexingRuleProvider, List<T>> ruleGetter,
      T rule1,
      T rule2,
      Function<T, String> idExtractor
  )
  {
    ReindexingRuleProvider provider1 = providerFactory.apply(ImmutableList.of(rule1));
    ReindexingRuleProvider provider2 = providerFactory.apply(ImmutableList.of(rule2));

    ComposingReindexingRuleProvider composing = new ComposingReindexingRuleProvider(
        ImmutableList.of(provider1, provider2)
    );

    List<T> result = ruleGetter.apply(composing);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("rule1", idExtractor.apply(result.get(0)));

    ReindexingRuleProvider emptyProvider = InlineReindexingRuleProvider.builder().build();
    composing = new ComposingReindexingRuleProvider(
        ImmutableList.of(emptyProvider, provider2)
    );

    result = ruleGetter.apply(composing);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("rule2", idExtractor.apply(result.get(0)));

    ReindexingRuleProvider emptyProvider2 = InlineReindexingRuleProvider.builder().build();
    composing = new ComposingReindexingRuleProvider(
        ImmutableList.of(emptyProvider, emptyProvider2)
    );

    result = ruleGetter.apply(composing);
    Assert.assertTrue(result.isEmpty());
  }

  private <T> void testComposingBehaviorForNonAdditiveRuleTypeWithInterval(
      Function<List<T>, ReindexingRuleProvider> providerFactory,
      BiFunction<ComposingReindexingRuleProvider, IntervalAndTime, T> ruleGetter,
      T rule1,
      T rule2,
      Function<T, String> idExtractor
  )
  {
    Interval interval = Intervals.of("2025-11-01/2025-11-15");

    ReindexingRuleProvider provider1 = providerFactory.apply(ImmutableList.of(rule1));
    ReindexingRuleProvider provider2 = providerFactory.apply(ImmutableList.of(rule2));

    ComposingReindexingRuleProvider composing = new ComposingReindexingRuleProvider(
        ImmutableList.of(provider1, provider2)
    );

    T result = ruleGetter.apply(composing, new IntervalAndTime(interval, REFERENCE_TIME));
    Assert.assertNotNull(result);
    Assert.assertEquals("rule1", idExtractor.apply(result));

    ReindexingRuleProvider emptyProvider = InlineReindexingRuleProvider.builder().build();
    composing = new ComposingReindexingRuleProvider(
        ImmutableList.of(emptyProvider, provider2)
    );

    result = ruleGetter.apply(composing, new IntervalAndTime(interval, REFERENCE_TIME));
    Assert.assertNotNull(result);
    Assert.assertEquals("rule2", idExtractor.apply(result));

    ReindexingRuleProvider emptyProvider2 = InlineReindexingRuleProvider.builder().build();
    composing = new ComposingReindexingRuleProvider(
        ImmutableList.of(emptyProvider, emptyProvider2)
    );

    result = ruleGetter.apply(composing, new IntervalAndTime(interval, REFERENCE_TIME));
    Assert.assertNull(result);
  }

  /**
   * Tests composing behavior for getXxxRules(interval, time) - all three scenarios:
   * 1. First provider has rules → returns first provider's rules
   * 2. First provider empty → falls through to second provider
   * 3. Both providers empty → returns empty list
   */
  private <T> void testComposingBehaviorForAdditiveRuleTypeWithInterval(
      Function<List<T>, ReindexingRuleProvider> providerFactory,
      BiFunction<ComposingReindexingRuleProvider, IntervalAndTime, List<T>> ruleGetter,
      T rule1,
      T rule2,
      Function<T, String> idExtractor
  )
  {
    Interval interval = Intervals.of("2025-11-01/2025-11-15");

    ReindexingRuleProvider provider1 = providerFactory.apply(ImmutableList.of(rule1));
    ReindexingRuleProvider provider2 = providerFactory.apply(ImmutableList.of(rule2));

    ComposingReindexingRuleProvider composing = new ComposingReindexingRuleProvider(
        ImmutableList.of(provider1, provider2)
    );

    List<T> result = ruleGetter.apply(composing, new IntervalAndTime(interval, REFERENCE_TIME));
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("rule1", idExtractor.apply(result.get(0)));

    ReindexingRuleProvider emptyProvider = InlineReindexingRuleProvider.builder().build();
    composing = new ComposingReindexingRuleProvider(
        ImmutableList.of(emptyProvider, provider2)
    );

    result = ruleGetter.apply(composing, new IntervalAndTime(interval, REFERENCE_TIME));
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("rule2", idExtractor.apply(result.get(0)));

    ReindexingRuleProvider emptyProvider2 = InlineReindexingRuleProvider.builder().build();
    composing = new ComposingReindexingRuleProvider(
        ImmutableList.of(emptyProvider, emptyProvider2)
    );

    result = ruleGetter.apply(composing, new IntervalAndTime(interval, REFERENCE_TIME));
    Assert.assertTrue(result.isEmpty());
  }

  /**
   * Creates a test provider that is not ready
   */
  private ReindexingRuleProvider createNotReadyProvider()
  {
    return new InlineReindexingRuleProvider(null, null, null, null, null, null, null)
    {
      @Override
      public boolean isReady()
      {
        return false;
      }
    };
  }

  private ReindexingDeletionRule createFilterRule(String id, Period period)
  {
    return new ReindexingDeletionRule(
        id,
        "Test rule",
        period,
        new SelectorDimFilter("test", "value", null),
        null
    );
  }

  private ReindexingGranularityRule createGranularityRule(String id, Period period)
  {
    return new ReindexingGranularityRule(
        id,
        "Test granularity rule",
        period,
        new UserCompactionTaskGranularityConfig(Granularities.DAY, null, false)
    );
  }

  private ReindexingMetricsRule createMetricsRule(String id, Period period)
  {
    return new ReindexingMetricsRule(
        id,
        "Test metrics rule",
        period,
        new AggregatorFactory[]{new CountAggregatorFactory("count")}
    );
  }

  private ReindexingDimensionsRule createDimensionsRule(String id, Period period)
  {
    return new ReindexingDimensionsRule(
        id,
        "Test dimensions rule",
        period,
        new UserCompactionTaskDimensionsConfig(null)
    );
  }

  private ReindexingIOConfigRule createIOConfigRule(String id, Period period)
  {
    return new ReindexingIOConfigRule(
        id,
        "Test IO config rule",
        period,
        new UserCompactionTaskIOConfig(null)
    );
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
    return new ReindexingProjectionRule(
        id,
        "Test projection rule",
        period,
        ImmutableList.of(projectionSpec)
    );
  }

  private ReindexingTuningConfigRule createTuningConfigRule(String id, Period period)
  {
    return new ReindexingTuningConfigRule(
        id,
        "Test tuning config rule",
        period,
        new UserCompactionTaskQueryTuningConfig(null, null, null, null, null, null, null, null,
            null, null, null, null, null, null, null, null, null, null, null
        )
    );
  }

}
