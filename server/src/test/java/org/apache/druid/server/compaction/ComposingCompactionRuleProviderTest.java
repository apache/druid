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
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ComposingCompactionRuleProviderTest
{
  private static final DateTime REFERENCE_TIME = new DateTime("2025-12-19T12:00:00Z");

  @Test
  public void test_constructor_nullProviders_throwsNullPointerException()
  {
    Assert.assertThrows(
        NullPointerException.class,
        () -> new ComposingCompactionRuleProvider(null)
    );
  }

  @Test
  public void test_constructor_nullProviderInList_throwsNullPointerException()
  {
    List<CompactionRuleProvider> providers = new ArrayList<>();
    providers.add(createEmptyInlineProvider());
    providers.add(null); // Null provider

    NullPointerException exception = Assert.assertThrows(
        NullPointerException.class,
        () -> new ComposingCompactionRuleProvider(providers)
    );

    Assert.assertTrue(exception.getMessage().contains("index 1"));
  }

  @Test
  public void test_constructor_emptyProviderList_succeeds()
  {
    ComposingCompactionRuleProvider composing = new ComposingCompactionRuleProvider(
        Collections.emptyList()
    );

    Assert.assertEquals("composing", composing.getType());
    Assert.assertTrue(composing.isReady());
    Assert.assertTrue(composing.getFilterRules().isEmpty());
  }

  @Test
  public void test_getType_returnsComposing()
  {
    ComposingCompactionRuleProvider composing = new ComposingCompactionRuleProvider(
        ImmutableList.of(createEmptyInlineProvider())
    );

    Assert.assertEquals("composing", composing.getType());
  }

  @Test
  public void test_isReady_allProvidersReady_returnsTrue()
  {
    CompactionRuleProvider provider1 = createEmptyInlineProvider(); // Always ready
    CompactionRuleProvider provider2 = createEmptyInlineProvider(); // Always ready

    ComposingCompactionRuleProvider composing = new ComposingCompactionRuleProvider(
        ImmutableList.of(provider1, provider2)
    );

    Assert.assertTrue(composing.isReady());
  }

  @Test
  public void test_isReady_someProvidersNotReady_returnsFalse()
  {
    CompactionRuleProvider readyProvider = createEmptyInlineProvider();
    CompactionRuleProvider notReadyProvider = createNotReadyProvider();

    ComposingCompactionRuleProvider composing = new ComposingCompactionRuleProvider(
        ImmutableList.of(readyProvider, notReadyProvider)
    );

    Assert.assertFalse(composing.isReady());
  }

  @Test
  public void test_isReady_emptyProviderList_returnsTrue()
  {
    ComposingCompactionRuleProvider composing = new ComposingCompactionRuleProvider(
        Collections.emptyList()
    );

    Assert.assertTrue(composing.isReady());
  }

  @Test
  public void test_getFilterRules_firstWins_returnsFirstNonEmpty()
  {
    CompactionFilterRule rule1 = createFilterRule("rule1", Period.days(30));
    CompactionFilterRule rule2 = createFilterRule("rule2", Period.days(60));

    CompactionRuleProvider provider1 = createInlineProviderWithFilterRules(ImmutableList.of(rule1));
    CompactionRuleProvider provider2 = createInlineProviderWithFilterRules(ImmutableList.of(rule2));

    ComposingCompactionRuleProvider composing = new ComposingCompactionRuleProvider(
        ImmutableList.of(provider1, provider2)
    );

    List<CompactionFilterRule> result = composing.getFilterRules();

    Assert.assertEquals(1, result.size());
    Assert.assertEquals("rule1", result.get(0).getId());
  }

  @Test
  public void test_getFilterRules_firstProviderEmpty_returnsSecond()
  {
    CompactionFilterRule rule2 = createFilterRule("rule2", Period.days(60));

    CompactionRuleProvider emptyProvider = createEmptyInlineProvider();
    CompactionRuleProvider provider2 = createInlineProviderWithFilterRules(ImmutableList.of(rule2));

    ComposingCompactionRuleProvider composing = new ComposingCompactionRuleProvider(
        ImmutableList.of(emptyProvider, provider2)
    );

    List<CompactionFilterRule> result = composing.getFilterRules();

    Assert.assertEquals(1, result.size());
    Assert.assertEquals("rule2", result.get(0).getId());
  }

  @Test
  public void test_getFilterRules_allProvidersEmpty_returnsEmpty()
  {
    CompactionRuleProvider provider1 = createEmptyInlineProvider();
    CompactionRuleProvider provider2 = createEmptyInlineProvider();

    ComposingCompactionRuleProvider composing = new ComposingCompactionRuleProvider(
        ImmutableList.of(provider1, provider2)
    );

    List<CompactionFilterRule> result = composing.getFilterRules();

    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void test_getGranularityRules_firstWins_returnsFirstNonEmpty()
  {
    CompactionGranularityRule rule1 = createGranularityRule("rule1", Period.days(7));
    CompactionGranularityRule rule2 = createGranularityRule("rule2", Period.days(30));

    CompactionRuleProvider provider1 = createInlineProviderWithGranularityRules(ImmutableList.of(rule1));
    CompactionRuleProvider provider2 = createInlineProviderWithGranularityRules(ImmutableList.of(rule2));

    ComposingCompactionRuleProvider composing = new ComposingCompactionRuleProvider(
        ImmutableList.of(provider1, provider2)
    );

    List<CompactionGranularityRule> result = composing.getGranularityRules();

    Assert.assertEquals(1, result.size());
    Assert.assertEquals("rule1", result.get(0).getId());
  }

  @Test
  public void test_getFilterRulesWithInterval_firstWins_delegatesToFirstProvider()
  {
    Interval interval = new Interval("2025-11-01T00:00:00Z/2025-11-15T00:00:00Z");
    CompactionFilterRule rule1 = createFilterRule("rule1", Period.days(30));

    CompactionRuleProvider provider1 = createInlineProviderWithFilterRules(ImmutableList.of(rule1));
    CompactionRuleProvider provider2 = createEmptyInlineProvider();

    ComposingCompactionRuleProvider composing = new ComposingCompactionRuleProvider(
        ImmutableList.of(provider1, provider2)
    );

    List<CompactionFilterRule> result = composing.getFilterRules(interval, REFERENCE_TIME);

    Assert.assertEquals(1, result.size());
    Assert.assertEquals("rule1", result.get(0).getId());
  }

  @Test
  public void test_getCondensedAndSortedPeriods_mergesFromAllProviders()
  {
    CompactionFilterRule rule1 = createFilterRule("rule1", Period.days(7));
    CompactionFilterRule rule2 = createFilterRule("rule2", Period.days(30));
    CompactionFilterRule rule3 = createFilterRule("rule3", Period.days(7)); // Duplicate period

    CompactionRuleProvider provider1 = createInlineProviderWithFilterRules(ImmutableList.of(rule1));
    CompactionRuleProvider provider2 = createInlineProviderWithFilterRules(ImmutableList.of(rule2, rule3));

    ComposingCompactionRuleProvider composing = new ComposingCompactionRuleProvider(
        ImmutableList.of(provider1, provider2)
    );

    List<Period> result = composing.getCondensedAndSortedPeriods(REFERENCE_TIME);

    // Should be deduplicated and sorted: [P7D, P30D]
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(Period.days(7), result.get(0));
    Assert.assertEquals(Period.days(30), result.get(1));
  }

  @Test
  public void test_singleProvider_delegatesDirectly()
  {
    CompactionFilterRule rule = createFilterRule("rule1", Period.days(30));
    CompactionRuleProvider provider = createInlineProviderWithFilterRules(ImmutableList.of(rule));

    ComposingCompactionRuleProvider composing = new ComposingCompactionRuleProvider(
        ImmutableList.of(provider)
    );

    List<CompactionFilterRule> result = composing.getFilterRules();

    Assert.assertEquals(1, result.size());
    Assert.assertEquals("rule1", result.get(0).getId());
  }

  // ========== Helper Methods ==========

  private CompactionRuleProvider createEmptyInlineProvider()
  {
    return new InlineCompactionRuleProvider(
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList()
    );
  }

  private CompactionRuleProvider createInlineProviderWithFilterRules(List<CompactionFilterRule> rules)
  {
    return new InlineCompactionRuleProvider(
        rules,
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList()
    );
  }

  private CompactionRuleProvider createInlineProviderWithGranularityRules(List<CompactionGranularityRule> rules)
  {
    return new InlineCompactionRuleProvider(
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        rules,
        Collections.emptyList()
    );
  }

  private CompactionRuleProvider createNotReadyProvider()
  {
    return new CompactionRuleProvider()
    {
      @Override
      public String getType()
      {
        return "not-ready-test-provider";
      }

      @Override
      public boolean isReady()
      {
        return false;
      }

      @Override
      public List<Period> getCondensedAndSortedPeriods(DateTime referenceTime)
      {
        return Collections.emptyList();
      }

      @Override
      public List<CompactionFilterRule> getFilterRules(Interval interval, DateTime referenceTime)
      {
        return Collections.emptyList();
      }

      @Override
      public List<CompactionFilterRule> getFilterRules()
      {
        return Collections.emptyList();
      }

      @Override
      public List<CompactionMetricsRule> getMetricsRules(Interval interval, DateTime referenceTime)
      {
        return Collections.emptyList();
      }

      @Override
      public List<CompactionMetricsRule> getMetricsRules()
      {
        return Collections.emptyList();
      }

      @Override
      public List<CompactionDimensionsRule> getDimensionsRules(Interval interval, DateTime referenceTime)
      {
        return Collections.emptyList();
      }

      @Override
      public List<CompactionDimensionsRule> getDimensionsRules()
      {
        return Collections.emptyList();
      }

      @Override
      public List<CompactionIOConfigRule> getIOConfigRules(Interval interval, DateTime referenceTime)
      {
        return Collections.emptyList();
      }

      @Override
      public List<CompactionIOConfigRule> getIOConfigRules()
      {
        return Collections.emptyList();
      }

      @Override
      public List<CompactionProjectionRule> getProjectionRules(Interval interval, DateTime referenceTime)
      {
        return Collections.emptyList();
      }

      @Override
      public List<CompactionProjectionRule> getProjectionRules()
      {
        return Collections.emptyList();
      }

      @Override
      public List<CompactionGranularityRule> getGranularityRules(Interval interval, DateTime referenceTime)
      {
        return Collections.emptyList();
      }

      @Override
      public List<CompactionGranularityRule> getGranularityRules()
      {
        return Collections.emptyList();
      }

      @Override
      public List<CompactionTuningConfigRule> getTuningConfigRules(Interval interval, DateTime referenceTime)
      {
        return Collections.emptyList();
      }

      @Override
      public List<CompactionTuningConfigRule> getTuningConfigRules()
      {
        return Collections.emptyList();
      }
    };
  }

  private CompactionFilterRule createFilterRule(String id, Period period)
  {
    return new CompactionFilterRule(
        id,
        "Test rule",
        period,
        new SelectorDimFilter("test", "value", null)
    );
  }

  private CompactionGranularityRule createGranularityRule(String id, Period period)
  {
    return new CompactionGranularityRule(
        id,
        "Test granularity rule",
        period,
        new UserCompactionTaskGranularityConfig(Granularities.DAY, null, false)
    );
  }
}
