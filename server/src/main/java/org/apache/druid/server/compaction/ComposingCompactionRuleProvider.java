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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A meta-provider that composes multiple {@link CompactionRuleProvider}s with first-wins semantics.
 * <p>
 * This provider delegates rule queries to a list of child providers in order. For each rule type,
 * it returns the result from the first provider that has non-empty rules of that type.
 * <p>
 * <b>First-Wins Strategy:</b>
 * Provider order determines precedence. If provider A returns rules of a given type, those rules
 * are used and subsequent providers are not consulted for that type. This applies to all rule
 * types, regardless of whether they are additive or non-additive.
 * <p>
 * <b>Readiness:</b>
 * The composing provider is considered ready only when ALL child providers are ready.
 * This ensures consistent behavior during startup.
 * <p>
 * <b>Example Usage:</b>
 * <pre>{@code
 * {
 *   "type": "composing",
 *   "providers": [
 *     {
 *       "type": "inline",
 *       "granularityRules": [{
 *         "id": "recent-data-granularity",
 *         "period": "P7D",
 *         "granularity": "HOUR"
 *       }]
 *     },
 *     {
 *       "type": "inline",
 *       "granularityRules": [{
 *         "id": "default-granularity",
 *         "period": "P1D",
 *         "granularity": "DAY"
 *       }],
 *       "filterRules": [{
 *         "id": "remove-bots",
 *         "period": "P30D",
 *         "filter": {
 *           "type": "selector",
 *           "dimension": "isRobot",
 *           "value": "true"
 *         }
 *       }]
 *     }
 *   ]
 * }
 * }</pre>
 * In this example:
 * <ul>
 *   <li>Granularity rules come from the first provider (HOUR granularity for recent data)</li>
 *   <li>Filter rules come from the second provider (first provider with filters)</li>
 * </ul>
 */
public class ComposingCompactionRuleProvider implements CompactionRuleProvider
{
  public static final String TYPE = "composing";

  private final List<CompactionRuleProvider> providers;

  @JsonCreator
  public ComposingCompactionRuleProvider(
      @JsonProperty("providers") List<CompactionRuleProvider> providers
  )
  {
    this.providers = Objects.requireNonNull(providers, "providers cannot be null");

    // Validate that no provider in the list is null
    for (int i = 0; i < providers.size(); i++) {
      if (providers.get(i) == null) {
        throw new NullPointerException("provider at index " + i + " is null");
      }
    }
  }

  @JsonProperty("providers")
  public List<CompactionRuleProvider> getProviders()
  {
    return providers;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean isReady()
  {
    // All providers must be ready
    return providers.stream().allMatch(CompactionRuleProvider::isReady);
  }

  @Override
  public List<Period> getCondensedAndSortedPeriods(DateTime referenceTime)
  {
    // Collect all unique periods from all providers, sorted ascending
    return providers.stream()
                    .flatMap(p -> p.getCondensedAndSortedPeriods(referenceTime).stream())
                    .distinct()
                    .sorted(Comparator.comparing(Period::toStandardDuration))
                    .collect(Collectors.toList());
  }

  @Override
  public List<CompactionFilterRule> getFilterRules()
  {
    return providers.stream()
                    .map(CompactionRuleProvider::getFilterRules)
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  public List<CompactionFilterRule> getFilterRules(Interval interval, DateTime referenceTime)
  {
    return providers.stream()
                    .map(p -> p.getFilterRules(interval, referenceTime))
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  public List<CompactionMetricsRule> getMetricsRules()
  {
    return providers.stream()
                    .map(CompactionRuleProvider::getMetricsRules)
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  public List<CompactionMetricsRule> getMetricsRules(Interval interval, DateTime referenceTime)
  {
    return providers.stream()
                    .map(p -> p.getMetricsRules(interval, referenceTime))
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  public List<CompactionDimensionsRule> getDimensionsRules()
  {
    return providers.stream()
                    .map(CompactionRuleProvider::getDimensionsRules)
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  public List<CompactionDimensionsRule> getDimensionsRules(Interval interval, DateTime referenceTime)
  {
    return providers.stream()
                    .map(p -> p.getDimensionsRules(interval, referenceTime))
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  public List<CompactionIOConfigRule> getIOConfigRules()
  {
    return providers.stream()
                    .map(CompactionRuleProvider::getIOConfigRules)
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  public List<CompactionIOConfigRule> getIOConfigRules(Interval interval, DateTime referenceTime)
  {
    return providers.stream()
                    .map(p -> p.getIOConfigRules(interval, referenceTime))
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  public List<CompactionProjectionRule> getProjectionRules()
  {
    return providers.stream()
                    .map(CompactionRuleProvider::getProjectionRules)
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  public List<CompactionProjectionRule> getProjectionRules(Interval interval, DateTime referenceTime)
  {
    return providers.stream()
                    .map(p -> p.getProjectionRules(interval, referenceTime))
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  public List<CompactionGranularityRule> getGranularityRules()
  {
    return providers.stream()
                    .map(CompactionRuleProvider::getGranularityRules)
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  public List<CompactionGranularityRule> getGranularityRules(Interval interval, DateTime referenceTime)
  {
    return providers.stream()
                    .map(p -> p.getGranularityRules(interval, referenceTime))
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  public List<CompactionTuningConfigRule> getTuningConfigRules()
  {
    return providers.stream()
                    .map(CompactionRuleProvider::getTuningConfigRules)
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  public List<CompactionTuningConfigRule> getTuningConfigRules(Interval interval, DateTime referenceTime)
  {
    return providers.stream()
                    .map(p -> p.getTuningConfigRules(interval, referenceTime))
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ComposingCompactionRuleProvider that = (ComposingCompactionRuleProvider) o;
    return Objects.equals(providers, that.providers);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(providers);
  }

  @Override
  public String toString()
  {
    return "ComposingCompactionRuleProvider{" +
           "providers=" + providers +
           ", ready=" + isReady() +
           '}';
  }
}
