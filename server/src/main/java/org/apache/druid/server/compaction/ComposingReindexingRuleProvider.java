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

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A meta-provider that composes multiple {@link ReindexingRuleProvider}s with first-wins semantics.
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
 *       "segmentGranularityRules": [
 *         {
 *           "id": "recent-data-granularity",
 *           "olderThan": "P7D",
 *           "segmentGranularity": "HOUR"
 *         }
 *       ]
 *     }
 *   ]
 * }
 * }</pre>
 * In this example:
 * <ul>
 *   <li>Composing rule with a single provider to simply show the inline definition.</li>
 *   <li>Once multiple provider types exist, this will allow operators to chain them as needed.</li>
 * </ul>
 */
public class ComposingReindexingRuleProvider implements ReindexingRuleProvider
{
  public static final String TYPE = "composing";

  private final List<ReindexingRuleProvider> providers;

  @JsonCreator
  public ComposingReindexingRuleProvider(
      @JsonProperty("providers") List<ReindexingRuleProvider> providers
  )
  {
    this.providers = Objects.requireNonNull(providers, "providers cannot be null");

    for (ReindexingRuleProvider provider : providers) {
      Objects.requireNonNull(provider, "providers list contains null element");
    }
  }

  @JsonProperty("providers")
  public List<ReindexingRuleProvider> getProviders()
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
    return providers.stream().allMatch(ReindexingRuleProvider::isReady);
  }

  @Override
  public List<ReindexingDeletionRule> getDeletionRules()
  {
    return providers.stream()
                    .map(ReindexingRuleProvider::getDeletionRules)
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  public List<ReindexingDeletionRule> getDeletionRules(Interval interval, DateTime referenceTime)
  {
    return providers.stream()
                    .map(p -> p.getDeletionRules(interval, referenceTime))
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  @Nullable
  public ReindexingDataSchemaRule getDataSchemaRule(Interval interval, DateTime referenceTime)
  {
    return providers.stream()
                    .map(p -> p.getDataSchemaRule(interval, referenceTime))
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElse(null);
  }

  @Override
  public List<ReindexingDataSchemaRule> getDataSchemaRules()
  {
    return providers.stream()
                    .map(ReindexingRuleProvider::getDataSchemaRules)
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  public List<ReindexingIOConfigRule> getIOConfigRules()
  {
    return providers.stream()
                    .map(ReindexingRuleProvider::getIOConfigRules)
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  @Nullable
  public ReindexingIOConfigRule getIOConfigRule(Interval interval, DateTime referenceTime)
  {
    return providers.stream()
                    .map(p -> p.getIOConfigRule(interval, referenceTime))
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElse(null);
  }

  @Override
  @Nullable
  public ReindexingSegmentGranularityRule getSegmentGranularityRule(Interval interval, DateTime referenceTime)
  {
    return providers.stream()
                    .map(p -> p.getSegmentGranularityRule(interval, referenceTime))
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElse(null);
  }

  @Override
  public List<ReindexingSegmentGranularityRule> getSegmentGranularityRules()
  {
    return providers.stream()
                    .map(ReindexingRuleProvider::getSegmentGranularityRules)
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  public List<ReindexingTuningConfigRule> getTuningConfigRules()
  {
    return providers.stream()
                    .map(ReindexingRuleProvider::getTuningConfigRules)
                    .filter(rules -> !rules.isEmpty())
                    .findFirst()
                    .orElse(Collections.emptyList());
  }

  @Override
  @Nullable
  public ReindexingTuningConfigRule getTuningConfigRule(Interval interval, DateTime referenceTime)
  {
    return providers.stream()
                    .map(p -> p.getTuningConfigRule(interval, referenceTime))
                    .filter(Objects::nonNull)
                    .findFirst()
                    .orElse(null);
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
    ComposingReindexingRuleProvider that = (ComposingReindexingRuleProvider) o;
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
    return "ComposingReindexingRuleProvider{" +
           "providers=" + providers +
           ", ready=" + isReady() +
           '}';
  }
}
