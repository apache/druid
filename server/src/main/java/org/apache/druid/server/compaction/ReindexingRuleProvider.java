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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Stream;

/**
 * Provides compaction rules for different aspects of reindexing configuration.
 * <p>
 * This abstraction allows rules to be sourced from different locations: inline definitions,
 * database storage, external services, or dynamically generated based on metrics. Each method
 * returns rules for a specific reindexing aspect (granularity, filters, tuning, etc.), either
 * for all rules or filtered by interval applicability.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = InlineReindexingRuleProvider.TYPE, value = InlineReindexingRuleProvider.class),
    @JsonSubTypes.Type(name = ComposingReindexingRuleProvider.TYPE, value = ComposingReindexingRuleProvider.class)
})
public interface ReindexingRuleProvider
{
  /**
   * Returns the type identifier for this provider implementation.
   * <p>
   * This value is used in JSON serialization to identify which provider implementation
   * to use when deserializing.
   *
   * @return the type identifier (e.g., "inline", "external")
   */
  String getType();

  /**
   * Returns true if this provider is ready to supply rules.
   * <p>
   * Providers that depend on external state (HTTP services, databases) should return false
   * until they have successfully initialized and loaded their rules. Reindexing supervisors
   * should check this before generating tasks to avoid creating tasks with incomplete rule sets.
   * <p>
   * The default implementation returns true, which is appropriate for providers that have
   * their rules available immediately (such as inline providers with static configuration).
   *
   * @return true if the provider is ready to supply rules, false otherwise
   */
  default boolean isReady()
  {
    return true;
  }

  /**
   * Returns all reindexing deletion rules that apply to the given interval.
   * <p>
   * Handling partial overlaps is the responsibility of the provider implementation and should be clearly documented.
   * </p>
   * @param interval      The interval to check applicability against.
   * @param referenceTime The reference time to use for period calculations while determining rule applicability for an interval.
   *                      e.g., a rule with period P7D applies to data older than 7 days from the reference time.
   * @return The list of {@link ReindexingDeletionRule} rules that apply to the given interval.
   */
  List<ReindexingDeletionRule> getDeletionRules(Interval interval, DateTime referenceTime);

  /**
   * Returns ALL reindexing deletion rules.
   */
  List<ReindexingDeletionRule> getDeletionRules();

  /**
   * Returns the matched reindexing data schema rule that applies to the given interval.
   * <p>
   * Handling cases of multiple applicable rules and/orpartial overlaps is the responsibility of the provider
   * implementation and should be clearly documented.
   * </p>
   *
   * @param interval      The interval to check applicability against.
   * @param referenceTime The reference time to use for period calculations while determining rule applicability for an interval.
   *                      e.g., a rule with period P7D applies to data older than 7 days from the reference time.
   * @return {@link ReindexingDataSchemaRule} rule that applies to the given interval.
   */
  @Nullable
  ReindexingDataSchemaRule getDataSchemaRule(Interval interval, DateTime referenceTime);

  /**
   * Returns ALL reindexing data schema rules.
   */
  List<ReindexingDataSchemaRule> getDataSchemaRules();

  /**
   * Returns the matched reindexing IO config rule that applies to the given interval.
   * <p>
   * Handling cases of multiple applicable rules and/or partial overlaps is the responsibility of the provider
   * implementation and should be clearly documented.
   * </p>
   *
   * @param interval      The interval to check applicability against.
   * @param referenceTime The reference time to use for period calculations while determining rule applicability for an interval.
   *                      e.g., a rule with period P7D applies to data older than 7 days from the reference time.
   * @return {@link ReindexingIOConfigRule} that applies to the given interval.
   */
  @Nullable
  ReindexingIOConfigRule getIOConfigRule(Interval interval, DateTime referenceTime);

  /**
    * Returns ALL reindexing IO config rules.
   */
  List<ReindexingIOConfigRule> getIOConfigRules();

  /**
   * Returns the matched reindexing segment granularity rule that applies to the given interval.
   * <p>
   * Handling cases of multiple applicable rules and/or partial overlaps is the responsibility of the provider
   * implementation and should be clearly documented.
   * </p>
   *
   * @param interval      The interval to check applicability against.
   * @param referenceTime The reference time to use for period calculations while determining rule applicability for an interval.
   *                      e.g., a rule with period P7D applies to data older than 7 days from the reference time.
   * @return {@link ReindexingSegmentGranularityRule} rule that applies to the given interval.
   */
  @Nullable
  ReindexingSegmentGranularityRule getSegmentGranularityRule(Interval interval, DateTime referenceTime);

  /**
   * Returns ALL reindexing segment granularity rules.
   */
  List<ReindexingSegmentGranularityRule> getSegmentGranularityRules();

  /**
   * Returns the matched reindexing tuning config rule that applies to the given interval.
   * <p>
   * Handling cases of multiple applicable rules and/or partial overlaps is the responsibility of the provider
   * implementation and should be clearly documented.
   * </p>
   *
   * @param interval      The interval to check applicability against.
   * @param referenceTime The reference time to use for period calculations while determining rule applicability for an interval.
   *                      e.g., a rule with period P7D applies to data older than 7 days from the reference time.
   */
  @Nullable
  ReindexingTuningConfigRule getTuningConfigRule(Interval interval, DateTime referenceTime);

  /**
   * Returns ALL reindexing tuning config rules.
   */
  List<ReindexingTuningConfigRule> getTuningConfigRules();

  /**
   * Returns a stream of all reindexing rules across all types.
   * <p>
   * This provides a flexible way to filter, map, and process rules without needing
   * specific methods for every possible combination. For example, to get all non-segment-granularity
   * rules, you can filter: {@code streamAllRules().filter(rule -> !(rule instanceof ReindexingSegmentGranularityRule))}
   *
   * @return a stream of all rules from all rule types
   */
  default Stream<ReindexingRule> streamAllRules()
  {
    return Stream.of(
        getIOConfigRules().stream(),
        getTuningConfigRules().stream(),
        getDeletionRules().stream(),
        getSegmentGranularityRules().stream(),
        getDataSchemaRules().stream()
    ).flatMap(s -> s);
  }
}
