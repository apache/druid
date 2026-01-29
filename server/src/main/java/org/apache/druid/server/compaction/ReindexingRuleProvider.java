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
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

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
   * Returns all unique periods used by the rules provided by this provider, condensed and sorted in ascending order.
   * <p>
   * Ascending order means from shortest to longest period. For example, [P1D, P7D, P30D].
   * </p>
   */
  @Nonnull
  List<Period> getCondensedAndSortedPeriods(DateTime referenceTime);

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
   * Returns the matched reindexing metrics rule that applies to the given interval.
   * <p>
   * Handling cases of multiple applicable rules and/or partial overlaps is the responsibility of the provider
   * implementation and should be clearly documented.
   * </p>
   *
   * @param interval      The interval to check applicability against.
   * @param referenceTime The reference time to use for period calculations while determining rule applicability for an interval.
   *                      e.g., a rule with period P7D applies to data older than 7 days from the reference time.
   * @return {@link ReindexingMetricsRule} rule that applies to the given interval.
   */
  @Nullable
  ReindexingMetricsRule getMetricsRule(Interval interval, DateTime referenceTime);

  /**
   * Returns ALL reindexing metrics rules.
   */
  List<ReindexingMetricsRule> getMetricsRules();

  /**
   * Returns the matched reindexing dimensions rule that applies to the given interval.
   * <p>
   * Handling cases of multiple applicable rules and/or partial overlaps is the responsibility of the provider
   * implementation and should be clearly documented.
   * </p>
   *
   * @param interval      The interval to check applicability against.
   * @param referenceTime The reference time to use for period calculations while determining rule applicability for an interval.
   *                      e.g., a rule with period P7D applies to data older than 7 days from the reference time.
   * @return {@link ReindexingDimensionsRule} rule that applies to the given interval.
   */
  @Nullable
  ReindexingDimensionsRule getDimensionsRule(Interval interval, DateTime referenceTime);

  /**
   * Returns ALL reindexing dimensions rules.
   */
  List<ReindexingDimensionsRule> getDimensionsRules();

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
   * Returns the matched reindexing projection rule that applies to the given interval.
   * <p>
   * Handling cases of multiple applicable rules and/or partial overlaps is the responsibility of the provider
   * implementation and should be clearly documented.
   * </p>
   *
   * @param interval      The interval to check applicability against.
   * @param referenceTime The reference time to use for period calculations while determining rule applicability for an interval.
   *                      e.g., a rule with period P7D applies to data older than 7 days from the reference time.
   * @return {@link ReindexingProjectionRule} rule that applies to the given interval.
   */
  @Nullable
  ReindexingProjectionRule getProjectionRule(Interval interval, DateTime referenceTime);

  /**
   * Returns ALL reindexing projection rules.
   */
  List<ReindexingProjectionRule> getProjectionRules();

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
   * Returns the matched reindexing query granularity rule that applies to the given interval.
   * <p>
   * Handling cases of multiple applicable rules and/or partial overlaps is the responsibility of the provider
   * implementation and should be clearly documented.
   * </p>
   *
   * @param interval      The interval to check applicability against.
   * @param referenceTime The reference time to use for period calculations while determining rule applicability for an interval.
   *                      e.g., a rule with period P7D applies to data older than 7 days from the reference time.
   * @return {@link ReindexingQueryGranularityRule} rule that applies to the given interval.
   */
  @Nullable
  ReindexingQueryGranularityRule getQueryGranularityRule(Interval interval, DateTime referenceTime);

  /**
   * Returns ALL reindexing query granularity rules.
   */
  List<ReindexingQueryGranularityRule> getQueryGranularityRules();

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
}
