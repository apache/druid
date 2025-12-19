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

import java.util.List;

/**
 * Provides compaction rules for different aspects of compaction configuration.
 * <p>
 * This abstraction allows rules to be sourced from different locations: inline definitions,
 * database storage, external services, or dynamically generated based on metrics. Each method
 * returns rules for a specific compaction aspect (granularity, filters, tuning, etc.), either
 * for all rules or filtered by interval applicability.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = InlineCompactionRuleProvider.TYPE, value = InlineCompactionRuleProvider.class),
    @JsonSubTypes.Type(name = ComposingCompactionRuleProvider.TYPE, value = ComposingCompactionRuleProvider.class)
})
public interface CompactionRuleProvider
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
   * until they have successfully initialized and loaded their rules. Compaction supervisors
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
  List<Period> getCondensedAndSortedPeriods(DateTime referenceTime);

  /**
   * Returns all compaction filter rules that apply to the given interval.
   * <p>
   * Handling partial overlaps is the responsibility of the provider implementation and should be clearly documented.
   * </p>
   * @param interval      The interval to check applicability against.
   * @param referenceTime The reference time to use for period calculations while determining rule applicability for an interval.
   *                      e.g., a rule with period P7D applies to data older than 7 days from the reference time.
   * @return The list of {@link CompactionFilterRule} rules that apply to the given interval.
   */
  List<CompactionFilterRule> getFilterRules(Interval interval, DateTime referenceTime);

  /**
   * Returns ALL compaction filter rules.
   */
  List<CompactionFilterRule> getFilterRules();

  /**
   * Returns all compaction metrics rules that apply to the given interval.
   * <p>
   * Handling partial overlaps is the responsibility of the provider implementation and should be clearly documented.
   * </p>
   * @param interval      The interval to check applicability against.
   * @param referenceTime The reference time to use for period calculations while determining rule applicability for an interval.
   *                      e.g., a rule with period P7D applies to data older than 7 days from the reference time.
   * @return The list of {@link CompactionMetricsRule} rules that apply to the given interval.
   */
  List<CompactionMetricsRule> getMetricsRules(Interval interval, DateTime referenceTime);

  /**
   * Returns ALL compaction metrics rules.
   * <p>
   * Handling partial overlaps is the responsibility of the provider implementation and should be clearly documented.
   * </p>
   */
  List<CompactionMetricsRule> getMetricsRules();

  /**
   * Returns all compaction dimensions rules that apply to the given interval.
   * <p>
   * Handling partial overlaps is the responsibility of the provider implementation and should be clearly documented.
   * </p>
   * @param interval      The interval to check applicability against.
   * @param referenceTime The reference time to use for period calculations while determining rule applicability for an interval.
   *                      e.g., a rule with period P7D applies to data older than 7 days from the reference time.
   * @return The list of {@link CompactionDimensionsRule} rules that apply to the given interval.
   */
  List<CompactionDimensionsRule> getDimensionsRules(Interval interval, DateTime referenceTime);

  /**
   * Returns ALL compaction dimensions rules.
   */
  List<CompactionDimensionsRule> getDimensionsRules();

  /**
   * Returns all compaction IO config rules that apply to the given interval.
   * <p>
   * Handling partial overlaps is the responsibility of the provider implementation and should be clearly documented.
   * </p>
   * @param interval      The interval to check applicability against.
   * @param referenceTime The reference time to use for period calculations while determining rule applicability for an interval.
   *                      e.g., a rule with period P7D applies to data older than 7 days from the reference time.
   * @return The list of {@link CompactionIOConfigRule} rules that apply to the given interval.
   */
  List<CompactionIOConfigRule> getIOConfigRules(Interval interval, DateTime referenceTime);

  /**
    * Returns ALL compaction IO config rules.
   */
  List<CompactionIOConfigRule> getIOConfigRules();

  /**
   * Returns all compaction projection rules that apply to the given interval.
   * <p>
   * Handling partial overlaps is the responsibility of the provider implementation and should be clearly documented.
   * </p>
   * @param interval      The interval to check applicability against.
   * @param referenceTime The reference time to use for period calculations while determining rule applicability for an interval.
   *                      e.g., a rule with period P7D applies to data older than 7 days from the reference time.
   * @return The list of {@link CompactionProjectionRule} rules that apply to the given interval.
   */
  List<CompactionProjectionRule> getProjectionRules(Interval interval, DateTime referenceTime);

  /**
   * Returns ALL compaction projection rules.
   */
  List<CompactionProjectionRule> getProjectionRules();

  /**
   * Returns all compaction granularity rules that apply to the given interval.
   * <p>
   * Handling partial overlaps is the responsibility of the provider implementation and should be clearly documented.
   * </p>
   * @param interval      The interval to check applicability against.
   * @param referenceTime The reference time to use for period calculations while determining rule applicability for an interval.
   *                      e.g., a rule with period P7D applies to data older than 7 days from the reference time.
   * @return The list of {@link CompactionGranularityRule} rules that apply to the given interval.
   */
  List<CompactionGranularityRule> getGranularityRules(Interval interval, DateTime referenceTime);

  /**
   * Returns ALL compaction granularity rules.
   */
  List<CompactionGranularityRule> getGranularityRules();

  /**
   * Returns all compaction tuning config rules that apply to the given interval.
   * <p>
   * Handling partial overlaps is the responsibility of the provider implementation and should be clearly documented.
   * </p>
   * @param interval      The interval to check applicability against.
   * @param referenceTime The reference time to use for period calculations while determining rule applicability for an interval.
   *                      e.g., a rule with period P7D applies to data older than 7 days from the reference time.
   */
  List<CompactionTuningConfigRule> getTuningConfigRules(Interval interval, DateTime referenceTime);

  /**
   * Returns ALL compaction tuning config rules.
   */
  List<CompactionTuningConfigRule> getTuningConfigRules();
}
