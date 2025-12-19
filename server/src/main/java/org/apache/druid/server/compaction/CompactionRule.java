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

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;

/**
 * Defines a compaction configuration that applies to data based on age thresholds.
 * <p>
 * Rules encapsulate specific aspects of compaction (granularity, filters, tuning, etc.)
 * and specify when they should apply via a period threshold.
 */
public interface CompactionRule
{
  /**
   * Indicates how a rule applies to a given time interval based on the rule's period threshold.
   * <ul>
   * <li>PARTIAL: The rule applies to part of the interval.</li>
   * <li>FULL: The rule applies to the entire interval.</li>
   * <li>NONE: The rule does not apply to the interval at all.</li
   * </ul>
   */
  enum AppliesToMode
  {
    PARTIAL,
    FULL,
    NONE
  }

  String getId();

  String getDescription();

  Period getPeriod();

  /**
   * Check if this rule applies to the given interval.
   * <p>
   * <ul>
   * <li>If the rule applies to the entire interval, return {@link AppliesToMode#FULL}.</li>
   * <li>If the rule applies to only part of the interval, return {@link AppliesToMode#PARTIAL}.</li>
   * <li>If the rule does not apply to the interval at all, return {@link AppliesToMode#NONE}.</li>
   * </ul>
   * @param interval      The interval to check applicability against.
   * @param referenceTime The reference time to use for period calculations. null results in using current UTC instant at runtime.
   * @return The applicability mode of the rule for the given interval and reference time.
   */
  AppliesToMode appliesTo(Interval interval, @Nullable DateTime referenceTime);

  /**
   * Indicates whether the rule is additive, meaning it can be combined with other rules.
   * <p>
   * An additive rule can be merged with other rules of its type within the same interval. An example would be dimension
   * filter rules that can be combined using OR logic. Such as, rule 1: filter out segments where country = 'US' OR
   * rule 2: device = 'mobile'.
   * </p>
   * <p>
   * A non-addditive rule cannot be combined with other rules of its type within the same interval. An example would be
   * segment granularity rules, where only one granularity can be applied to a given interval.
   * </p>
   */
  boolean isAdditive();
}
