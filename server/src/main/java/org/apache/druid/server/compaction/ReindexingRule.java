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
 * Defines a reindexing configuration that applies to data based on age thresholds.
 * <p>
 * Rules encapsulate specific aspects of reindexing (granularity, filters, tuning, etc.)
 * and specify when they should apply via a {@link Period} which defines the age threshold for applicability.
 * <p>
 * Rules conditionally apply to data "older than" the rules threshold relative to the time of rule evaluation.
 */
public interface ReindexingRule
{
  /**
   * Indicates how a rule applies to a given time interval based on the rule's period threshold.
   * <ul>
   * <li>PARTIAL: The rule applies to part of the interval.</li>
   * <li>FULL: The rule applies to the entire interval.</li>
   * <li>NONE: The rule does not apply to the interval at all.</li>
   * </ul>
   */
  enum AppliesToMode
  {
    PARTIAL,
    FULL,
    NONE
  }

  /**
   * Provides an identifier for the rule, which can be used for referencing, logging, or management purposes.
   * <p>
   * Note that there is no inherent contract for uniqueness across rule sets provided by this interface.
   */
  String getId();

  /**
   * Provides a human-readable description of the rule, which can be used for logging, debugging, or user interfaces to explain the purpose and behavior of the rule.
   */
  String getDescription();

  /**
   * Defines the age threshold for rule applicability. The rule applies to data older than a reference time minus this period.
   * <p>
   * For example, if the period is "P30D" (30 days) and the reference time is "2024-01-31T00:00:00Z", the rule applies to data older than "2024-01-01T00:00:00Z".
   * @return The period representing the age threshold for rule applicability.
   */
  Period getOlderThan();

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

}
