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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Base implementation for reindexing rules that apply based on data age thresholds.
 * <p>
 * Provides period-based applicability logic: a rule with period P7D applies to data
 * older than 7 days. Subclasses define specific reindexing configuration (granularity,
 * filters, tuning, etc.) and whether multiple rules can combine (additive vs non-additive).
 * <p>
 * The {@link #appliesTo(Interval, DateTime)} method determines if an interval is fully,
 * partially, or not covered by this rule's threshold, enabling cascading reindexing
 * strategies where different rules apply to different age tiers of data.
 */
public abstract class AbstractReindexingRule implements ReindexingRule
{
  private static final Logger LOG = new Logger(AbstractReindexingRule.class);

  private final String id;
  private final String description;
  private final Period period;

  public AbstractReindexingRule(
      @Nonnull String id,
      @Nullable String description,
      @Nonnull Period period
  )
  {
    this.id = Objects.requireNonNull(id, "id cannot be null");
    this.description = description;
    this.period = Objects.requireNonNull(period, "period cannot be null");

    validatePeriodIsPositive(period);
  }

  /**
   * Validates that a period represents a positive duration.
   * <p>
   * For periods with precise units (days, hours, minutes, seconds), validates by converting
   * to a standard duration. For periods with variable-length units (months, years), validates
   * that at least one component is positive, since these cannot be converted to a precise duration.
   *
   * @param period the period to validate
   * @throws IllegalArgumentException if the period is not positive
   */
  private static void validatePeriodIsPositive(Period period)
  {
    if (hasMonthsOrYears(period)) {
      if (!isPeriodPositive(period)) {
        throw new IllegalArgumentException("period must be positive. Supplied period: " + period);
      }
    } else {
      if (period.toStandardDuration().getMillis() <= 0) {
        throw new IllegalArgumentException("period must be positive. Supplied period: " + period);
      }
    }
  }

  /**
   * Checks if a period with variable-length components (months/years) is positive.
   *
   * @param period the period to check
   * @return true if any component is positive and no components are negative
   */
  private static boolean isPeriodPositive(Period period)
  {
    boolean hasPositiveComponent = period.getYears() > 0
                                   || period.getMonths() > 0
                                   || period.getWeeks() > 0
                                   || period.getDays() > 0
                                   || period.getHours() > 0
                                   || period.getMinutes() > 0
                                   || period.getSeconds() > 0
                                   || period.getMillis() > 0;

    boolean hasNegativeComponent = period.getYears() < 0
                                   || period.getMonths() < 0
                                   || period.getWeeks() < 0
                                   || period.getDays() < 0
                                   || period.getHours() < 0
                                   || period.getMinutes() < 0
                                   || period.getSeconds() < 0
                                   || period.getMillis() < 0;

    return hasPositiveComponent && !hasNegativeComponent;
  }

  @JsonProperty
  @Override
  public String getId()
  {
    return id;
  }

  @JsonProperty
  @Override
  public String getDescription()
  {
    return description;
  }

  @JsonProperty
  @Override
  public Period getPeriod()
  {
    return period;
  }

  @Override
  public AppliesToMode appliesTo(Interval interval, @Nullable DateTime referenceTime)
  {
    DateTime now = (referenceTime != null) ? referenceTime : DateTimes.nowUtc();
    DateTime intervalEnd = interval.getEnd();
    DateTime intervalStart = interval.getStart();

    DateTime threshold = now.minus(period);

    if (intervalEnd.isBefore(threshold) || intervalEnd.isEqual(threshold)) {
      LOG.debug("Reindexing rule [%s] applies FULLY to interval [%s]. Threshold: [%s]", id, interval, threshold);
      return AppliesToMode.FULL;
    } else if (intervalStart.isAfter(threshold)) {
      LOG.debug("Reindexing rule [%s] does NOT apply to interval [%s]. Threshold: [%s]", id, interval, threshold);
      return AppliesToMode.NONE;
    } else {
      LOG.debug("Reindexing rule [%s] applies PARTIALLY to interval [%s]. Threshold: [%s]", id, interval, threshold);
      return AppliesToMode.PARTIAL;
    }
  }

  /**
   * Checks if a period contains months or years components which have variable lenghts and require special handling
   */
  private static boolean hasMonthsOrYears(Period period)
  {
    return period.getYears() != 0 || period.getMonths() != 0;
  }

}
