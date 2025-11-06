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

package org.apache.druid.indexing.compact;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.DateTime;
import org.joda.time.Period;

/**
 * A single rule used inside {@link CascadingCompactionTemplate}.
 */
public class CompactionRule
{
  private final Period period;
  private final CompactionJobTemplate template;

  @JsonCreator
  public CompactionRule(
      @JsonProperty("period") Period period,
      @JsonProperty("template") CompactionJobTemplate template
  )
  {
    this.period = period;
    this.template = template;
  }

  @JsonProperty
  public CompactionJobTemplate getTemplate()
  {
    return template;
  }

  @JsonProperty
  public Period getPeriod()
  {
    return period;
  }

  /**
   * Computes the start time of this rule by subtracting its period from the
   * reference timestamp.
   * <p>
   * If both this rule and the {@code beforeRule} explicitly specify a target
   * segment granularity, the start time may be adjusted to ensure that there
   * are no uncompacted gaps left in the timeline.
   *
   * @param referenceTime Current time when the rules are being evaluated
   * @param beforeRule    The rule before this one in chronological order
   */
  public DateTime computeStartTime(DateTime referenceTime, CompactionRule beforeRule)
  {
    final Granularity granularity = template.getSegmentGranularity();
    final Granularity beforeGranularity = beforeRule.template.getSegmentGranularity();

    final DateTime calculatedStartTime = referenceTime.minus(period);

    if (granularity == null || beforeGranularity == null) {
      return calculatedStartTime;
    } else {
      // The gap can be filled only if it is bigger than the granularity of this rule.
      // If beforeGranularity > granularity, gap would always be smaller than both
      final DateTime beforeRuleEffectiveEnd = beforeGranularity.bucketStart(calculatedStartTime);
      final DateTime possibleStartTime = granularity.bucketStart(beforeRuleEffectiveEnd);
      return possibleStartTime.isBefore(beforeRuleEffectiveEnd)
             ? granularity.increment(possibleStartTime)
             : possibleStartTime;
    }
  }
}
