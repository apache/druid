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
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A compaction granularity rule that specifies segment and query granularity for segments older than a specified period.
 * <p>
 * This rule controls how time-series data is bucketed during compaction. For example, changing from
 * 15-minute segments to hourly segments reduces segment count and improves query performance for
 * older data that doesn't require fine-grained time resolution.
 * <p>
 * Rules are evaluated at compaction time based on segment age. A rule with period P7D will apply
 * to any segment where the segment's end time is before ("now" - 7 days).
 * <p>
 * This is a non-additive rule. Multiple granularity rules cannot be applied to the same interval safely,
 * as a segment can only have one granularity.
 * <p>
 * Example usage:
 * <pre>{@code
 * {
 *   "id": "daily-30d",
 *   "period": "P30D",
 *   "granularityConfig": {
 *     "segmentGranularity": "DAY",
 *     "queryGranularity": "HOUR"
 *   },
 *   "description": "Compact to daily segments for data older than 30 days"
 * }
 * }</pre>
 */
public class ReindexingGranularityRule extends AbstractReindexingRule
{

  private final UserCompactionTaskGranularityConfig granularityConfig;

  @JsonCreator
  public ReindexingGranularityRule(
      @JsonProperty("id") @Nonnull String id,
      @JsonProperty("description") @Nullable String description,
      @JsonProperty("period") @Nonnull Period period,
      @JsonProperty("granularityConfig") @Nonnull UserCompactionTaskGranularityConfig granularityConfig)
  {
    super(id, description, period);
    this.granularityConfig = Objects.requireNonNull(granularityConfig, "granularityConfig cannot be null");
  }

  @JsonProperty
  public UserCompactionTaskGranularityConfig getGranularityConfig()
  {
    return granularityConfig;
  }
}
