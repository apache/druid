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
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A compaction tuning config rule that specifies tuning parameters for segments older than a specified period.
 * <p>
 * This rule controls partitioning strategy, indexing behavior, and resource usage during compaction.
 * For example, applying range partitioning with specific dimensions to older data can optimize
 * query performance for common access patterns.
 * <p>
 * Rules are evaluated at compaction time based on segment age. A rule with period P30D will apply
 * to any segment where the segment's end time is before ("now" - 30 days).
 * <p>
 * This is a non-additive rule. Multiple tuning config rules cannot be applied to the same interval safely,
 * as a compaction job can only use one tuning configuration.
 * <p>
 * Example usage:
 * <pre>{@code
 * {
 *   "id": "range-partition-30d",
 *   "period": "P30D",
 *   "tuningConfig": {
 *     "partitionsSpec": {
 *       "type": "range",
 *       "targetRowsPerSegment": 5000000,
 *       "partitionDimensions": ["country", "city"]
 *     }
 *   },
 *   "description": "Use range partitioning for data older than 30 days"
 * }
 * }</pre>
 */
public class ReindexingTuningConfigRule extends AbstractReindexingRule
{
  private final UserCompactionTaskQueryTuningConfig tuningConfig;

  @JsonCreator
  public ReindexingTuningConfigRule(
      @JsonProperty("id") @Nonnull String id,
      @JsonProperty("description") @Nullable String description,
      @JsonProperty("period") @Nonnull Period period,
      @JsonProperty("tuningConfig") @Nonnull UserCompactionTaskQueryTuningConfig tuningConfig
  )
  {
    super(id, description, period);
    this.tuningConfig = Objects.requireNonNull(tuningConfig, "tuningConfig cannot be null");
  }

  @Override
  public boolean isAdditive()
  {
    return false;
  }

  @JsonProperty
  public UserCompactionTaskQueryTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }
}
