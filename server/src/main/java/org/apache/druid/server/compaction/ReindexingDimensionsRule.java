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
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A compaction dimensions rule that specifies dimension schema for segments older than a specified period.
 * <p>
 * This rule defines which dimensions to include in compacted segments and their types. For example,
 * dropping unused dimensions from older data can reduce storage size and improve query performance.
 * <p>
 * Rules are evaluated at compaction time based on segment age. A rule with period P90D will apply
 * to any segment where the segment's end time is before ("now" - 90 days).
 * <p>
 * This is a non-additive rule. Multiple dimensions rules cannot be applied to the same interval safely,
 * as a segment can only have one dimensions specification.
 * <p>
 * Example usage:
 * <pre>{@code
 * {
 *   "id": "optimize-dimensions-90d",
 *   "period": "P90D",
 *   "dimensionsSpec": {
 *     "dimensions": [
 *       "country",
 *       "city",
 *       { "type": "long", "name": "user_id" }
 *     ]
 *   },
 *   "description": "Optimize dimension schema for data older than 90 days"
 * }
 * }</pre>
 */
public class ReindexingDimensionsRule extends AbstractReindexingRule
{
  private final UserCompactionTaskDimensionsConfig dimensionsSpec;

  @JsonCreator
  public ReindexingDimensionsRule(
      @JsonProperty("id") @Nonnull String id,
      @JsonProperty("description") @Nullable String description,
      @JsonProperty("period") @Nonnull Period period,
      @JsonProperty("dimensionsSpec") @Nonnull UserCompactionTaskDimensionsConfig dimensionsSpec
  )
  {
    super(id, description, period);
    this.dimensionsSpec = Objects.requireNonNull(dimensionsSpec, "dimensionsSpec cannot be null");
  }

  @Override
  public boolean isAdditive()
  {
    return false;
  }

  @JsonProperty
  public UserCompactionTaskDimensionsConfig getDimensionsSpec()
  {
    return dimensionsSpec;
  }
}
