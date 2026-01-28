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
 * A {@link ReindexingRule} that specifies a {@link UserCompactionTaskDimensionsConfig} for tasks to configure.
 * <p>
 * This rule defines which dimensions and their types for reindexed segments. For example,
 * dropping unused dimensions from older data can reduce storage size.
 * <p>
 * This is a non-additive rule. Multiple dimensions rules cannot be applied to the same interval,
 * as a segment can only have one dimensions specification.
 * <p>
 * Example inline usage:
 * <pre>{@code
 * {
 *   "id": "optimize-dimensions-90d",
 *   "olderThan": "P90D",
 *   "dimensionsSpec": {
 *     "dimensions": [
 *       "country",
 *       "city",
 *       { "type": "long", "name": "user_id" }
 *     ]
 *   },
 *   "description": "modify dimension schema for data older than 90 days"
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
      @JsonProperty("olderThan") @Nonnull Period olderThan,
      @JsonProperty("dimensionsSpec") @Nonnull UserCompactionTaskDimensionsConfig dimensionsSpec
  )
  {
    super(id, description, olderThan);
    this.dimensionsSpec = Objects.requireNonNull(dimensionsSpec, "dimensionsSpec cannot be null");
  }

  @JsonProperty
  public UserCompactionTaskDimensionsConfig getDimensionsSpec()
  {
    return dimensionsSpec;
  }
}
