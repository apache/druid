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
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * A {@link ReindexingRule} that specifies {@link AggregateProjectionSpec}s to use while building segments.
 * <p>
 * This rule defines pre-aggregated views of data that can accelerate specific query patterns. Projections are
 * particularly useful for older data where query patterns are well-understood and storage efficiency is valuable.
 * <p>
 * This is a non-additive rule.
 * <p>
 * Example inline usage:
 * <pre>{@code
 * {
 *   "id": "hourly-projection-90d",
 *   "olderThan": "P90D",
 *   "projections": [
 *     {
 *       "name": "hourly_agg",
 *       "dimensions": ["country"],
 *       "metrics": [
 *         { "type": "longSum", "name": "total_views", "fieldName": "views" }
 *       ]
 *     }
 *   ],
 *   "description": "create hourly aggregation projection for data older than 90 days"
 * }
 * }</pre>
 */
public class ReindexingProjectionRule extends AbstractReindexingRule
{
  private final List<AggregateProjectionSpec> projections;

  @JsonCreator
  public ReindexingProjectionRule(
      @JsonProperty("id") @Nonnull String id,
      @JsonProperty("description") @Nullable String description,
      @JsonProperty("olderThan") @Nonnull Period olderThan,
      @JsonProperty("projections") @Nonnull List<AggregateProjectionSpec> projections
  )
  {
    super(id, description, olderThan);
    this.projections = Objects.requireNonNull(projections, "projections cannot be null");
  }

  @JsonProperty
  public List<AggregateProjectionSpec> getProjections()
  {
    return projections;
  }
}
