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
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A compaction metrics rule that specifies aggregation metrics for segments older than a specified period.
 * <p>
 * This rule defines the metrics specification used during compaction, enabling rollup and pre-aggregation
 * of older data. For example, applying sum and count aggregators to historical data can significantly
 * reduce storage size while preserving queryability for common aggregation queries.
 * <p>
 * Rules are evaluated at compaction time based on segment age. A rule with period P90D will apply
 * to any segment where the segment's end time is before ("now" - 90 days).
 * <p>
 * This is a non-additive rule. Multiple metrics rules cannot be applied to the same interval safely,
 * as a segment can only have one metrics specification.
 * <p>
 * Example usage:
 * <pre>{@code
 * {
 *   "id": "rollup-90d",
 *   "period": "P90D",
 *   "metricsSpec": [
 *     { "type": "count", "name": "count" },
 *     { "type": "longSum", "name": "total_views", "fieldName": "views" }
 *   ],
 *   "description": "Enable rollup for data older than 90 days"
 * }
 * }</pre>
 */
public class ReindexingMetricsRule extends AbstractReindexingRule
{
  private final AggregatorFactory[] metricsSpec;

  @JsonCreator
  public ReindexingMetricsRule(
      @JsonProperty("id") @Nonnull String id,
      @JsonProperty("description") @Nullable String description,
      @JsonProperty("period") @Nonnull Period period,
      @JsonProperty("metricsSpec") @Nonnull AggregatorFactory[] metricsSpec
  )
  {
    super(id, description, period);
    this.metricsSpec = Objects.requireNonNull(metricsSpec, "metricsSpec cannot be null");
  }

  @JsonProperty
  @Nonnull
  public AggregatorFactory[] getMetricsSpec()
  {
    return metricsSpec;
  }
}
