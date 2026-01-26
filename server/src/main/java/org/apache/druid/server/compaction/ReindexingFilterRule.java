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
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.VirtualColumns;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A compaction filter rule that specifies rows to remove from segments older than a specified period.
 * <p>
 * The filter defines rows to REMOVE from compacted segments. For example, a filter
 * {@code selector(isRobot=true)} means "remove rows where isRobot=true". The compaction framework
 * automatically wraps these filters in NOT logic during processing.
 * <p>
 * Rules are evaluated at compaction time based on segment age. A rule with period P90D will apply
 * to any segment where the segment's end time is before ("now" - 90 days).
 * <p>
 * Multiple rules can apply to the same segment. When multiple rules apply, they are combined as
 * NOT(A OR B OR C) for optimal bitmap performance, which is equivalent to NOT A AND NOT B AND NOT C
 * but uses fewer operations.
 * <p>
 * Example usage:
 * <pre>{@code
 * {
 *   "id": "remove-robots-90d",
 *   "period": "P90D",
 *   "filter": {
 *     "type": "selector",
 *     "dimension": "isRobot",
 *     "value": "true"
 *   },
 *   "description": "Remove robot traffic from segments older than 90 days"
 * }
 * }</pre>
 * <p>
 * Virtual column support for filtering on nested fields (MSQ engine only):
 * <p>
 * It is important to note that when using virtual columns in the filter, the virtual columns must be defined
 * with unique names. Users will have to take care to ensure a rule always has the same unique virtual column names
 * to not impact the fingerprinting of segments reindexed with the rule.
 * <pre>{@code
 * {
 *   "id": "remove-using-nested-field-filter",
 *   "period": "P90D",
 *   "filter": {
 *     "type": "selector",
 *     "dimension": "extractedField",
 *     "value": "unwantedValue"
 *   },
 *   "virtualColumns": [
 *     {
 *       "type": "expression",
 *       "name": "extractedField",
 *       "expression": "json_value(metadata, '$.category')",
 *       "outputType": "STRING"
 *     }
 *   ],
 *   "description": "Remove rows where metadata.category = 'unwantedValue' from segments older than 90 days"
 * }
 * }</pre>
 */
public class ReindexingFilterRule extends AbstractReindexingRule
{
  private final DimFilter filter;
  private final VirtualColumns virtualColumns;

  @JsonCreator
  public ReindexingFilterRule(
      @JsonProperty("id") @Nonnull String id,
      @JsonProperty("description") @Nullable String description,
      @JsonProperty("period") @Nonnull Period period,
      @JsonProperty("filter") @Nonnull DimFilter filter,
      @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns
  )
  {
    super(id, description, period);
    this.filter = Objects.requireNonNull(filter, "filter cannot be null");
    this.virtualColumns = virtualColumns;
  }

  @JsonProperty
  public DimFilter getFilter()
  {
    return filter;
  }

  @JsonProperty
  @Nullable
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }
}
