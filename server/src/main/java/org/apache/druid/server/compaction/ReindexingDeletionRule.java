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
 * A {@link ReindexingRule} that specifies patterns to match for removing rows during reindexing.
 * <p>
 * The {@link #deleteWhere} clause defines rows to REMOVE from reindexed segments. For example, a `deleteWhere` {@link DimFilter}
 * {@code selector(isRobot=true)} with {@link AbstractReindexingRule#olderThan} P90D will "remove rows where isRobot=true
 * from data older than 90 days". The reindexing framework automatically wraps these delete where clauses in NOT logic during
 * processing.
 * <p>
 * This is an additive rule. Multiple rules can apply to the same segment. When multiple rules apply, they are combined
 * as NOT(A OR B OR C) where A, B, and C come from distinct {@link ReindexingDeletionRule}s.
 * <p>
 * Example usage:
 * <pre>{@code
 * {
 *   "id": "remove-robots-90d",
 *   "olderThan": "P90D",
 *   "deleteWhere": {
 *     "type": "selector",
 *     "dimension": "isRobot",
 *     "value": "true"
 *   },
 *   "description": "Remove robot traffic from data older than 90 days"
 * }
 * }</pre>
 * <p>
 * Virtual column support for filtering on nested fields (MSQ engine only):
 * <p>
 * It is important to note that when using virtual columns in the filter, the virtual columns must be defined
 * with unique names. Users will have to take care to ensure a rule always has the same unique virtual column names
 * to not impact the fingerprinting of segments reindexed with the rule.
 * <p>
 * Example inline useage with virtual column:
 * <pre>{@code
 * {
 *   "id": "remove-using-nested-field-filter",
 *   "olderThan": "P90D",
 *   "deleteWhere": {
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
public class ReindexingDeletionRule extends AbstractReindexingRule
{
  private final DimFilter deleteWhere;
  private final VirtualColumns virtualColumns;

  @JsonCreator
  public ReindexingDeletionRule(
      @JsonProperty("id") @Nonnull String id,
      @JsonProperty("description") @Nullable String description,
      @JsonProperty("olderThan") @Nonnull Period olderThan,
      @JsonProperty("deleteWhere") @Nonnull DimFilter deleteWhere,
      @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns
  )
  {
    super(id, description, olderThan);
    this.deleteWhere = Objects.requireNonNull(deleteWhere, "deleteWhere cannot be null");
    this.virtualColumns = virtualColumns;
  }

  @JsonProperty
  public DimFilter getDeleteWhere()
  {
    return deleteWhere;
  }

  @JsonProperty
  @Nullable
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReindexingDeletionRule that = (ReindexingDeletionRule) o;
    return Objects.equals(getId(), that.getId())
           && Objects.equals(getDescription(), that.getDescription())
           && Objects.equals(getOlderThan(), that.getOlderThan())
           && Objects.equals(deleteWhere, that.deleteWhere)
           && Objects.equals(virtualColumns, that.virtualColumns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        getId(),
        getDescription(),
        getOlderThan(),
        deleteWhere,
        virtualColumns
    );
  }

  @Override
  public String toString()
  {
    return "ReindexingDeletionRule{"
           + "id='" + getId() + '\''
           + ", description='" + getDescription() + '\''
           + ", olderThan=" + getOlderThan()
           + ", deleteWhere=" + deleteWhere
           + ", virtualColumns=" + virtualColumns
           + '}';
  }
}
