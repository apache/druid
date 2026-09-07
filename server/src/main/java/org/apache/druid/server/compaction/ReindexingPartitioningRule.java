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
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.VirtualColumns;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * A {@link ReindexingRule} that specifies partitioning configuration for reindexing tasks.
 * <p>
 * This rule controls how data is physically laid out into segments during reindexing, combining
 * segment granularity (time bucketing) and partitions spec (how data within a time bucket is split).
 * It also supports optional virtual columns for partitioning by nested column fields.
 * <p>
 * This is a non-additive rule. Multiple partitioning rules cannot be applied to the same interval.
 * <p>
 * Example inline usage:
 * <pre>{@code
 * {
 *     "id": "daily-range-30d",
 *     "olderThan": "P30D",
 *     "segmentGranularity": "DAY",
 *     "partitionsSpec": {
 *       "type": "range",
 *       "targetRowsPerSegment": 5000000,
 *       "partitionDimensions": ["country", "city"]
 *     },
 *     "description": "Compact to daily segments with range partitioning for data older than 30 days"
 * }
 * }</pre>
 */
public class ReindexingPartitioningRule extends AbstractReindexingRule
{
  static final String SYNTHETIC_RULE_ID = "synthetic-rule";

  private static final List<Granularity> SUPPORTED_SEGMENT_GRANULARITIES = List.of(
      Granularities.MINUTE,
      Granularities.FIFTEEN_MINUTE,
      Granularities.HOUR,
      Granularities.DAY,
      Granularities.MONTH,
      Granularities.QUARTER,
      Granularities.YEAR
  );

  private final Granularity segmentGranularity;
  private final PartitionsSpec partitionsSpec;
  private final VirtualColumns virtualColumns;

  /**
   * Creates a synthetic partitioning rule used to carry default partitioning configuration
   * for intervals not covered by any user-defined partitioning rule.
   * <p>
   * Synthetic rules are created by {@link org.apache.druid.indexing.compact.CascadingReindexingTemplate}
   * to represent the template's default segment granularity, partitions spec, and optional virtual columns.
   * They use a fixed ID and zero-length period since these fields are not meaningful for synthetic rules —
   * only the partitioning configuration (granularity, partitionsSpec, virtualColumns) is used.
   */
  public static ReindexingPartitioningRule syntheticRule(
      Granularity segmentGranularity,
      PartitionsSpec partitionsSpec,
      @Nullable VirtualColumns virtualColumns
  )
  {
    return new ReindexingPartitioningRule(
        SYNTHETIC_RULE_ID,
        null,
        Period.ZERO,
        segmentGranularity,
        partitionsSpec,
        virtualColumns
    );
  }

  @JsonCreator
  public ReindexingPartitioningRule(
      @JsonProperty("id") @Nonnull String id,
      @JsonProperty("description") @Nullable String description,
      @JsonProperty("olderThan") @Nonnull Period olderThan,
      @JsonProperty("segmentGranularity") @Nonnull Granularity segmentGranularity,
      @JsonProperty("partitionsSpec") @Nonnull PartitionsSpec partitionsSpec,
      @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns
  )
  {
    super(id, description, olderThan);
    InvalidInput.conditionalException(
        SUPPORTED_SEGMENT_GRANULARITIES.contains(segmentGranularity),
        "Unsupported segment granularity [%s]. Supported values are: MINUTE, FIFTEEN_MINUTE, HOUR, DAY, MONTH, QUARTER, YEAR",
        segmentGranularity
    );
    InvalidInput.conditionalException(partitionsSpec != null, "'partitionsSpec' cannot be null");
    this.segmentGranularity = segmentGranularity;
    this.partitionsSpec = partitionsSpec;
    this.virtualColumns = virtualColumns;
  }

  @JsonProperty
  public Granularity getSegmentGranularity()
  {
    return segmentGranularity;
  }

  @JsonProperty
  public PartitionsSpec getPartitionsSpec()
  {
    return partitionsSpec;
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
    ReindexingPartitioningRule that = (ReindexingPartitioningRule) o;
    return Objects.equals(getId(), that.getId())
           && Objects.equals(getDescription(), that.getDescription())
           && Objects.equals(getOlderThan(), that.getOlderThan())
           && Objects.equals(segmentGranularity, that.segmentGranularity)
           && Objects.equals(partitionsSpec, that.partitionsSpec)
           && Objects.equals(virtualColumns, that.virtualColumns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        getId(),
        getDescription(),
        getOlderThan(),
        segmentGranularity,
        partitionsSpec,
        virtualColumns
    );
  }

  @Override
  public String toString()
  {
    return "ReindexingPartitioningRule{"
           + "id='" + getId() + '\''
           + ", description='" + getDescription() + '\''
           + ", olderThan=" + getOlderThan()
           + ", segmentGranularity=" + segmentGranularity
           + ", partitionsSpec=" + partitionsSpec
           + ", virtualColumns=" + virtualColumns
           + '}';
  }

}
