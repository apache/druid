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

package org.apache.druid.segment.projections;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import org.apache.druid.segment.Metadata;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * V10 Segment format projection metadata, containing projection row counts and schema information
 */
public class ProjectionMetadata
{
  public static final Interner<ProjectionSchema> SCHEMA_INTERNER = Interners.newWeakInterner();

  public static ProjectionMetadata forBaseTable(
      int numRows,
      @Nullable Long minTime,
      @Nullable Long maxTime,
      List<String> dims,
      Metadata metadata
  )
  {
    final ProjectionSchema schema;
    if (Boolean.TRUE.equals(metadata.isRollup())) {
      schema = RollupTableProjectionSchema.fromMetadata(dims, metadata);
    } else {
      schema = TableProjectionSchema.fromMetadata(dims, metadata);
    }
    return new ProjectionMetadata(numRows, schema, minTime, maxTime);
  }

  private final int numRows;
  private final ProjectionSchema schema;
  /**
   * Minimum {@code __time} value across all rows in this projection, or {@code null} if the writer didn't supply one
   * (e.g. older segments written before this field existed, or projections that don't track it). Populated by
   * {@link org.apache.druid.segment.IndexMergerV10} so that readers can answer time-boundary queries from metadata
   * alone without scanning the projection's {@code __time} column.
   * <p>
   * The value is independent of row order: it reflects the actual minimum timestamp across all walked rows even when
   * the projection is not time-sorted (e.g. when ingested with {@code DimensionsSpec.forceSegmentSortByTime = false}).
   * Readers can therefore treat both this field and {@link #maxTime} as exact bounds whenever they are present.
   */
  @Nullable
  private final Long minTime;
  /**
   * Maximum {@code __time} value across all rows in this projection. See {@link #minTime} for semantics.
   */
  @Nullable
  private final Long maxTime;

  @JsonCreator
  public ProjectionMetadata(
      @JsonProperty("numRows") int numRows,
      @JsonProperty("schema") ProjectionSchema schema,
      @JsonProperty("minTime") @Nullable Long minTime,
      @JsonProperty("maxTime") @Nullable Long maxTime
  )
  {
    this.numRows = numRows;
    this.schema = SCHEMA_INTERNER.intern(schema);
    this.minTime = minTime;
    this.maxTime = maxTime;
  }

  public ProjectionMetadata(int numRows, ProjectionSchema schema)
  {
    this(numRows, schema, null, null);
  }

  @JsonProperty
  public int getNumRows()
  {
    return numRows;
  }

  @JsonProperty
  public ProjectionSchema getSchema()
  {
    return schema;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public Long getMinTime()
  {
    return minTime;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public Long getMaxTime()
  {
    return maxTime;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProjectionMetadata that = (ProjectionMetadata) o;
    return numRows == that.numRows
           && Objects.equals(schema, that.schema)
           && Objects.equals(minTime, that.minTime)
           && Objects.equals(maxTime, that.maxTime);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(numRows, schema, minTime, maxTime);
  }

  @Override
  public String toString()
  {
    return "ProjectionMetadata{" +
           "numRows=" + numRows +
           ", schema=" + schema +
           ", minTime=" + minTime +
           ", maxTime=" + maxTime +
           '}';
  }
}
