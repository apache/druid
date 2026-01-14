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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.OptBoolean;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.VirtualColumns;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Internal JSON representation of the contents of a projection for a segment {@link ProjectionMetadata} to embed within
 * segment files. The word 'schema' is used very loosely here, as the contents of this object are not complete, and
 * must be combined with additional metadata from the segment for most uses. While this object will contain things like
 * the list of column names and how they are ordered, type information is not available, and in practice comes from the
 * {@link org.apache.druid.segment.column.ColumnDescriptor} declarations which define how to read the column data.
 *
 * For V10 segment format {@link org.apache.druid.segment.file.SegmentFileMetadata}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", requireTypeIdForSubtypes = OptBoolean.FALSE)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = AggregateProjectionSpec.TYPE_NAME, value = AggregateProjectionSchema.class),
    @JsonSubTypes.Type(name = TableProjectionSchema.TYPE_NAME, value = TableProjectionSchema.class),
    @JsonSubTypes.Type(name = RollupTableProjectionSchema.TYPE_NAME, value = RollupTableProjectionSchema.class)
})
public interface ProjectionSchema
{
  String getName();

  List<String> getColumnNames();

  VirtualColumns getVirtualColumns();

  @Nullable
  String getTimeColumnName();

  /**
   * The position of the time column in the column list
   */
  int getTimeColumnPosition();

  /**
   * If {@link #getTimeColumnName()} references a column in {@link #getVirtualColumns()} which is a time_floor
   * expression, this method translates it into a {@link Granularity}.
   */
  Granularity getEffectiveGranularity();

  /**
   * Projection sort order
   */
  List<OrderBy> getOrdering();

  /**
   * Projection sort order with {@link #getTimeColumnName()} substituted with
   * {@link org.apache.druid.segment.column.ColumnHolder#TIME_COLUMN_NAME}
   */
  List<OrderBy> getOrderingWithTimeColumnSubstitution();
}
