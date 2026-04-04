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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.AggregateProjectionMetadata;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.column.ColumnHolder;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Druid table schema expressed as a {@link ProjectionSchema}, this can be thought of as the super-projection that
 * all other projections of the table are built from.
 *
 * @see TableProjectionSchema
 * @see RollupTableProjectionSchema
 */
public interface BaseTableProjectionSchema extends ProjectionSchema
{
  @JsonProperty
  @Override
  default String getName()
  {
    return Projections.BASE_TABLE_PROJECTION_NAME;
  }

  @JsonProperty
  @Nullable
  @Override
  default String getTimeColumnName()
  {
    return ColumnHolder.TIME_COLUMN_NAME;
  }

  @JsonIgnore
  @Override
  default List<OrderBy> getOrderingWithTimeColumnSubstitution()
  {
    return getOrdering();
  }

  /**
   * Get list of 'dimension' columns, excluding {@link ColumnHolder#TIME_COLUMN_NAME}
   */
  List<String> getDimensionNames();

  /**
   * Convert to v9 {@link Metadata} when supplied with an added list of projections
   */
  Metadata asMetadata(@Nullable List<AggregateProjectionMetadata> projections);
}
