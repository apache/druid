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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.projections.BaseTableProjectionSchema;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Spec describing the shape of the 'base' table schema for a {@link org.apache.druid.segment.indexing.DataSchema}. This
 * is the foundation of the segments schema, on top of which optional {@link AggregateProjectionSpec} can be defined in
 * order to materialize pre-aggregated view tables of this schema in the segment.
 * <p>
 * Operator facing counterpart of the internal segment metadata-side {@link BaseTableProjectionSchema} hierarchy.
 * <p>
 * A base-table spec captures only schema-shape that will be used when creating segments: virtual columns, dimensions,
 * metrics, and segment ordering.
 * <p>
 * Note: {@link AdaptedBaseTableProjectionSpec} is intentionally not listed in {@link JsonSubTypes}. It exists only as
 * an internal adapter for legacy DataSchemas whose top-level v9-era fields are still the source of truth.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(
        name = ClusteredValueGroupsBaseTableProjectionSpec.TYPE_NAME,
        value = ClusteredValueGroupsBaseTableProjectionSpec.class
    )
})
public interface BaseTableProjectionSpec
{
  VirtualColumns getVirtualColumns();

  DimensionsSpec getDimensionsSpec();

  @Nullable
  AggregatorFactory[] getMetrics();

  List<OrderBy> getOrdering();

  /**
   * Returns the query granularity this spec represents. By default this is read from the
   * {@link Granularities#GRANULARITY_VIRTUAL_COLUMN_NAME} virtual column in {@link #getVirtualColumns()} (absent or
   * undecodable means {@link Granularities#NONE}); implementations that carry query granularity elsewhere override this.
   */
  default Granularity getQueryGranularity()
  {
    final VirtualColumn granularityVirtualColumn =
        getVirtualColumns().getVirtualColumn(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
    if (granularityVirtualColumn == null) {
      return Granularities.NONE;
    }
    final Granularity granularity = Granularities.fromVirtualColumn(granularityVirtualColumn);
    return granularity == null ? Granularities.NONE : granularity;
  }

  /**
   * Returns a copy of this spec with the given query granularity applied (implementation-defined representation). Used
   * by the ingestion and compaction config paths to attach the query-derived granularity to the operator-supplied spec.
   */
  BaseTableProjectionSpec withQueryGranularity(@Nullable Granularity queryGranularity);

  /**
   * Returns true if this spec is equivalent to {@code other} for the purpose of deciding whether a segment is already
   * compacted. Segment granularity, query granularity, and rollup are each compared by their own compaction check
   * (query granularity in particular lives in {@link #getVirtualColumns()} as a granularity-carrier virtual column), so
   * an implementation must ignore those and compare all other state it carries, returning false for a different
   * implementation type.
   * <p>
   * The default compares the whole spec via {@link #equals}; an implementation that carries state compared separately
   * (such as the query-granularity carrier) overrides this to exclude it.
   */
  default boolean hasEqualCompactionState(BaseTableProjectionSpec other)
  {
    return equals(other);
  }
}
