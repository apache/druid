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
import com.google.common.collect.Sets;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.projections.BaseTableProjectionSchema;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

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
    @JsonSubTypes.Type(name = TableProjectionSpec.TYPE_NAME, value = TableProjectionSpec.class),
    @JsonSubTypes.Type(name = RollupTableProjectionSpec.TYPE_NAME, value = RollupTableProjectionSpec.class),
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
    final Granularity granularity = Granularities.fromTimeVirtualColumn(granularityVirtualColumn);
    return granularity == null ? Granularities.NONE : granularity;
  }

  /**
   * Returns true if rows with identical grouping values are aggregated into one at ingest time.
   */
  default boolean isRollup()
  {
    return false;
  }

  /**
   * Returns a copy of this spec with the given query granularity applied (implementation-defined representation). Used
   * by the ingestion and compaction config paths to attach the query-derived granularity to the operator-supplied spec.
   */
  BaseTableProjectionSpec withQueryGranularity(@Nullable Granularity queryGranularity);

  /**
   * Returns a copy of this spec with the given columns appended to those it already declares.
   */
  BaseTableProjectionSpec withAdditionalColumns(@Nullable List<DimensionSchema> additionalColumns);

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

  /**
   * Validates that {@code queryGranularity} can be used by a base-table spec: only period granularities in the UTC
   * time zone without an origin are currently allowed.
   */
  static void validateQueryGranularity(Granularity queryGranularity, String typeName)
  {
    if (!Granularities.isStandardUtcPeriod(queryGranularity)) {
      throw InvalidInput.exception(
          "Query granularity[%s] is not supported for [%s] base tables; only period granularities in the UTC time"
          + " zone without an origin are supported",
          queryGranularity,
          typeName
      );
    }
  }

  /**
   * Validates a {@link Granularities#GRANULARITY_VIRTUAL_COLUMN_NAME} supplied directly to a spec (catalog JSON, or
   * a translated DDL body): it must decode to a granularity {@link #validateQueryGranularity} accepts, so a
   * spec cannot be constructed claiming a granularity that reads back as something else.
   */
  static void validateGranularity(VirtualColumn granularityColumn, String typeName)
  {
    if (!Collections.singletonList(ColumnHolder.TIME_COLUMN_NAME).equals(granularityColumn.requiredColumns())) {
      throw InvalidInput.exception(
          "virtual column [%s] must be computed from [%s] alone, but reads %s",
          Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME,
          ColumnHolder.TIME_COLUMN_NAME,
          granularityColumn.requiredColumns()
      );
    }
    final Granularity granularity = Granularities.fromTimeVirtualColumn(granularityColumn);
    if (granularity == null || Granularities.NONE.equals(granularity) || Granularities.ALL.equals(granularity)) {
      throw InvalidInput.exception(
          "virtual column [%s] does not encode a query granularity",
          Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
      );
    }
    validateQueryGranularity(granularity, typeName);
  }

  /**
   * Validates a declared column list: non-empty, unique names, an explicit {@link ColumnHolder#TIME_COLUMN_NAME} entry
   * to define the time position, and no entry named for the query-granularity carrier (which is a virtual column, not
   * a stored column).
   */
  static void validateDeclaredColumns(
      @Nullable List<DimensionSchema> columns,
      String columnsProperty,
      String typeName
  )
  {
    if (columns == null || columns.isEmpty()) {
      throw InvalidInput.exception("'%s' must be non-empty for [%s] base table", columnsProperty, typeName);
    }
    final Set<String> seen = Sets.newHashSetWithExpectedSize(columns.size());
    for (DimensionSchema d : columns) {
      if (!seen.add(d.getName())) {
        throw InvalidInput.exception("'%s' contains duplicate name [%s]", columnsProperty, d.getName());
      }
    }
    boolean foundTime = false;
    for (DimensionSchema column : columns) {
      final String name = column.getName();
      // The query-granularity virtual column is a granularity carrier in virtualColumns (it floors the stored __time
      // column); it is not itself a stored column, so it must not be declared as one.
      if (Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME.equals(name)) {
        throw InvalidInput.exception(
            "[%s] is the query-granularity virtual column, not a stored column; declare it in 'virtualColumns' and use"
            + " [%s] as the time column in '%s'",
            Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME,
            ColumnHolder.TIME_COLUMN_NAME,
            columnsProperty
        );
      }
      if (ColumnHolder.TIME_COLUMN_NAME.equals(name)) {
        foundTime = true;
      }
    }
    if (!foundTime) {
      throw InvalidInput.exception(
          "[%s] base table must include [%s] in '%s' to define the time position",
          typeName,
          ColumnHolder.TIME_COLUMN_NAME,
          columnsProperty
      );
    }
  }

  /**
   * Validates that {@code virtualColumns} holds nothing but the {@link Granularities#GRANULARITY_VIRTUAL_COLUMN_NAME}
   * carrier, for the layouts whose write path does not evaluate spec virtual columns (any other virtual column would
   * be dead metadata whose column never gets materialized; computed columns belong in a {@code transformSpec}). The
   * carrier itself is validated by {@link #validateGranularity}.
   */
  static void validateGranularityOnlyVirtualColumns(VirtualColumns virtualColumns, String typeName)
  {
    for (VirtualColumn virtualColumn : virtualColumns.getVirtualColumns()) {
      if (!Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME.equals(virtualColumn.getOutputName())) {
        throw InvalidInput.exception(
            "virtual column [%s] is not supported: a [%s] base table stores only declared columns and does not"
            + " materialize virtual columns; use a transformSpec to compute columns at ingest, or the [%s] virtual"
            + " column to carry query granularity",
            virtualColumn.getOutputName(),
            typeName,
            Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
        );
      }
      validateGranularity(virtualColumn, typeName);
    }
  }

  /**
   * Convert a list of {@link DimensionSchema} into ascending {@link OrderBy}, preserving declared order.
   */
  static List<OrderBy> declaredOrderAscending(List<DimensionSchema> columns)
  {
    final List<OrderBy> ordering = new ArrayList<>(columns.size());
    for (DimensionSchema d : columns) {
      ordering.add(OrderBy.ascending(d.getName()));
    }
    return Collections.unmodifiableList(ordering);
  }
}
