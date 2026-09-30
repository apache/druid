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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Sets;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * {@link BaseTableProjectionSpec} for a plain (non-clustered, non-rollup) table: the operator declares a single
 * ordered {@link #columns} list, the full set of columns in segment order, and rows are stored and sorted by every
 * column in declared order. The time position is an explicit positional entry in {@link #columns} named
 * {@code __time}. A plain base table is never rollup and has no metric columns.
 * <p>
 * Operator-facing counterpart of the segment metadata-side {@link org.apache.druid.segment.projections.TableProjectionSchema}.
 * <p>
 * {@link #getDimensionsSpec()} returns the unified spec built from {@link #columns} in declared order with
 * {@code forceSegmentSortByTime=false}; {@link #getOrdering()} is computed as every column of {@link #columns}
 * ascending, in list order.
 * <p>
 * Query granularity, when wanted, is a virtual column in {@link #getVirtualColumns()} named
 * {@link Granularities#GRANULARITY_VIRTUAL_COLUMN_NAME}. It is a granularity <em>carrier</em>: it supplies the
 * granularity that floors the stored {@code __time} column, and is NOT itself a stored column, so it never appears in
 * {@link #columns} (declare {@code __time} there as the time column). Absent that virtual column the query granularity
 * is {@code NONE}.
 * <p>
 * The granularity carrier is the only virtual column a plain base table accepts. The standard segment-generation path
 * this spec lowers into does not evaluate spec virtual columns (only the clustered write path materializes them), so a
 * materialized virtual column here would be silently dropped; computed columns belong in a
 * {@code transformSpec} instead. This can be relaxed if the plain write path learns to materialize virtual columns.
 */
@JsonTypeName(TableProjectionSpec.TYPE_NAME)
public final class TableProjectionSpec implements BaseTableProjectionSpec
{
  public static final String TYPE_NAME = "table";

  private final VirtualColumns virtualColumns;
  private final List<DimensionSchema> columns;
  private final DimensionsSpec dimensionsSpec;
  private final List<OrderBy> ordering;

  public static Builder builder()
  {
    return new Builder();
  }

  @JsonCreator
  public TableProjectionSpec(
      @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns,
      @JsonProperty("columns") List<DimensionSchema> columns
  )
  {
    validate(columns);
    this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
    validateVirtualColumns(this.virtualColumns);
    this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
    this.dimensionsSpec = DimensionsSpec.builder()
                                        .setDimensions(this.columns)
                                        .setForceSegmentSortByTime(false)
                                        .build();
    this.ordering = computeOrdering(this.columns);
  }

  @Override
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  /**
   * The full, ordered list of columns in segment order, with the explicit {@code __time} (or query-granularity)
   * marker at its declared position.
   */
  @JsonProperty("columns")
  public List<DimensionSchema> getColumns()
  {
    return columns;
  }

  @Override
  public AggregatorFactory[] getMetrics()
  {
    return new AggregatorFactory[0];
  }

  @Override
  @JsonIgnore
  public List<OrderBy> getOrdering()
  {
    return ordering;
  }

  @Override
  @JsonIgnore
  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  /**
   * Returns a copy of this spec with a new {@code queryGranularity}, expressed as a
   * {@link Granularities#GRANULARITY_VIRTUAL_COLUMN_NAME} virtual column added to {@link #getVirtualColumns()}. A
   * {@code null} or {@code NONE} granularity is a no-op (no flooring), so this returns {@code this} unchanged.
   * <p>
   * {@code ALL} is rejected: it has no granularity virtual column representation, so accepting it would silently
   * degrade to {@code NONE} when the spec is read back.
   * <p>
   * Idempotent: if the spec already declares a query-granularity virtual column, that one is authoritative and this is
   * a no-op. (The compaction path attaches the virtual column up front; the MSQ generation path then calls this again
   * with the query-derived granularity, which must not double-add.)
   */
  @Override
  public TableProjectionSpec withQueryGranularity(@Nullable Granularity queryGranularity)
  {
    if (Granularities.ALL.equals(queryGranularity)) {
      throw InvalidInput.exception(
          "Query granularity[ALL] is not supported for [%s] base tables",
          TYPE_NAME
      );
    }
    if (queryGranularity == null
        || Granularities.NONE.equals(queryGranularity)
        || virtualColumns.getVirtualColumn(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME) != null) {
      return this;
    }
    BaseTableProjectionSpec.validateQueryGranularity(queryGranularity, TYPE_NAME);
    final VirtualColumn granularityVirtualColumn =
        Granularities.toVirtualColumn(queryGranularity, Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
    final List<VirtualColumn> merged = new ArrayList<>(Arrays.asList(virtualColumns.getVirtualColumns()));
    merged.add(granularityVirtualColumn);
    return new TableProjectionSpec(VirtualColumns.create(merged), columns);
  }

  /**
   * Compares table-spec state for compaction up-to-date checks: query granularity is compared separately (via its
   * carrier virtual column), so it is stripped from both sides; everything else (the columns, and any other virtual
   * columns) must match. A spec of a different type is never equivalent.
   */
  @Override
  public boolean hasEqualCompactionState(BaseTableProjectionSpec other)
  {
    if (!(other instanceof TableProjectionSpec)) {
      return false;
    }
    return withoutQueryGranularity().equals(((TableProjectionSpec) other).withoutQueryGranularity());
  }

  @Override
  public TableProjectionSpec withAdditionalColumns(@Nullable List<DimensionSchema> additionalColumns)
  {
    if (additionalColumns == null || additionalColumns.isEmpty()) {
      return this;
    }
    final List<DimensionSchema> revised = new ArrayList<>(columns.size() + additionalColumns.size());
    revised.addAll(columns);
    for (DimensionSchema additionalColumn : additionalColumns) {
      if (ColumnHolder.TIME_COLUMN_NAME.equals(additionalColumn.getName())) {
        throw InvalidInput.exception(
            "Cannot append column [%s] to a [%s] base table; it must be declared at its position in the column list",
            ColumnHolder.TIME_COLUMN_NAME,
            TYPE_NAME
        );
      }
      revised.add(additionalColumn);
    }
    // Duplicates of a declared column, and a column named for the query-granularity carrier, are rejected by the
    // constructor's validation.
    return new TableProjectionSpec(virtualColumns, revised);
  }

  /**
   * Returns a copy of this spec with the {@link Granularities#GRANULARITY_VIRTUAL_COLUMN_NAME} virtual column removed,
   * the inverse of {@link #withQueryGranularity(Granularity)}. If no such virtual column is present this returns
   * {@code this} unchanged. Used to compare schema independently of query granularity in {@link #hasEqualCompactionState}.
   */
  private TableProjectionSpec withoutQueryGranularity()
  {
    if (virtualColumns.getVirtualColumn(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME) == null) {
      return this;
    }
    final List<VirtualColumn> remaining = new ArrayList<>();
    for (VirtualColumn vc : virtualColumns.getVirtualColumns()) {
      if (!Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME.equals(vc.getOutputName())) {
        remaining.add(vc);
      }
    }
    return new TableProjectionSpec(VirtualColumns.create(remaining), columns);
  }

  private static void validate(List<DimensionSchema> columns)
  {
    if (columns == null || columns.isEmpty()) {
      throw InvalidInput.exception("columns must be non-empty for [%s] base table", TYPE_NAME);
    }
    final Set<String> seen = Sets.newHashSetWithExpectedSize(columns.size());
    for (DimensionSchema d : columns) {
      if (!seen.add(d.getName())) {
        throw InvalidInput.exception("columns contains duplicate name [%s]", d.getName());
      }
    }
    boolean foundTime = false;
    for (DimensionSchema column : columns) {
      final String name = column.getName();
      // The query-granularity virtual column is a granularity carrier in virtualColumns (it floors the stored __time
      // column); it is not itself a stored column, so it must not be declared in 'columns'.
      if (Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME.equals(name)) {
        throw InvalidInput.exception(
            "[%s] is the query-granularity virtual column, not a stored column; declare it in 'virtualColumns' and use"
            + " [%s] as the time column in 'columns'",
            Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME,
            ColumnHolder.TIME_COLUMN_NAME
        );
      }
      if (ColumnHolder.TIME_COLUMN_NAME.equals(name)) {
        foundTime = true;
      }
    }
    if (!foundTime) {
      throw InvalidInput.exception(
          "[%s] base table must include [%s] in 'columns' to define the time position",
          TYPE_NAME,
          ColumnHolder.TIME_COLUMN_NAME
      );
    }
  }

  /**
   * A plain base table accepts only the {@link Granularities#GRANULARITY_VIRTUAL_COLUMN_NAME} carrier: the standard
   * segment-generation path does not evaluate spec virtual columns, so any other virtual column would be dead metadata
   * whose column never gets materialized. Computed columns belong in a {@code transformSpec}.
   */
  private static void validateVirtualColumns(VirtualColumns virtualColumns)
  {
    for (VirtualColumn virtualColumn : virtualColumns.getVirtualColumns()) {
      if (!Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME.equals(virtualColumn.getOutputName())) {
        throw InvalidInput.exception(
            "virtual column [%s] is not supported: a [%s] base table stores only declared columns and does not"
            + " materialize virtual columns; use a transformSpec to compute columns at ingest, or the [%s] virtual"
            + " column to carry query granularity",
            virtualColumn.getOutputName(),
            TYPE_NAME,
            Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
        );
      }
      BaseTableProjectionSpec.validateGranularity(virtualColumn, TYPE_NAME);
    }
  }

  private static List<OrderBy> computeOrdering(List<DimensionSchema> columns)
  {
    final List<OrderBy> ordering = new ArrayList<>(columns.size());
    for (DimensionSchema d : columns) {
      ordering.add(OrderBy.ascending(d.getName()));
    }
    return Collections.unmodifiableList(ordering);
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
    TableProjectionSpec that = (TableProjectionSpec) o;
    return Objects.equals(virtualColumns, that.virtualColumns)
           && Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(virtualColumns, columns);
  }

  @Override
  public String toString()
  {
    return "TableProjectionSpec{" +
           "virtualColumns=" + virtualColumns +
           ", columns=" + columns +
           '}';
  }

  /**
   * Fluent builder for {@link TableProjectionSpec}, avoiding the constructor's positional nullable leading
   * {@code virtualColumns} argument. {@link #columns} (the full ordered column list) is required.
   */
  public static final class Builder
  {
    @Nullable
    private VirtualColumns virtualColumns;
    private List<DimensionSchema> columns = Collections.emptyList();

    public Builder virtualColumns(@Nullable VirtualColumns virtualColumns)
    {
      this.virtualColumns = virtualColumns;
      return this;
    }

    /**
     * The full, ordered list of columns in segment order, including the explicit time marker.
     */
    public Builder columns(List<DimensionSchema> columns)
    {
      this.columns = columns;
      return this;
    }

    public Builder columns(DimensionSchema... columns)
    {
      return columns(Arrays.asList(columns));
    }

    public TableProjectionSpec build()
    {
      return new TableProjectionSpec(virtualColumns, columns);
    }
  }
}
