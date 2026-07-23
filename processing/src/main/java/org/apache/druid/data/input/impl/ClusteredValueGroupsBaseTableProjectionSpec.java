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
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * {@link BaseTableProjectionSpec} for the clustered-value-groups base table mode: rows are partitioned into per-tuple
 * "cluster groups" keyed by one or more typed clustering columns, optionally derived from {@link #virtualColumns}.
 * Essentially, each group is stored as its own internal sub-segment. Logically, a clustered base table spec is a
 * projection of an (imaginary/unstored) base table into the stored clustered table.
 * <p>
 * The operator declares a single ordered {@link #columns} list, the full set of columns in segment order, plus a
 * {@link #clusteringColumns} list of NAMES designating the leading prefix of {@link #columns} that rows are
 * clustered by. The time position is an explicit positional entry in {@link #columns} named {@code __time}; clustering
 * by the time column is not yet supported, so {@code __time} must be a non-clustering column. A clustered base table
 * is never rollup and has no metric columns.
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
 * {@link org.apache.druid.indexer.granularity.SegmentGranularitySpec}, not here.
 */
@JsonTypeName(ClusteredValueGroupsBaseTableProjectionSpec.TYPE_NAME)
public final class ClusteredValueGroupsBaseTableProjectionSpec implements BaseTableProjectionSpec
{
  public static final String TYPE_NAME = "clusteredValueGroups";

  private final VirtualColumns virtualColumns;
  private final List<DimensionSchema> columns;
  private final List<String> clusteringColumns;
  private final List<DimensionSchema> clusteringColumnSchemas;
  private final List<DimensionSchema> nonClusteringColumnSchemas;
  private final DimensionsSpec dimensionsSpec;
  private final List<OrderBy> ordering;

  public static Builder builder()
  {
    return new Builder();
  }

  @JsonCreator
  public ClusteredValueGroupsBaseTableProjectionSpec(
      @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns,
      @JsonProperty("columns") List<DimensionSchema> columns,
      @JsonProperty("clusteringColumns") List<String> clusteringColumns
  )
  {
    validate(columns, clusteringColumns);
    this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
    validateVirtualColumns(this.virtualColumns, columns);
    this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
    this.clusteringColumns = Collections.unmodifiableList(new ArrayList<>(clusteringColumns));
    this.clusteringColumnSchemas = this.columns.subList(0, this.clusteringColumns.size());
    this.nonClusteringColumnSchemas = this.columns.subList(this.clusteringColumns.size(), this.columns.size());
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
   * The full, ordered list of columns in segment order; the clustering prefix followed by the remaining
   * (non-clustering) columns, with the explicit {@code __time} (or query-granularity) marker at its declared
   * position.
   */
  @JsonProperty("columns")
  public List<DimensionSchema> getColumns()
  {
    return columns;
  }

  /**
   * The names of the leading prefix of {@link #getColumns()} that rows are clustered by.
   */
  @JsonProperty("clusteringColumns")
  public List<String> getClusteringColumnNames()
  {
    return clusteringColumns;
  }

  /**
   * The clustering columns as {@link DimensionSchema}s; the leading prefix of {@link #getColumns()}.
   */
  @JsonIgnore
  public List<DimensionSchema> getClusteringColumns()
  {
    return clusteringColumnSchemas;
  }

  /**
   * The non-clustering columns as {@link DimensionSchema}s; the suffix of {@link #getColumns()} after the
   * clustering prefix. Includes the explicit time marker.
   */
  @JsonIgnore
  public List<DimensionSchema> getNonClusteringColumns()
  {
    return nonClusteringColumnSchemas;
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
   * {@code ALL} is rejected: it would floor {@code __time} to a single constant (the interval start) for the whole
   * segment, which clustered base tables do not yet support.
   * <p>
   * Idempotent: if the spec already declares a query-granularity virtual column, that one is authoritative and this is
   * a no-op. (The compaction path attaches the virtual column up front; the MSQ generation path then calls this again
   * with the query-derived granularity, which must not double-add.)
   */
  @Override
  public ClusteredValueGroupsBaseTableProjectionSpec withQueryGranularity(@Nullable Granularity queryGranularity)
  {
    if (Granularities.ALL.equals(queryGranularity)) {
      throw InvalidInput.exception(
          "Query granularity[ALL] is not supported for clusteredValueGroups base tables"
      );
    }
    if (queryGranularity == null
        || Granularities.NONE.equals(queryGranularity)
        || virtualColumns.getVirtualColumn(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME) != null) {
      return this;
    }
    final VirtualColumn granularityVirtualColumn =
        Granularities.toVirtualColumn(queryGranularity, Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
    final List<VirtualColumn> merged = new ArrayList<>(Arrays.asList(virtualColumns.getVirtualColumns()));
    merged.add(granularityVirtualColumn);
    return builder()
        .virtualColumns(VirtualColumns.create(merged))
        .clusteringColumns(clusteringColumns)
        .columns(columns)
        .build();
  }

  /**
   * Compares clustered-spec state for compaction up-to-date checks: query granularity is compared separately (via its
   * carrier virtual column), so it is stripped from both sides; everything else (columns, clustering columns, any other
   * virtual columns) must match. A spec of a different type is never equivalent.
   */
  @Override
  public boolean hasEqualCompactionState(BaseTableProjectionSpec other)
  {
    if (!(other instanceof ClusteredValueGroupsBaseTableProjectionSpec)) {
      return false;
    }
    return withoutQueryGranularity()
        .equals(((ClusteredValueGroupsBaseTableProjectionSpec) other).withoutQueryGranularity());
  }

  /**
   * Returns a copy of this spec with the {@link Granularities#GRANULARITY_VIRTUAL_COLUMN_NAME} virtual column removed,
   * the inverse of {@link #withQueryGranularity(Granularity)}. If no such virtual column is present this returns
   * {@code this} unchanged. Used to compare schema independently of query granularity in {@link #hasEqualCompactionState}.
   */
  private ClusteredValueGroupsBaseTableProjectionSpec withoutQueryGranularity()
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
    return builder()
        .virtualColumns(VirtualColumns.create(remaining))
        .clusteringColumns(clusteringColumns)
        .columns(columns)
        .build();
  }

  private static void validate(List<DimensionSchema> columns, List<String> clusteringColumns)
  {
    if (CollectionUtils.isNullOrEmpty(clusteringColumns)) {
      throw InvalidInput.exception("clusteringColumns must be non-empty for clusteredValueGroups base table");
    }
    if (CollectionUtils.isNullOrEmpty(columns)) {
      throw InvalidInput.exception("columns must be non-empty for clusteredValueGroups base table");
    }
    if (clusteringColumns.size() > columns.size()) {
      throw clusteringPrefixException(columns, clusteringColumns);
    }
    for (int i = 0; i < clusteringColumns.size(); i++) {
      final DimensionSchema clusteringColumn = columns.get(i);
      if (!clusteringColumn.getName().equals(clusteringColumns.get(i))) {
        throw clusteringPrefixException(columns, clusteringColumns);
      }
      // Clustering values are dictionary-encoded into per-type dictionaries on the write side, which supports only
      // these scalar types; reject anything else up front rather than failing later at ingest.
      if (!Projections.isAllowedClusteringType(clusteringColumn.getColumnType())) {
        throw InvalidInput.exception(
            "clustering column [%s] has unsupported type [%s]; clustering columns must be STRING, LONG, DOUBLE, or FLOAT",
            clusteringColumn.getName(),
            clusteringColumn.getColumnType()
        );
      }
    }

    final Set<String> seen = Sets.newHashSetWithExpectedSize(columns.size());
    for (DimensionSchema d : columns) {
      if (!seen.add(d.getName())) {
        throw InvalidInput.exception("columns contains duplicate name [%s]", d.getName());
      }
    }

    int timeIndex = -1;
    for (int i = 0; i < columns.size(); i++) {
      final String name = columns.get(i).getName();
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
        timeIndex = i;
      }
    }
    if (timeIndex < 0) {
      throw InvalidInput.exception(
          "clustered base table must include [%s] in 'columns' to define the time position",
          ColumnHolder.TIME_COLUMN_NAME
      );
    }
    if (timeIndex < clusteringColumns.size()) {
      throw InvalidInput.exception(
          "clustering by [%s] is not yet supported; the time column must be a non-clustering column",
          ColumnHolder.TIME_COLUMN_NAME
      );
    }
  }

  /**
   * Rules to keep virtual columns definitions reasonable:
   * <ul>
   *   <li><b>inputs</b>: every input of a virtual column must be a stored column (declared in {@code columns}) or
   *   another virtual column in the spec.</li>
   *   <li><b>outputs</b>: every virtual column must either be materialized (its output declared in {@code columns}) or
   *   be an intermediary that feeds another virtual column. A virtual column that is neither materializes nothing and
   *   is used by nothing and dead metadata and so it is rejected.</li>
   * </ul>
   * The query-granularity carrier ({@link Granularities#GRANULARITY_VIRTUAL_COLUMN_NAME}) is special handled to
   * capture how __time is computed, so it is exempt from the output rule.
   */
  private static void validateVirtualColumns(VirtualColumns virtualColumns, List<DimensionSchema> columns)
  {
    final VirtualColumn[] all = virtualColumns.getVirtualColumns();
    if (all.length == 0) {
      return;
    }
    final Set<String> columnNames = Sets.newHashSetWithExpectedSize(columns.size());
    for (DimensionSchema column : columns) {
      columnNames.add(column.getName());
    }
    // The output rule below lets a virtual column go unstored when it is exempt: an intermediary that feeds another
    // virtual column (collected during the input pass), or the metadata-only query-granularity carrier (seeded here).
    final Set<String> outputExempt = new HashSet<>();
    outputExempt.add(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
    // input rule: every input must be a stored column or another virtual column; an input that is a virtual column
    // makes that virtual column an intermediary, exempt from having to be materialized itself.
    for (VirtualColumn virtualColumn : all) {
      for (String input : virtualColumn.requiredColumns()) {
        final boolean isStored = columnNames.contains(input);
        final boolean isVirtual = virtualColumns.exists(input);
        if (!isStored && !isVirtual) {
          throw InvalidInput.exception(
              "virtual column [%s] reads column [%s], which is neither a stored column nor another virtual column;"
              + " clustered base table virtual columns must be computable from stored columns (retain [%s] in"
              + " 'columns', or use a transformSpec for in-place transforms)",
              virtualColumn.getOutputName(),
              input,
              input
          );
        }
        if (isVirtual) {
          outputExempt.add(input);
        }
      }
    }
    // output rule: every virtual column must be materialized (stored) or exempt (intermediary / granularity carrier).
    for (VirtualColumn virtualColumn : all) {
      final String outputName = virtualColumn.getOutputName();
      if (!columnNames.contains(outputName) && !outputExempt.contains(outputName)) {
        throw InvalidInput.exception(
            "virtual column [%s] is not stored (not declared in 'columns') and does not feed another virtual column;"
            + " clustered base table virtual columns must materialize a stored column or be an input to one that does",
            outputName
        );
      }
    }
  }

  private static DruidException clusteringPrefixException(
      List<DimensionSchema> columns,
      List<String> clusteringColumns
  )
  {
    final List<String> columnPrefix = new ArrayList<>(clusteringColumns.size());
    for (int i = 0; i < Math.min(clusteringColumns.size(), columns.size()); i++) {
      columnPrefix.add(columns.get(i).getName());
    }
    return InvalidInput.exception(
        "clusteringColumns must be the leading prefix of columns, in order; got %s vs columns prefix %s",
        clusteringColumns,
        columnPrefix
    );
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
    ClusteredValueGroupsBaseTableProjectionSpec that = (ClusteredValueGroupsBaseTableProjectionSpec) o;
    return Objects.equals(virtualColumns, that.virtualColumns)
           && Objects.equals(columns, that.columns)
           && Objects.equals(clusteringColumns, that.clusteringColumns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        virtualColumns,
        columns,
        clusteringColumns
    );
  }

  @Override
  public String toString()
  {
    return "ClusteredValueGroupsBaseTableProjectionSpec{" +
           "virtualColumns=" + virtualColumns +
           ", columns=" + columns +
           ", clusteringColumns=" + clusteringColumns +
           '}';
  }

  /**
   * Fluent builder for {@link ClusteredValueGroupsBaseTableProjectionSpec}, avoiding the constructor's positional
   * nullable arguments (notably the leading {@code virtualColumns}). Both {@link #columns} (the full ordered column
   * list) and {@link #clusteringColumns} (the leading prefix names) are required.
   */
  public static final class Builder
  {
    @Nullable
    private VirtualColumns virtualColumns;
    private List<DimensionSchema> columns = Collections.emptyList();
    private List<String> clusteringColumns = Collections.emptyList();

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

    /**
     * The names of the leading prefix of {@link #columns} that rows are clustered by.
     */
    public Builder clusteringColumns(List<String> clusteringColumns)
    {
      this.clusteringColumns = clusteringColumns;
      return this;
    }

    public Builder clusteringColumns(String... clusteringColumns)
    {
      return clusteringColumns(Arrays.asList(clusteringColumns));
    }

    public ClusteredValueGroupsBaseTableProjectionSpec build()
    {
      return new ClusteredValueGroupsBaseTableProjectionSpec(
          virtualColumns,
          columns,
          clusteringColumns
      );
    }
  }
}
