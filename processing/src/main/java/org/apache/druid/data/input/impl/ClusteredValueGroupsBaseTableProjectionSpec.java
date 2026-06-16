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
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * {@link BaseTableProjectionSpec} for the clustered-value-groups base table mode: rows are partitioned into per-tuple
 * "cluster groups" keyed by one or more typed clustering columns, optionally derived from {@link #virtualColumns}.
 * Essentially, each group is stored as its own internal sub-segment..
 * <p>
 * The operator declares a single ordered {@link #columns} list — the full set of columns in segment order, plus a
 * {@link #clusteringColumns} list of NAMES designating the leading prefix of {@link #columns} that rows are
 * clustered by. The time position is an explicit positional entry in {@link #columns} (named {@code __time}, or the
 * query-granularity column {@link Granularities#GRANULARITY_VIRTUAL_COLUMN_NAME}); clustering by the time column is not
 * yet supported, so the time marker must be a non-clustering column. A clustered base table is never rollup and has
 * no metric columns.
 * <p>
 * {@link #getDimensionsSpec()} returns the unified spec built from {@link #columns} in declared order with
 * {@code forceSegmentSortByTime=false}; {@link #getOrdering()} is computed as every column of {@link #columns}
 * ascending, in list order.
 * <p>
 * Query granularity, when wanted, is just another entry in {@link #getVirtualColumns()} named
 * {@link Granularities#GRANULARITY_VIRTUAL_COLUMN_NAME}; absent that virtual column the query granularity is
 * {@code NONE}. Segment granularity and intervals live on the top-level
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
      if (!columns.get(i).getName().equals(clusteringColumns.get(i))) {
        throw clusteringPrefixException(columns, clusteringColumns);
      }
    }

    final Set<String> seen = Sets.newHashSetWithExpectedSize(columns.size());
    for (DimensionSchema d : columns) {
      if (!seen.add(d.getName())) {
        throw InvalidInput.exception("columns contains duplicate name [%s]", d.getName());
      }
    }

    int timeIndex = -1;
    boolean bothPresent = false;
    for (int i = 0; i < columns.size(); i++) {
      final String name = columns.get(i).getName();
      if (ColumnHolder.TIME_COLUMN_NAME.equals(name) || Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME.equals(name)) {
        if (timeIndex >= 0) {
          bothPresent = true;
        }
        timeIndex = i;
      }
    }
    if (timeIndex < 0) {
      throw InvalidInput.exception(
          "clustered base table must include %s (or the query-granularity column [%s]) in 'columns' to define the"
          + " time position",
          ColumnHolder.TIME_COLUMN_NAME,
          Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
      );
    }
    if (bothPresent) {
      throw InvalidInput.exception(
          "clustered base table must include exactly one of %s / %s in 'columns' to define the time position",
          ColumnHolder.TIME_COLUMN_NAME,
          Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
      );
    }
    if (timeIndex < clusteringColumns.size()) {
      throw InvalidInput.exception(
          "clustering by %s / %s is not yet supported; the time column must be a non-clustering column",
          ColumnHolder.TIME_COLUMN_NAME,
          Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
      );
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
