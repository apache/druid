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
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
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
 * {@link BaseTableProjectionSpec} for the clustered-value-groups base table mode: rows are partitioned into per-tuple
 * "cluster groups" keyed by one or more typed clustering columns, optionally derived from {@link #virtualColumns}.
 * Essentially, each group is stored as its own internal sub-segment..
 * <p>
 * The operator declares (1) the typed clustering columns (as ordinary {@link DimensionSchema}s, they're real
 * dimensions, just stored differently), (2) optional virtual columns, and (3) the non-clustering
 * dimensions and metrics. {@link #getDimensionsSpec()} returns the unified spec in canonical order
 * (clustering dims first, then non-clustering); {@link #getOrdering()} is either the declared ordering or, by
 * default, all clustering dims ascending followed by {@code __time} ascending.
 * <p>
 * A clustered base table is never rollup. Query granularity, when wanted, is just another entry in
 * {@link #getVirtualColumns()} named
 * {@link org.apache.druid.java.util.common.granularity.Granularities#GRANULARITY_VIRTUAL_COLUMN_NAME}; absent that
 * virtual column the query granularity is {@code NONE}. Segment granularity and intervals live on the top-level
 * {@link org.apache.druid.indexer.granularity.SegmentGranularitySpec}, not here.
 */
@JsonTypeName(ClusteredValueGroupsBaseTableProjectionSpec.TYPE_NAME)
public final class ClusteredValueGroupsBaseTableProjectionSpec implements BaseTableProjectionSpec
{
  public static final String TYPE_NAME = "clusteredValueGroups";

  private final VirtualColumns virtualColumns;
  private final List<DimensionSchema> clusteringColumns;
  private final List<DimensionSchema> nonClusteringDimensions;
  private final AggregatorFactory[] metrics;
  private final DimensionsSpec dimensionsSpec;
  private final List<OrderBy> ordering;

  public static Builder builder()
  {
    return new Builder();
  }

  @JsonCreator
  public ClusteredValueGroupsBaseTableProjectionSpec(
      @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns,
      @JsonProperty("clusteringColumns") List<DimensionSchema> clusteringColumns,
      @JsonProperty("dimensions") @Nullable List<DimensionSchema> nonClusteringDimensions,
      @JsonProperty("metrics") @Nullable AggregatorFactory[] metrics,
      @JsonProperty("ordering") @Nullable List<OrderBy> declaredOrdering
  )
  {
    if (clusteringColumns == null || clusteringColumns.isEmpty()) {
      throw InvalidInput.exception("clusteringColumns must be non-empty for clusteredValueGroups base table");
    }
    this.clusteringColumns = Collections.unmodifiableList(new ArrayList<>(clusteringColumns));
    this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
    this.nonClusteringDimensions = nonClusteringDimensions == null
                                   ? Collections.emptyList()
                                   : Collections.unmodifiableList(new ArrayList<>(nonClusteringDimensions));
    this.metrics = metrics == null ? new AggregatorFactory[0] : metrics;
    validateNoOverlap(this.clusteringColumns, this.nonClusteringDimensions);
    this.dimensionsSpec = computeDimensionsSpec(this.clusteringColumns, this.nonClusteringDimensions);
    this.ordering = declaredOrdering != null
                    ? Collections.unmodifiableList(new ArrayList<>(declaredOrdering))
                    : computeDefaultOrdering(this.clusteringColumns);
  }

  @Override
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  public List<DimensionSchema> getClusteringColumns()
  {
    return clusteringColumns;
  }

  @JsonProperty("dimensions")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<DimensionSchema> getNonClusteringDimensions()
  {
    return nonClusteringDimensions;
  }

  @Override
  @JsonProperty("metrics")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public AggregatorFactory[] getMetrics()
  {
    return metrics;
  }

  @Override
  @JsonProperty
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

  private static void validateNoOverlap(
      List<DimensionSchema> clustering,
      List<DimensionSchema> nonClustering
  )
  {
    final Set<String> clusteringNames = Sets.newHashSetWithExpectedSize(clustering.size());
    for (DimensionSchema d : clustering) {
      if (!clusteringNames.add(d.getName())) {
        throw InvalidInput.exception("clusteringColumns contains duplicate name [%s]", d.getName());
      }
    }
    for (DimensionSchema d : nonClustering) {
      if (clusteringNames.contains(d.getName())) {
        throw InvalidInput.exception(
            "dimension [%s] appears in both clusteringColumns and non-clustering dimensions",
            d.getName()
        );
      }
    }
  }

  private static DimensionsSpec computeDimensionsSpec(
      List<DimensionSchema> clustering,
      List<DimensionSchema> nonClustering
  )
  {
    final List<DimensionSchema> all = new ArrayList<>(clustering.size() + nonClustering.size());
    all.addAll(clustering);
    all.addAll(nonClustering);
    return DimensionsSpec.builder().setDimensions(all).build();
  }

  private static List<OrderBy> computeDefaultOrdering(List<DimensionSchema> clustering)
  {
    final List<OrderBy> ordering = new ArrayList<>(clustering.size() + 1);
    for (DimensionSchema d : clustering) {
      ordering.add(OrderBy.ascending(d.getName()));
    }
    ordering.add(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME));
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
    return Objects.equals(clusteringColumns, that.clusteringColumns)
           && Objects.equals(virtualColumns, that.virtualColumns)
           && Objects.equals(nonClusteringDimensions, that.nonClusteringDimensions)
           && Arrays.equals(metrics, that.metrics)
           && Objects.equals(ordering, that.ordering);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        clusteringColumns,
        virtualColumns,
        nonClusteringDimensions,
        Arrays.hashCode(metrics),
        ordering
    );
  }

  @Override
  public String toString()
  {
    return "ClusteredValueGroupsBaseTableProjectionSpec{" +
           "clusteringColumns=" + clusteringColumns +
           ", virtualColumns=" + virtualColumns +
           ", nonClusteringDimensions=" + nonClusteringDimensions +
           ", metrics=" + Arrays.toString(metrics) +
           ", ordering=" + ordering +
           '}';
  }

  /**
   * Fluent builder for {@link ClusteredValueGroupsBaseTableProjectionSpec}, avoiding the constructor's positional
   * nullable arguments (notably the leading {@code virtualColumns}). Only {@link #clusteringColumns} is required; the
   * rest default to empty/none.
   */
  public static final class Builder
  {
    @Nullable
    private VirtualColumns virtualColumns;
    private List<DimensionSchema> clusteringColumns = Collections.emptyList();
    @Nullable
    private List<DimensionSchema> nonClusteringDimensions;
    @Nullable
    private AggregatorFactory[] metrics;
    @Nullable
    private List<OrderBy> ordering;

    public Builder virtualColumns(@Nullable VirtualColumns virtualColumns)
    {
      this.virtualColumns = virtualColumns;
      return this;
    }

    public Builder clusteringColumns(List<DimensionSchema> clusteringColumns)
    {
      this.clusteringColumns = clusteringColumns;
      return this;
    }

    public Builder clusteringColumns(DimensionSchema... clusteringColumns)
    {
      return clusteringColumns(Arrays.asList(clusteringColumns));
    }

    /**
     * Non-clustering dimensions (clustering columns are declared separately via {@link #clusteringColumns}).
     */
    public Builder dimensions(@Nullable List<DimensionSchema> nonClusteringDimensions)
    {
      this.nonClusteringDimensions = nonClusteringDimensions;
      return this;
    }

    public Builder dimensions(DimensionSchema... nonClusteringDimensions)
    {
      return dimensions(Arrays.asList(nonClusteringDimensions));
    }

    public Builder metrics(@Nullable AggregatorFactory... metrics)
    {
      this.metrics = metrics;
      return this;
    }

    public Builder ordering(@Nullable List<OrderBy> ordering)
    {
      this.ordering = ordering;
      return this;
    }

    public ClusteredValueGroupsBaseTableProjectionSpec build()
    {
      return new ClusteredValueGroupsBaseTableProjectionSpec(
          virtualColumns,
          clusteringColumns,
          nonClusteringDimensions,
          metrics,
          ordering
      );
    }
  }
}
