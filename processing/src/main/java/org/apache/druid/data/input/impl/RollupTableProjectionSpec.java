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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * {@link BaseTableProjectionSpec} for a rollup table: the plain {@link TableProjectionSpec} shape plus
 * {@link #aggregators}. The declared {@link #groupingColumns} are the grouping columns, rows with identical grouping values
 * (after query-granularity flooring of {@code __time}) are aggregated into one at ingest time, with each aggregator
 * producing a metric column after the grouping columns. As in the plain layout, the time position is an explicit
 * positional entry in {@link #groupingColumns} named {@code __time}, declared column order is the segment storage and sort
 * order, and query granularity rides as the {@link Granularities#GRANULARITY_VIRTUAL_COLUMN_NAME} carrier virtual
 * column (the only virtual column accepted).
 * <p>
 * Operator-facing counterpart of the segment metadata-side
 * {@link org.apache.druid.segment.projections.RollupTableProjectionSchema}.
 * <p>
 * Aggregators may be empty: a rollup table without metrics collapses rows with identical grouping values, mirroring
 * the legacy {@code rollup=true} granularity spec without a {@code metricsSpec}.
 */
@JsonTypeName(RollupTableProjectionSpec.TYPE_NAME)
public final class RollupTableProjectionSpec implements BaseTableProjectionSpec
{
  public static final String TYPE_NAME = "rollupTable";

  private final VirtualColumns virtualColumns;
  private final List<DimensionSchema> groupingColumns;
  private final AggregatorFactory[] aggregators;
  private final DimensionsSpec dimensionsSpec;
  private final List<OrderBy> ordering;

  public static Builder builder()
  {
    return new Builder();
  }

  @JsonCreator
  public RollupTableProjectionSpec(
      @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns,
      @JsonProperty("groupingColumns") List<DimensionSchema> groupingColumns,
      @JsonProperty("aggregators") @Nullable AggregatorFactory[] aggregators
  )
  {
    BaseTableProjectionSpec.validateDeclaredColumns(groupingColumns, "groupingColumns", TYPE_NAME);
    this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
    BaseTableProjectionSpec.validateGranularityOnlyVirtualColumns(this.virtualColumns, TYPE_NAME);
    this.groupingColumns = Collections.unmodifiableList(new ArrayList<>(groupingColumns));
    this.aggregators = aggregators == null ? new AggregatorFactory[0] : aggregators;
    validateAggregators(this.groupingColumns, this.aggregators);
    this.dimensionsSpec = DimensionsSpec.builder()
                                        .setDimensions(this.groupingColumns)
                                        .setForceSegmentSortByTime(false)
                                        .build();
    this.ordering = BaseTableProjectionSpec.declaredOrderAscending(this.groupingColumns);
  }

  @Override
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  /**
   * The full, ordered list of grouping columns in segment order, with the explicit {@code __time} (or
   * query-granularity) marker at its declared position.
   */
  @JsonProperty("groupingColumns")
  public List<DimensionSchema> getGroupingColumns()
  {
    return groupingColumns;
  }

  /**
   * The aggregators computed over the rows collapsed into each grouping tuple, each producing a metric column stored
   * after the grouping columns.
   */
  @Override
  @JsonProperty("aggregators")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public AggregatorFactory[] getMetrics()
  {
    return aggregators;
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

  @Override
  public boolean isRollup()
  {
    return true;
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
  public RollupTableProjectionSpec withQueryGranularity(@Nullable Granularity queryGranularity)
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
    return new RollupTableProjectionSpec(VirtualColumns.create(merged), groupingColumns, aggregators);
  }

  /**
   * Compares rollup-spec state for compaction up-to-date checks: query granularity is compared separately (via its
   * carrier virtual column), so it is stripped from both sides; everything else (the grouping columns, the
   * aggregators, and any other virtual columns) must match. A spec of a different type is never equivalent.
   */
  @Override
  public boolean hasEqualCompactionState(BaseTableProjectionSpec other)
  {
    if (!(other instanceof RollupTableProjectionSpec)) {
      return false;
    }
    return withoutQueryGranularity().equals(((RollupTableProjectionSpec) other).withoutQueryGranularity());
  }

  @Override
  public RollupTableProjectionSpec withAdditionalColumns(@Nullable List<DimensionSchema> additionalColumns)
  {
    if (additionalColumns == null || additionalColumns.isEmpty()) {
      return this;
    }
    final List<DimensionSchema> revised = new ArrayList<>(groupingColumns.size() + additionalColumns.size());
    revised.addAll(groupingColumns);
    for (DimensionSchema additionalColumn : additionalColumns) {
      if (ColumnHolder.TIME_COLUMN_NAME.equals(additionalColumn.getName())) {
        throw InvalidInput.exception(
            "Cannot append column [%s] to a [%s] base table; it must be declared at its position in the grouping column list",
            ColumnHolder.TIME_COLUMN_NAME,
            TYPE_NAME
        );
      }
      revised.add(additionalColumn);
    }
    // Duplicates of a declared column or aggregator, and a column named for the query-granularity carrier, are
    // rejected by the constructor's validation.
    return new RollupTableProjectionSpec(virtualColumns, revised, aggregators);
  }

  /**
   * Returns a copy of this spec with the {@link Granularities#GRANULARITY_VIRTUAL_COLUMN_NAME} virtual column removed,
   * the inverse of {@link #withQueryGranularity(Granularity)}. If no such virtual column is present this returns
   * {@code this} unchanged. Used to compare schema independently of query granularity in {@link #hasEqualCompactionState}.
   */
  private RollupTableProjectionSpec withoutQueryGranularity()
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
    return new RollupTableProjectionSpec(VirtualColumns.create(remaining), groupingColumns, aggregators);
  }

  /**
   * Aggregators produce the metric columns, so their names share a namespace with the grouping columns — a collision
   * means one column with two definitions — and with the query-granularity carrier: virtual columns shadow stored
   * columns when selectors resolve, so a metric named for the carrier would be unreadable once a query granularity is
   * attached.
   */
  private static void validateAggregators(List<DimensionSchema> groupingColumns, AggregatorFactory[] aggregators)
  {
    final Set<String> names = new HashSet<>();
    for (DimensionSchema column : groupingColumns) {
      names.add(column.getName());
    }
    for (AggregatorFactory aggregator : aggregators) {
      if (aggregator == null) {
        throw InvalidInput.exception("aggregators must not contain null entries");
      }
      if (Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME.equals(aggregator.getName())) {
        throw InvalidInput.exception(
            "aggregator cannot be named [%s]; it is the query-granularity virtual column, not a metric column",
            Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
        );
      }
      if (!names.add(aggregator.getName())) {
        throw InvalidInput.exception(
            "aggregator [%s] duplicates the name of a column or another aggregator",
            aggregator.getName()
        );
      }
      // The spec's aggregators are applied uniformly by every consumer: ingestion combines whatever arrives under the
      // metric column's name, and compaction re-aggregates the rows the table has already stored. Both are only
      // correct for an aggregator that combines its own output, so anything else (a COUNT, a sketch build, an input
      // field that differs from the output) is rejected rather than silently changing results on re-aggregation.
      final AggregatorFactory combining = aggregator.getCombiningFactory().withName(aggregator.getName());
      if (!aggregator.equals(combining)) {
        throw InvalidInput.exception(
            "aggregator [%s] is not its own combining form: a rollup table re-aggregates the rows it has stored, so"
            + " its aggregators must combine their own output. Declare the combining form instead, for example [%s]",
            aggregator.getName(),
            combining
        );
      }
    }
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
    RollupTableProjectionSpec that = (RollupTableProjectionSpec) o;
    return Objects.equals(virtualColumns, that.virtualColumns)
           && Objects.equals(groupingColumns, that.groupingColumns)
           && Arrays.equals(aggregators, that.aggregators);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(virtualColumns, groupingColumns, Arrays.hashCode(aggregators));
  }

  @Override
  public String toString()
  {
    return "RollupTableProjectionSpec{" +
           "virtualColumns=" + virtualColumns +
           ", groupingColumns=" + groupingColumns +
           ", aggregators=" + Arrays.toString(aggregators) +
           '}';
  }

  /**
   * Fluent builder for {@link RollupTableProjectionSpec}, avoiding the constructor's positional nullable leading
   * {@code virtualColumns} argument. {@link #groupingColumns} (the full ordered grouping column list) is required.
   */
  public static final class Builder
  {
    @Nullable
    private VirtualColumns virtualColumns;
    private List<DimensionSchema> groupingColumns = Collections.emptyList();
    @Nullable
    private AggregatorFactory[] aggregators;

    public Builder virtualColumns(@Nullable VirtualColumns virtualColumns)
    {
      this.virtualColumns = virtualColumns;
      return this;
    }

    /**
     * The full, ordered list of grouping columns in segment order, including the explicit time marker.
     */
    public Builder groupingColumns(List<DimensionSchema> groupingColumns)
    {
      this.groupingColumns = groupingColumns;
      return this;
    }

    public Builder groupingColumns(DimensionSchema... groupingColumns)
    {
      return groupingColumns(Arrays.asList(groupingColumns));
    }

    public Builder aggregators(AggregatorFactory... aggregators)
    {
      this.aggregators = aggregators;
      return this;
    }

    public RollupTableProjectionSpec build()
    {
      return new RollupTableProjectionSpec(virtualColumns, groupingColumns, aggregators);
    }
  }
}
