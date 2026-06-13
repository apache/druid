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

import org.apache.druid.indexer.granularity.GranularitySpec;
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

/**
 * Internal adapter to make a {@link org.apache.druid.segment.indexing.DataSchema} that doesn't declare an explicit
 * {@code baseTable} into a {@link BaseTableProjectionSpec}. It is backed by the existing top-level schema fields so
 * that consumers can treat all DataSchemas uniformly as a {@link BaseTableProjectionSpec}.
 * <p>
 * Not registered in {@link com.fasterxml.jackson.annotation.JsonSubTypes} on {@link BaseTableProjectionSpec}, this
 * spec is not serialized for backwards compatibility.
 */
public final class AdaptedBaseTableProjectionSpec implements BaseTableProjectionSpec
{
  private final GranularitySpec granularitySpec;
  private final DimensionsSpec dimensionsSpec;
  private final AggregatorFactory[] metrics;
  private final List<OrderBy> ordering;

  public AdaptedBaseTableProjectionSpec(
      GranularitySpec granularitySpec,
      @Nullable DimensionsSpec dimensionsSpec,
      @Nullable AggregatorFactory[] metrics
  )
  {
    this.granularitySpec = Objects.requireNonNull(granularitySpec, "granularitySpec");
    this.dimensionsSpec = dimensionsSpec == null ? DimensionsSpec.EMPTY : dimensionsSpec;
    this.metrics = metrics == null ? new AggregatorFactory[0] : metrics;
    this.ordering = computeOrdering(this.dimensionsSpec);
  }

  /**
   * The wrapped legacy {@link GranularitySpec}, exposed only so {@link org.apache.druid.segment.indexing.DataSchema}
   * can read back the full legacy granularity (segment + query granularity + rollup) for a schema in legacy mode.
   */
  public GranularitySpec getGranularitySpec()
  {
    return granularitySpec;
  }

  @Override
  public VirtualColumns getVirtualColumns()
  {
    return VirtualColumns.EMPTY;
  }

  @Override
  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  @Override
  public AggregatorFactory[] getMetrics()
  {
    return metrics;
  }

  @Override
  public List<OrderBy> getOrdering()
  {
    return ordering;
  }

  /**
   * Mirrors the legacy IncrementalIndex segment-sort semantics: when {@link DimensionsSpec#isForceSegmentSortByTime()}
   * is true (the default), ordering is just {@code __time asc}; otherwise the segment is sorted by the declared
   * dimensions in declaration order.
   */
  private static List<OrderBy> computeOrdering(DimensionsSpec dimensionsSpec)
  {
    if (dimensionsSpec.isForceSegmentSortByTime()) {
      return List.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME));
    }
    final List<OrderBy> ordering = new ArrayList<>(dimensionsSpec.getDimensions().size());
    for (DimensionSchema d : dimensionsSpec.getDimensions()) {
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
    AdaptedBaseTableProjectionSpec that = (AdaptedBaseTableProjectionSpec) o;
    return Objects.equals(granularitySpec, that.granularitySpec)
           && Objects.equals(dimensionsSpec, that.dimensionsSpec)
           && Arrays.equals(metrics, that.metrics)
           && Objects.equals(ordering, that.ordering);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(granularitySpec, dimensionsSpec, Arrays.hashCode(metrics), ordering);
  }

  @Override
  public String toString()
  {
    return "AdaptedBaseTableProjectionSpec{" +
           "granularitySpec=" + granularitySpec +
           ", dimensionsSpec=" + dimensionsSpec +
           ", metrics=" + Arrays.toString(metrics) +
           ", ordering=" + ordering +
           '}';
  }
}
