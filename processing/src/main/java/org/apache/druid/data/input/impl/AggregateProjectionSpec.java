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
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Lists;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.AggregateProjectionMetadata;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * API type to specify an aggregating projection on {@link org.apache.druid.segment.incremental.IncrementalIndexSchema}
 *
 * Decorated with {@link JsonTypeInfo} annotations as a future-proofing mechanism in the event we add other types of
 * projections and need to extract out a base interface from this class.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonTypeName(AggregateProjectionSpec.TYPE_NAME)
public class AggregateProjectionSpec
{
  public static final String TYPE_NAME = "aggregate";

  public static Builder builder()
  {
    return new Builder();
  }

  public static Builder builder(String name)
  {
    return new Builder().name(name);
  }

  public static Builder builder(AggregateProjectionSpec spec)
  {
    return new Builder().name(spec.getName())
                        .virtualColumns(spec.getVirtualColumns())
                        .filter(spec.getFilter())
                        .groupingColumns(spec.getGroupingColumns())
                        .aggregators(spec.getAggregators());
  }

  private final String name;
  @Nullable
  private final DimFilter filter;
  private final VirtualColumns virtualColumns;
  private final List<DimensionSchema> groupingColumns;
  private final AggregatorFactory[] aggregators;
  private final List<OrderBy> ordering;
  @Nullable
  private final String timeColumnName;

  @JsonCreator
  public AggregateProjectionSpec(
      @JsonProperty("name") String name,
      @JsonProperty("filter") @Nullable DimFilter filter,
      @JsonProperty("virtualColumns") @Nullable VirtualColumns virtualColumns,
      @JsonProperty("groupingColumns") @Nullable List<DimensionSchema> groupingColumns,
      @JsonProperty("aggregators") @Nullable AggregatorFactory[] aggregators
  )
  {
    if (name == null || name.isEmpty()) {
      throw InvalidInput.exception("projection name cannot be null or empty");
    }
    this.name = name;
    if (CollectionUtils.isNullOrEmpty(groupingColumns) && (aggregators == null || aggregators.length == 0)) {
      throw InvalidInput.exception(
          "projection[%s] groupingColumns and aggregators must not both be null or empty",
          name
      );
    }
    this.filter = filter;
    this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
    this.groupingColumns = groupingColumns == null ? Collections.emptyList() : groupingColumns;
    // in the future this should be expanded to support user specified ordering, but for now we compute it based on
    // the grouping columns, which is consistent with how rollup ordering works for incremental index base table
    final ProjectionOrdering ordering = computeOrdering(this.virtualColumns, this.groupingColumns);
    this.ordering = ordering.ordering;
    this.timeColumnName = ordering.timeColumnName;
    this.aggregators = aggregators == null ? new AggregatorFactory[0] : aggregators;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public DimFilter getFilter()
  {
    return filter;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public List<DimensionSchema> getGroupingColumns()
  {
    return groupingColumns;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public AggregatorFactory[] getAggregators()
  {
    return aggregators;
  }

  @JsonProperty
  public List<OrderBy> getOrdering()
  {
    return ordering;
  }

  @JsonIgnore
  public AggregateProjectionMetadata.Schema toMetadataSchema()
  {
    return new AggregateProjectionMetadata.Schema(
        name,
        timeColumnName,
        filter,
        virtualColumns,
        groupingColumns.stream().map(DimensionSchema::getName).collect(Collectors.toList()),
        aggregators,
        ordering
    );
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
    AggregateProjectionSpec that = (AggregateProjectionSpec) o;
    return Objects.equals(name, that.name)
           && Objects.equals(filter, that.filter)
           && Objects.equals(groupingColumns, that.groupingColumns)
           && Objects.equals(virtualColumns, that.virtualColumns)
           && Objects.deepEquals(aggregators, that.aggregators)
           && Objects.equals(ordering, that.ordering);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, filter, virtualColumns, groupingColumns, Arrays.hashCode(aggregators), ordering);
  }

  @Override
  public String toString()
  {
    return "AggregateProjectionSpec{" +
           "name='" + name + '\'' +
           ", filter=" + filter +
           ", virtualColumns=" + virtualColumns +
           ", groupingColumns=" + groupingColumns +
           ", aggregators=" + Arrays.toString(aggregators) +
           ", ordering=" + ordering +
           '}';
  }

  private static ProjectionOrdering computeOrdering(VirtualColumns virtualColumns, List<DimensionSchema> groupingColumns)
  {
    if (groupingColumns.isEmpty()) {
      // no ordering since there is only 1 row for this projection
      return new ProjectionOrdering(List.of(), null);
    }
    final List<OrderBy> ordering = Lists.newArrayListWithCapacity(groupingColumns.size());

    String timeColumnName = null;
    Granularity granularity = null;
    // try to find the __time column equivalent, which might be a time_floor expression to model granularity
    // bucketing. The time column is decided as the finest granularity on __time detected. If the projection does
    // not have a time-like column, the granularity will be handled as ALL for the projection and all projection
    // rows will use a synthetic timestamp of the minimum timestamp of the incremental index
    for (final DimensionSchema dimension : groupingColumns) {
      ordering.add(OrderBy.ascending(dimension.getName()));
      if (ColumnHolder.TIME_COLUMN_NAME.equals(dimension.getName())) {
        timeColumnName = dimension.getName();
        granularity = Granularities.NONE;
      } else {
        final VirtualColumn vc = virtualColumns.getVirtualColumn(dimension.getName());
        final Granularity maybeGranularity = Granularities.fromVirtualColumn(vc);
        if (granularity == null && maybeGranularity != null) {
          granularity = maybeGranularity;
          timeColumnName = dimension.getName();
        } else if (granularity != null && maybeGranularity != null && maybeGranularity.isFinerThan(granularity)) {
          granularity = maybeGranularity;
          timeColumnName = dimension.getName();
        }
      }
    }
    return new ProjectionOrdering(ordering, timeColumnName);
  }

  private static final class ProjectionOrdering
  {
    private final List<OrderBy> ordering;
    @Nullable
    private final String timeColumnName;

    private ProjectionOrdering(List<OrderBy> ordering, @Nullable String timeColumnName)
    {
      this.ordering = ordering;
      this.timeColumnName = timeColumnName;
    }
  }

  public static final class Builder
  {
    private String name;
    private DimFilter filter;
    private VirtualColumns virtualColumns = VirtualColumns.EMPTY;
    private List<DimensionSchema> groupingColumns;
    private AggregatorFactory[] aggregators;

    public Builder name(String name)
    {
      this.name = name;
      return this;
    }

    public Builder filter(@Nullable DimFilter filter)
    {
      this.filter = filter;
      return this;
    }

    public Builder virtualColumns(@Nullable VirtualColumns virtualColumns)
    {
      this.virtualColumns = virtualColumns;
      return this;
    }

    public Builder virtualColumns(VirtualColumn... virtualColumns)
    {
      this.virtualColumns = VirtualColumns.create(virtualColumns);
      return this;
    }

    public Builder groupingColumns(@Nullable List<DimensionSchema> groupingColumns)
    {
      this.groupingColumns = groupingColumns;
      return this;
    }

    public Builder groupingColumns(DimensionSchema... groupingColumns)
    {
      this.groupingColumns = Arrays.asList(groupingColumns);
      return this;
    }

    public Builder aggregators(@Nullable AggregatorFactory... aggregators)
    {
      this.aggregators = aggregators;
      return this;
    }

    public AggregateProjectionSpec build()
    {
      return new AggregateProjectionSpec(
          name,
          filter,
          virtualColumns,
          groupingColumns,
          aggregators
      );
    }
  }
}
