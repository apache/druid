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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.filter.Filter;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CursorBuildSpec
{
  public static final CursorBuildSpec FULL_SCAN = CursorBuildSpec.builder().build();

  public static CursorBuildSpecBuilder builder()
  {
    return new CursorBuildSpecBuilder();
  }

  public static CursorBuildSpecBuilder builder(CursorBuildSpec spec)
  {
    return new CursorBuildSpecBuilder(spec);
  }

  @Nullable
  private final Filter filter;
  private final Interval interval;
  @Nullable
  private final List<String> groupingColumns;
  private final VirtualColumns virtualColumns;
  @Nullable
  private final List<AggregatorFactory> aggregators;

  private final QueryContext queryContext;

  private final boolean descending;
  @Nullable
  private final QueryMetrics<?> queryMetrics;

  public CursorBuildSpec(
      @Nullable Filter filter,
      Interval interval,
      @Nullable List<String> groupingColumns,
      VirtualColumns virtualColumns,
      @Nullable List<AggregatorFactory> aggregators,
      QueryContext queryContext,
      boolean descending,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    this.filter = filter;
    this.interval = interval;
    this.groupingColumns = groupingColumns;
    this.virtualColumns = virtualColumns;
    this.aggregators = aggregators;
    this.descending = descending;
    this.queryContext = queryContext;
    this.queryMetrics = queryMetrics;
  }

  @Nullable
  public Filter getFilter()
  {
    return filter;
  }

  public Interval getInterval()
  {
    return interval;
  }

  @Nullable
  public List<String> getGroupingColumns()
  {
    return groupingColumns;
  }

  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  @Nullable
  public List<AggregatorFactory> getAggregators()
  {
    return aggregators;
  }

  public boolean isDescending()
  {
    return descending;
  }

  public QueryContext getQueryContext()
  {
    return queryContext;
  }

  @Nullable
  public QueryMetrics<?> getQueryMetrics()
  {
    return queryMetrics;
  }

  public static class CursorBuildSpecBuilder
  {
    @Nullable
    private Filter filter;
    private Interval interval = Intervals.ETERNITY;

    @Nullable
    private List<String> groupingColumns;
    private VirtualColumns virtualColumns = VirtualColumns.EMPTY;
    @Nullable
    private List<AggregatorFactory> aggregators;
    private boolean descending = false;

    private QueryContext queryContext = QueryContext.empty();
    @Nullable
    private QueryMetrics<?> queryMetrics;

    private CursorBuildSpecBuilder()
    {
      // initialize with defaults
    }

    private CursorBuildSpecBuilder(CursorBuildSpec buildSpec)
    {
      this.filter = buildSpec.filter;
      this.interval = buildSpec.interval;
      this.groupingColumns = buildSpec.groupingColumns;
      this.virtualColumns = buildSpec.virtualColumns;
      this.aggregators = buildSpec.aggregators;
      this.descending = buildSpec.descending;
      this.queryContext = buildSpec.queryContext;
      this.queryMetrics = buildSpec.queryMetrics;
    }

    public CursorBuildSpecBuilder setFilter(@Nullable Filter filter)
    {
      this.filter = filter;
      return this;
    }

    public CursorBuildSpecBuilder setInterval(Interval interval)
    {
      this.interval = interval;
      return this;
    }

    public CursorBuildSpecBuilder setGroupingAndVirtualColumns(
        Granularity granularity,
        @Nullable List<String> groupingColumns,
        VirtualColumns virtualColumns
    )
    {
      final VirtualColumn granularityVirtual = Granularities.toVirtualColumn(granularity);
      if (granularityVirtual == null) {
        this.virtualColumns = virtualColumns;
        this.groupingColumns = groupingColumns;
      } else {
        this.virtualColumns = VirtualColumns.fromIterable(
            Iterables.concat(
                Collections.singletonList(granularityVirtual),
                () -> Arrays.stream(virtualColumns.getVirtualColumns()).iterator()
            )
        );
        ImmutableList.Builder<String> bob = ImmutableList.<String>builder()
                                                         .add(granularityVirtual.getOutputName());
        if (groupingColumns != null) {
          bob.addAll(groupingColumns);
        }
        this.groupingColumns = bob.build();
      }
      return this;
    }

    public CursorBuildSpecBuilder setVirtualColumns(VirtualColumns virtualColumns)
    {
      this.virtualColumns = virtualColumns;
      return this;
    }

    public CursorBuildSpecBuilder setAggregators(@Nullable List<AggregatorFactory> aggregators)
    {
      this.aggregators = aggregators;
      return this;
    }

    public CursorBuildSpecBuilder isDescending(boolean descending)
    {
      this.descending = descending;
      return this;
    }

    public CursorBuildSpecBuilder setQueryContext(QueryContext queryContext)
    {
      this.queryContext = queryContext;
      return this;
    }

    public CursorBuildSpecBuilder setQueryMetrics(@Nullable QueryMetrics<?> queryMetrics)
    {
      this.queryMetrics = queryMetrics;
      return this;
    }

    public CursorBuildSpec build()
    {
      return new CursorBuildSpec(
          filter,
          interval,
          groupingColumns,
          virtualColumns,
          aggregators,
          queryContext,
          descending,
          queryMetrics
      );
    }
  }
}
