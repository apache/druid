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

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.filter.Filter;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;

public class CursorBuildSpec
{
  public static final CursorBuildSpec FULL_SCAN = CursorBuildSpec.builder().setGranularity(Granularities.ALL).build();

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
  private final Granularity granularity;
  @Nullable
  private final List<String> columns;
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
      Granularity granularity,
      @Nullable List<String> columns,
      VirtualColumns virtualColumns,
      @Nullable List<AggregatorFactory> aggregators,
      QueryContext queryContext,
      boolean descending,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    this.filter = filter;
    this.interval = interval;
    this.granularity = granularity;
    this.columns = columns;
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

  public Granularity getGranularity()
  {
    return granularity;
  }

  @Nullable
  public List<String> getColumns()
  {
    return columns;
  }

  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

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
    private Granularity granularity = Granularities.NONE;

    @Nullable
    private List<String> columns = null;
    private VirtualColumns virtualColumns = VirtualColumns.EMPTY;
    @Nullable
    private List<AggregatorFactory> aggregators = null;
    private boolean descending = false;

    private QueryContext queryContext = QueryContext.empty();
    @Nullable
    private QueryMetrics<?> queryMetrics;

    public CursorBuildSpecBuilder()
    {

    }

    public CursorBuildSpecBuilder(CursorBuildSpec buildSpec)
    {
      this.filter = buildSpec.filter;
      this.interval = buildSpec.interval;
      this.granularity = buildSpec.granularity;
      this.columns = buildSpec.columns;
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

    public CursorBuildSpecBuilder setGranularity(Granularity granularity)
    {
      this.granularity = granularity;
      return this;
    }

    public CursorBuildSpecBuilder setColumns(@Nullable List<String> columns)
    {
      this.columns = columns;
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
          granularity,
          columns,
          virtualColumns,
          aggregators,
          queryContext,
          descending,
          queryMetrics
      );
    }
  }
}
