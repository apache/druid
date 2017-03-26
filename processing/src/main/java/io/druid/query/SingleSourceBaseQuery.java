/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.spec.QuerySegmentSpec;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class SingleSourceBaseQuery<T extends Comparable<T>> extends BaseQuery<T>
{
  private final DataSourceWithSegmentSpec dataSourceWithSegment;
  private volatile Duration duration;

  public SingleSourceBaseQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      boolean descending,
      Map<String, Object> context
  )
  {
    super(descending, context);
    Objects.requireNonNull(dataSource, "dataSource can't be null");
    Objects.requireNonNull(querySegmentSpec, "querySegmentSpec can't be null");

    this.dataSourceWithSegment = new DataSourceWithSegmentSpec(dataSource, querySegmentSpec);
  }

  public abstract Query<T> withQuerySegmentSpec(QuerySegmentSpec spec);
  public abstract Query<T> withDataSource(DataSource dataSource);

  @Override
  public Iterable<DataSourceWithSegmentSpec> getDataSources()
  {
    return ImmutableList.of(dataSourceWithSegment);
  }

  public DataSourceWithSegmentSpec getDataSourceWithSegmentSpec()
  {
    return dataSourceWithSegment;
  }

  @JsonProperty
  public DataSource getDataSource()
  {
    return dataSourceWithSegment.getDataSource();
  }

  @JsonProperty("intervals")
  public QuerySegmentSpec getQuerySegmentSpec()
  {
    return dataSourceWithSegment.getQuerySegmentSpec();
  }

  @Override
  public Sequence<T> run(QuerySegmentWalker walker, Map<String, Object> context)
  {
    return run(dataSourceWithSegment.getQuerySegmentSpec().lookup(this, walker), context);
  }

  public List<Interval> getIntervals()
  {
    return dataSourceWithSegment.getQuerySegmentSpec().getIntervals();
  }

  @Override
  public Duration getDuration(DataSource dataSource)
  {
    Preconditions.checkArgument(this.dataSourceWithSegment.getDataSource().equals(dataSource));
    return getDuration();
  }

  public Duration getDuration()
  {
    if (duration == null) {
      duration = initDuration(dataSourceWithSegment.getQuerySegmentSpec());
    }

    return duration;
  }

  @Override
  public Query<T> replaceQuerySegmentSpecWith(DataSource dataSource, QuerySegmentSpec spec)
  {
    Preconditions.checkArgument(this.dataSourceWithSegment.getDataSource().equals(dataSource));
    final Query<T> query = withQuerySegmentSpec(spec);
    if (getDistributionTarget() != null) {
      if (dataSource.equals(getDistributionTarget().getDataSource())) {
        return query.distributeBy(((SingleSourceBaseQuery<T>) query).getDataSourceWithSegmentSpec());
      }
    }
    return query;
  }

  @Override
  public Query<T> replaceQuerySegmentSpecWith(String dataSource, QuerySegmentSpec spec)
  {
    Preconditions.checkArgument(Iterables.getOnlyElement(this.dataSourceWithSegment.getDataSource().getNames()).equals(dataSource));
    final Query<T> query = withQuerySegmentSpec(spec);
    if (getDistributionTarget() != null) {
      if (dataSource.equals(getDistributionTarget().getDataSource().getConcatenatedName())) {
        return query.distributeBy(((SingleSourceBaseQuery<T>) query).getDataSourceWithSegmentSpec());
      }
    }
    return query;
  }

  @Override
  public Query<T> replaceDataSourceWith(DataSource src, DataSource dst)
  {
    Preconditions.checkArgument(this.dataSourceWithSegment.getDataSource().equals(src));
    return withDataSource(dst);
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

    SingleSourceBaseQuery baseQuery = (SingleSourceBaseQuery) o;

    if (isDescending() != baseQuery.isDescending()) {
      return false;
    }
    if (getContext() != null ? !getContext().equals(baseQuery.getContext()) : baseQuery.getContext() != null) {
      return false;
    }
    if (!dataSourceWithSegment.equals(baseQuery.dataSourceWithSegment)) {
      return false;
    }
    if (duration != null ? !duration.equals(baseQuery.duration) : baseQuery.duration != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dataSourceWithSegment.hashCode();
    result = 31 * result + (isDescending() ? 1 : 0);
    result = 31 * result + (getContext() != null ? getContext().hashCode() : 0);
    result = 31 * result + (duration != null ? duration.hashCode() : 0);
    return result;
  }

  public static <T extends Comparable<T>> DataSourceWithSegmentSpec getLeafDataSourceWithSegmentSpec(
      SingleSourceBaseQuery<T> query
  )
  {
    final DataSourceWithSegmentSpec sourceWithSegmentSpec = query.getDataSourceWithSegmentSpec();
    if (sourceWithSegmentSpec.getDataSource() instanceof QueryDataSource) {
      final QueryDataSource queryDataSource = (QueryDataSource) sourceWithSegmentSpec.getDataSource();
      return getLeafDataSourceWithSegmentSpec((SingleSourceBaseQuery<T>) queryDataSource.getQuery());
    } else {
      return sourceWithSegmentSpec;
    }
  }

  public static <T extends Comparable<T>> DataSource getLeafDataSource(DataSource dataSource)
  {
    if (dataSource instanceof QueryDataSource) {
      final QueryDataSource queryDataSource = (QueryDataSource) dataSource;
      return getLeafDataSourceWithSegmentSpec((SingleSourceBaseQuery<T>) queryDataSource.getQuery()).getDataSource();
    } else {
      return dataSource;
    }
  }
}
