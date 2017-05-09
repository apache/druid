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
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.spec.QuerySegmentSpec;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 */
public abstract class BaseQuery<T extends Comparable<T>> implements Query<T>
{
  public static void checkInterrupted()
  {
    if (Thread.interrupted()) {
      throw new QueryInterruptedException(new InterruptedException());
    }
  }

  private final DataSource dataSource;
  private final boolean descending;
  private final Map<String, Object> context;
  private final QuerySegmentSpec querySegmentSpec;
  private volatile Duration duration;

  public BaseQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      boolean descending,
      Map<String, Object> context
  )
  {
    Preconditions.checkNotNull(dataSource, "dataSource can't be null");
    Preconditions.checkNotNull(querySegmentSpec, "querySegmentSpec can't be null");

    this.dataSource = dataSource;
    this.context = context == null ? Maps.newTreeMap() : context;
    this.querySegmentSpec = querySegmentSpec;
    this.descending = descending;
  }

  @JsonProperty
  public DataSource getDataSource()
  {
    return dataSource;
  }

  @Override
  public List<DataSourceWithSegmentSpec> getDataSources()
  {
    return ImmutableList.of(new DataSourceWithSegmentSpec(dataSource, querySegmentSpec));
  }

  @JsonProperty
  @Override
  public boolean isDescending()
  {
    return descending;
  }

  @JsonProperty("intervals")
  public QuerySegmentSpec getQuerySegmentSpec()
  {
    return querySegmentSpec;
  }

  @Override
  public Sequence<T> run(QuerySegmentWalker walker, Map<String, Object> context)
  {
    return run(querySegmentSpec.lookup(this, walker), context);
  }

  public List<Interval> getIntervals()
  {
    return querySegmentSpec.getIntervals();
  }

  @Override
  public Duration getDuration(DataSource dataSource)
  {
    Preconditions.checkArgument(this.dataSource.equals(dataSource));
    return getDuration();
  }

  public Duration getDuration()
  {
    if (duration == null) {
      Duration totalDuration = new Duration(0);
      for (Interval interval : querySegmentSpec.getIntervals()) {
        if (interval != null) {
          totalDuration = totalDuration.plus(interval.toDuration());
        }
      }
      duration = totalDuration;
    }

    return duration;
  }

  @Override
  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
  }

  @Override
  public Ordering<T> getResultOrdering()
  {
    Ordering<T> retVal = Ordering.natural();
    return descending ? retVal.reverse() : retVal;
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

    BaseQuery baseQuery = (BaseQuery) o;

    if (descending != baseQuery.descending) {
      return false;
    }
    if (!context.equals(baseQuery.context)) {
      return false;
    }
    if (!dataSource.equals(baseQuery.dataSource)) {
      return false;
    }
    if (duration != null ? !duration.equals(baseQuery.duration) : baseQuery.duration != null) {
      return false;
    }
    if (!querySegmentSpec.equals(baseQuery.querySegmentSpec)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dataSource.hashCode();
    result = 31 * result + (descending ? 1 : 0);
    result = 31 * result + context.hashCode();
    result = 31 * result + querySegmentSpec.hashCode();
    result = 31 * result + (duration != null ? duration.hashCode() : 0);
    return result;
  }

  public Query<T> updateDistributionTarget()
  {
    return distributeBy(new DataSourceWithSegmentSpec(BaseQuery.getLeafDataSource(dataSource), querySegmentSpec));
  }

  @Override
  public Query<T> withQuerySegmentSpec(DataSource dataSource, QuerySegmentSpec spec)
  {
    Preconditions.checkArgument(this.dataSource.equals(dataSource));
    final BaseQuery<T> result = (BaseQuery<T>) withQuerySegmentSpec(spec);
    if (getDistributionTarget() != null && getDistributionTarget().getDataSource().equals(dataSource)) {
      return result.updateDistributionTarget();
    } else {
      return result;
    }
  }

  @Override
  public Query<T> withQuerySegmentSpec(String concatenatedDataSourceName, QuerySegmentSpec spec)
  {
    Preconditions.checkArgument(this.dataSource.getConcatenatedName().equals(concatenatedDataSourceName));
    return withQuerySegmentSpec(this.dataSource, spec);
  }

  @Override
  public Query<T> replaceDataSource(DataSource oldDataSource, DataSource newDataSource)
  {
    Preconditions.checkArgument(this.dataSource.equals(oldDataSource));
    return withDataSource(newDataSource);
  }

  public abstract Query<T> withQuerySegmentSpec(QuerySegmentSpec spec);
  public abstract Query<T> withDataSource(DataSource dataSource);

  public static <T extends Comparable<T>> DataSource getLeafDataSource(
      BaseQuery<T> query
  )
  {
    return getLeafDataSource(query.getDataSource());
  }

  public static DataSource getLeafDataSource(DataSource dataSource)
  {
    if (dataSource instanceof QueryDataSource) {
      final QueryDataSource queryDataSource = (QueryDataSource) dataSource;
      return getLeafDataSource((BaseQuery<?>) queryDataSource.getQuery());
    } else {
      return dataSource;
    }
  }
}
