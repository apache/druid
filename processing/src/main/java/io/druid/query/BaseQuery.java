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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import io.druid.guice.annotations.ExtensionPoint;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.java.util.common.granularity.PeriodGranularity;
import io.druid.query.spec.QuerySegmentSpec;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
@ExtensionPoint
public abstract class BaseQuery<T extends Comparable<T>> implements Query<T>
{
  public static void checkInterrupted()
  {
    if (Thread.interrupted()) {
      throw new QueryInterruptedException(new InterruptedException());
    }
  }

  public static final String QUERYID = "queryId";
  private final DataSource dataSource;
  private final boolean descending;
  private final Map<String, Object> context;
  private final QuerySegmentSpec querySegmentSpec;
  private volatile Duration duration;
  private final Granularity granularity;

  public BaseQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      boolean descending,
      Map<String, Object> context
  )
  {
    this(dataSource, querySegmentSpec, descending, context, Granularities.ALL);
  }

  public BaseQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      boolean descending,
      Map<String, Object> context,
      Granularity granularity
  )
  {
    Preconditions.checkNotNull(dataSource, "dataSource can't be null");
    Preconditions.checkNotNull(querySegmentSpec, "querySegmentSpec can't be null");
    Preconditions.checkNotNull(granularity, "Must specify a granularity");

    this.dataSource = dataSource;
    this.context = context;
    this.querySegmentSpec = querySegmentSpec;
    this.descending = descending;
    this.granularity = granularity;
  }

  @JsonProperty
  @Override
  public DataSource getDataSource()
  {
    return dataSource;
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
  public QueryRunner<T> getRunner(QuerySegmentWalker walker)
  {
    return getQuerySegmentSpecForLookUp(this).lookup(this, walker);
  }

  @VisibleForTesting
  public static QuerySegmentSpec getQuerySegmentSpecForLookUp(BaseQuery query)
  {
    if (query.getDataSource() instanceof QueryDataSource) {
      QueryDataSource ds = (QueryDataSource) query.getDataSource();
      Query subquery = ds.getQuery();
      if (subquery instanceof BaseQuery) {
        return getQuerySegmentSpecForLookUp((BaseQuery) subquery);
      }
      throw new IllegalStateException("Invalid subquery type " + subquery.getClass());
    }
    return query.getQuerySegmentSpec();
  }

  @Override
  public List<Interval> getIntervals()
  {
    return querySegmentSpec.getIntervals();
  }

  @Override
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
  public Granularity getGranularity()
  {
    return granularity;
  }

  @Override
  public DateTimeZone getTimezone()
  {
    return granularity instanceof PeriodGranularity
           ? ((PeriodGranularity) granularity).getTimeZone()
           : DateTimeZone.UTC;
  }

  @Override
  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
  }

  @Override
  public <ContextType> ContextType getContextValue(String key)
  {
    return context == null ? null : (ContextType) context.get(key);
  }

  @Override
  public <ContextType> ContextType getContextValue(String key, ContextType defaultValue)
  {
    ContextType retVal = getContextValue(key);
    return retVal == null ? defaultValue : retVal;
  }

  @Override
  public boolean getContextBoolean(String key, boolean defaultValue)
  {
    return QueryContexts.parseBoolean(this, key, defaultValue);
  }

  /**
   * @deprecated use {@link #computeOverriddenContext(Map, Map) computeOverriddenContext(getContext(), overrides))}
   * instead. This method may be removed in the next minor or major version of Druid.
   */
  @Deprecated
  protected Map<String, Object> computeOverridenContext(final Map<String, Object> overrides)
  {
    return computeOverriddenContext(getContext(), overrides);
  }

  protected static Map<String, Object> computeOverriddenContext(
      final Map<String, Object> context,
      final Map<String, Object> overrides
  )
  {
    Map<String, Object> overridden = Maps.newTreeMap();
    if (context != null) {
      overridden.putAll(context);
    }
    overridden.putAll(overrides);

    return overridden;
  }

  @Override
  public Ordering<T> getResultOrdering()
  {
    Ordering<T> retVal = Ordering.natural();
    return descending ? retVal.reverse() : retVal;
  }

  @Override
  public String getId()
  {
    return (String) getContextValue(QUERYID);
  }

  @Override
  public Query withId(String id)
  {
    return withOverriddenContext(ImmutableMap.of(QUERYID, id));
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
    BaseQuery<?> baseQuery = (BaseQuery<?>) o;
    return descending == baseQuery.descending &&
           Objects.equals(dataSource, baseQuery.dataSource) &&
           Objects.equals(context, baseQuery.context) &&
           Objects.equals(querySegmentSpec, baseQuery.querySegmentSpec) &&
           Objects.equals(duration, baseQuery.duration) &&
           Objects.equals(granularity, baseQuery.granularity);
  }

  @Override
  public int hashCode()
  {

    return Objects.hash(dataSource, descending, context, querySegmentSpec, duration, granularity);
  }
}
