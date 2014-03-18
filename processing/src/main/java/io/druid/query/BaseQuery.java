/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import io.druid.query.spec.QuerySegmentSpec;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 */
public abstract class BaseQuery<T> implements Query<T>
{
  public static String QUERYID = "queryId";
  private final DataSource dataSource;
  private final Map<String, String> context;
  private final QuerySegmentSpec querySegmentSpec;
  private volatile Duration duration;

  public BaseQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      Map<String, String> context
  )
  {
    Preconditions.checkNotNull(dataSource, "dataSource can't be null");
    Preconditions.checkNotNull(querySegmentSpec, "querySegmentSpec can't be null");

    this.dataSource = dataSource;
    this.context = context;
    this.querySegmentSpec = querySegmentSpec;
  }

  @JsonProperty
  @Override
  public DataSource getDataSource()
  {
    return dataSource;
  }

  @JsonProperty("intervals")
  public QuerySegmentSpec getQuerySegmentSpec()
  {
    return querySegmentSpec;
  }

  @Override
  public Sequence<T> run(QuerySegmentWalker walker)
  {
    return run(querySegmentSpec.lookup(this, walker));
  }

  public Sequence<T> run(QueryRunner<T> runner)
  {
    return runner.run(this);
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

  @JsonProperty
  public Map<String, String> getContext()
  {
    return context;
  }

  @Override
  public String getContextValue(String key)
  {
    return context == null ? null : context.get(key);
  }

  @Override
  public String getContextValue(String key, String defaultValue)
  {
    String retVal = getContextValue(key);
    return retVal == null ? defaultValue : retVal;
  }

  protected Map<String, String> computeOverridenContext(Map<String, String> overrides)
  {
    Map<String, String> overridden = Maps.newTreeMap();
    final Map<String, String> context = getContext();
    if (context != null) {
      overridden.putAll(context);
    }
    overridden.putAll(overrides);

    return overridden;
  }

  @Override
  public String getId()
  {
    return getContextValue(QUERYID);
  }

  @Override
  public Query withId(String id)
  {
    return withOverriddenContext(ImmutableMap.of(QUERYID, id));
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    BaseQuery baseQuery = (BaseQuery) o;

    if (context != null ? !context.equals(baseQuery.context) : baseQuery.context != null) return false;
    if (dataSource != null ? !dataSource.equals(baseQuery.dataSource) : baseQuery.dataSource != null) return false;
    if (duration != null ? !duration.equals(baseQuery.duration) : baseQuery.duration != null) return false;
    if (querySegmentSpec != null ? !querySegmentSpec.equals(baseQuery.querySegmentSpec) : baseQuery.querySegmentSpec != null)
      return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dataSource != null ? dataSource.hashCode() : 0;
    result = 31 * result + (context != null ? context.hashCode() : 0);
    result = 31 * result + (querySegmentSpec != null ? querySegmentSpec.hashCode() : 0);
    result = 31 * result + (duration != null ? duration.hashCode() : 0);
    return result;
  }
}
