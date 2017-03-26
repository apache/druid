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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.spec.QuerySegmentSpec;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.Map;

/**
 */
public abstract class BaseQuery<T extends Comparable<T>> implements Query<T>
{
  public static <T> int getContextPriority(Query<T> query, int defaultValue)
  {
    return parseInt(query, "priority", defaultValue);
  }

  public static <T> boolean getContextBySegment(Query<T> query, boolean defaultValue)
  {
    return parseBoolean(query, "bySegment", defaultValue);
  }

  public static <T> boolean getContextPopulateCache(Query<T> query, boolean defaultValue)
  {
    return parseBoolean(query, "populateCache", defaultValue);
  }

  public static <T> boolean getContextUseCache(Query<T> query, boolean defaultValue)
  {
    return parseBoolean(query, "useCache", defaultValue);
  }

  public static <T> boolean getContextFinalize(Query<T> query, boolean defaultValue)
  {
    return parseBoolean(query, "finalize", defaultValue);
  }

  public static <T> int getContextUncoveredIntervalsLimit(Query<T> query, int defaultValue)
  {
    return parseInt(query, "uncoveredIntervalsLimit", defaultValue);
  }

  private static <T> int parseInt(Query<T> query, String key, int defaultValue)
  {
    Object val = query.getContextValue(key);
    if (val == null) {
      return defaultValue;
    }
    if (val instanceof String) {
      return Integer.parseInt((String) val);
    } else if (val instanceof Integer) {
      return (int) val;
    } else {
      throw new ISE("Unknown type [%s]", val.getClass());
    }
  }

  private static <T> boolean parseBoolean(Query<T> query, String key, boolean defaultValue)
  {
    Object val = query.getContextValue(key);
    if (val == null) {
      return defaultValue;
    }
    if (val instanceof String) {
      return Boolean.parseBoolean((String) val);
    } else if (val instanceof Boolean) {
      return (boolean) val;
    } else {
      throw new ISE("Unknown type [%s]. Cannot parse!", val.getClass());
    }
  }

  public static void checkInterrupted()
  {
    if (Thread.interrupted()) {
      throw new QueryInterruptedException(new InterruptedException());
    }
  }

  public static final String QUERYID = "queryId";

  private final boolean descending;
  private final Map<String, Object> context;

  public BaseQuery(
      boolean descending,
      Map<String, Object> context
  )
  {
    this.context = context;
    this.descending = descending;
  }

  @JsonProperty
  @Override
  public boolean isDescending()
  {
    return descending;
  }

  public Sequence<T> run(QueryRunner<T> runner, Map<String, Object> context)
  {
    return runner.run(this, context);
  }

  public static Duration initDuration(QuerySegmentSpec querySegmentSpec)
  {
    Duration totalDuration = new Duration(0);
    for (Interval interval : querySegmentSpec.getIntervals()) {
      if (interval != null) {
        totalDuration = totalDuration.plus(interval.toDuration());
      }
    }
    return totalDuration;
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
    return parseBoolean(this, key, defaultValue);
  }

  protected Map<String, Object> computeOverridenContext(Map<String, Object> overrides)
  {
    Map<String, Object> overridden = Maps.newTreeMap();
    final Map<String, Object> context = getContext();
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
    return withOverriddenContext(ImmutableMap.<String, Object>of(QUERYID, id));
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
    if (context != null ? !context.equals(baseQuery.context) : baseQuery.context != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = (descending ? 1 : 0);
    result = 31 * result + (context != null ? context.hashCode() : 0);
    return result;
  }
}
