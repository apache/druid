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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.java.util.common.StringUtils;

import java.util.concurrent.TimeUnit;

@PublicApi
public class QueryContexts
{
  public static final String FINALIZE_KEY = "finalize";
  public static final String PRIORITY_KEY = "priority";
  public static final String LANE_KEY = "lane";
  public static final String TIMEOUT_KEY = "timeout";
  public static final String MAX_SCATTER_GATHER_BYTES_KEY = "maxScatterGatherBytes";
  public static final String MAX_QUEUED_BYTES_KEY = "maxQueuedBytes";
  public static final String DEFAULT_TIMEOUT_KEY = "defaultTimeout";
  public static final String BROKER_PARALLEL_MERGE_KEY = "enableParallelMerge";
  public static final String BROKER_PARALLEL_MERGE_INITIAL_YIELD_ROWS_KEY = "parallelMergeInitialYieldRows";
  public static final String BROKER_PARALLEL_MERGE_SMALL_BATCH_ROWS_KEY = "parallelMergeSmallBatchRows";
  public static final String BROKER_PARALLELISM = "parallelMergeParallelism";
  public static final String VECTORIZE_KEY = "vectorize";
  public static final String VECTOR_SIZE_KEY = "vectorSize";
  public static final String MAX_SUBQUERY_ROWS_KEY = "maxSubqueryRows";
  public static final String JOIN_FILTER_PUSH_DOWN_KEY = "enableJoinFilterPushDown";
  public static final String JOIN_FILTER_REWRITE_ENABLE_KEY = "enableJoinFilterRewrite";
  public static final String JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY = "enableJoinFilterRewriteValueColumnFilters";
  public static final String JOIN_FILTER_REWRITE_MAX_SIZE_KEY = "joinFilterRewriteMaxSize";
  public static final String USE_FILTER_CNF_KEY = "useFilterCNF";

  public static final boolean DEFAULT_BY_SEGMENT = false;
  public static final boolean DEFAULT_POPULATE_CACHE = true;
  public static final boolean DEFAULT_USE_CACHE = true;
  public static final boolean DEFAULT_POPULATE_RESULTLEVEL_CACHE = true;
  public static final boolean DEFAULT_USE_RESULTLEVEL_CACHE = true;
  public static final Vectorize DEFAULT_VECTORIZE = Vectorize.TRUE;
  public static final int DEFAULT_PRIORITY = 0;
  public static final int DEFAULT_UNCOVERED_INTERVALS_LIMIT = 0;
  public static final long DEFAULT_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(5);
  public static final long NO_TIMEOUT = 0;
  public static final boolean DEFAULT_ENABLE_PARALLEL_MERGE = true;
  public static final boolean DEFAULT_ENABLE_JOIN_FILTER_PUSH_DOWN = true;
  public static final boolean DEFAULT_ENABLE_JOIN_FILTER_REWRITE = true;
  public static final boolean DEFAULT_ENABLE_JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS = false;
  public static final long DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE = 10000;
  public static final boolean DEFAULT_USE_FILTER_CNF = false;

  @SuppressWarnings("unused") // Used by Jackson serialization
  public enum Vectorize
  {
    FALSE {
      @Override
      public boolean shouldVectorize(final boolean canVectorize)
      {
        return false;
      }
    },
    TRUE {
      @Override
      public boolean shouldVectorize(final boolean canVectorize)
      {
        return canVectorize;
      }
    },
    FORCE {
      @Override
      public boolean shouldVectorize(final boolean canVectorize)
      {
        if (!canVectorize) {
          throw new ISE("Cannot vectorize!");
        }

        return true;
      }
    };

    public abstract boolean shouldVectorize(boolean canVectorize);

    @JsonCreator
    public static Vectorize fromString(String str)
    {
      return Vectorize.valueOf(StringUtils.toUpperCase(str));
    }

    @Override
    @JsonValue
    public String toString()
    {
      return StringUtils.toLowerCase(name()).replace('_', '-');
    }
  }

  public static <T> boolean isBySegment(Query<T> query)
  {
    return isBySegment(query, DEFAULT_BY_SEGMENT);
  }

  public static <T> boolean isBySegment(Query<T> query, boolean defaultValue)
  {
    return parseBoolean(query, "bySegment", defaultValue);
  }

  public static <T> boolean isPopulateCache(Query<T> query)
  {
    return isPopulateCache(query, DEFAULT_POPULATE_CACHE);
  }

  public static <T> boolean isPopulateCache(Query<T> query, boolean defaultValue)
  {
    return parseBoolean(query, "populateCache", defaultValue);
  }

  public static <T> boolean isUseCache(Query<T> query)
  {
    return isUseCache(query, DEFAULT_USE_CACHE);
  }

  public static <T> boolean isUseCache(Query<T> query, boolean defaultValue)
  {
    return parseBoolean(query, "useCache", defaultValue);
  }

  public static <T> boolean isPopulateResultLevelCache(Query<T> query)
  {
    return isPopulateResultLevelCache(query, DEFAULT_POPULATE_RESULTLEVEL_CACHE);
  }

  public static <T> boolean isPopulateResultLevelCache(Query<T> query, boolean defaultValue)
  {
    return parseBoolean(query, "populateResultLevelCache", defaultValue);
  }

  public static <T> boolean isUseResultLevelCache(Query<T> query)
  {
    return isUseResultLevelCache(query, DEFAULT_USE_RESULTLEVEL_CACHE);
  }

  public static <T> boolean isUseResultLevelCache(Query<T> query, boolean defaultValue)
  {
    return parseBoolean(query, "useResultLevelCache", defaultValue);
  }

  public static <T> boolean isFinalize(Query<T> query, boolean defaultValue)
  {
    return parseBoolean(query, FINALIZE_KEY, defaultValue);
  }

  public static <T> boolean isSerializeDateTimeAsLong(Query<T> query, boolean defaultValue)
  {
    return parseBoolean(query, "serializeDateTimeAsLong", defaultValue);
  }

  public static <T> boolean isSerializeDateTimeAsLongInner(Query<T> query, boolean defaultValue)
  {
    return parseBoolean(query, "serializeDateTimeAsLongInner", defaultValue);
  }

  public static <T> Vectorize getVectorize(Query<T> query, Vectorize defaultValue)
  {
    return parseEnum(query, VECTORIZE_KEY, Vectorize.class, defaultValue);
  }

  public static <T> int getVectorSize(Query<T> query, int defaultSize)
  {
    return parseInt(query, VECTOR_SIZE_KEY, defaultSize);
  }

  public static <T> int getMaxSubqueryRows(Query<T> query, int defaultSize)
  {
    return parseInt(query, MAX_SUBQUERY_ROWS_KEY, defaultSize);
  }

  public static <T> int getUncoveredIntervalsLimit(Query<T> query)
  {
    return getUncoveredIntervalsLimit(query, DEFAULT_UNCOVERED_INTERVALS_LIMIT);
  }

  public static <T> int getUncoveredIntervalsLimit(Query<T> query, int defaultValue)
  {
    return parseInt(query, "uncoveredIntervalsLimit", defaultValue);
  }

  public static <T> int getPriority(Query<T> query)
  {
    return getPriority(query, DEFAULT_PRIORITY);
  }

  public static <T> int getPriority(Query<T> query, int defaultValue)
  {
    return parseInt(query, PRIORITY_KEY, defaultValue);
  }

  public static <T> String getLane(Query<T> query)
  {
    return (String) query.getContextValue(LANE_KEY);
  }

  public static <T> boolean getEnableParallelMerges(Query<T> query)
  {
    return parseBoolean(query, BROKER_PARALLEL_MERGE_KEY, DEFAULT_ENABLE_PARALLEL_MERGE);
  }

  public static <T> int getParallelMergeInitialYieldRows(Query<T> query, int defaultValue)
  {
    return parseInt(query, BROKER_PARALLEL_MERGE_INITIAL_YIELD_ROWS_KEY, defaultValue);
  }

  public static <T> int getParallelMergeSmallBatchRows(Query<T> query, int defaultValue)
  {
    return parseInt(query, BROKER_PARALLEL_MERGE_SMALL_BATCH_ROWS_KEY, defaultValue);
  }

  public static <T> int getParallelMergeParallelism(Query<T> query, int defaultValue)
  {
    return parseInt(query, BROKER_PARALLELISM, defaultValue);
  }
  public static <T> boolean getEnableJoinFilterRewriteValueColumnFilters(Query<T> query)
  {
    return parseBoolean(
        query,
        JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY,
        DEFAULT_ENABLE_JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS
    );
  }

  public static <T> long getJoinFilterRewriteMaxSize(Query<T> query)
  {
    return parseLong(query, JOIN_FILTER_REWRITE_MAX_SIZE_KEY, DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE);
  }

  public static <T> boolean getEnableJoinFilterPushDown(Query<T> query)
  {
    return parseBoolean(query, JOIN_FILTER_PUSH_DOWN_KEY, DEFAULT_ENABLE_JOIN_FILTER_PUSH_DOWN);
  }

  public static <T> boolean getEnableJoinFilterRewrite(Query<T> query)
  {
    return parseBoolean(query, JOIN_FILTER_REWRITE_ENABLE_KEY, DEFAULT_ENABLE_JOIN_FILTER_REWRITE);
  }


  public static <T> Query<T> withMaxScatterGatherBytes(Query<T> query, long maxScatterGatherBytesLimit)
  {
    Object obj = query.getContextValue(MAX_SCATTER_GATHER_BYTES_KEY);
    if (obj == null) {
      return query.withOverriddenContext(ImmutableMap.of(MAX_SCATTER_GATHER_BYTES_KEY, maxScatterGatherBytesLimit));
    } else {
      long curr = ((Number) obj).longValue();
      if (curr > maxScatterGatherBytesLimit) {
        throw new IAE(
            "configured [%s = %s] is more than enforced limit of [%s].",
            MAX_SCATTER_GATHER_BYTES_KEY,
            curr,
            maxScatterGatherBytesLimit
        );
      } else {
        return query;
      }
    }
  }

  public static <T> Query<T> verifyMaxQueryTimeout(Query<T> query, long maxQueryTimeout)
  {
    long timeout = getTimeout(query);
    if (timeout > maxQueryTimeout) {
      throw new IAE(
          "configured [%s = %s] is more than enforced limit of maxQueryTimeout [%s].",
          TIMEOUT_KEY,
          timeout,
          maxQueryTimeout
      );
    } else {
      return query;
    }
  }

  public static <T> long getMaxQueuedBytes(Query<T> query, long defaultValue)
  {
    return parseLong(query, MAX_QUEUED_BYTES_KEY, defaultValue);
  }

  public static <T> long getMaxScatterGatherBytes(Query<T> query)
  {
    return parseLong(query, MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE);
  }

  public static <T> boolean hasTimeout(Query<T> query)
  {
    return getTimeout(query) != NO_TIMEOUT;
  }

  public static <T> long getTimeout(Query<T> query)
  {
    return getTimeout(query, getDefaultTimeout(query));
  }

  public static <T> long getTimeout(Query<T> query, long defaultTimeout)
  {
    final long timeout = parseLong(query, TIMEOUT_KEY, defaultTimeout);
    Preconditions.checkState(timeout >= 0, "Timeout must be a non negative value, but was [%s]", timeout);
    return timeout;
  }

  public static <T> Query<T> withTimeout(Query<T> query, long timeout)
  {
    return query.withOverriddenContext(ImmutableMap.of(TIMEOUT_KEY, timeout));
  }

  public static <T> Query<T> withDefaultTimeout(Query<T> query, long defaultTimeout)
  {
    return query.withOverriddenContext(ImmutableMap.of(QueryContexts.DEFAULT_TIMEOUT_KEY, defaultTimeout));
  }

  static <T> long getDefaultTimeout(Query<T> query)
  {
    final long defaultTimeout = parseLong(query, DEFAULT_TIMEOUT_KEY, DEFAULT_TIMEOUT_MILLIS);
    Preconditions.checkState(defaultTimeout >= 0, "Timeout must be a non negative value, but was [%s]", defaultTimeout);
    return defaultTimeout;
  }

  static <T> long parseLong(Query<T> query, String key, long defaultValue)
  {
    final Object val = query.getContextValue(key);
    return val == null ? defaultValue : Numbers.parseLong(val);
  }

  static <T> int parseInt(Query<T> query, String key, int defaultValue)
  {
    final Object val = query.getContextValue(key);
    return val == null ? defaultValue : Numbers.parseInt(val);
  }

  static <T> boolean parseBoolean(Query<T> query, String key, boolean defaultValue)
  {
    final Object val = query.getContextValue(key);
    return val == null ? defaultValue : Numbers.parseBoolean(val);
  }

  private QueryContexts()
  {
  }

  static <T, E extends Enum<E>> E parseEnum(Query<T> query, String key, Class<E> clazz, E defaultValue)
  {
    Object val = query.getContextValue(key);
    if (val == null) {
      return defaultValue;
    }
    if (val instanceof String) {
      return Enum.valueOf(clazz, StringUtils.toUpperCase((String) val));
    } else if (val instanceof Boolean) {
      return Enum.valueOf(clazz, StringUtils.toUpperCase(String.valueOf(val)));
    } else {
      throw new ISE("Unknown type [%s]. Cannot parse!", val.getClass());
    }
  }
}
