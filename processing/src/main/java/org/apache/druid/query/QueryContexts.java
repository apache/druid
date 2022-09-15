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
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.QueryableIndexStorageAdapter;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.TreeMap;
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
  public static final String VECTORIZE_VIRTUAL_COLUMNS_KEY = "vectorizeVirtualColumns";
  public static final String VECTOR_SIZE_KEY = "vectorSize";
  public static final String MAX_SUBQUERY_ROWS_KEY = "maxSubqueryRows";
  public static final String JOIN_FILTER_PUSH_DOWN_KEY = "enableJoinFilterPushDown";
  public static final String JOIN_FILTER_REWRITE_ENABLE_KEY = "enableJoinFilterRewrite";
  public static final String JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY = "enableJoinFilterRewriteValueColumnFilters";
  public static final String REWRITE_JOIN_TO_FILTER_ENABLE_KEY = "enableRewriteJoinToFilter";
  public static final String JOIN_FILTER_REWRITE_MAX_SIZE_KEY = "joinFilterRewriteMaxSize";
  public static final String MAX_NUMERIC_IN_FILTERS = "maxNumericInFilters";
  // This flag controls whether a SQL join query with left scan should be attempted to be run as direct table access
  // instead of being wrapped inside a query. With direct table access enabled, Druid can push down the join operation to
  // data servers.
  public static final String SQL_JOIN_LEFT_SCAN_DIRECT = "enableJoinLeftTableScanDirect";
  public static final String USE_FILTER_CNF_KEY = "useFilterCNF";
  public static final String NUM_RETRIES_ON_MISSING_SEGMENTS_KEY = "numRetriesOnMissingSegments";
  public static final String RETURN_PARTIAL_RESULTS_KEY = "returnPartialResults";
  public static final String USE_CACHE_KEY = "useCache";
  public static final String SECONDARY_PARTITION_PRUNING_KEY = "secondaryPartitionPruning";
  public static final String ENABLE_DEBUG = "debug";
  public static final String BY_SEGMENT_KEY = "bySegment";
  public static final String BROKER_SERVICE_NAME = "brokerService";
  public static final String IN_SUB_QUERY_THRESHOLD_KEY = "inSubQueryThreshold";
  public static final String TIME_BOUNDARY_PLANNING_KEY = "enableTimeBoundaryPlanning";
  public static final String POPULATE_CACHE_KEY = "populateCache";
  public static final String POPULATE_RESULT_LEVEL_CACHE_KEY = "populateResultLevelCache";
  public static final String USE_RESULT_LEVEL_CACHE_KEY = "useResultLevelCache";
  public static final String SERIALIZE_DATE_TIME_AS_LONG_KEY = "serializeDateTimeAsLong";
  public static final String SERIALIZE_DATE_TIME_AS_LONG_INNER_KEY = "serializeDateTimeAsLongInner";
  public static final String UNCOVERED_INTERVALS_LIMIT_KEY = "uncoveredIntervalsLimit";

  public static final boolean DEFAULT_BY_SEGMENT = false;
  public static final boolean DEFAULT_POPULATE_CACHE = true;
  public static final boolean DEFAULT_USE_CACHE = true;
  public static final boolean DEFAULT_POPULATE_RESULTLEVEL_CACHE = true;
  public static final boolean DEFAULT_USE_RESULTLEVEL_CACHE = true;
  public static final Vectorize DEFAULT_VECTORIZE = Vectorize.TRUE;
  public static final Vectorize DEFAULT_VECTORIZE_VIRTUAL_COLUMN = Vectorize.TRUE;
  public static final int DEFAULT_PRIORITY = 0;
  public static final int DEFAULT_UNCOVERED_INTERVALS_LIMIT = 0;
  public static final long DEFAULT_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(5);
  public static final long NO_TIMEOUT = 0;
  public static final boolean DEFAULT_ENABLE_PARALLEL_MERGE = true;
  public static final boolean DEFAULT_ENABLE_JOIN_FILTER_PUSH_DOWN = true;
  public static final boolean DEFAULT_ENABLE_JOIN_FILTER_REWRITE = true;
  public static final boolean DEFAULT_ENABLE_JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS = false;
  public static final boolean DEFAULT_ENABLE_REWRITE_JOIN_TO_FILTER = true;
  public static final long DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE = 10000;
  public static final boolean DEFAULT_ENABLE_SQL_JOIN_LEFT_SCAN_DIRECT = false;
  public static final boolean DEFAULT_USE_FILTER_CNF = false;
  public static final boolean DEFAULT_SECONDARY_PARTITION_PRUNING = true;
  public static final boolean DEFAULT_ENABLE_DEBUG = false;
  public static final int DEFAULT_IN_SUB_QUERY_THRESHOLD = Integer.MAX_VALUE;
  public static final boolean DEFAULT_ENABLE_TIME_BOUNDARY_PLANNING = false;

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
    return query.getContextBoolean(BY_SEGMENT_KEY, defaultValue);
  }

  public static <T> boolean isPopulateCache(Query<T> query)
  {
    return isPopulateCache(query, DEFAULT_POPULATE_CACHE);
  }

  public static <T> boolean isPopulateCache(Query<T> query, boolean defaultValue)
  {
    return query.getContextBoolean(POPULATE_CACHE_KEY, defaultValue);
  }

  public static <T> boolean isUseCache(Query<T> query)
  {
    return isUseCache(query, DEFAULT_USE_CACHE);
  }

  public static <T> boolean isUseCache(Query<T> query, boolean defaultValue)
  {
    return query.getContextBoolean(USE_CACHE_KEY, defaultValue);
  }

  public static <T> boolean isPopulateResultLevelCache(Query<T> query)
  {
    return isPopulateResultLevelCache(query, DEFAULT_POPULATE_RESULTLEVEL_CACHE);
  }

  public static <T> boolean isPopulateResultLevelCache(Query<T> query, boolean defaultValue)
  {
    return query.getContextBoolean(POPULATE_RESULT_LEVEL_CACHE_KEY, defaultValue);
  }

  public static <T> boolean isUseResultLevelCache(Query<T> query)
  {
    return isUseResultLevelCache(query, DEFAULT_USE_RESULTLEVEL_CACHE);
  }

  public static <T> boolean isUseResultLevelCache(Query<T> query, boolean defaultValue)
  {
    return query.getContextBoolean(USE_RESULT_LEVEL_CACHE_KEY, defaultValue);
  }

  public static <T> boolean isFinalize(Query<T> query, boolean defaultValue)

  {
    return query.getContextBoolean(FINALIZE_KEY, defaultValue);
  }

  public static <T> boolean isSerializeDateTimeAsLong(Query<T> query, boolean defaultValue)
  {
    return query.getContextBoolean(SERIALIZE_DATE_TIME_AS_LONG_KEY, defaultValue);
  }

  public static <T> boolean isSerializeDateTimeAsLongInner(Query<T> query, boolean defaultValue)
  {
    return query.getContextBoolean(SERIALIZE_DATE_TIME_AS_LONG_INNER_KEY, defaultValue);
  }

  public static <T> Vectorize getVectorize(Query<T> query)
  {
    return getVectorize(query, QueryContexts.DEFAULT_VECTORIZE);
  }

  public static <T> Vectorize getVectorize(Query<T> query, Vectorize defaultValue)
  {
    return query.getQueryContext().getAsEnum(VECTORIZE_KEY, Vectorize.class, defaultValue);
  }

  public static <T> Vectorize getVectorizeVirtualColumns(Query<T> query)
  {
    return getVectorizeVirtualColumns(query, QueryContexts.DEFAULT_VECTORIZE_VIRTUAL_COLUMN);
  }

  public static <T> Vectorize getVectorizeVirtualColumns(Query<T> query, Vectorize defaultValue)
  {
    return query.getQueryContext().getAsEnum(VECTORIZE_VIRTUAL_COLUMNS_KEY, Vectorize.class, defaultValue);
  }

  public static <T> int getVectorSize(Query<T> query)
  {
    return getVectorSize(query, QueryableIndexStorageAdapter.DEFAULT_VECTOR_SIZE);
  }

  public static <T> int getVectorSize(Query<T> query, int defaultSize)
  {
    return query.getQueryContext().getAsInt(VECTOR_SIZE_KEY, defaultSize);
  }

  public static <T> int getMaxSubqueryRows(Query<T> query, int defaultSize)
  {
    return query.getQueryContext().getAsInt(MAX_SUBQUERY_ROWS_KEY, defaultSize);
  }

  public static <T> int getUncoveredIntervalsLimit(Query<T> query)
  {
    return getUncoveredIntervalsLimit(query, DEFAULT_UNCOVERED_INTERVALS_LIMIT);
  }

  public static <T> int getUncoveredIntervalsLimit(Query<T> query, int defaultValue)
  {
    return query.getQueryContext().getAsInt(UNCOVERED_INTERVALS_LIMIT_KEY, defaultValue);
  }

  public static <T> int getPriority(Query<T> query)
  {
    return getPriority(query, DEFAULT_PRIORITY);
  }

  public static <T> int getPriority(Query<T> query, int defaultValue)
  {
    return query.getQueryContext().getAsInt(PRIORITY_KEY, defaultValue);
  }

  public static <T> String getLane(Query<T> query)
  {
    return query.getQueryContext().getAsString(LANE_KEY);
  }

  public static <T> boolean getEnableParallelMerges(Query<T> query)
  {
    return query.getContextBoolean(BROKER_PARALLEL_MERGE_KEY, DEFAULT_ENABLE_PARALLEL_MERGE);
  }

  public static <T> int getParallelMergeInitialYieldRows(Query<T> query, int defaultValue)
  {
    return query.getQueryContext().getAsInt(BROKER_PARALLEL_MERGE_INITIAL_YIELD_ROWS_KEY, defaultValue);
  }

  public static <T> int getParallelMergeSmallBatchRows(Query<T> query, int defaultValue)
  {
    return query.getQueryContext().getAsInt(BROKER_PARALLEL_MERGE_SMALL_BATCH_ROWS_KEY, defaultValue);
  }

  public static <T> int getParallelMergeParallelism(Query<T> query, int defaultValue)
  {
    return query.getQueryContext().getAsInt(BROKER_PARALLELISM, defaultValue);
  }

  public static <T> boolean getEnableJoinFilterRewriteValueColumnFilters(Query<T> query)
  {
    return query.getContextBoolean(
        JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY,
        DEFAULT_ENABLE_JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS
    );
  }

  public static <T> boolean getEnableRewriteJoinToFilter(Query<T> query)
  {
    return query.getContextBoolean(
        REWRITE_JOIN_TO_FILTER_ENABLE_KEY,
        DEFAULT_ENABLE_REWRITE_JOIN_TO_FILTER
    );
  }

  public static <T> long getJoinFilterRewriteMaxSize(Query<T> query)
  {
    return query.getQueryContext().getAsLong(JOIN_FILTER_REWRITE_MAX_SIZE_KEY, DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE);
  }

  public static <T> boolean getEnableJoinFilterPushDown(Query<T> query)
  {
    return query.getContextBoolean(JOIN_FILTER_PUSH_DOWN_KEY, DEFAULT_ENABLE_JOIN_FILTER_PUSH_DOWN);
  }

  public static <T> boolean getEnableJoinFilterRewrite(Query<T> query)
  {
    return query.getContextBoolean(JOIN_FILTER_REWRITE_ENABLE_KEY, DEFAULT_ENABLE_JOIN_FILTER_REWRITE);
  }

  public static boolean getEnableJoinLeftScanDirect(Map<String, Object> context)
  {
    return parseBoolean(context, SQL_JOIN_LEFT_SCAN_DIRECT, DEFAULT_ENABLE_SQL_JOIN_LEFT_SCAN_DIRECT);
  }

  public static <T> boolean isSecondaryPartitionPruningEnabled(Query<T> query)
  {
    return query.getContextBoolean(SECONDARY_PARTITION_PRUNING_KEY, DEFAULT_SECONDARY_PARTITION_PRUNING);
  }

  public static <T> boolean isDebug(Query<T> query)
  {
    return query.getContextBoolean(ENABLE_DEBUG, DEFAULT_ENABLE_DEBUG);
  }

  public static boolean isDebug(Map<String, Object> queryContext)
  {
    return parseBoolean(queryContext, ENABLE_DEBUG, DEFAULT_ENABLE_DEBUG);
  }

  public static int getInSubQueryThreshold(Map<String, Object> context)
  {
    return getInSubQueryThreshold(context, DEFAULT_IN_SUB_QUERY_THRESHOLD);
  }

  public static int getInSubQueryThreshold(Map<String, Object> context, int defaultValue)
  {
    return parseInt(context, IN_SUB_QUERY_THRESHOLD_KEY, defaultValue);
  }

  public static boolean isTimeBoundaryPlanningEnabled(Map<String, Object> queryContext)
  {
    return parseBoolean(queryContext, TIME_BOUNDARY_PLANNING_KEY, DEFAULT_ENABLE_TIME_BOUNDARY_PLANNING);
  }

  public static <T> Query<T> withMaxScatterGatherBytes(Query<T> query, long maxScatterGatherBytesLimit)
  {
    Long curr = query.getQueryContext().getAsLong(MAX_SCATTER_GATHER_BYTES_KEY);
    if (curr == null) {
      return query.withOverriddenContext(ImmutableMap.of(MAX_SCATTER_GATHER_BYTES_KEY, maxScatterGatherBytesLimit));
    } else {
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
    return query.getQueryContext().getAsLong(MAX_QUEUED_BYTES_KEY, defaultValue);
  }

  public static <T> long getMaxScatterGatherBytes(Query<T> query)
  {
    return query.getQueryContext().getAsLong(MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE);
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
    try {
      final long timeout = query.getQueryContext().getAsLong(TIMEOUT_KEY, defaultTimeout);
      Preconditions.checkState(timeout >= 0, "Timeout must be a non negative value, but was [%s]", timeout);
      return timeout;
    }
    catch (IAE e) {
      throw new BadQueryContextException(e);
    }
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
    final long defaultTimeout = query.getQueryContext().getAsLong(DEFAULT_TIMEOUT_KEY, DEFAULT_TIMEOUT_MILLIS);
    Preconditions.checkState(defaultTimeout >= 0, "Timeout must be a non negative value, but was [%s]", defaultTimeout);
    return defaultTimeout;
  }

  public static <T> int getNumRetriesOnMissingSegments(Query<T> query, int defaultValue)
  {
    return query.getQueryContext().getAsInt(NUM_RETRIES_ON_MISSING_SEGMENTS_KEY, defaultValue);
  }

  public static <T> boolean allowReturnPartialResults(Query<T> query, boolean defaultValue)
  {
    return query.getContextBoolean(RETURN_PARTIAL_RESULTS_KEY, defaultValue);
  }

  public static String getBrokerServiceName(Map<String, Object> queryContext)
  {
    return queryContext == null ? null : (String) queryContext.get(BROKER_SERVICE_NAME);
  }

  @SuppressWarnings("unused")
  static <T> long parseLong(Map<String, Object> context, String key, long defaultValue)
  {
    return getAsLong(key, context.get(key), defaultValue);
  }

  static int parseInt(Map<String, Object> context, String key, int defaultValue)
  {
    return getAsInt(key, context.get(key), defaultValue);
  }

  static boolean parseBoolean(Map<String, Object> context, String key, boolean defaultValue)
  {
    return getAsBoolean(key, context.get(key), defaultValue);
  }

  public static String getAsString(
      final String key,
      final Object value,
      final String defaultValue
  )
  {
    if (value == null) {
      return defaultValue;
    } else if (value instanceof String) {
      return (String) value;
    } else {
      throw new IAE("Expected key [%s] to be a String, but got [%s]", key, value.getClass().getName());
    }
  }

  @Nullable
  public static Boolean getAsBoolean(
      final String parameter,
      final Object value
  )
  {
    if (value == null) {
      return null;
    } else if (value instanceof String) {
      return Boolean.parseBoolean((String) value);
    } else if (value instanceof Boolean) {
      return (Boolean) value;
    } else {
      throw new IAE("Expected parameter [%s] to be a Boolean, but got [%s]", parameter, value.getClass().getName());
    }
  }

  /**
   * Get the value of a parameter as a {@code boolean}. The parameter is expected
   * to be {@code null}, a string or a {@code Boolean} object.
   */
  public static boolean getAsBoolean(
      final String key,
      final Object value,
      final boolean defaultValue
  )
  {
    Boolean val = getAsBoolean(key, value);
    return val == null ? defaultValue : val;
  }

  @Nullable
  public static Integer getAsInt(String key, Object value)
  {
    if (value == null) {
      return null;
    } else if (value instanceof Number) {
      return ((Number) value).intValue();
    } else if (value instanceof String) {
      try {
        return Numbers.parseInt(value);
      }
      catch (NumberFormatException ignored) {
        throw new IAE("Expected key [%s] in integer format, but got [%s]", key, value);
      }
    }

    throw new IAE("Expected key [%s] to be an Integer, but got [%s]", key, value.getClass().getName());
  }

  /**
   * Get the value of a parameter as an {@code int}. The parameter is expected
   * to be {@code null}, a string or a {@code Number} object.
   */
  public static int getAsInt(
      final String ke,
      final Object value,
      final int defaultValue
  )
  {
    Integer val = getAsInt(ke, value);
    return val == null ? defaultValue : val;
  }

  @Nullable
  public static Long getAsLong(String key, Object value)
  {
    if (value == null) {
      return null;
    } else if (value instanceof Number) {
      return ((Number) value).longValue();
    } else if (value instanceof String) {
      try {
        return Numbers.parseLong(value);
      }
      catch (NumberFormatException ignored) {
        throw new IAE("Expected key [%s] in long format, but got [%s]", key, value);
      }
    }
    throw new IAE("Expected key [%s] to be a Long, but got [%s]", key, value.getClass().getName());
  }

  /**
   * Get the value of a parameter as an {@code long}. The parameter is expected
   * to be {@code null}, a string or a {@code Number} object.
   */
  public static long getAsLong(
      final String key,
      final Object value,
      final long defaultValue
  )
  {
    Long val = getAsLong(key, value);
    return val == null ? defaultValue : val;
  }

  public static HumanReadableBytes getAsHumanReadableBytes(
      final String parameter,
      final Object value,
      final HumanReadableBytes defaultValue
  )
  {
    if (null == value) {
      return defaultValue;
    } else if (value instanceof Number) {
      return HumanReadableBytes.valueOf(Numbers.parseLong(value));
    } else if (value instanceof String) {
      try {
        return HumanReadableBytes.valueOf(HumanReadableBytes.parse((String) value));
      }
      catch (IAE e) {
        throw new IAE("Expected key [%s] in human readable format, but got [%s]", parameter, value);
      }
    }

    throw new IAE("Expected key [%s] to be a human readable number, but got [%s]", parameter, value.getClass().getName());
  }

  public static float getAsFloat(String key, Object value, float defaultValue)
  {
    if (null == value) {
      return defaultValue;
    } else if (value instanceof Number) {
      return ((Number) value).floatValue();
    } else if (value instanceof String) {
      try {
        return Float.parseFloat((String) value);
      }
      catch (NumberFormatException ignored) {
        throw new IAE("Expected key [%s] in float format, but got [%s]", key, value);
      }
    }
    throw new IAE("Expected key [%s] to be a Float, but got [%s]", key, value.getClass().getName());
  }

  public static Map<String, Object> override(
      final Map<String, Object> context,
      final Map<String, Object> overrides
  )
  {
    Map<String, Object> overridden = new TreeMap<>();
    if (context != null) {
      overridden.putAll(context);
    }
    overridden.putAll(overrides);

    return overridden;
  }

  private QueryContexts()
  {
  }

  public static <E extends Enum<E>> E getAsEnum(String key, Object val, Class<E> clazz, E defaultValue)
  {
    if (val == null) {
      return defaultValue;
    }

    try {
      if (val instanceof String) {
        return Enum.valueOf(clazz, StringUtils.toUpperCase((String) val));
      } else if (val instanceof Boolean) {
        return Enum.valueOf(clazz, StringUtils.toUpperCase(String.valueOf(val)));
      }
    }
    catch (IllegalArgumentException e) {
      throw new IAE("Expected key [%s] must be value of enum [%s], but got [%s].",
                    key,
                    clazz.getName(),
                    val.toString());
    }

    throw new ISE(
        "Expected key [%s] must be type of [%s], actual type is [%s].",
        key,
        clazz.getName(),
        val.getClass()
    );
  }
}
