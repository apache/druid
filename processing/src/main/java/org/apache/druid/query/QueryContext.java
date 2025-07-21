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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.QueryContexts.Vectorize;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.TypedInFilter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Immutable holder for query context parameters with typed access methods.
 * Code builds up a map of context values from serialization or during
 * planning. Once that map is handed to the {@code QueryContext}, that map
 * is effectively immutable.
 * <p>
 * The implementation uses a {@link TreeMap} so that the serialized form of a query
 * lists context values in a deterministic order. Jackson will call
 * {@code getContext()} on the query, which will call {@link #asMap()} here,
 * which returns the sorted {@code TreeMap}.
 * <p>
 * The {@code TreeMap} is a mutable class. We'd prefer an immutable class, but
 * we can choose either ordering or immutability. Since the semantics of the context
 * is that it is immutable once it is placed in a query. Code should NEVER get the
 * context map from a query and modify it, even if the actual implementation
 * allows it.
 */
public class QueryContext
{
  private static final QueryContext EMPTY = new QueryContext(null);

  @JsonInclude(value = Include.NON_NULL)
  @JsonValue
  private final Map<String, Object> context;

  @JsonCreator
  public QueryContext(Map<String, Object> context)
  {
    // There is no semantic difference between an empty and a null context.
    // Ensure that a context always exists to avoid the need to check for
    // a null context. Jackson serialization will omit empty contexts.
    this.context = context == null
        ? Collections.emptyMap()
        : Collections.unmodifiableMap(new TreeMap<>(context));
  }

  public static QueryContext empty()
  {
    return EMPTY;
  }

  public static QueryContext of(Map<String, Object> context)
  {
    return new QueryContext(context);
  }

  public boolean isEmpty()
  {
    return context.isEmpty();
  }

  public Map<String, Object> asMap()
  {
    return context;
  }

  /**
   * Check if the given key is set. If the client will then fetch the value,
   * consider using one of the {@code get<Type>(String key)} methods instead:
   * they each return {@code null} if the value is not set.
   */
  public boolean containsKey(String key)
  {
    return context.containsKey(key);
  }

  /**
   * Return a value as a generic {@code Object}, returning {@code null} if the
   * context value is not set.
   */
  @Nullable
  public Object get(String key)
  {
    return context.get(key);
  }

  /**
   * Return a value as an {@code String}, returning {@link null} if the
   * context value is not set.
   *
   * @throws BadQueryContextException for an invalid value
   */
  @Nullable
  public String getString(String key)
  {
    return getString(key, null);
  }

  public String getString(String key, String defaultValue)
  {
    return QueryContexts.parseString(context, key, defaultValue);
  }

  /**
   * Return a value as an {@code Boolean}, returning {@link null} if the
   * context value is not set.
   *
   * @throws BadQueryContextException for an invalid value
   */
  public Boolean getBoolean(final String key)
  {
    return QueryContexts.getAsBoolean(key, get(key));
  }

  /**
   * Return a value as an {@code boolean}, returning the default value if the
   * context value is not set.
   *
   * @throws BadQueryContextException for an invalid value
   */
  public boolean getBoolean(final String key, final boolean defaultValue)
  {
    return QueryContexts.parseBoolean(context, key, defaultValue);
  }

  /**
   * Return a value as an {@code Integer}, returning {@link null} if the
   * context value is not set.
   *
   * @throws BadQueryContextException for an invalid value
   */
  public Integer getInt(final String key)
  {
    return QueryContexts.getAsInt(key, get(key));
  }

  /**
   * Return a value as an {@code int}, returning the default value if the
   * context value is not set.
   *
   * @throws BadQueryContextException for an invalid value
   */
  public int getInt(final String key, final int defaultValue)
  {
    return QueryContexts.parseInt(context, key, defaultValue);
  }

  /**
   * Return a value as an {@code Long}, returning {@link null} if the
   * context value is not set.
   *
   * @throws BadQueryContextException for an invalid value
   */
  public Long getLong(final String key)
  {
    return QueryContexts.getAsLong(key, get(key));
  }

  /**
   * Return a value as an {@code long}, returning the default value if the
   * context value is not set.
   *
   * @throws BadQueryContextException for an invalid value
   */
  public long getLong(final String key, final long defaultValue)
  {
    return QueryContexts.parseLong(context, key, defaultValue);
  }

  /**
   * Return a value as an {@code Float}, returning {@link null} if the
   * context value is not set.
   *
   * @throws BadQueryContextException for an invalid value
   */
  @SuppressWarnings("unused")
  public Float getFloat(final String key)
  {
    return QueryContexts.getAsFloat(key, get(key));
  }

  /**
   * Return a value as an {@code float}, returning the default value if the
   * context value is not set.
   *
   * @throws BadQueryContextException for an invalid value
   */
  public float getFloat(final String key, final float defaultValue)
  {
    return QueryContexts.getAsFloat(key, get(key), defaultValue);
  }

  public HumanReadableBytes getHumanReadableBytes(final String key, final HumanReadableBytes defaultValue)
  {
    return QueryContexts.getAsHumanReadableBytes(key, get(key), defaultValue);
  }

  public HumanReadableBytes getHumanReadableBytes(final String key, final long defaultBytes)
  {
    return QueryContexts.getAsHumanReadableBytes(key, get(key), HumanReadableBytes.valueOf(defaultBytes));
  }

  public <E extends Enum<E>> E getEnum(String key, Class<E> clazz, E defaultValue)
  {
    return QueryContexts.getAsEnum(key, get(key), clazz, defaultValue);
  }

  public Granularity getGranularity(String key, ObjectMapper jsonMapper)
  {
    final String granularityString = getString(key);
    if (granularityString == null) {
      return null;
    }

    try {
      return jsonMapper.readValue(granularityString, Granularity.class);
    }
    catch (IOException e) {
      throw QueryContexts.badTypeException(key, "a Granularity", granularityString);
    }
  }

  public boolean isDebug()
  {
    return getBoolean(QueryContexts.ENABLE_DEBUG, QueryContexts.DEFAULT_ENABLE_DEBUG);
  }

  public boolean isBySegment()
  {
    return isBySegment(QueryContexts.DEFAULT_BY_SEGMENT);
  }

  public boolean isBySegment(boolean defaultValue)
  {
    return getBoolean(QueryContexts.BY_SEGMENT_KEY, defaultValue);
  }

  public boolean isPopulateCache()
  {
    return isPopulateCache(QueryContexts.DEFAULT_POPULATE_CACHE);
  }

  public boolean isPopulateCache(boolean defaultValue)
  {
    return getBoolean(QueryContexts.POPULATE_CACHE_KEY, defaultValue);
  }

  public boolean isUseCache()
  {
    return isUseCache(QueryContexts.DEFAULT_USE_CACHE);
  }

  public boolean isUseCache(boolean defaultValue)
  {
    return getBoolean(QueryContexts.USE_CACHE_KEY, defaultValue);
  }

  public boolean isPopulateResultLevelCache()
  {
    return isPopulateResultLevelCache(QueryContexts.DEFAULT_POPULATE_RESULTLEVEL_CACHE);
  }

  public boolean isPopulateResultLevelCache(boolean defaultValue)
  {
    return getBoolean(QueryContexts.POPULATE_RESULT_LEVEL_CACHE_KEY, defaultValue);
  }

  public boolean isUseResultLevelCache()
  {
    return isUseResultLevelCache(QueryContexts.DEFAULT_USE_RESULTLEVEL_CACHE);
  }

  public boolean isUseResultLevelCache(boolean defaultValue)
  {
    return getBoolean(QueryContexts.USE_RESULT_LEVEL_CACHE_KEY, defaultValue);
  }

  public boolean isFinalize(boolean defaultValue)

  {
    return getBoolean(QueryContexts.FINALIZE_KEY, defaultValue);
  }

  public boolean isSerializeDateTimeAsLong(boolean defaultValue)
  {
    return getBoolean(QueryContexts.SERIALIZE_DATE_TIME_AS_LONG_KEY, defaultValue);
  }

  public boolean isSerializeDateTimeAsLongInner(boolean defaultValue)
  {
    return getBoolean(QueryContexts.SERIALIZE_DATE_TIME_AS_LONG_INNER_KEY, defaultValue);
  }

  public Vectorize getVectorize()
  {
    return getVectorize(QueryContexts.DEFAULT_VECTORIZE);
  }

  public Vectorize getVectorize(Vectorize defaultValue)
  {
    return getEnum(QueryContexts.VECTORIZE_KEY, Vectorize.class, defaultValue);
  }

  public Vectorize getVectorizeVirtualColumns()
  {
    return getVectorizeVirtualColumns(QueryContexts.DEFAULT_VECTORIZE_VIRTUAL_COLUMN);
  }

  public Vectorize getVectorizeVirtualColumns(Vectorize defaultValue)
  {
    return getEnum(
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY,
        Vectorize.class,
        defaultValue
    );
  }

  public int getVectorSize()
  {
    return getVectorSize(QueryContexts.DEFAULT_VECTOR_SIZE);
  }

  public int getVectorSize(int defaultSize)
  {
    return getInt(QueryContexts.VECTOR_SIZE_KEY, defaultSize);
  }

  public int getMaxSubqueryRows(int defaultSize)
  {
    return getInt(QueryContexts.MAX_SUBQUERY_ROWS_KEY, defaultSize);
  }

  public String getMaxSubqueryMemoryBytes(String defaultMemoryBytes)
  {
    // Generic to allow for both strings and numbers to be passed as values in the query context
    Object maxSubqueryBytesObject = get(QueryContexts.MAX_SUBQUERY_BYTES_KEY);
    if (maxSubqueryBytesObject == null) {
      maxSubqueryBytesObject = defaultMemoryBytes;
    }
    return String.valueOf(maxSubqueryBytesObject);
  }

  public boolean isUseNestedForUnknownTypeInSubquery(boolean defaultUseNestedForUnkownTypeInSubquery)
  {
    return getBoolean(QueryContexts.USE_NESTED_FOR_UNKNOWN_TYPE_IN_SUBQUERY, defaultUseNestedForUnkownTypeInSubquery);
  }

  public boolean isUseNestedForUnknownTypeInSubquery()
  {
    return isUseNestedForUnknownTypeInSubquery(QueryContexts.DEFAULT_USE_NESTED_FOR_UNKNOWN_TYPE_IN_SUBQUERY);
  }

  public int getUncoveredIntervalsLimit()
  {
    return getUncoveredIntervalsLimit(QueryContexts.DEFAULT_UNCOVERED_INTERVALS_LIMIT);
  }

  public int getUncoveredIntervalsLimit(int defaultValue)
  {
    return getInt(QueryContexts.UNCOVERED_INTERVALS_LIMIT_KEY, defaultValue);
  }

  public int getPriority()
  {
    return getPriority(QueryContexts.DEFAULT_PRIORITY);
  }

  public int getPriority(int defaultValue)
  {
    return getInt(QueryContexts.PRIORITY_KEY, defaultValue);
  }

  public String getLane()
  {
    return getString(QueryContexts.LANE_KEY);
  }

  public boolean getEnableParallelMerges()
  {
    return getBoolean(
        QueryContexts.BROKER_PARALLEL_MERGE_KEY,
        QueryContexts.DEFAULT_ENABLE_PARALLEL_MERGE
    );
  }

  public int getParallelMergeInitialYieldRows(int defaultValue)
  {
    return getInt(QueryContexts.BROKER_PARALLEL_MERGE_INITIAL_YIELD_ROWS_KEY, defaultValue);
  }

  public int getParallelMergeSmallBatchRows(int defaultValue)
  {
    return getInt(QueryContexts.BROKER_PARALLEL_MERGE_SMALL_BATCH_ROWS_KEY, defaultValue);
  }

  public int getParallelMergeParallelism(int defaultValue)
  {
    return getInt(QueryContexts.BROKER_PARALLELISM, defaultValue);
  }

  public long getJoinFilterRewriteMaxSize()
  {
    return getLong(
        QueryContexts.JOIN_FILTER_REWRITE_MAX_SIZE_KEY,
        QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE
    );
  }

  public boolean getEnableJoinFilterPushDown()
  {
    return getBoolean(
        QueryContexts.JOIN_FILTER_PUSH_DOWN_KEY,
        QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_PUSH_DOWN
    );
  }

  public boolean getEnableJoinFilterRewrite()
  {
    return getBoolean(
        QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY,
        QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE
    );
  }

  public boolean isSecondaryPartitionPruningEnabled()
  {
    return getBoolean(
        QueryContexts.SECONDARY_PARTITION_PRUNING_KEY,
        QueryContexts.DEFAULT_SECONDARY_PARTITION_PRUNING
    );
  }

  public long getMaxQueuedBytes(long defaultValue)
  {
    return getLong(QueryContexts.MAX_QUEUED_BYTES_KEY, defaultValue);
  }

  public long getMaxScatterGatherBytes()
  {
    return getLong(QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE);
  }

  public String getEngine()
  {
    return QueryContexts.parseString(
        context,
        QueryContexts.ENGINE,
        QueryContexts.DEFAULT_ENGINE
    );
  }

  public boolean hasTimeout()
  {
    return getTimeout() != QueryContexts.NO_TIMEOUT;
  }

  public long getTimeout()
  {
    return getTimeout(getDefaultTimeout());
  }

  public long getTimeout(long defaultTimeout)
  {
    final long timeout = getLong(QueryContexts.TIMEOUT_KEY, defaultTimeout);
    if (timeout >= 0) {
      return timeout;
    }
    throw new BadQueryContextException(
        StringUtils.format(
            "Timeout [%s] must be a non negative value, but was %d",
            QueryContexts.TIMEOUT_KEY,
            timeout
        )
    );
  }

  @Nullable
  public Duration getTimeoutDuration()
  {
    if (hasTimeout()) {
      return Duration.ofMillis(getTimeout());
    }
    return null;
  }

  public long getDefaultTimeout()
  {
    final long defaultTimeout = getLong(QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS);
    if (defaultTimeout >= 0) {
      return defaultTimeout;
    }
    throw new BadQueryContextException(
        StringUtils.format(
            "Timeout [%s] must be a non negative value, but was %d",
            QueryContexts.DEFAULT_TIMEOUT_KEY,
            defaultTimeout
        )
    );
  }

  public void verifyMaxQueryTimeout(long maxQueryTimeout)
  {
    long timeout = getTimeout();
    if (timeout > maxQueryTimeout) {
      throw new BadQueryContextException(
          StringUtils.format(
              "Configured %s = %d is more than enforced limit of %d.",
              QueryContexts.TIMEOUT_KEY,
              timeout,
              maxQueryTimeout
          )
      );
    }
  }

  public void verifyMaxScatterGatherBytes(long maxScatterGatherBytesLimit)
  {
    long curr = getLong(QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, 0);
    if (curr > maxScatterGatherBytesLimit) {
      throw new BadQueryContextException(
          StringUtils.format(
            "Configured %s = %d is more than enforced limit of %d.",
            QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY,
            curr,
            maxScatterGatherBytesLimit
          )
      );
    }
  }

  public int getNumRetriesOnMissingSegments(int defaultValue)
  {
    return getInt(QueryContexts.NUM_RETRIES_ON_MISSING_SEGMENTS_KEY, defaultValue);
  }

  public boolean allowReturnPartialResults(boolean defaultValue)
  {
    return getBoolean(QueryContexts.RETURN_PARTIAL_RESULTS_KEY, defaultValue);
  }

  public boolean getEnableJoinFilterRewriteValueColumnFilters()
  {
    return getBoolean(
        QueryContexts.JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY,
        QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS
    );
  }

  public CloneQueryMode getCloneQueryMode()
  {
    return getEnum(
        QueryContexts.CLONE_QUERY_MODE,
        CloneQueryMode.class,
        QueryContexts.DEFAULT_CLONE_QUERY_MODE
    );
  }

  public boolean getEnableRewriteJoinToFilter()
  {
    return getBoolean(
        QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY,
        QueryContexts.DEFAULT_ENABLE_REWRITE_JOIN_TO_FILTER
    );
  }

  public boolean getEnableJoinLeftScanDirect()
  {
    return getBoolean(
        QueryContexts.SQL_JOIN_LEFT_SCAN_DIRECT,
        QueryContexts.DEFAULT_ENABLE_SQL_JOIN_LEFT_SCAN_DIRECT
    );
  }

  public int getInSubQueryThreshold()
  {
    return getInSubQueryThreshold(QueryContexts.DEFAULT_IN_SUB_QUERY_THRESHOLD);
  }

  public int getInSubQueryThreshold(int defaultValue)
  {
    return getInt(
        QueryContexts.IN_SUB_QUERY_THRESHOLD_KEY,
        defaultValue
    );
  }

  /**
   * At or above this threshold number of values, when planning SQL queries, use the SQL SCALAR_IN_ARRAY operator rather
   * than a stack of SQL ORs. This speeds up planning for large sets of points because it is opaque to various
   * expensive optimizations. But, because this does bypass certain optimizations, we only do the transformation above
   * a certain threshold. The SCALAR_IN_ARRAY operator is still able to convert to {@link InDimFilter} or
   * {@link TypedInFilter}.
   */
  public int getInFunctionThreshold()
  {
    return getInt(
        QueryContexts.IN_FUNCTION_THRESHOLD,
        QueryContexts.DEFAULT_IN_FUNCTION_THRESHOLD
    );
  }

  /**
   * At or above this threshold, when converting the SEARCH operator to a native expression, use the "scalar_in_array"
   * function rather than a sequence of equals (==) separated by or (||). This is typically a lower threshold
   * than {@link #getInFunctionThreshold()}, because it does not prevent any SQL planning optimizations, and it
   * speeds up query execution.
   */
  public int getInFunctionExprThreshold()
  {
    return getInt(
        QueryContexts.IN_FUNCTION_EXPR_THRESHOLD,
        QueryContexts.DEFAULT_IN_FUNCTION_EXPR_THRESHOLD
    );
  }

  public boolean isTimeBoundaryPlanningEnabled()
  {
    return getBoolean(
        QueryContexts.TIME_BOUNDARY_PLANNING_KEY,
        QueryContexts.DEFAULT_ENABLE_TIME_BOUNDARY_PLANNING
    );
  }

  public boolean isCatalogValidationEnabled()
  {
    return getBoolean(
        QueryContexts.CATALOG_VALIDATION_ENABLED,
        QueryContexts.DEFAULT_CATALOG_VALIDATION_ENABLED
    );
  }

  public boolean isExtendedFilteredSumRewrite()
  {
    return getBoolean(
        QueryContexts.EXTENDED_FILTERED_SUM_REWRITE_ENABLED,
        QueryContexts.DEFAULT_EXTENDED_FILTERED_SUM_REWRITE_ENABLED
    );
  }

  /**
   * Returns true if {@link QueryContexts#CTX_FULL_REPORT} is set to true, false if it is set to false or not set.
   */
  public boolean getFullReport()
  {
    return getBoolean(
        QueryContexts.CTX_FULL_REPORT,
        QueryContexts.DEFAULT_CTX_FULL_REPORT
    );
  }


  public QueryResourceId getQueryResourceId()
  {
    return new QueryResourceId(getString(QueryContexts.QUERY_RESOURCE_ID));
  }

  public String getBrokerServiceName()
  {
    return getString(QueryContexts.BROKER_SERVICE_NAME);
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
    QueryContext other = (QueryContext) o;
    return context.equals(other.context);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(context);
  }

  @Override
  public String toString()
  {
    return "QueryContext{" +
           "context=" + context +
           '}';
  }

  public boolean isDecoupledMode()
  {
    String value = getString(
        QueryContexts.CTX_NATIVE_QUERY_SQL_PLANNING_MODE,
        QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_COUPLED
    );
    return QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED.equals(value);
  }

  public QueryContext override(Map<String, Object> contextOverride)
  {
    if (contextOverride == null || contextOverride.isEmpty()) {
      return this;
    }
    return QueryContext.of(QueryContexts.override(asMap(), contextOverride));
  }

  public QueryContext override(QueryContext queryContext)
  {
    if (queryContext == null || queryContext.isEmpty()) {
      return this;
    }
    return override(queryContext.asMap());
  }

  public boolean isPrePlanned()
  {
    return getBoolean(QueryContexts.CTX_PREPLANNED, QueryContexts.DEFAULT_PREPLANNED);
  }
}
