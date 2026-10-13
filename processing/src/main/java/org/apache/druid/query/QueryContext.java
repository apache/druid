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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.QueryContexts.RealtimeSegmentsMode;
import org.apache.druid.query.QueryContexts.Vectorize;
import org.apache.druid.query.context.QueryContextParameter;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.TypedInFilter;

import javax.annotation.Nullable;
import java.io.IOException;
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

  /**
   * Creates a builder for a query context map.
   */
  public static QueryContextBuilder builder()
  {
    return new QueryContextBuilder();
  }

  public static QueryContext of(Map<String, Object> context)
  {
    return new QueryContext(context);
  }

  /**
   * Creates a query context from one declared query context parameter.
   */
  public static <T> QueryContext of(
      final QueryContextParameter<T> parameter,
      @Nullable final T value
  )
  {
    return new QueryContext(ofMap(parameter, value));
  }

  /**
   * Creates a query context map from one declared query context parameter.
   */
  public static <T> Map<String, Object> ofMap(
      final QueryContextParameter<T> parameter,
      @Nullable final T value
  )
  {
    return builder().put(parameter, value).toMap();
  }

  /**
   * Creates a query context from two declared query context parameters.
   */
  public static <T1, T2> QueryContext of(
      final QueryContextParameter<T1> parameter1,
      @Nullable final T1 value1,
      final QueryContextParameter<T2> parameter2,
      @Nullable final T2 value2
  )
  {
    return new QueryContext(ofMap(parameter1, value1, parameter2, value2));
  }

  /**
   * Creates a query context map from two declared query context parameters.
   */
  public static <T1, T2> Map<String, Object> ofMap(
      final QueryContextParameter<T1> parameter1,
      @Nullable final T1 value1,
      final QueryContextParameter<T2> parameter2,
      @Nullable final T2 value2
  )
  {
    return builder()
        .put(parameter1, value1)
        .put(parameter2, value2)
        .toMap();
  }

  /**
   * Creates a query context from three declared query context parameters.
   */
  public static <T1, T2, T3> QueryContext of(
      final QueryContextParameter<T1> parameter1,
      @Nullable final T1 value1,
      final QueryContextParameter<T2> parameter2,
      @Nullable final T2 value2,
      final QueryContextParameter<T3> parameter3,
      @Nullable final T3 value3
  )
  {
    return new QueryContext(ofMap(parameter1, value1, parameter2, value2, parameter3, value3));
  }

  /**
   * Creates a query context map from three declared query context parameters.
   */
  public static <T1, T2, T3> Map<String, Object> ofMap(
      final QueryContextParameter<T1> parameter1,
      @Nullable final T1 value1,
      final QueryContextParameter<T2> parameter2,
      @Nullable final T2 value2,
      final QueryContextParameter<T3> parameter3,
      @Nullable final T3 value3
  )
  {
    return builder()
        .put(parameter1, value1)
        .put(parameter2, value2)
        .put(parameter3, value3)
        .toMap();
  }

  /**
   * Creates a query context from four declared query context parameters.
   */
  public static <T1, T2, T3, T4> QueryContext of(
      final QueryContextParameter<T1> parameter1,
      @Nullable final T1 value1,
      final QueryContextParameter<T2> parameter2,
      @Nullable final T2 value2,
      final QueryContextParameter<T3> parameter3,
      @Nullable final T3 value3,
      final QueryContextParameter<T4> parameter4,
      @Nullable final T4 value4
  )
  {
    return new QueryContext(ofMap(
        parameter1,
        value1,
        parameter2,
        value2,
        parameter3,
        value3,
        parameter4,
        value4
    ));
  }

  /**
   * Creates a query context map from four declared query context parameters.
   */
  public static <T1, T2, T3, T4> Map<String, Object> ofMap(
      final QueryContextParameter<T1> parameter1,
      @Nullable final T1 value1,
      final QueryContextParameter<T2> parameter2,
      @Nullable final T2 value2,
      final QueryContextParameter<T3> parameter3,
      @Nullable final T3 value3,
      final QueryContextParameter<T4> parameter4,
      @Nullable final T4 value4
  )
  {
    return builder()
        .put(parameter1, value1)
        .put(parameter2, value2)
        .put(parameter3, value3)
        .put(parameter4, value4)
        .toMap();
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
   * Check if the given declared query context parameter is set to a non-null value. An explicit {@code null} is
   * treated the same as an absent key.
   */
  public boolean has(final QueryContextParameter<?> parameter)
  {
    return get(parameter.getName()) != null;
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
   * Returns the parsed parameter value. If the key is absent or explicitly {@code null}, returns the declared default,
   * or {@code null} if the parameter has no declared default.
   *
   * @throws BadQueryContextException if the value is invalid
   */
  @Nullable
  public <T> T get(final QueryContextParameter<T> parameter)
  {
    return parseOrDefault(parameter, parameter.getDefaultValue().orElse(null));
  }

  /**
   * Returns the parsed parameter value. If the key is absent or explicitly {@code null}, returns the declared default.
   *
   * @throws ISE if the parameter has no declared default
   * @throws BadQueryContextException if the value is invalid
   */
  public <T> T getOrDefault(final QueryContextParameter<T> parameter)
  {
    return getOrDefault(
        parameter,
        parameter.getDefaultValue().orElseThrow(
            () -> new ISE("Query context parameter [%s] has no declared default", parameter.getName())
        )
    );
  }

  /**
   * Returns the parsed parameter value. If the key is absent or explicitly {@code null}, returns the supplied default.
   *
   * @throws BadQueryContextException if the value is invalid
   */
  public <T> T getOrDefault(final QueryContextParameter<T> parameter, final T defaultValue)
  {
    return parseOrDefault(parameter, defaultValue);
  }

  @Nullable
  private <T> T parseOrDefault(final QueryContextParameter<T> parameter, @Nullable final T defaultValue)
  {
    if (!containsKey(parameter.getName())) {
      return defaultValue;
    }
    // Parse even an explicit null so that non-nullable parameters reject it.
    final T parsed = parameter.parse(get(parameter.getName()));
    return parsed == null ? defaultValue : parsed;
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
    return getOrDefault(QueryContextParameters.DEBUG);
  }

  public boolean isBySegment()
  {
    return getOrDefault(QueryContextParameters.BY_SEGMENT);
  }

  public boolean isBySegment(boolean defaultValue)
  {
    return getOrDefault(QueryContextParameters.BY_SEGMENT, defaultValue);
  }

  public boolean isPopulateCache()
  {
    return getOrDefault(QueryContextParameters.POPULATE_CACHE);
  }

  public boolean isPopulateCache(boolean defaultValue)
  {
    return getOrDefault(QueryContextParameters.POPULATE_CACHE, defaultValue);
  }

  public boolean isUseCache()
  {
    return getOrDefault(QueryContextParameters.USE_CACHE);
  }

  public boolean isUseCache(boolean defaultValue)
  {
    return getOrDefault(QueryContextParameters.USE_CACHE, defaultValue);
  }

  public boolean isPopulateResultLevelCache()
  {
    return getOrDefault(QueryContextParameters.POPULATE_RESULT_LEVEL_CACHE);
  }

  public boolean isPopulateResultLevelCache(boolean defaultValue)
  {
    return getOrDefault(QueryContextParameters.POPULATE_RESULT_LEVEL_CACHE, defaultValue);
  }

  public boolean isUseResultLevelCache()
  {
    return getOrDefault(QueryContextParameters.USE_RESULT_LEVEL_CACHE);
  }

  public boolean isUseResultLevelCache(boolean defaultValue)
  {
    return getOrDefault(QueryContextParameters.USE_RESULT_LEVEL_CACHE, defaultValue);
  }

  public boolean isFinalize(boolean defaultValue)

  {
    return getOrDefault(QueryContextParameters.FINALIZE, defaultValue);
  }

  public boolean isSerializeDateTimeAsLong(boolean defaultValue)
  {
    return getOrDefault(QueryContextParameters.SERIALIZE_DATE_TIME_AS_LONG, defaultValue);
  }

  public boolean isSerializeDateTimeAsLongInner(boolean defaultValue)
  {
    return getOrDefault(QueryContextParameters.SERIALIZE_DATE_TIME_AS_LONG_INNER, defaultValue);
  }

  public Vectorize getVectorize()
  {
    return getOrDefault(QueryContextParameters.VECTORIZE);
  }

  public Vectorize getVectorize(Vectorize defaultValue)
  {
    return getOrDefault(QueryContextParameters.VECTORIZE, defaultValue);
  }

  public Vectorize getVectorizeVirtualColumns()
  {
    return getOrDefault(QueryContextParameters.VECTORIZE_VIRTUAL_COLUMNS);
  }

  public Vectorize getVectorizeVirtualColumns(Vectorize defaultValue)
  {
    return getOrDefault(QueryContextParameters.VECTORIZE_VIRTUAL_COLUMNS, defaultValue);
  }

  public int getVectorSize()
  {
    return getOrDefault(QueryContextParameters.VECTOR_SIZE);
  }

  public int getVectorSize(int defaultSize)
  {
    return getOrDefault(QueryContextParameters.VECTOR_SIZE, defaultSize);
  }

  public int getMaxSubqueryRows(int defaultSize)
  {
    return getOrDefault(QueryContextParameters.MAX_SUBQUERY_ROWS, defaultSize);
  }

  public String getMaxSubqueryMemoryBytes(String defaultMemoryBytes)
  {
    // Generic to allow for both strings and numbers to be passed as values in the query context
    Object maxSubqueryBytesObject = get(QueryContextParameters.MAX_SUBQUERY_BYTES);
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
    return getOrDefault(QueryContextParameters.PRIORITY);
  }

  public int getPriority(int defaultValue)
  {
    return getOrDefault(QueryContextParameters.PRIORITY, defaultValue);
  }

  public String getLane()
  {
    return get(QueryContextParameters.LANE);
  }

  public boolean getEnableParallelMerges()
  {
    return getOrDefault(QueryContextParameters.ENABLE_PARALLEL_MERGE);
  }

  public int getParallelMergeInitialYieldRows(int defaultValue)
  {
    return getOrDefault(QueryContextParameters.PARALLEL_MERGE_INITIAL_YIELD_ROWS, defaultValue);
  }

  public int getParallelMergeSmallBatchRows(int defaultValue)
  {
    return getOrDefault(QueryContextParameters.PARALLEL_MERGE_SMALL_BATCH_ROWS, defaultValue);
  }

  public int getParallelMergeParallelism(int defaultValue)
  {
    return getOrDefault(QueryContextParameters.PARALLEL_MERGE_PARALLELISM, defaultValue);
  }

  public long getJoinFilterRewriteMaxSize()
  {
    return getOrDefault(QueryContextParameters.JOIN_FILTER_REWRITE_MAX_SIZE);
  }

  public boolean getEnableJoinFilterPushDown()
  {
    return getOrDefault(QueryContextParameters.ENABLE_JOIN_FILTER_PUSH_DOWN);
  }

  public boolean getEnableJoinFilterRewrite()
  {
    return getOrDefault(QueryContextParameters.ENABLE_JOIN_FILTER_REWRITE);
  }

  public boolean isSecondaryPartitionPruningEnabled()
  {
    return getOrDefault(QueryContextParameters.SECONDARY_PARTITION_PRUNING);
  }

  public boolean isOptimizeAggregators()
  {
    return getBoolean(
        QueryContexts.OPTIMIZE_AGGREGATORS_KEY,
        QueryContexts.DEFAULT_OPTIMIZE_AGGREGATORS
    );
  }

  public long getMaxQueuedBytes(long defaultValue)
  {
    return getOrDefault(QueryContextParameters.MAX_QUEUED_BYTES, defaultValue);
  }

  public long getMaxScatterGatherBytes()
  {
    return getOrDefault(QueryContextParameters.MAX_SCATTER_GATHER_BYTES, Long.MAX_VALUE);
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
    final long timeout = getOrDefault(QueryContextParameters.TIMEOUT, defaultTimeout);
    if (timeout >= 0) {
      return timeout;
    }
    throw new BadQueryContextException(
        StringUtils.format(
            "Timeout [%s] must be a non negative value, but was %d",
            QueryContextParameters.TIMEOUT,
            timeout
        )
    );
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
              QueryContextParameters.TIMEOUT,
              timeout,
              maxQueryTimeout
          )
      );
    }
  }

  public long getPerSegmentTimeout()
  {
    return getPerSegmentTimeout(QueryContexts.NO_TIMEOUT);
  }

  public long getPerSegmentTimeout(long defaultPerSegmentTimeout)
  {
    final long timeout = getOrDefault(QueryContextParameters.PER_SEGMENT_TIMEOUT, defaultPerSegmentTimeout);
    if (timeout >= 0) {
      return timeout;
    }

    throw new BadQueryContextException(
        StringUtils.format(
            "Per-segment timeout [%s] must be a non negative value, but was [%d]",
            QueryContextParameters.PER_SEGMENT_TIMEOUT,
            timeout
        )
    );
  }

  public boolean usePerSegmentTimeout()
  {
    return getPerSegmentTimeout() != QueryContexts.NO_TIMEOUT;
  }

  public void verifyMaxScatterGatherBytes(long maxScatterGatherBytesLimit)
  {
    final long curr = getOrDefault(QueryContextParameters.MAX_SCATTER_GATHER_BYTES, 0L);
    if (curr > maxScatterGatherBytesLimit) {
      throw new BadQueryContextException(
          StringUtils.format(
            "Configured %s = %d is more than enforced limit of %d.",
            QueryContextParameters.MAX_SCATTER_GATHER_BYTES,
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
    return getOrDefault(QueryContextParameters.ENABLE_JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS);
  }

  public CloneQueryMode getCloneQueryMode()
  {
    return getOrDefault(QueryContextParameters.CLONE_QUERY_MODE);
  }

  public boolean getEnableRewriteJoinToFilter()
  {
    return getOrDefault(QueryContextParameters.ENABLE_REWRITE_JOIN_TO_FILTER);
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
    return get(QueryContextParameters.BROKER_SERVICE);
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

  /**
   * Returns the realtime segments mode for this query. If {@link QueryContexts#REALTIME_SEGMENTS_MODE} is absent
   * or null, falls back to the deprecated {@code realtimeSegmentsOnly} boolean: {@code true} maps
   * to {@link RealtimeSegmentsMode#EXCLUSIVE}; otherwise returns {@link RealtimeSegmentsMode#INCLUDE}.
   * Throws {@link BadQueryContextException} if both fields are set simultaneously.
   */
  public RealtimeSegmentsMode getRealtimeSegmentsMode()
  {
    final RealtimeSegmentsMode mode = has(QueryContextParameters.REALTIME_SEGMENTS_MODE)
                                      ? get(QueryContextParameters.REALTIME_SEGMENTS_MODE)
                                      : null;
    final boolean hasDeprecatedFlag = has(QueryContextParameters.REALTIME_SEGMENTS_ONLY);
    if (mode != null && hasDeprecatedFlag) {
      throw new BadQueryContextException(
          StringUtils.format(
              "Cannot set both [%s] and deprecated [%s]; use [%s] only.",
              QueryContextParameters.REALTIME_SEGMENTS_MODE,
              QueryContextParameters.REALTIME_SEGMENTS_ONLY,
              QueryContextParameters.REALTIME_SEGMENTS_MODE
          )
      );
    }
    if (mode != null) {
      return mode;
    }
    if (hasDeprecatedFlag) {
      // Backward-compat: honour the deprecated realtimeSegmentsOnly flag.
      return getOrDefault(QueryContextParameters.REALTIME_SEGMENTS_ONLY)
             ? RealtimeSegmentsMode.EXCLUSIVE
             : QueryContexts.DEFAULT_REALTIME_SEGMENTS_MODE;
    }
    return QueryContexts.DEFAULT_REALTIME_SEGMENTS_MODE;
  }
}
