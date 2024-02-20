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
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
  public static final String MAX_SUBQUERY_BYTES_KEY = "maxSubqueryBytes";
  public static final String USE_NESTED_FOR_UNKNOWN_TYPE_IN_SUBQUERY = "useNestedForUnknownTypeInSubquery";
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
  public static final String MIN_TOP_N_THRESHOLD = "minTopNThreshold";
  public static final String WINDOWING_STRICT_VALIDATION = "windowingStrictValidation";


  // SQL query context keys
  public static final String CTX_SQL_QUERY_ID = BaseQuery.SQL_QUERY_ID;
  public static final String CTX_SQL_STRINGIFY_ARRAYS = "sqlStringifyArrays";

  // SQL statement resource specific keys
  public static final String CTX_EXECUTION_MODE = "executionMode";

  // Defaults
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
  public static final boolean DEFAULT_WINDOWING_STRICT_VALIDATION = true;

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

  private QueryContexts()
  {
  }

  public static long parseLong(Map<String, Object> context, String key, long defaultValue)
  {
    return getAsLong(key, context.get(key), defaultValue);
  }

  public static int parseInt(Map<String, Object> context, String key, int defaultValue)
  {
    return getAsInt(key, context.get(key), defaultValue);
  }

  @Nullable
  public static String parseString(Map<String, Object> context, String key)
  {
    return parseString(context, key, null);
  }

  public static boolean parseBoolean(Map<String, Object> context, String key, boolean defaultValue)
  {
    return getAsBoolean(key, context.get(key), defaultValue);
  }

  public static String parseString(Map<String, Object> context, String key, String defaultValue)
  {
    return getAsString(key, context.get(key), defaultValue);
  }

  @SuppressWarnings("unused") // To keep IntelliJ inspections happy
  public static float parseFloat(Map<String, Object> context, String key, float defaultValue)
  {
    return getAsFloat(key, context.get(key), defaultValue);
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
    }
    throw badTypeException(key, "a String", value);
  }

  @Nullable
  public static Boolean getAsBoolean(
      final String key,
      final Object value
  )
  {
    if (value == null) {
      return null;
    } else if (value instanceof String) {
      return Boolean.parseBoolean((String) value);
    } else if (value instanceof Boolean) {
      return (Boolean) value;
    }
    throw badTypeException(key, "a Boolean", value);
  }

  /**
   * Get the value of a context value as a {@code boolean}. The value is expected
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

        // Attempt to handle trivial decimal values: 12.00, etc.
        // This mimics how Jackson will convert "12.00" to a Integer on request.
        try {
          return new BigDecimal((String) value).intValueExact();
        }
        catch (Exception nfe) {
          // That didn't work either. Give up.
          throw badValueException(key, "in integer format", value);
        }
      }
    }

    throw badTypeException(key, "an Integer", value);
  }

  /**
   * Get the value of a context value as an {@code int}. The value is expected
   * to be {@code null}, a string or a {@code Number} object.
   */
  public static int getAsInt(
      final String key,
      final Object value,
      final int defaultValue
  )
  {
    Integer val = getAsInt(key, value);
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

        // Attempt to handle trivial decimal values: 12.00, etc.
        // This mimics how Jackson will convert "12.00" to a Long on request.
        try {
          return new BigDecimal((String) value).longValueExact();
        }
        catch (Exception nfe) {
          // That didn't work either. Give up.
          throw badValueException(key, "in long format", value);
        }
      }
    }
    throw badTypeException(key, "a Long", value);
  }

  /**
   * Get the value of a context value as an {@code long}. The value is expected
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

  /**
   * Get the value of a context value as an {@code Float}. The value is expected
   * to be {@code null}, a string or a {@code Number} object.
   */
  public static Float getAsFloat(final String key, final Object value)
  {
    if (value == null) {
      return null;
    } else if (value instanceof Number) {
      return ((Number) value).floatValue();
    } else if (value instanceof String) {
      try {
        return Float.parseFloat((String) value);
      }
      catch (NumberFormatException ignored) {
        throw badValueException(key, "in float format", value);
      }
    }
    throw badTypeException(key, "a Float", value);
  }

  public static float getAsFloat(
      final String key,
      final Object value,
      final float defaultValue
  )
  {
    Float val = getAsFloat(key, value);
    return val == null ? defaultValue : val;
  }

  public static HumanReadableBytes getAsHumanReadableBytes(
      final String key,
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
        throw badValueException(key, "a human readable number", value);
      }
    }

    throw badTypeException(key, "a human readable number", value);
  }

  /**
   * Insert, update or remove a single key to produce an overridden context.
   * Leaves the original context unchanged.
   *
   * @param context context to override
   * @param key     key to insert, update or remove
   * @param value   if {@code null}, remove the key. Otherwise, insert or replace
   *                the key.
   * @return a new context map
   */
  public static Map<String, Object> override(
      final Map<String, Object> context,
      final String key,
      final Object value
  )
  {
    Map<String, Object> overridden = new HashMap<>(context);
    if (value == null) {
      overridden.remove(key);
    } else {
      overridden.put(key, value);
    }
    return overridden;
  }

  /**
   * Insert or replace multiple keys to produce an overridden context.
   * Leaves the original context unchanged.
   *
   * @param context   context to override
   * @param overrides map of values to insert or replace
   * @return a new context map
   */
  public static Map<String, Object> override(
      final Map<String, Object> context,
      final Map<String, Object> overrides
  )
  {
    Map<String, Object> overridden = new HashMap<>();
    if (context != null) {
      overridden.putAll(context);
    }
    if (overrides != null) {
      overridden.putAll(overrides);
    }

    return overridden;
  }

  public static <E extends Enum<E>> E getAsEnum(String key, Object value, Class<E> clazz, E defaultValue)
  {
    E result = getAsEnum(key, value, clazz);
    if (result == null) {
      return defaultValue;
    } else {
      return result;
    }
  }


  @Nullable
  public static <E extends Enum<E>> E getAsEnum(String key, Object value, Class<E> clazz)
  {
    if (value == null) {
      return null;
    }

    try {
      if (value instanceof String) {
        return Enum.valueOf(clazz, StringUtils.toUpperCase((String) value));
      } else if (value instanceof Boolean) {
        return Enum.valueOf(clazz, StringUtils.toUpperCase(String.valueOf(value)));
      }
    }
    catch (IllegalArgumentException e) {
      throw badValueException(
          key,
          StringUtils.format(
              "referring to one of the values [%s] of enum [%s]",
              Arrays.stream(clazz.getEnumConstants()).map(E::name).collect(
                  Collectors.joining(",")),
              clazz.getSimpleName()
          ),
          value
      );
    }

    throw badTypeException(
        key,
        StringUtils.format("of type [%s]", clazz.getSimpleName()),
        value
    );
  }

  public static BadQueryContextException badValueException(
      final String key,
      final String expected,
      final Object actual
  )
  {
    return new BadQueryContextException(
        StringUtils.format(
            "Expected key [%s] to be %s, but got [%s]",
            key,
            expected,
            actual
        )
    );
  }

  public static BadQueryContextException badTypeException(
      final String key,
      final String expected,
      final Object actual
  )
  {
    return new BadQueryContextException(
        StringUtils.format(
            "Expected key [%s] to be %s, but got [%s]",
            key,
            expected,
            actual.getClass().getName()
        )
    );
  }

  public static void addDefaults(Map<String, Object> context, Map<String, Object> defaults)
  {
    for (Entry<String, Object> entry : defaults.entrySet()) {
      context.putIfAbsent(entry.getKey(), entry.getValue());
    }
  }
}
