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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;

public class QueryContexts
{
  public static final String PRIORITY_KEY = "priority";
  public static final String TIMEOUT_KEY = "timeout";
  public static final String MAX_SCATTER_GATHER_BYTES_KEY = "maxScatterGatherBytes";
  public static final String DEFAULT_TIMEOUT_KEY = "defaultTimeout";
  public static final String CHUNK_PERIOD_KEY = "chunkPeriod";

  public static final boolean DEFAULT_BY_SEGMENT = false;
  public static final boolean DEFAULT_POPULATE_CACHE = true;
  public static final boolean DEFAULT_USE_CACHE = true;
  public static final int DEFAULT_PRIORITY = 0;
  public static final int DEFAULT_UNCOVERED_INTERVALS_LIMIT = 0;
  public static final long DEFAULT_TIMEOUT_MILLIS = 300_000; // 5 minutes
  public static final long NO_TIMEOUT = 0;

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

  public static <T> boolean isFinalize(Query<T> query, boolean defaultValue)
  {
    return parseBoolean(query, "finalize", defaultValue);
  }

  public static <T> boolean isSerializeDateTimeAsLong(Query<T> query, boolean defaultValue)
  {
    return parseBoolean(query, "serializeDateTimeAsLong", defaultValue);
  }

  public static <T> boolean isSerializeDateTimeAsLongInner(Query<T> query, boolean defaultValue)
  {
    return parseBoolean(query, "serializeDateTimeAsLongInner", defaultValue);
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

  public static <T> String getChunkPeriod(Query<T> query)
  {
    return query.getContextValue(CHUNK_PERIOD_KEY, "P0D");
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
    Object val = query.getContextValue(key);
    if (val == null) {
      return defaultValue;
    }
    if (val instanceof String) {
      return Long.parseLong((String) val);
    } else if (val instanceof Number) {
      return ((Number) val).longValue();
    } else {
      throw new ISE("Unknown type [%s]", val.getClass());
    }
  }

  static <T> int parseInt(Query<T> query, String key, int defaultValue)
  {
    Object val = query.getContextValue(key);
    if (val == null) {
      return defaultValue;
    }
    if (val instanceof String) {
      return Integer.parseInt((String) val);
    } else if (val instanceof Number) {
      return ((Number) val).intValue();
    } else {
      throw new ISE("Unknown type [%s]", val.getClass());
    }
  }

  static <T> boolean parseBoolean(Query<T> query, String key, boolean defaultValue)
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
}
