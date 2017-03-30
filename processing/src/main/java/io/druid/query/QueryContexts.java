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

import io.druid.java.util.common.ISE;

public class QueryContexts
{
  public static final String PRIORITY = "priority";
  public static final String TIMEOUT = "timeout";
  public static final String CHUNK_PERIOD = "chunkPeriod";

  public static final long DEFAULT_TIMEOUT = 0;

  public static <T> boolean isBySegment(Query<T> query, boolean defaultValue)
  {
    return parseBoolean(query, "bySegment", defaultValue);
  }

  public static <T> boolean isPopulateCache(Query<T> query, boolean defaultValue)
  {
    return parseBoolean(query, "populateCache", defaultValue);
  }

  public static <T> boolean isUseCache(Query<T> query, boolean defaultValue)
  {
    return parseBoolean(query, "useCache", defaultValue);
  }

  public static <T> boolean isFinalize(Query<T> query, boolean defaultValue)
  {
    return parseBoolean(query, "finalize", defaultValue);
  }

  public static <T> int getUncoveredIntervalsLimit(Query<T> query, int defaultValue)
  {
    return parseInt(query, "uncoveredIntervalsLimit", defaultValue);
  }

  public static <T> int getPriority(Query<T> query, int defaultValue)
  {
    return parseInt(query, PRIORITY, defaultValue);
  }

  public static <T> String getChunkPeriod(Query<T> query)
  {
    return query.getContextValue(CHUNK_PERIOD, "P0D");
  }

  public static <T> boolean hasTimeout(Query<T> query)
  {
    return getTimeout(query) != 0;
  }

  public static <T> long getTimeout(Query<T> query)
  {
    return getTimeout(query, DEFAULT_TIMEOUT);
  }

  public static <T> long getTimeout(Query<T> query, long defaultValue)
  {
    return parseLong(query, TIMEOUT, defaultValue);
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
