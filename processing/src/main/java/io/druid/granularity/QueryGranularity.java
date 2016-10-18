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

package io.druid.granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.druid.java.util.common.IAE;
import org.joda.time.DateTime;
import org.joda.time.ReadableDuration;

import java.util.List;
import java.util.Objects;

public abstract class QueryGranularity
{
  public abstract long next(long offset);

  public abstract long truncate(long offset);

  public abstract byte[] cacheKey();

  public abstract DateTime toDateTime(long offset);

  public abstract Iterable<Long> iterable(final long start, final long end);

  @JsonCreator
  public static QueryGranularity fromString(String str)
  {
    String name = str.toUpperCase();
    if (name.equals("ALL")) {
      return QueryGranularities.ALL;
    } else if (name.equals("NONE")) {
      return QueryGranularities.NONE;
    } else if (QueryGranularities.CALENDRIC_GRANULARITIES.containsKey(name)) {
      return QueryGranularities.CALENDRIC_GRANULARITIES.get(name);
    }
    return new DurationGranularity(convertValue(str), 0);
  }

  private static enum MillisIn
  {
    SECOND(1000),
    MINUTE(60 * 1000),
    FIFTEEN_MINUTE(15 * 60 * 1000),
    THIRTY_MINUTE(30 * 60 * 1000),
    HOUR(3600 * 1000),
    DAY(24 * 3600 * 1000);

    private final long millis;

    MillisIn(final long millis) { this.millis = millis; }
  }

  private static long convertValue(Object o)
  {
    if (o instanceof String) {
      return MillisIn.valueOf(((String) o).toUpperCase()).millis;
    } else if (o instanceof ReadableDuration) {
      return ((ReadableDuration) o).getMillis();
    } else if (o instanceof Number) {
      return ((Number) o).longValue();
    }
    throw new IAE("Cannot convert [%s] to QueryGranularity", o.getClass());
  }

  //simple merge strategy on query granularity that checks if all are equal or else
  //returns null. this can be improved in future but is good enough for most use-cases.
  public static QueryGranularity mergeQueryGranularities(List<QueryGranularity> toMerge)
  {
    if (toMerge == null || toMerge.size() == 0) {
      return null;
    }

    QueryGranularity result = toMerge.get(0);
    for (int i = 1; i < toMerge.size(); i++) {
      if (!Objects.equals(result, toMerge.get(i))) {
        return null;
      }
    }

    return result;
  }
}
