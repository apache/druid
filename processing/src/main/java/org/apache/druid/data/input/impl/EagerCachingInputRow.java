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

package org.apache.druid.data.input.impl;

import com.google.common.collect.Maps;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.Rows;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class EagerCachingInputRow implements InputRow
{
  final InputRow delegate;
  final Map<String, Object> cachedData;
  final List<String> dimensions;
  final DateTime timestamp;

  public EagerCachingInputRow(final InputRow delegate)
  {
    this.delegate = delegate;
    this.timestamp = delegate.getTimestamp();
    this.dimensions = delegate.getDimensions();
    this.cachedData = Maps.newHashMapWithExpectedSize(dimensions.size());
    for (String dimension : dimensions) {
      cachedData.put(dimension, delegate.getRaw(dimension));
    }
  }

  @Nullable
  public static EagerCachingInputRow cacheNullableRow(@Nullable final InputRow delegate)
  {
    return (delegate == null) ? null : new EagerCachingInputRow(delegate);
  }

  @Override
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @Override
  public long getTimestampFromEpoch()
  {
    return timestamp.getMillis();
  }

  @Override
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @Override
  public List<String> getDimension(String dimension)
  {
    return Rows.objectToStrings(getRaw(dimension));
  }

  @Nullable
  @Override
  public Object getRaw(String dimension)
  {
    if (cachedData.containsKey(dimension)) {
      return cachedData.get(dimension);
    } else {
      return delegate.getRaw(dimension);
    }
  }

  @Nullable
  @Override
  public Number getMetric(String metric)
  {
    return Rows.objectToNumber(metric, getRaw(metric), true);
  }

  @Override
  public int compareTo(Row row)
  {
    return timestamp.compareTo(row.getTimestamp());
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
    if (!super.equals(o)) {
      return false;
    }
    EagerCachingInputRow that = (EagerCachingInputRow) o;
    return Objects.equals(timestamp, that.timestamp) &&
            Objects.equals(dimensions, that.dimensions) &&
            Objects.equals(cachedData, that.cachedData) &&
            Objects.equals(delegate, that.delegate);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(delegate, cachedData, dimensions, timestamp);
  }

  @Override
  public String toString()
  {
    return "EagerCachingInputRow{" +
           "timestamp=" + DateTimes.utc(getTimestampFromEpoch()) +
           ", cachedData=" + cachedData +
           ", dimensions=" + dimensions +
           ", delegate=" + delegate +
           '}';
  }
}
