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

package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import io.druid.guice.annotations.PublicApi;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.parsers.ParseException;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
@PublicApi
public class MapBasedRow implements Row
{
  private static final Long LONG_ZERO = 0L;

  private final DateTime timestamp;
  private final Map<String, Object> event;

  @JsonCreator
  public MapBasedRow(
      @JsonProperty("timestamp") DateTime timestamp,
      @JsonProperty("event") Map<String, Object> event
  )
  {
    this.timestamp = timestamp;
    this.event = event;
  }

  public MapBasedRow(
      long timestamp,
      Map<String, Object> event
  )
  {
    this(DateTimes.utc(timestamp), event);
  }

  @Override
  public long getTimestampFromEpoch()
  {
    return timestamp.getMillis();
  }

  @Override
  @JsonProperty
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @JsonProperty
  public Map<String, Object> getEvent()
  {
    return event;
  }

  @Override
  public List<String> getDimension(String dimension)
  {
    final Object dimValue = event.get(dimension);

    if (dimValue == null) {
      return Collections.emptyList();
    } else if (dimValue instanceof List) {
      // guava's toString function fails on null objects, so please do not use it
      return Lists.transform((List) dimValue, String::valueOf);
    } else {
      return Collections.singletonList(String.valueOf(dimValue));
    }
  }

  @Override
  public Object getRaw(String dimension)
  {
    return event.get(dimension);
  }

  @Override
  public Number getMetric(String metric)
  {
    Object metricValue = event.get(metric);

    if (metricValue == null) {
      return LONG_ZERO;
    }

    if (metricValue instanceof Number) {
      return (Number) metricValue;
    } else if (metricValue instanceof String) {
      try {
        String metricValueString = StringUtils.removeChar(((String) metricValue).trim(), ',');
        // Longs.tryParse() doesn't support leading '+', so we need to trim it ourselves
        metricValueString = trimLeadingPlusOfLongString(metricValueString);
        Long v = Longs.tryParse(metricValueString);
        // Do NOT use ternary operator here, because it makes Java to convert Long to Double
        if (v != null) {
          return v;
        } else {
          return Double.valueOf(metricValueString);
        }
      }
      catch (Exception e) {
        throw new ParseException(e, "Unable to parse metrics[%s], value[%s]", metric, metricValue);
      }
    } else {
      throw new ParseException("Unknown type[%s]", metricValue.getClass());
    }
  }

  private static String trimLeadingPlusOfLongString(String metricValueString)
  {
    if (metricValueString.length() > 1 && metricValueString.charAt(0) == '+') {
      char secondChar = metricValueString.charAt(1);
      if (secondChar >= '0' && secondChar <= '9') {
        metricValueString = metricValueString.substring(1);
      }
    }
    return metricValueString;
  }

  @Override
  public String toString()
  {
    return "MapBasedRow{" +
           "timestamp=" + timestamp +
           ", event=" + event +
           '}';
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

    MapBasedRow that = (MapBasedRow) o;

    if (!event.equals(that.event)) {
      return false;
    }
    if (!timestamp.equals(that.timestamp)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = timestamp.hashCode();
    result = 31 * result + event.hashCode();
    return result;
  }

  @Override
  public int compareTo(Row o)
  {
    return timestamp.compareTo(o.getTimestamp());
  }
}
