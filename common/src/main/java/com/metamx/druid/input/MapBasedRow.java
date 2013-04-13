/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.IAE;
import com.metamx.common.exception.FormattedException;

import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class MapBasedRow implements Row
{
  private final DateTime timestamp;
  private final Map<String, Object> event;

  @JsonCreator
  public MapBasedRow(
      @JsonProperty("timestamp") DateTime timestamp,
      @JsonProperty("event")  Map<String, Object> event
  )
  {
    this.timestamp = timestamp;
    this.event = event;
  }

  public MapBasedRow(
      long timestamp,
      Map<String, Object> event
  ) {
    this(new DateTime(timestamp), event);
  }

  @Override
  public long getTimestampFromEpoch()
  {
    return timestamp.getMillis();
  }

  @Override
  public List<String> getDimension(String dimension)
  {
    Object dimValue = event.get(dimension);

    if (dimValue == null) {
      return Lists.newArrayList();
    } else if (dimValue instanceof List) {
      return Lists.transform(
          (List) dimValue,
          new Function<Object, String>()
          {
            @Override
            public String apply(@Nullable Object input)
            {
              return String.valueOf(input);
            }
          }
      );
    } else if (dimValue instanceof Object) {
      return Arrays.asList(String.valueOf(event.get(dimension)));
    } else {
      throw new IAE("Unknown dim type[%s]", dimValue.getClass());
    }
  }

  @Override
  public float getFloatMetric(String metric)
  {
    Object metricValue = event.get(metric);

    if (metricValue == null) {
      return 0.0f;
    }

    if (metricValue instanceof Number) {
      return ((Number) metricValue).floatValue();
    } else if (metricValue instanceof String) {
      try {
        return Float.valueOf(((String) metricValue).replace(",", ""));
      }
      catch (Exception e) {
        throw new FormattedException.Builder()
            .withErrorCode(FormattedException.ErrorCode.UNPARSABLE_METRIC)
            .withDetails(ImmutableMap.<String, Object>of("metricName", metric, "metricValue", metricValue))
            .withMessage(e.getMessage())
            .build();
      }
    } else {
      throw new IAE("Unknown type[%s]", metricValue.getClass());
    }
  }

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
}
