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

package org.apache.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
@PublicApi
public class MapBasedRow implements Row
{
  private final DateTime timestamp;
  private final Map<String, Object> event;

  @JsonCreator
  public MapBasedRow(
      @JsonProperty("timestamp") DateTime timestamp,
      @JsonProperty("event") Map<String, Object> event
  )
  {
    this.timestamp = timestamp;
    this.event = Collections.unmodifiableMap(Preconditions.checkNotNull(event, "event"));
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
    return Rows.objectToStrings(event.get(dimension));
  }

  @Override
  public Object getRaw(String dimension)
  {
    return event.get(dimension);
  }

  @Override
  public Number getMetric(String metric)
  {
    return Rows.objectToNumber(metric, event.get(metric), true);
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
