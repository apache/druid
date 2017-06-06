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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import io.druid.java.util.common.ISE;
import org.joda.time.DateTime;

import java.util.Map;

/**
 */
public class EventHolder
{
  public static final String timestampKey = "timestamp";

  private final String segmentId;
  private final int offset;
  private final Map<String, Object> event;

  @JsonCreator
  public EventHolder(
      @JsonProperty("segmentId") String segmentId,
      @JsonProperty("offset") int offset,
      @JsonProperty("event") Map<String, Object> event
  )
  {
    this.segmentId = segmentId;
    this.offset = offset;
    this.event = event;
  }

  public DateTime getTimestamp()
  {
    Object retVal = event.get(timestampKey);
    if (retVal instanceof String || retVal instanceof Long) {
      return new DateTime(retVal);
    } else if (retVal instanceof DateTime) {
      return (DateTime) retVal;
    } else {
      throw new ISE("Do not understand format [%s]", retVal.getClass());
    }
  }

  @JsonProperty
  public String getSegmentId()
  {
    return segmentId;
  }

  @JsonProperty
  public int getOffset()
  {
    return offset;
  }

  @JsonProperty
  public Map<String, Object> getEvent()
  {
    return event;
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

    EventHolder that = (EventHolder) o;

    if (offset != that.offset) {
      return false;
    }
    if (!Maps.difference(event, ((EventHolder) o).event).areEqual()) {
      return false;
    }
    if (segmentId != null ? !segmentId.equals(that.segmentId) : that.segmentId != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = segmentId != null ? segmentId.hashCode() : 0;
    result = 31 * result + offset;
    result = 31 * result + (event != null ? event.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "EventHolder{" +
           "segmentId='" + segmentId + '\'' +
           ", offset=" + offset +
           ", event=" + event +
           '}';
  }
}
