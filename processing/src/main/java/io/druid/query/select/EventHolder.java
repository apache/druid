/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
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
    if (retVal instanceof String) {
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
