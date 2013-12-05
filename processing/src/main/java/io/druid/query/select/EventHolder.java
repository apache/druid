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
    return (DateTime) event.get(timestampKey);
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
}
