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
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class SelectResultValue implements Iterable<EventHolder>
{
  private final Map<String, Integer> pagingIdentifiers;
  private final List<EventHolder> events;

  @JsonCreator
  public SelectResultValue(
      @JsonProperty("pagingIdentifiers") Map<String, Integer> pagingIdentifiers,
      @JsonProperty("events") List<EventHolder> events)
  {
    this.pagingIdentifiers = pagingIdentifiers;
    this.events = events;
  }

  public SelectResultValue(
      Object pagingIdentifiers,
      List<?> events
  )
  {
    this.pagingIdentifiers = null;
    this.events = null;
  }

  @JsonProperty
  public Map<String, Integer> getPagingIdentifiers()
  {
    return pagingIdentifiers;
  }

  @JsonProperty
  public List<EventHolder> getEvents()
  {
    return events;
  }

  @Override
  public Iterator<EventHolder> iterator()
  {
    return events.iterator();
  }
}
