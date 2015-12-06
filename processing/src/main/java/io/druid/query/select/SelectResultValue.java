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
      @JsonProperty("events") List<EventHolder> events
  )
  {
    this.pagingIdentifiers = pagingIdentifiers;
    this.events = events;
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SelectResultValue that = (SelectResultValue) o;

    if (events != null ? !events.equals(that.events) : that.events != null) {
      return false;
    }
    if (pagingIdentifiers != null
        ? !pagingIdentifiers.equals(that.pagingIdentifiers)
        : that.pagingIdentifiers != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = pagingIdentifiers != null ? pagingIdentifiers.hashCode() : 0;
    result = 31 * result + (events != null ? events.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "SelectResultValue{" +
           "pagingIdentifiers=" + pagingIdentifiers +
           ", events=" + events +
           '}';
  }
}
