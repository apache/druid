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

package org.apache.druid.java.util.emitter.core;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.HashMap;
import java.util.Map;

/**
 * EventMap is a hash map implementation. It can be safely serialzed to JSON using Jackson serializer as it
 * respects the polymorphic annotations on entires (unlike standard Map). The example of polymorphic class is a query
 * interface, where different native query types are resolved by additional field called "queryType".
 * This implementation ensures that the annotation on the values are respected during serialization.
 */
@JsonSerialize(using = EventMapSerializer.class)
public class EventMap extends HashMap<String, Object>
{
  /**
   * Returns builder with Fluent API to build EventMap instance using method chaining
   */
  public static Builder builder()
  {
    return new Builder();
  }

  /**
   * Convert this EventMap to a builder. Performs copy of the whole EventMap.
   */
  public Builder asBuilder()
  {
    return new Builder().putAll(this);
  }

  public static class Builder
  {

    private final EventMap map;

    protected Builder()
    {
      map = new EventMap();
    }

    /**
     * Adds key -> value pair to the map
     */
    public Builder put(String key, Object value)
    {
      map.put(key, value);
      return this;
    }

    /**
     * Adds key -> value pair to the map only if value is not null
     */
    public Builder putNonNull(String key, Object value)
    {
      if (value != null) {
        map.put(key, value);
      }
      return this;
    }

    /**
     * Adds map entry to the map
     */
    public Builder put(Map.Entry<String, Object> entry)
    {
      map.put(entry.getKey(), entry.getValue());
      return this;
    }

    /**
     * Adds all key -> value pairs from other map
     */
    public Builder putAll(Map<? extends String, ? extends Object> other)
    {
      map.putAll(other);
      return this;
    }

    /**
     * Builds and returns the EventMap
     */
    public EventMap build()
    {
      return map;
    }
  }

}
