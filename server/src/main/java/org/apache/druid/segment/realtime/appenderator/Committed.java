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

package org.apache.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Committed
{
  private static final Committed NIL = new Committed(ImmutableMap.of(), null);

  // Map of segment identifierAsString -> number of committed hydrants
  private final ImmutableMap<String, Integer> hydrants;
  private final Object metadata;

  @JsonCreator
  public Committed(
      @JsonProperty("hydrants") Map<String, Integer> hydrants,
      @JsonProperty("metadata") Object metadata
  )
  {
    this.hydrants = ImmutableMap.copyOf(hydrants);
    this.metadata = metadata;
  }

  public static Committed create(
      Map<SegmentIdWithShardSpec, Integer> hydrants0,
      Object metadata
  )
  {
    final ImmutableMap.Builder<String, Integer> hydrants = ImmutableMap.builder();
    for (Map.Entry<SegmentIdWithShardSpec, Integer> entry : hydrants0.entrySet()) {
      hydrants.put(entry.getKey().toString(), entry.getValue());
    }
    return new Committed(hydrants.build(), metadata);
  }

  @JsonProperty
  public ImmutableMap<String, Integer> getHydrants()
  {
    return hydrants;
  }

  @JsonProperty
  public Object getMetadata()
  {
    return metadata;
  }

  public int getCommittedHydrants(final String identifierAsString)
  {
    final Integer committedHydrant = hydrants.get(identifierAsString);
    return committedHydrant == null ? 0 : committedHydrant;
  }

  public Committed without(final String identifierAsString)
  {
    final Map<String, Integer> newHydrants = new HashMap<>();
    newHydrants.putAll(hydrants);
    newHydrants.remove(identifierAsString);
    return new Committed(newHydrants, metadata);
  }

  public Committed with(final Map<String, Integer> hydrantsToAdd)
  {
    final Map<String, Integer> newHydrants = new HashMap<>();
    newHydrants.putAll(hydrants);
    newHydrants.putAll(hydrantsToAdd);
    return new Committed(newHydrants, metadata);
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
    Committed committed = (Committed) o;
    return Objects.equals(hydrants, committed.hydrants) &&
           Objects.equals(metadata, committed.metadata);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(hydrants, metadata);
  }

  @Override
  public String toString()
  {
    return "Committed{" +
           "hydrants=" + hydrants +
           ", metadata=" + metadata +
           '}';
  }

  public static Committed nil()
  {
    return NIL;
  }
}
