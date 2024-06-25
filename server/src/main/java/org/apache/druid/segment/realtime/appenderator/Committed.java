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

  // Map of segment identifierAsString -> number of committed PartialSegment
  private final ImmutableMap<String, Integer> partialSegments;
  private final Object metadata;

  @JsonCreator
  public Committed(
      @JsonProperty("hydrants") Map<String, Integer> partialSegments,
      @JsonProperty("metadata") Object metadata
  )
  {
    this.partialSegments = ImmutableMap.copyOf(partialSegments);
    this.metadata = metadata;
  }

  public static Committed create(
      Map<SegmentIdWithShardSpec, Integer> newPartialSegments,
      Object metadata
  )
  {
    final ImmutableMap.Builder<String, Integer> partialSegments = ImmutableMap.builder();
    for (Map.Entry<SegmentIdWithShardSpec, Integer> entry : newPartialSegments.entrySet()) {
      partialSegments.put(entry.getKey().toString(), entry.getValue());
    }
    return new Committed(partialSegments.build(), metadata);
  }

  @JsonProperty
  public ImmutableMap<String, Integer> getPartialSegments()
  {
    return partialSegments;
  }

  @JsonProperty
  public Object getMetadata()
  {
    return metadata;
  }

  public int getCommittedPartialSegments(final String identifierAsString)
  {
    final Integer committedPartialSegment = partialSegments.get(identifierAsString);
    return committedPartialSegment == null ? 0 : committedPartialSegment;
  }

  public Committed without(final String identifierAsString)
  {
    final Map<String, Integer> newPartialSegments = new HashMap<>(partialSegments);
    newPartialSegments.remove(identifierAsString);
    return new Committed(newPartialSegments, metadata);
  }

  public Committed with(final Map<String, Integer> partialSegmentsToAdd)
  {
    final Map<String, Integer> newPartialSegments = new HashMap<>();
    newPartialSegments.putAll(partialSegments);
    newPartialSegments.putAll(partialSegmentsToAdd);
    return new Committed(newPartialSegments, metadata);
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
    return Objects.equals(partialSegments, committed.partialSegments) &&
           Objects.equals(metadata, committed.metadata);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(partialSegments, metadata);
  }

  @Override
  public String toString()
  {
    return "Committed{" +
           "hydrants=" + partialSegments +
           ", metadata=" + metadata +
           '}';
  }

  public static Committed nil()
  {
    return NIL;
  }
}
