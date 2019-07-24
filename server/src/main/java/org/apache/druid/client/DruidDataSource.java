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

package org.apache.druid.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

/**
 * A mutable collection of metadata of segments ({@link DataSegment} objects), belonging to a particular data source.
 *
 * Concurrency: could be updated concurrently via {@link #addSegment}, {@link #removeSegment}, and {@link
 * #removeSegmentsIf}, and accessed concurrently (e. g. via {@link #getSegments}) as well.
 *
 * @see ImmutableDruidDataSource - an immutable counterpart of this class
 */
public class DruidDataSource
{
  private final String name;
  private final Map<String, String> properties;
  /**
   * This map needs to be concurrent to support concurrent iteration and updates.
   */
  private final ConcurrentMap<SegmentId, DataSegment> idToSegmentMap = new ConcurrentHashMap<>();

  public DruidDataSource(String name, Map<String, String> properties)
  {
    this.name = Preconditions.checkNotNull(name);
    this.properties = properties;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public Map<String, String> getProperties()
  {
    return properties;
  }

  @Nullable
  public DataSegment getSegment(SegmentId segmentId)
  {
    return idToSegmentMap.get(segmentId);
  }

  public Collection<DataSegment> getSegments()
  {
    return Collections.unmodifiableCollection(idToSegmentMap.values());
  }

  /**
   * Removes segments for which the given filter returns true.
   */
  public void removeSegmentsIf(Predicate<DataSegment> filter)
  {
    idToSegmentMap.values().removeIf(filter);
  }

  public DruidDataSource addSegment(DataSegment dataSegment)
  {
    idToSegmentMap.put(dataSegment.getId(), dataSegment);
    return this;
  }

  /**
   * Returns true if the segment was added, false if a segment with the same {@link SegmentId} already existed in this
   * DruidDataSource.
   */
  public boolean addSegmentIfAbsent(DataSegment dataSegment)
  {
    return idToSegmentMap.putIfAbsent(dataSegment.getId(), dataSegment) == null;
  }

  /**
   * Returns the removed segment, or null if there was no segment with the given {@link SegmentId} in this
   * DruidDataSource.
   */
  public @Nullable DataSegment removeSegment(SegmentId segmentId)
  {
    return idToSegmentMap.remove(segmentId);
  }

  public boolean isEmpty()
  {
    return idToSegmentMap.isEmpty();
  }

  public ImmutableDruidDataSource toImmutableDruidDataSource()
  {
    return new ImmutableDruidDataSource(name, properties, idToSegmentMap);
  }

  @Override
  public String toString()
  {
    return "DruidDataSource{" +
           "properties=" + properties +
           ", partitions=" + idToSegmentMap.values() +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    throw new UnsupportedOperationException("Use ImmutableDruidDataSource instead");
  }

  @Override
  public int hashCode()
  {
    throw new UnsupportedOperationException("Use ImmutableDruidDataSource instead");
  }
}
