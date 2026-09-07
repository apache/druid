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
import org.apache.druid.server.coordinator.loading.PartialLoadProfile;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A mutable collection of metadata of {@link DataSegment}, belonging to a particular data source.
 * By default segments are stored as bare {@code DataSegment} when fully loading; segments that carry a
 * {@link PartialLoadProfile} are stored as {@link DataSegmentAndLoadProfile} (a {@code DataSegment} subclass) instead,
 * so partial-load metadata travels alongside the segment.
 *
 * @see ImmutableDruidDataSource - an immutable counterpart of this class
 */
public class DruidDataSource
{
  private final String name;
  private final Map<String, String> properties;
  /**
   * Concurrent to support concurrent iteration and updates. Values are bare {@link DataSegment} for full-load slots
   * and {@link DataSegmentAndLoadProfile} for partial-load slots.
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

  /**
   * Returns the partial-load profile for the given segment, or {@code null} if the segment was loaded as a regular
   * full-load (no partial-load metadata announced) or is not present in this data source.
   */
  @Nullable
  public PartialLoadProfile getPartialLoadProfile(SegmentId segmentId)
  {
    return DataSegmentAndLoadProfile.profileOf(idToSegmentMap.get(segmentId));
  }

  public Collection<DataSegment> getSegments()
  {
    return Collections.unmodifiableCollection(idToSegmentMap.values());
  }

  public DruidDataSource addSegment(DataSegment dataSegment)
  {
    return addSegment(dataSegment, null);
  }

  public DruidDataSource addSegment(DataSegment dataSegment, @Nullable PartialLoadProfile profile)
  {
    idToSegmentMap.put(dataSegment.getId(), wrapIfNeeded(dataSegment, profile));
    return this;
  }

  /**
   * Returns true if the segment was added, false if a segment with the same {@link SegmentId} already existed in this
   * DruidDataSource.
   */
  public boolean addSegmentIfAbsent(DataSegment dataSegment)
  {
    return addSegmentIfAbsent(dataSegment, null);
  }

  public boolean addSegmentIfAbsent(DataSegment dataSegment, @Nullable PartialLoadProfile profile)
  {
    return idToSegmentMap.putIfAbsent(dataSegment.getId(), wrapIfNeeded(dataSegment, profile)) == null;
  }

  /**
   * Returns the removed segment, or {@code null} if there was no segment with the given {@link SegmentId} in this
   * DruidDataSource. The returned value may be a {@link DataSegmentAndLoadProfile} when the removed entry carried a
   * partial-load profile; callers that need the profile or its {@code loadedBytes} should use
   * {@link DataSegmentAndLoadProfile#profileOf} / {@link DataSegmentAndLoadProfile#effectiveSizeOf}.
   */
  @Nullable
  public DataSegment removeSegment(SegmentId segmentId)
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

  private static DataSegment wrapIfNeeded(DataSegment dataSegment, @Nullable PartialLoadProfile profile)
  {
    return profile == null ? dataSegment : new DataSegmentAndLoadProfile(dataSegment, profile);
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
