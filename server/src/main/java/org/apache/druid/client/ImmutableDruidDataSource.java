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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import org.apache.druid.server.coordinator.loading.PartialLoadProfile;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * An immutable collection of metadata of segments ({@link DataSegment} objects), belonging to a particular data
 * source. Like {@link DruidDataSource}, partial-load segments are stored as {@link DataSegmentAndLoadProfile} (a
 * {@link DataSegment} subclass); full-load segments are stored as bare {@code DataSegment} so the common case pays no
 * per-segment wrapper overhead.
 *
 * @see DruidDataSource - a mutable counterpart of this class
 */
public class ImmutableDruidDataSource
{
  private final String name;
  private final ImmutableMap<String, String> properties;
  private final ImmutableSortedMap<SegmentId, DataSegment> idToSegments;
  private final long totalSizeOfSegments;

  /**
   * Concurrency: idToSegments argument might be a {@link java.util.concurrent.ConcurrentMap} that is being updated
   * concurrently while this constructor is executed.
   */
  public ImmutableDruidDataSource(
      String name,
      Map<String, String> properties,
      Map<SegmentId, DataSegment> idToSegments
  )
  {
    this.name = Preconditions.checkNotNull(name);
    this.properties = ImmutableMap.copyOf(properties);
    this.idToSegments = ImmutableSortedMap.copyOf(idToSegments);
    this.totalSizeOfSegments = this.idToSegments.values().stream()
                                                .mapToLong(DataSegmentAndLoadProfile::effectiveSizeOf)
                                                .sum();
  }

  @JsonCreator
  public ImmutableDruidDataSource(
      @JsonProperty("name") String name,
      @JsonProperty("properties") Map<String, String> properties,
      @JsonProperty("segments") Collection<DataSegment> segments
  )
  {
    this.name = Preconditions.checkNotNull(name);
    this.properties = ImmutableMap.copyOf(properties);

    final ImmutableSortedMap.Builder<SegmentId, DataSegment> idToSegmentsBuilder = ImmutableSortedMap.naturalOrder();
    long totalSizeOfSegments = 0;
    for (DataSegment segment : segments) {
      // JSON deserialization carries no partial-load profile state; segments are stored bare.
      idToSegmentsBuilder.put(segment.getId(), segment);
      totalSizeOfSegments += segment.getSize();
    }
    this.idToSegments = idToSegmentsBuilder.build();
    this.totalSizeOfSegments = totalSizeOfSegments;
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

  @JsonProperty
  public Collection<DataSegment> getSegments()
  {
    return Collections.unmodifiableCollection(idToSegments.values());
  }

  @JsonIgnore
  public int getNumSegments()
  {
    return idToSegments.size();
  }

  @JsonIgnore
  @Nullable
  public DataSegment getSegment(SegmentId segmentId)
  {
    return idToSegments.get(segmentId);
  }

  /**
   * Returns the partial-load profile for the given segment, or {@code null} if the segment was loaded as a regular
   * full-load (no partial-load metadata announced) or is not present in this data source.
   */
  @JsonIgnore
  @Nullable
  public PartialLoadProfile getPartialLoadProfile(SegmentId segmentId)
  {
    return DataSegmentAndLoadProfile.profileOf(idToSegments.get(segmentId));
  }

  /**
   * Returns the sum of {@link DataSegmentAndLoadProfile#effectiveSizeOf effective sizes} of all segments in this
   * ImmutableDruidDataSource, i.e. {@code loadedBytes} for partial replicas and {@link DataSegment#getSize()} for
   * full-load replicas.
   */
  @JsonIgnore
  public long getTotalSizeOfSegments()
  {
    return totalSizeOfSegments;
  }

  @Override
  public String toString()
  {
    // The detail of idToSegments is intentionally ignored because it is usually large
    return "ImmutableDruidDataSource{"
           + "name='" + name
           + "', # of segments='" + idToSegments.size()
           + "', properties='" + properties
           + "'}";
  }

  /**
   * ImmutableDruidDataSource should be considered a container, not a data class. The idea is the same as behind
   * prohibiting/limiting equals() (and therefore usage as HashSet/HashMap keys) of DataSegment: see
   * https://github.com/apache/druid/issues/6358. When somebody wants to deduplicate ImmutableDruidDataSource
   * objects, they would need to put them into a Map<String, ImmutableDruidDataSource> and resolve conflicts by name
   * manually.
   *
   * See https://github.com/apache/druid/issues/7858
   */
  @Override
  public boolean equals(Object o)
  {
    throw new UnsupportedOperationException("ImmutableDruidDataSource shouldn't be used as the key in containers");
  }

  /**
   * ImmutableDruidDataSource should be considered a container, not a data class. The idea is the same as behind
   * prohibiting/limiting hashCode() (and therefore usage as HashSet/HashMap keys) of DataSegment: see
   * https://github.com/apache/druid/issues/6358. When somebody wants to deduplicate ImmutableDruidDataSource
   * objects, they would need to put them into a Map<String, ImmutableDruidDataSource> and resolve conflicts by name
   * manually.
   *
   * See https://github.com/apache/druid/issues/7858
   */
  @Override
  public int hashCode()
  {
    throw new UnsupportedOperationException("ImmutableDruidDataSource shouldn't be used as the key in containers");
  }

  /**
   * This method should only be used in tests.
   */
  @VisibleForTesting
  public boolean equalsForTesting(Object o)
  {
    if (this == o) {
      return true;
    }

    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }

    final ImmutableDruidDataSource that = (ImmutableDruidDataSource) o;
    if (!this.name.equals(that.name)) {
      return false;
    }

    if (!this.properties.equals(that.properties)) {
      return false;
    }

    return this.idToSegments.equals(that.idToSegments);
  }
}
