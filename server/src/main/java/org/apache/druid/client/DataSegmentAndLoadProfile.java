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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.druid.server.coordination.SegmentChangeRequestLoad;
import org.apache.druid.server.coordinator.loading.PartialLoadProfile;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A {@link DataSegment} carrying optional partial-load metadata describing how it was loaded onto a particular server.
 * Extends {@code DataSegment} so the inventory ({@link DruidDataSource} / {@link ImmutableDruidDataSource}) can store
 * either a bare {@code DataSegment} (full-load, the common case) or a {@code DataSegmentAndLoadProfile} (partial-load)
 * in a single map slot. Full-load segments pay zero per-segment wrapper overhead, only segments with a non-null
 * profile allocate the subclass.
 * <p>
 * Inherits all {@code DataSegment} JSON serialization, equality, hashCode, and ordering. The added profile field is
 * marked to be ignored by JSON serialization since the profile travels via {@link SegmentChangeRequestLoad} wire
 * fields, not as a segment property.
 */
public class DataSegmentAndLoadProfile extends DataSegment
{
  @JsonIgnore
  private final PartialLoadProfile profile;

  public DataSegmentAndLoadProfile(DataSegment delegate, PartialLoadProfile profile)
  {
    super(
        delegate.getDataSource(),
        delegate.getInterval(),
        delegate.getVersion(),
        delegate.getLoadSpec(),
        delegate.getDimensions(),
        delegate.getMetrics(),
        delegate.getProjections(),
        delegate.getClusterGroups(),
        delegate.getShardSpec(),
        delegate.getLastCompactionState(),
        delegate.getBinaryVersion(),
        delegate.getSize(),
        delegate.getTotalRows(),
        delegate.getIndexingStateFingerprint(),
        PruneSpecsHolder.DEFAULT
    );
    this.profile = Objects.requireNonNull(profile, "profile");
  }

  /**
   * The {@link PartialLoadProfile} for this segment on the host server. Never null, bare {@link DataSegment}
   * instances are used to represent the no-profile case.
   */
  public PartialLoadProfile profile()
  {
    return profile;
  }

  /**
   * The realized on-disk footprint of this segment on the server: {@link PartialLoadProfile#loadedBytes()} when the
   * profile carries it, else {@link DataSegment#getSize()}.
   */
  public long effectiveSize()
  {
    final Long loadedBytes = profile.loadedBytes();
    return loadedBytes != null ? loadedBytes : getSize();
  }

  /**
   * Returns the partial-load profile attached to {@code segment} if it is a {@link DataSegmentAndLoadProfile}, else
   * {@code null}. Convenience for the common pattern of reading the profile out of the inventory's
   * {@code Map<SegmentId, DataSegment>} value type.
   */
  @Nullable
  public static PartialLoadProfile profileOf(DataSegment segment)
  {
    return segment instanceof DataSegmentAndLoadProfile p ? p.profile() : null;
  }

  /**
   * Returns the on-disk footprint of {@code segment}: the partial-load profile's {@code loadedBytes} when present,
   * else the segment's full {@link DataSegment#getSize() size}.
   */
  public static long effectiveSizeOf(DataSegment segment)
  {
    return segment instanceof DataSegmentAndLoadProfile p ? p.effectiveSize() : segment.getSize();
  }
}
