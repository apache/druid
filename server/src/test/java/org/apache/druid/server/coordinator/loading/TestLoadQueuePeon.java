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

package org.apache.druid.server.coordinator.loading;

import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Test implementation of {@link LoadQueuePeon} that maintains a set of segments
 * that have been queued for load or drop.
 */
public class TestLoadQueuePeon implements LoadQueuePeon
{
  private final ConcurrentSkipListSet<DataSegment> segmentsToLoad = new ConcurrentSkipListSet<>();
  private final ConcurrentSkipListSet<DataSegment> segmentsToDrop = new ConcurrentSkipListSet<>();
  private final ConcurrentHashMap<DataSegment, PartialLoadProfile> segmentToProfile = new ConcurrentHashMap<>();
  private final ConcurrentSkipListSet<SegmentHolder> queuedHolders = new ConcurrentSkipListSet<>();

  private final CoordinatorRunStats stats = new CoordinatorRunStats();

  /**
   * Adds a {@link SegmentHolder} to the in-flight queue as if a previous coordinator run had queued the load. Tests
   * use this to simulate pre-existing in-flight loads (e.g., stale-fingerprint loads from a prior run) so the
   * reconciler can observe and act on them.
   */
  public void addInFlightHolder(SegmentHolder holder)
  {
    queuedHolders.add(holder);
    segmentsToLoad.add(holder.getSegment());
    if (holder.getProfile() != null) {
      segmentToProfile.put(holder.getSegment(), holder.getProfile());
    }
  }

  @Override
  public void loadSegment(DataSegment segment, SegmentAction action, @Nullable LoadPeonCallback callback)
  {
    segmentsToLoad.add(segment);
  }

  @Override
  public void loadSegment(
      DataSegment segment,
      SegmentAction action,
      @Nullable PartialLoadProfile profile,
      @Nullable LoadPeonCallback callback
  )
  {
    segmentsToLoad.add(segment);
    if (profile != null) {
      segmentToProfile.put(segment, profile);
    }
  }

  /**
   * Returns the {@link PartialLoadProfile} that was passed alongside the load request for the given segment, or
   * {@code null} if the segment was loaded as a regular full-load (no profile threaded).
   */
  @Nullable
  public PartialLoadProfile getProfileFor(DataSegment segment)
  {
    return segmentToProfile.get(segment);
  }

  @Override
  public void dropSegment(DataSegment segment, LoadPeonCallback callback)
  {
    segmentsToDrop.add(segment);
  }

  @Override
  public long getSizeOfSegmentsToLoad()
  {
    return 0;
  }

  @Override
  public long getLoadRateKbps()
  {
    return 0;
  }

  @Override
  public CoordinatorRunStats getAndResetStats()
  {
    return stats;
  }

  @Override
  public boolean cancelOperation(DataSegment segment)
  {
    final boolean removedFromLoad = segmentsToLoad.remove(segment);
    final boolean removedFromDrop = segmentsToDrop.remove(segment);
    queuedHolders.removeIf(h -> h.getSegment().equals(segment));
    segmentToProfile.remove(segment);
    return removedFromLoad || removedFromDrop;
  }

  @Override
  public void start()
  {

  }

  @Override
  public void stop()
  {

  }

  @Override
  public ConcurrentSkipListSet<DataSegment> getSegmentsToLoad()
  {
    return segmentsToLoad;
  }

  @Override
  public Set<SegmentHolder> getSegmentsInQueue()
  {
    return queuedHolders;
  }

  @Override
  public Set<DataSegment> getSegmentsToDrop()
  {
    return segmentsToDrop;
  }

  @Override
  public Set<DataSegment> getTimedOutSegments()
  {
    return Collections.emptySet();
  }

  @Override
  public void markSegmentToDrop(DataSegment segmentToLoad)
  {

  }

  @Override
  public void unmarkSegmentToDrop(DataSegment segmentToLoad)
  {

  }

  @Override
  public Set<DataSegment> getSegmentsMarkedToDrop()
  {
    return Collections.emptySet();
  }
}
