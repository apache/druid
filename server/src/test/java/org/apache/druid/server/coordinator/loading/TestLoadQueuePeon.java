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
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Test implementation of {@link LoadQueuePeon} that maintains a set of segments
 * that have been queued for load or drop.
 */
public class TestLoadQueuePeon implements LoadQueuePeon
{
  private final ConcurrentSkipListSet<DataSegment> segmentsToLoad = new ConcurrentSkipListSet<>();
  private final ConcurrentSkipListSet<DataSegment> segmentsToDrop = new ConcurrentSkipListSet<>();

  private final CoordinatorRunStats stats = new CoordinatorRunStats();

  @Override
  public void loadSegment(DataSegment segment, SegmentAction action, @Nullable LoadPeonCallback callback)
  {
    segmentsToLoad.add(segment);
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
  public CoordinatorRunStats getAndResetStats()
  {
    return stats;
  }

  @Override
  public boolean cancelOperation(DataSegment segment)
  {
    return false;
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
    return Collections.emptySet();
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
