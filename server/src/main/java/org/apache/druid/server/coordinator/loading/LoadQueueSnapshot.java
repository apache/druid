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

import org.apache.druid.timeline.DataSegment;

import java.util.Set;

/**
 * An atomically captured view of the pending work on a single {@link LoadQueuePeon}.
 * <p>
 * The two sets must be captured together under the peon's lock. A completing move takes the segment out of
 * {@code segmentsMarkedToDrop} and into the queue as a DROP, so a reader interleaved between the two sees it in
 * neither and counts the replica as plainly loaded. See apache/druid#18764.
 */
public class LoadQueueSnapshot
{
  private final Set<SegmentHolder> segmentsInQueue;
  private final Set<DataSegment> segmentsMarkedToDrop;

  public LoadQueueSnapshot(Set<SegmentHolder> segmentsInQueue, Set<DataSegment> segmentsMarkedToDrop)
  {
    this.segmentsInQueue = segmentsInQueue;
    this.segmentsMarkedToDrop = segmentsMarkedToDrop;
  }

  /**
   * Segments queued for load, drop or move, including those acknowledged by the server but not yet confirmed by the
   * inventory view.
   */
  public Set<SegmentHolder> getSegmentsInQueue()
  {
    return segmentsInQueue;
  }

  /**
   * Segments marked to be dropped once the corresponding MOVE_TO has finished.
   */
  public Set<DataSegment> getSegmentsMarkedToDrop()
  {
    return segmentsMarkedToDrop;
  }
}
