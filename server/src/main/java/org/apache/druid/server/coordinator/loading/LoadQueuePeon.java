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

import java.util.Set;

/**
 * Supports load queue management.
 */
@Deprecated
public interface LoadQueuePeon
{
  void start();

  void stop();

  Set<DataSegment> getSegmentsToLoad();

  Set<SegmentHolder> getSegmentsInQueue();

  Set<DataSegment> getSegmentsToDrop();

  Set<DataSegment> getTimedOutSegments();

  void markSegmentToDrop(DataSegment segmentToLoad);

  void unmarkSegmentToDrop(DataSegment segmentToLoad);

  Set<DataSegment> getSegmentsMarkedToDrop();

  void loadSegment(DataSegment segment, SegmentAction action, LoadPeonCallback callback);

  void dropSegment(DataSegment segment, LoadPeonCallback callback);

  long getSizeOfSegmentsToLoad();

  CoordinatorRunStats getAndResetStats();

  /**
   * Tries to cancel the current operation queued for the given segment on this
   * server, if any. A request that has already been sent to the server cannot
   * be cancelled.
   *
   * @return true if the operation was successfully cancelled
   */
  boolean cancelOperation(DataSegment segment);

}
