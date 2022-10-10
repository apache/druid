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

package org.apache.druid.server.coordinator;

import org.apache.druid.timeline.DataSegment;

import java.util.Map;
import java.util.Set;

/**
 * This interface exists only to support configurable load queue management via curator or http. Once HttpLoadQueuePeon
 * has been verified enough in production, CuratorLoadQueuePeon and this interface would be removed.
 */
@Deprecated
public interface LoadQueuePeon
{
  void start();

  void stop();

  Set<DataSegment> getSegmentsToLoad();

  Map<DataSegment, SegmentAction> getSegmentsInQueue();

  Set<DataSegment> getSegmentsToDrop();

  Set<DataSegment> getTimedOutSegments();

  void markSegmentToDrop(DataSegment segmentToLoad);

  void unmarkSegmentToDrop(DataSegment segmentToLoad);

  Set<DataSegment> getSegmentsMarkedToDrop();

  void loadSegment(DataSegment segment, LoadPeonCallback callback);

  void loadSegment(DataSegment segment, SegmentAction action, LoadPeonCallback callback);

  void dropSegment(DataSegment segment, LoadPeonCallback callback);

  long getLoadQueueSize();

  int getAndResetFailedAssignCount();

  int getNumberOfSegmentsInQueue();

  boolean cancelLoad(DataSegment segment);

  boolean cancelDrop(DataSegment segment);

}
