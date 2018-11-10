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

import java.util.Set;

/**
 * This interface exists only to support configurable load queue management via curator or http. Once HttpLoadQueuePeon
 * has been verified enough in production, CuratorLoadQueuePeon and this interface would be removed.
 */
@Deprecated
public abstract class LoadQueuePeon
{
  public abstract void start();
  public abstract void stop();

  public abstract Set<DataSegment> getSegmentsToLoad();

  public abstract Set<DataSegment> getSegmentsToDrop();

  public abstract void unmarkSegmentToDrop(DataSegment segmentToLoad);


  public abstract void markSegmentToDrop(DataSegment segmentToLoad);

  public abstract void loadSegment(DataSegment segment, LoadPeonCallback callback);
  public abstract void dropSegment(DataSegment segment, LoadPeonCallback callback);

  public abstract long getLoadQueueSize();

  public abstract int getAndResetFailedAssignCount();

  public abstract int getNumberOfSegmentsInQueue();
  public abstract Set<DataSegment> getSegmentsMarkedToDrop();

}
