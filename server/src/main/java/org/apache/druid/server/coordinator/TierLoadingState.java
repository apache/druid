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

import org.apache.druid.timeline.SegmentId;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Contains segments being loaded or replicated in a tier.
 */
public class TierLoadingState
{
  private int lifetime;
  private final ConcurrentHashMap<SegmentId, String> processingSegmentToHost = new ConcurrentHashMap<>();

  TierLoadingState(int maxLifetime)
  {
    this.lifetime = maxLifetime;
  }

  public int getNumProcessingSegments()
  {
    return processingSegmentToHost.size();
  }

  public int getLifetime()
  {
    return lifetime;
  }

  public List<String> getCurrentlyProcessingSegmentsAndHosts()
  {
    List<String> segmentsAndHosts = new ArrayList<>();
    processingSegmentToHost.forEach((segmentId, serverId) -> segmentsAndHosts.add(segmentId + " ON " + serverId));
    return segmentsAndHosts;
  }

  // Write methods are to be accessed only by the SegmentStateManager

  void markStarted(SegmentId segmentId, String host)
  {
    processingSegmentToHost.put(segmentId, host);
  }

  void markCompleted(SegmentId segmentId)
  {
    processingSegmentToHost.remove(segmentId);
  }

  void reduceLifetime()
  {
    --lifetime;
  }
}
