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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.timeline.SegmentId;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The ReplicationThrottler is used to throttle the number of replicants that are created.
 */
public class ReplicationThrottler
{
  private static final EmittingLogger log = new EmittingLogger(ReplicationThrottler.class);

  private final Map<String, Boolean> replicatingLookup = Maps.newHashMap();
  private final ReplicatorSegmentHolder currentlyReplicating = new ReplicatorSegmentHolder();

  private volatile int maxReplicants;
  private volatile int maxLifetime;

  public ReplicationThrottler(int maxReplicants, int maxLifetime)
  {
    updateParams(maxReplicants, maxLifetime);
  }

  public void updateParams(int maxReplicants, int maxLifetime)
  {
    this.maxReplicants = maxReplicants;
    this.maxLifetime = maxLifetime;
  }

  public void updateReplicationState(String tier)
  {
    update(tier, currentlyReplicating, replicatingLookup, "create");
  }

  private void update(String tier, ReplicatorSegmentHolder holder, Map<String, Boolean> lookup, String type)
  {
    int size = holder.getNumProcessing(tier);
    if (size != 0) {
      log.info(
          "[%s]: Replicant %s queue still has %d segments. Lifetime[%d]. Segments %s",
          tier,
          type,
          size,
          holder.getLifetime(tier),
          holder.getCurrentlyProcessingSegmentsAndHosts(tier)
      );
      holder.reduceLifetime(tier);
      lookup.put(tier, false);

      if (holder.getLifetime(tier) < 0) {
        log.makeAlert("[%s]: Replicant %s queue stuck after %d+ runs!", tier, type, maxLifetime)
           .addData("segments", holder.getCurrentlyProcessingSegmentsAndHosts(tier))
           .emit();
      }
    } else {
      log.info("[%s]: Replicant %s queue is empty.", tier, type);
      lookup.put(tier, true);
      holder.resetLifetime(tier);
    }
  }

  public boolean canCreateReplicant(String tier)
  {
    return replicatingLookup.get(tier) && !currentlyReplicating.isAtMaxReplicants(tier);
  }

  public void registerReplicantCreation(String tier, SegmentId segmentId, String serverId)
  {
    currentlyReplicating.addSegment(tier, segmentId, serverId);
  }

  public void unregisterReplicantCreation(String tier, SegmentId segmentId)
  {
    currentlyReplicating.removeSegment(tier, segmentId);
  }

  private class ReplicatorSegmentHolder
  {
    private final Map<String, ConcurrentHashMap<SegmentId, String>> currentlyProcessingSegments = Maps.newHashMap();
    private final Map<String, Integer> lifetimes = Maps.newHashMap();

    public boolean isAtMaxReplicants(String tier)
    {
      final ConcurrentHashMap<SegmentId, String> segments = currentlyProcessingSegments.get(tier);
      return (segments != null && segments.size() >= maxReplicants);
    }

    public void addSegment(String tier, SegmentId segmentId, String serverId)
    {
      ConcurrentHashMap<SegmentId, String> segments =
          currentlyProcessingSegments.computeIfAbsent(tier, t -> new ConcurrentHashMap<>());

      if (!isAtMaxReplicants(tier)) {
        segments.put(segmentId, serverId);
      }
    }

    public void removeSegment(String tier, SegmentId segmentId)
    {
      Map<SegmentId, String> segments = currentlyProcessingSegments.get(tier);
      if (segments != null) {
        segments.remove(segmentId);
      }
    }

    public int getNumProcessing(String tier)
    {
      Map<SegmentId, String> segments = currentlyProcessingSegments.get(tier);
      return (segments == null) ? 0 : segments.size();
    }

    public int getLifetime(String tier)
    {
      Integer lifetime = lifetimes.putIfAbsent(tier, maxLifetime);
      return lifetime != null ? lifetime : maxLifetime;
    }

    public void reduceLifetime(String tier)
    {
      lifetimes.compute(
          tier,
          (t, lifetime) -> {
            if (lifetime == null) {
              return maxLifetime - 1;
            }
            return lifetime - 1;
          }
      );
    }

    public void resetLifetime(String tier)
    {
      lifetimes.put(tier, maxLifetime);
    }

    public List<String> getCurrentlyProcessingSegmentsAndHosts(String tier)
    {
      Map<SegmentId, String> segments = currentlyProcessingSegments.get(tier);
      List<String> retVal = Lists.newArrayList();
      segments.forEach((segmentId, serverId) -> retVal.add(StringUtils.format("%s ON %s", segmentId, serverId)));
      return retVal;
    }
  }
}
