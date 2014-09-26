/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.coordinator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.emitter.EmittingLogger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The ReplicationThrottler is used to throttle the number of replicants that are created and destroyed.
 */
public class ReplicationThrottler
{
  private static final EmittingLogger log = new EmittingLogger(ReplicationThrottler.class);

  private final Map<String, Boolean> replicatingLookup = Maps.newHashMap();
  private final Map<String, Boolean> terminatingLookup = Maps.newHashMap();
  private final ReplicatorSegmentHolder currentlyReplicating = new ReplicatorSegmentHolder();
  private final ReplicatorSegmentHolder currentlyTerminating = new ReplicatorSegmentHolder();

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

  public void updateTerminationState(String tier)
  {
    update(tier, currentlyTerminating, terminatingLookup, "terminate");
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

  public boolean canDestroyReplicant(String tier)
  {
    return terminatingLookup.get(tier) && !currentlyTerminating.isAtMaxReplicants(tier);
  }

  public void registerReplicantCreation(String tier, String segmentId, String serverId)
  {
    currentlyReplicating.addSegment(tier, segmentId, serverId);
  }

  public void unregisterReplicantCreation(String tier, String segmentId, String serverId)
  {
    currentlyReplicating.removeSegment(tier, segmentId, serverId);
  }

  public void registerReplicantTermination(String tier, String segmentId, String serverId)
  {
    currentlyTerminating.addSegment(tier, segmentId, serverId);
  }

  public void unregisterReplicantTermination(String tier, String segmentId, String serverId)
  {
    currentlyTerminating.removeSegment(tier, segmentId, serverId);
  }

  private class ReplicatorSegmentHolder
  {
    private final Map<String, ConcurrentHashMap<String, String>> currentlyProcessingSegments = Maps.newHashMap();
    private final Map<String, Integer> lifetimes = Maps.newHashMap();

    public boolean isAtMaxReplicants(String tier)
    {
      final ConcurrentHashMap<String, String> segments = currentlyProcessingSegments.get(tier);
      return (segments != null && segments.size() >= maxReplicants);
    }

    public void addSegment(String tier, String segmentId, String serverId)
    {
      ConcurrentHashMap<String, String> segments = currentlyProcessingSegments.get(tier);
      if (segments == null) {
        segments = new ConcurrentHashMap<String, String>();
        currentlyProcessingSegments.put(tier, segments);
      }

      if (!isAtMaxReplicants(tier)) {
        segments.put(segmentId, serverId);
      }
    }

    public void removeSegment(String tier, String segmentId, String serverId)
    {
      Map<String, String> segments = currentlyProcessingSegments.get(tier);
      if (segments != null) {
        segments.remove(segmentId);
      }
    }

    public int getNumProcessing(String tier)
    {
      Map<String, String> segments = currentlyProcessingSegments.get(tier);
      return (segments == null) ? 0 : segments.size();
    }

    public int getLifetime(String tier)
    {
      Integer lifetime = lifetimes.get(tier);
      if (lifetime == null) {
        lifetime = maxLifetime;
        lifetimes.put(tier, lifetime);
      }
      return lifetime;
    }

    public void reduceLifetime(String tier)
    {
      Integer lifetime = lifetimes.get(tier);
      if (lifetime == null) {
        lifetime = maxLifetime;
        lifetimes.put(tier, lifetime);
      }
      lifetimes.put(tier, --lifetime);
    }

    public void resetLifetime(String tier)
    {
      lifetimes.put(tier, maxLifetime);
    }

    public List<String> getCurrentlyProcessingSegmentsAndHosts(String tier)
    {
      Map<String, String> segments = currentlyProcessingSegments.get(tier);
      List<String> retVal = Lists.newArrayList();
      for (Map.Entry<String, String> entry : segments.entrySet()) {
        retVal.add(
            String.format("%s ON %s", entry.getKey(), entry.getValue())
        );
      }
      return retVal;
    }
  }
}
