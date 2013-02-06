/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.master;

import com.google.common.collect.Maps;
import com.metamx.emitter.EmittingLogger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * The ReplicationThrottler is used to throttle the number of replicants that are created and destroyed.
 */
public class ReplicationThrottler
{
  private static final EmittingLogger log = new EmittingLogger(ReplicationThrottler.class);
  private final int maxReplicants;
  private final int maxLifetime;

  private final Map<String, Boolean> replicatingLookup = Maps.newHashMap();
  private final Map<String, Boolean> terminatingLookup = Maps.newHashMap();
  private final ReplicatorSegmentHolder currentlyReplicating = new ReplicatorSegmentHolder();
  private final ReplicatorSegmentHolder currentlyTerminating = new ReplicatorSegmentHolder();

  public ReplicationThrottler(int maxReplicants, int maxLifetime)
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
          "[%s]: Replicant %s queue still has %d segments. Lifetime[%d]",
          tier,
          type,
          size,
          holder.getLifetime(tier)
      );
      holder.reduceLifetime(tier);
      lookup.put(tier, false);

      if (holder.getLifetime(tier) < 0) {
        log.makeAlert("[%s]: Replicant %s queue stuck after %d+ runs!", tier, type, maxLifetime).emit();
      }
    } else {
      log.info("[%s]: Replicant %s queue is empty.", tier, type);
      lookup.put(tier, true);
    }
  }

  public boolean canAddReplicant(String tier)
  {
    return replicatingLookup.get(tier);
  }

  public boolean canDestroyReplicant(String tier)
  {
    return terminatingLookup.get(tier);
  }

  public boolean registerReplicantCreation(String tier, String segmentId)
  {
    return currentlyReplicating.addSegment(tier, segmentId);
  }

  public void unregisterReplicantCreation(String tier, String segmentId)
  {
    currentlyReplicating.removeSegment(tier, segmentId);
  }

  public boolean registerReplicantTermination(String tier, String segmentId)
  {
    return currentlyTerminating.addSegment(tier, segmentId);
  }

  public void unregisterReplicantTermination(String tier, String segmentId)
  {
    currentlyTerminating.removeSegment(tier, segmentId);
  }

  private class ReplicatorSegmentHolder
  {
    private final Map<String, ConcurrentSkipListSet<String>> currentlyProcessingSegments = Maps.newHashMap();
    private final Map<String, Integer> lifetimes = Maps.newHashMap();

    public boolean addSegment(String tier, String segmentId)
    {
      ConcurrentSkipListSet<String> segments = currentlyProcessingSegments.get(tier);
      if (segments == null) {
        segments = new ConcurrentSkipListSet<String>();
        currentlyProcessingSegments.put(tier, segments);
      }
      if (segments.size() < maxReplicants) {
        segments.add(segmentId);
        return true;
      }

      return false;
    }

    public void removeSegment(String tier, String segmentId)
    {
      Set<String> segments = currentlyProcessingSegments.get(tier);
      if (segments != null) {
        segments.remove(segmentId);
      }
    }

    public int getNumProcessing(String tier)
    {
      Set<String> segments = currentlyProcessingSegments.get(tier);
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
  }
}
