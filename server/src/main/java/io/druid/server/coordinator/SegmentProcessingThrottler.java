/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.emitter.EmittingLogger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The SegmentProcessingThrottler is used to throttle the number of segments that are loaded, replicated and destroyed.
 */
public class SegmentProcessingThrottler
{
  private static final EmittingLogger log = new EmittingLogger(SegmentProcessingThrottler.class);

  private final Map<String, Boolean> processingLookup = Maps.newHashMap();
  private final ProcessingSegmentHolder currentlyProcessing = new ProcessingSegmentHolder();

  private volatile int maxReplicants;
  private volatile int maxLifetime;
  private boolean enableThrottling;
  private final String type;

  public SegmentProcessingThrottler(int maxReplicants, int maxLifetime, String type)
  {
    this.type = type;
    updateParams(maxReplicants, maxLifetime);
  }

  public void updateParams(int maxReplicants, int maxLifetime)
  {
    this.maxReplicants = maxReplicants;
    this.maxLifetime = maxLifetime;
    this.enableThrottling = maxReplicants > 0;
  }

  public void updateState(String tier)
  {
    int size = currentlyProcessing.getNumProcessing(tier);
    if (size != 0) {
      log.info(
          "[%s]: Processing %s queue still has %d segments. Lifetime[%d]. Segments %s",
          tier,
          type,
          size,
          currentlyProcessing.getLifetime(tier),
          currentlyProcessing.getCurrentlyProcessingSegmentsAndHosts(tier)
      );
      currentlyProcessing.reduceLifetime(tier);
      processingLookup.put(tier, false);

      if (currentlyProcessing.getLifetime(tier) < 0) {
        log.makeAlert("[%s]: Processing %s queue stuck after %d+ runs!", tier, type, maxLifetime)
           .addData("segments", currentlyProcessing.getCurrentlyProcessingSegmentsAndHosts(tier))
           .emit();
      }
    } else {
      log.info("[%s]: Processing %s queue is empty.", tier, type);
      processingLookup.put(tier, true);
      currentlyProcessing.resetLifetime(tier);
    }
  }

  public boolean canProcessingSegment(String tier)
  {
    return !enableThrottling || (processingLookup.get(tier) && !currentlyProcessing.isAtMaxReplicants(tier));
  }

  public void register(String tier, String segmentId, String serverId)
  {
    currentlyProcessing.addSegment(tier, segmentId, serverId);
  }

  public void unregister(String tier, String segmentId, String serverId)
  {
    currentlyProcessing.removeSegment(tier, segmentId, serverId);
  }

  @Override
  public String toString()
  {
    return "SegmentProcessingThrottler{" +
           "maxReplicants=" + maxReplicants +
           ", maxLifetime=" + maxLifetime +
           ", type='" + type + '\'' +
           '}';
  }

  private class ProcessingSegmentHolder
  {
    private final Map<String, ConcurrentHashMap<String, String>> currentlyProcessingSegments = Maps.newHashMap();
    private final Map<String, Integer> lifetimes = Maps.newHashMap();

    public boolean isAtMaxReplicants(String tier)
    {
      final ConcurrentHashMap<String, String> segments = currentlyProcessingSegments.get(tier);
      return segments != null && segments.size() >= maxReplicants;
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
