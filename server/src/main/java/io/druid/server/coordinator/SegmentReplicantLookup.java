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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Table;
import io.druid.client.ImmutableDruidServer;
import io.druid.timeline.DataSegment;

import java.util.Map;

/**
 * A lookup for the number of replicants of a given segment for a certain tier.
 */
public class SegmentReplicantLookup
{
  public static SegmentReplicantLookup make(DruidCluster cluster)
  {
    final Table<String, String, Integer> segmentsInCluster = HashBasedTable.create();
    final Table<String, String, Integer> loadingSegments = HashBasedTable.create();

    for (MinMaxPriorityQueue<ServerHolder> serversByType : cluster.getSortedHistoricalsByTier()) {
      for (ServerHolder serverHolder : serversByType) {
        ImmutableDruidServer server = serverHolder.getServer();

        for (DataSegment segment : server.getSegments().values()) {
          Integer numReplicants = segmentsInCluster.get(segment.getIdentifier(), server.getTier());
          if (numReplicants == null) {
            numReplicants = 0;
          }
          segmentsInCluster.put(segment.getIdentifier(), server.getTier(), ++numReplicants);
        }

        // Also account for queued segments
        for (DataSegment segment : serverHolder.getPeon().getSegmentsToLoad()) {
          Integer numReplicants = loadingSegments.get(segment.getIdentifier(), server.getTier());
          if (numReplicants == null) {
            numReplicants = 0;
          }
          loadingSegments.put(segment.getIdentifier(), server.getTier(), ++numReplicants);
        }
      }
    }

    return new SegmentReplicantLookup(segmentsInCluster, loadingSegments);
  }

  private final Table<String, String, Integer> segmentsInCluster;
  private final Table<String, String, Integer> loadingSegments;

  private SegmentReplicantLookup(
      Table<String, String, Integer> segmentsInCluster,
      Table<String, String, Integer> loadingSegments
  )
  {
    this.segmentsInCluster = segmentsInCluster;
    this.loadingSegments = loadingSegments;
  }

  public Map<String, Integer> getClusterTiers(String segmentId)
  {
    Map<String, Integer> retVal = segmentsInCluster.row(segmentId);
    return (retVal == null) ? Maps.<String, Integer>newHashMap() : retVal;
  }

  public Map<String, Integer> getLoadingTiers(String segmentId)
  {
    Map<String, Integer> retVal = loadingSegments.row(segmentId);
    return (retVal == null) ? Maps.<String, Integer>newHashMap() : retVal;
  }

  public int getLoadedReplicants(String segmentId)
  {
    Map<String, Integer> allTiers = segmentsInCluster.row(segmentId);
    int retVal = 0;
    for (Integer replicants : allTiers.values()) {
      retVal += replicants;
    }
    return retVal;
  }

  public int getLoadedReplicants(String segmentId, String tier)
  {
    Integer retVal = segmentsInCluster.get(segmentId, tier);
    return (retVal == null) ? 0 : retVal;
  }

  public int getLoadingReplicants(String segmentId, String tier)
  {
    Integer retVal = loadingSegments.get(segmentId, tier);
    return (retVal == null) ? 0 : retVal;
  }

  public int getLoadingReplicants(String segmentId)
  {
    Map<String, Integer> allTiers = loadingSegments.row(segmentId);
    int retVal = 0;
    for (Integer replicants : allTiers.values()) {
      retVal += replicants;
    }
    return retVal;
  }

  public int getTotalReplicants(String segmentId)
  {
    return getLoadedReplicants(segmentId) + getLoadingReplicants(segmentId);
  }

  public int getTotalReplicants(String segmentId, String tier)
  {
    return getLoadedReplicants(segmentId, tier) + getLoadingReplicants(segmentId, tier);
  }
}
