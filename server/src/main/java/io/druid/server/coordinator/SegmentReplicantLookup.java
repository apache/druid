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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Table;
import io.druid.client.DruidServer;
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

    for (MinMaxPriorityQueue<ServerHolder> serversByType : cluster.getSortedServersByTier()) {
      for (ServerHolder serverHolder : serversByType) {
        DruidServer server = serverHolder.getServer();

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
