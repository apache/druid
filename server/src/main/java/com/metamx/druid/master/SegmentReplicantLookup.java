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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Table;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidDataSource;
import com.metamx.druid.client.DruidServer;

import java.util.Map;

/**
 * A lookup for the number of replicants of a given segment for a certain tier.
 */
public class SegmentReplicantLookup
{
  public static SegmentReplicantLookup make(DruidCluster cluster)
  {
    final Table<String, String, Integer> segmentsInCluster = HashBasedTable.create();

    for (MinMaxPriorityQueue<ServerHolder> serversByType : cluster.getSortedServersByTier()) {
      for (ServerHolder serverHolder : serversByType) {
        DruidServer server = serverHolder.getServer();

        for (DruidDataSource dataSource : server.getDataSources()) {
          for (DataSegment segment : dataSource.getSegments()) {
            Integer numReplicants = segmentsInCluster.get(segment.getIdentifier(), server.getTier());
            if (numReplicants == null) {
              numReplicants = 0;
            }
            segmentsInCluster.put(segment.getIdentifier(), server.getTier(), ++numReplicants);
          }
        }

        // Also account for queued segments
        for (DataSegment segment : serverHolder.getPeon().getSegmentsToLoad()) {
          Integer numReplicants = segmentsInCluster.get(segment.getIdentifier(), server.getTier());
          if (numReplicants == null) {
            numReplicants = 0;
          }
          segmentsInCluster.put(segment.getIdentifier(), server.getTier(), ++numReplicants);
        }
      }
    }

    return new SegmentReplicantLookup(segmentsInCluster);
  }

  private final Table<String, String, Integer> table;

  private SegmentReplicantLookup(Table<String, String, Integer> table)
  {
    this.table = table;
  }

  public Map<String, Integer> getTiers(String segmentId)
  {
    Map<String, Integer> retVal = table.row(segmentId);
    return (retVal == null) ? Maps.<String, Integer>newHashMap() : retVal;
  }

  public int lookup(String segmentId, String tier)
  {
    Integer retVal = table.get(segmentId, tier);
    return (retVal == null) ? 0 : retVal;
  }
}
