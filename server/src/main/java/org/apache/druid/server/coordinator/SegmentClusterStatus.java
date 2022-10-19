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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Maintains a map containing the state of a segment on all servers of the
 * cluster.
 */
public class SegmentClusterStatus
{
  private final Map<String, Map<SegmentState, List<ServerHolder>>> tierToSegmentStates = new HashMap<>();

  public SegmentClusterStatus(DataSegment segment, DruidCluster cluster)
  {
    cluster.getHistoricals().forEach((tier, historicals) -> {
      Map<SegmentState, List<ServerHolder>> stateInTier =
          tierToSegmentStates.computeIfAbsent(tier, t -> new HashMap<>());
      historicals.forEach(server -> {
        SegmentState stateOnServer = server.getSegmentState(segment);
        stateInTier.computeIfAbsent(stateOnServer, s -> new ArrayList<>()).add(server);
      });
    });
  }

  /**
   * Total number of replicas loaded and loading on this tier.
   * Does not include replicas which are being dropped.
   */
  public int getProjectedReplicas(String tier)
  {
    return getServers(tier, SegmentState.LOADING).size()
           + getServers(tier, SegmentState.LOADED).size();
  }

  /**
   * Number of replicas loaded on this tier. Does not include replicas which are
   * being dropped.
   */
  public int getLoadedReplicas(String tier)
  {
    return getServers(tier, SegmentState.LOADED).size();
  }

  /**
   * Total number of replicas loaded on the cluster. Does not include replicas
   * which are being dropped.
   */
  public int getTotalLoadedReplicas()
  {
    int numLoaded = 0;
    for (String tier : tierToSegmentStates.keySet()) {
      numLoaded += getLoadedReplicas(tier);
    }
    return numLoaded;
  }

  /**
   * Gets the servers on this tier which have the specified state for this
   * segment.
   */
  public List<ServerHolder> getServers(String tier, SegmentState state)
  {
    Map<SegmentState, List<ServerHolder>> stateInTier = tierToSegmentStates.get(tier);
    if (stateInTier == null) {
      return Collections.emptyList();
    }

    return stateInTier.getOrDefault(state, Collections.emptyList());
  }

}
