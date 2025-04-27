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

package org.apache.druid.server.coordinator.duty;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.CloneStatusManager;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerCloneStatus;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.SegmentAction;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handles cloning of historicals. Given the historical to historical clone mappings, based on
 * {@link CoordinatorDynamicConfig#getCloneServers()}, copies any segments load or unload requests from the source
 * historical to the target historical.
 */
public class CloneHistoricals implements CoordinatorDuty
{
  private static final Logger log = new Logger(CloneHistoricals.class);
  private final SegmentLoadQueueManager loadQueueManager;
  private final CloneStatusManager cloneStatusManager;

  public CloneHistoricals(
      final SegmentLoadQueueManager loadQueueManager,
      final CloneStatusManager cloneStatusManager
  )
  {
    this.loadQueueManager = loadQueueManager;
    this.cloneStatusManager = cloneStatusManager;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final Map<String, String> cloneServers = params.getCoordinatorDynamicConfig().getCloneServers();
    final CoordinatorRunStats stats = params.getCoordinatorStats();
    final DruidCluster cluster = params.getDruidCluster();

    if (cloneServers.isEmpty()) {
      // No servers to be cloned.
      return params;
    }

    // Create a map of host to historical.
    final Map<String, ServerHolder> hostToHistorical = cluster.getHistoricals()
                                                              .values()
                                                              .stream()
                                                              .flatMap(Collection::stream)
                                                              .collect(Collectors.toMap(
                                                                  serverHolder -> serverHolder.getServer().getHost(),
                                                                  serverHolder -> serverHolder
                                                              ));

    for (Map.Entry<String, String> entry : cloneServers.entrySet()) {
      final String targetHistoricalName = entry.getKey();
      final ServerHolder targetServer = hostToHistorical.get(targetHistoricalName);

      final String sourceHistoricalName = entry.getValue();
      final ServerHolder sourceServer = hostToHistorical.get(sourceHistoricalName);

      if (sourceServer == null || targetServer == null) {
        log.error(
            "Could not process clone mapping[%s] as historical[%s] does not exist.",
            entry,
            (sourceServer == null ? sourceHistoricalName : targetHistoricalName)
        );
        continue;
      }

      final Set<DataSegment> sourceProjectedSegments = sourceServer.getProjectedSegments();
      final Set<DataSegment> targetProjectedSegments = targetServer.getProjectedSegments();
      // Load any segments missing in the clone target.
      for (DataSegment segment : sourceProjectedSegments) {
        if (!targetProjectedSegments.contains(segment) && loadQueueManager.loadSegment(segment, targetServer, SegmentAction.LOAD)) {
          stats.add(
              Stats.Segments.ASSIGNED_TO_CLONE,
              RowKey.of(Dimension.SERVER, targetServer.getServer().getHost()),
              1L
          );
        }
      }

      // Drop any segments missing from the clone source.
      for (DataSegment segment : targetProjectedSegments) {
        if (!sourceProjectedSegments.contains(segment) && loadQueueManager.dropSegment(segment, targetServer)) {
          stats.add(
              Stats.Segments.DROPPED_FROM_CLONE,
              RowKey.of(Dimension.SERVER, targetServer.getServer().getHost()),
              1L
          );
        }
      }
    }

    Map<String, ServerCloneStatus> newStatusMap = createCurrentStatusMap(hostToHistorical, cloneServers);
    cloneStatusManager.updateStatus(newStatusMap);

    return params;
  }

  public Map<String, ServerCloneStatus> createCurrentStatusMap(
      Map<String, ServerHolder> historicalMap,
      Map<String, String> cloneServers
  )
  {
    final Map<String, ServerCloneStatus> newStatusMap = new HashMap<>();

    for (Map.Entry<String, String> entry : cloneServers.entrySet()) {
      final String targetServerName = entry.getKey();
      final ServerHolder targetServer = historicalMap.get(entry.getKey());
      final String sourceServerName = entry.getValue();

      long segmentLoad = 0L;
      long bytesLeft = 0L;
      long segmentDrop = 0L;

      ServerCloneStatus newStatus;
      if (targetServer == null) {
        newStatus = ServerCloneStatus.unknown(sourceServerName, targetServerName);
      } else {

        ServerCloneStatus.State state;
        if (!historicalMap.containsKey(sourceServerName)) {
          state = ServerCloneStatus.State.SOURCE_SERVER_MISSING;
        } else {
          state = ServerCloneStatus.State.IN_PROGRESS;
        }

        for (Map.Entry<DataSegment, SegmentAction> queuedSegment : targetServer.getQueuedSegments().entrySet()) {
          if (queuedSegment.getValue().isLoad()) {
            segmentLoad += 1;
            bytesLeft += queuedSegment.getKey().getSize();
          } else {
            segmentDrop += 1;
          }
        }
        newStatus = new ServerCloneStatus(sourceServerName, targetServerName, state, segmentLoad, segmentDrop, bytesLeft);
      }
      newStatusMap.put(targetServerName, newStatus);
    }

    return newStatusMap;
  }
}
