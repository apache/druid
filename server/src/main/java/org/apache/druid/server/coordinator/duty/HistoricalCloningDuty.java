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
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.SegmentAction;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Handles cloning of historicals. Given the historical to historical clone mappings, based on
 * {@link CoordinatorDynamicConfig#getCloneServers()}, copies any segments load or unload requests from the source
 * historical to the target historical.
 */
public class HistoricalCloningDuty implements CoordinatorDuty
{
  private static final Logger log = new Logger(HistoricalCloningDuty.class);

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final Map<String, String> cloneServers = params.getCoordinatorDynamicConfig().getCloneServers();

    if (cloneServers.isEmpty()) {
      // No servers to be cloned.
      return params;
    }

    final CoordinatorRunStats stats = params.getCoordinatorStats();
    // TODO: clean up
    final Map<String, ServerHolder> historicalMap = params.getDruidCluster()
                                                          .getHistoricals()
                                                          .values()
                                                          .stream()
                                                          .flatMap(Collection::stream)
                                                          .collect(Collectors.toMap(
                                                              serverHolder -> serverHolder.getServer().getHost(),
                                                              serverHolder -> serverHolder
                                                          ));

    for (Map.Entry<String, String> entry : cloneServers.entrySet()) {
      String sourceHistoricalName = entry.getKey();
      ServerHolder sourceServer = historicalMap.get(sourceHistoricalName);

      String targetHistoricalName = entry.getValue();
      ServerHolder targetServer = historicalMap.get(targetHistoricalName);

      if (sourceServer == null) {
        log.info("Could not find source historical [%s]", sourceHistoricalName);
        continue;
      }

      if (targetServer == null) {
        log.info("Could not find target historical [%s]", targetHistoricalName);
        continue;
      }

      for (DataSegment segment : sourceServer.getProjectedSegments().getSegments()) {
        if (!targetServer.getProjectedSegments().getSegments().contains(segment)) {
          targetServer.getPeon().loadSegment(segment, SegmentAction.LOAD, null);
          stats.add(
              Stats.CoordinatorRun.CLONE_LOAD,
              RowKey.of(Dimension.SERVER, targetServer.getServer().getHost()),
              1L
          );
        }
      }

      for (DataSegment segment : targetServer.getProjectedSegments().getSegments()) {
        if (!sourceServer.getProjectedSegments().getSegments().contains(segment)) {
          targetServer.getPeon().dropSegment(segment, null);
          stats.add(
              Stats.CoordinatorRun.CLONE_DROP,
              RowKey.of(Dimension.SERVER, targetServer.getServer().getHost()),
              1L
          );
        }
      }
    }
    return params;
  }
}
