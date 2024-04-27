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

package org.apache.druid.server.coordinator.balancer;

import org.apache.druid.client.DruidDataSource;
import org.apache.druid.client.DruidServer;
import org.apache.druid.server.coordinator.loading.LoadQueuePeon;
import org.apache.druid.timeline.DataSegment;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Maintains the sorting cost computers on a server level
 */
public class ServerCostComputer
{
  private final SortingCostComputer serverComputer;
  private final Map<String, SortingCostComputer> computerPerDatasource = new HashMap<>();

  ServerCostComputer(
      DruidServer server,
      LoadQueuePeon peon
  )
  {
    Set<DataSegment> segments = new HashSet<>();
    Map<String, Set<DataSegment>> datasourceSegments = new HashMap<>();
    if (peon != null) {
      for (DataSegment segment : peon.getSegmentsToLoad()) {
        segments.add(segment);
        String dsName = segment.getDataSource();
        datasourceSegments.computeIfAbsent(dsName, ds -> new HashSet<>()).add(segment);
      }
    }
    for (DruidDataSource dataSource : server.getDataSources()) {
      final String dsName = dataSource.getName();
      datasourceSegments.computeIfAbsent(dsName, ds -> new HashSet<>())
                        .addAll(dataSource.getSegments());
      computerPerDatasource.put(
          dsName,
          new SortingCostComputer(datasourceSegments.get(dsName))
      );
      segments.addAll(dataSource.getSegments());
    }
    serverComputer = new SortingCostComputer(segments);
  }

  double computeCost(DataSegment segment)
  {
    return serverComputer.cost(segment) + computeDataSourceCost(segment);
  }

  private double computeDataSourceCost(DataSegment segment)
  {
    SortingCostComputer datasourceComputer = computerPerDatasource.get(segment.getDataSource());
    return (datasourceComputer == null) ? 0.0 : datasourceComputer.cost(segment);
  }
}
