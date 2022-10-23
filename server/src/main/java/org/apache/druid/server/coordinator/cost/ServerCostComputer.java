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

package org.apache.druid.server.coordinator.cost;

import org.apache.druid.client.DruidDataSource;
import org.apache.druid.client.DruidServer;
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
      DruidServer server
  )
  {
    Set<DataSegment> segments = new HashSet<>();
    for (DruidDataSource dataSource : server.getDataSources()) {
      Set<DataSegment> dataSourceSegments = new HashSet<>(dataSource.getSegments());
      computerPerDatasource.put(dataSource.getName(), new SortingCostComputer(dataSourceSegments));
      segments.addAll(dataSourceSegments);
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
