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

import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.timeline.DataSegment;

import java.util.HashMap;
import java.util.Map;

/**
 * Maintain ServerCostComputers per historical
 */
public class ClusterCostComputer
{
  private final Map<String, ServerCostComputer> serverCostComputerMap = new HashMap<>();

  public ClusterCostComputer(ServerInventoryView serverInventoryView)
  {
    for (DruidServer server : serverInventoryView.getInventory()) {
      serverCostComputerMap.put(server.getName(), new ServerCostComputer(server));
    }
  }

  public double computeCost(String serverName, DataSegment dataSegment)
  {
    ServerCostComputer serverCostCache = serverCostComputerMap.get(serverName);
    return (serverCostCache != null) ? serverCostCache.computeCost(dataSegment) : 0.0;
  }
}
