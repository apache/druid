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

package org.apache.druid.server.coordinator.simulate;

import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.metrics.MetricsVerifier;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.timeline.DataSegment;

import java.util.List;

/**
 * Runner for a coordinator simulation.
 */
public interface CoordinatorSimulation
{
  /**
   * Starts the simulation if not already started.
   */
  void start();

  /**
   * Stops the simulation.
   */
  void stop();

  /**
   * State of the coordinator during the simulation.
   */
  CoordinatorState coordinator();

  /**
   * State of the cluster during the simulation.
   */
  ClusterState cluster();

  static CoordinatorSimulationBuilder builder()
  {
    return new CoordinatorSimulationBuilder();
  }

  interface CoordinatorState
  {
    /**
     * Runs a single coordinator cycle.
     */
    void runCoordinatorCycle();

    /**
     * Synchronizes the inventory view maintained by the coordinator with the
     * actual state of the cluster.
     */
    void syncInventoryView();

    /**
     * Sets the CoordinatorDynamicConfig.
     */
    void setDynamicConfig(CoordinatorDynamicConfig dynamicConfig);

    /**
     * Sets the retention rules for the given datasource.
     */
    void setRetentionRules(String datasource, Rule... rules);

    /**
     * Gets the inventory view of the specified server as maintained by the
     * coordinator.
     */
    DruidServer getInventoryView(String serverName);

    /**
     * Returns a MetricsVerifier which can be used to extract and verify the
     * metric values emitted in the previous coordinator run.
     */
    MetricsVerifier getMetricsVerifier();

    /**
     * Gets the load percentage of the specified datasource as seen by the coordinator.
     */
    double getLoadPercentage(String datasource);
  }

  interface ClusterState
  {
    /**
     * Finishes load of all the segments that were queued in the previous
     * coordinator run. Also handles the responses and executes the respective
     * callbacks on the coordinator.
     */
    void loadQueuedSegments();

    /**
     * Finishes load of all the segments that were queued in the previous
     * coordinator run. Does not execute the respective callbacks on the coordinator.
     */
    void loadQueuedSegmentsSkipCallbacks();

    /**
     * Removes the specified server from the cluster.
     */
    void removeServer(DruidServer server);

    /**
     * Adds the specified server to the cluster.
     */
    void addServer(DruidServer server);

    /**
     * Publishes the given segments to the cluster.
     */
    void addSegments(List<DataSegment> segments);
  }
}
