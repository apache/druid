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

package org.apache.druid.consul.discovery;

import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.NodeRole;

import java.io.Closeable;
import java.util.List;

/**
 * Interface for interacting with Consul's HTTP API for service discovery.
 */
public interface ConsulApiClient extends Closeable
{
  /**
   * Register a service with Consul.
   *
   * @param node The Druid node to register
   * @throws Exception if registration fails
   */
  void registerService(DiscoveryDruidNode node) throws Exception;

  /**
   * Deregister a service from Consul.
   *
   * @param serviceId The service ID to deregister
   * @throws Exception if deregistration fails
   */
  void deregisterService(String serviceId) throws Exception;

  /**
   * Mark the TTL health check for a registered service as passing.
   *
   * @param serviceId The service ID whose TTL check to mark passing
   * @param note Optional note to include with the check update
   * @throws Exception if the TTL update fails
   */
  void passTtlCheck(String serviceId, String note) throws Exception;

  /**
   * Get all healthy services for a given node role.
   *
   * @param nodeRole The node role to query
   * @return List of discovered Druid nodes
   * @throws Exception if query fails
   */
  List<DiscoveryDruidNode> getHealthyServices(NodeRole nodeRole) throws Exception;

  /**
   * Watch for service changes using Consul's blocking queries.
   * This method blocks until changes are detected or timeout occurs.
   *
   * @param nodeRole The node role to watch
   * @param lastIndex The last Consul index seen (0 for first call)
   * @param waitSeconds How many seconds to block waiting for changes
   * @return ConsulWatchResult containing nodes and new index
   * @throws Exception if watch fails
   */
  ConsulWatchResult watchServices(NodeRole nodeRole, long lastIndex, long waitSeconds) throws Exception;

  /**
   * Result from watching Consul services.
   */
  class ConsulWatchResult
  {
    private final List<DiscoveryDruidNode> nodes;
    private final long consulIndex;

    public ConsulWatchResult(List<DiscoveryDruidNode> nodes, long consulIndex)
    {
      this.nodes = nodes;
      this.consulIndex = consulIndex;
    }

    public List<DiscoveryDruidNode> getNodes()
    {
      return nodes;
    }

    public long getConsulIndex()
    {
      return consulIndex;
    }
  }
}
