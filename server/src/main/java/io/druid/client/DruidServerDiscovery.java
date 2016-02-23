/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.client;

import io.druid.server.coordination.DruidServerMetadata;

import java.util.List;

/**
 * This interface is only used internally for discovering Druid servers in the cluster, its implementation should not
 * involve any external announcement or discovery, and should never change the state of a Druid cluster.
 */
public interface DruidServerDiscovery
{

  /**
   * Find all the Druid servers that have the specified type
   *
   * @param type the type of Druid servers we want to find
   * @return a list of server metadata from matched Druid servers
   * @throws Exception
   */
  List<DruidServerMetadata> getServersForType(String type);

  /**
   * Find the Druid server that is the leader of the specified type
   *
   * @param type the type of the leader
   * @return the leader's server metadata
   * @throws Exception
   */
  DruidServerMetadata getLeaderForType(String type);

  /**
   * Find all the Druid servers that have the specified type and service name
   *
   * @param type the type of Druid servers we want to find
   * @param service the service name of Druid servers we want to find. It is specified in druid.service runtime properties
   * @return a list of server metadata from matched Druid servers
   * @throws Exception
   */
  List<DruidServerMetadata> getServersForTypeWithService(String type, String service);


  /**
   * Discuss:
   * Discovery of a descriptor which returns a list of nodes which it describes
   * (example: "can load deep-storage segments"). But any particular node may have any number of descriptors.
   *
   * @param capability
   * @return
   * @throws Exception
   */
  List<DruidServerMetadata> getServersWithCapability(String capability);
}
