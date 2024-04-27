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

package org.apache.druid.catalog.sync;

import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.server.DruidNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Provides an up-to-date list of Druid nodes of the given types each
 * time the list is requested.
 *
 * The algorithm could be improved to cache the list and update it only
 * when the set of nodes changes. For the catalog, the rate of change is
 * likely to be low, so creating the list each time is fine. If this code
 * is used for high-speed updates, then caching would be desirable.
 */
public class ListeningNodeSupplier implements Supplier<Iterable<DruidNode>>
{
  private final List<NodeRole> nodeTypes;
  private final DruidNodeDiscoveryProvider discoveryProvider;

  public ListeningNodeSupplier(
      List<NodeRole> nodeTypes,
      DruidNodeDiscoveryProvider discoveryProvider
  )
  {
    this.nodeTypes = nodeTypes;
    this.discoveryProvider = discoveryProvider;
  }

  @Override
  public Iterable<DruidNode> get()
  {
    List<DruidNode> druidNodes = new ArrayList<>();
    for (NodeRole nodeRole : nodeTypes) {
      DruidNodeDiscovery nodeDiscovery = discoveryProvider.getForNodeRole(nodeRole);
      Collection<DiscoveryDruidNode> nodes = nodeDiscovery.getAllNodes();
      nodes.forEach(node -> druidNodes.add(node.getDruidNode()));
    }
    return druidNodes;
  }
}
