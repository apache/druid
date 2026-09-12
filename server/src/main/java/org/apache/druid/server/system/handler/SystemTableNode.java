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

package org.apache.druid.server.system.handler;

import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.NodeRole;

import java.util.LinkedHashSet;
import java.util.Set;

/** A physical Druid process selected to contribute rows to a native system-table query. */
class SystemTableNode
{
  private final DiscoveryDruidNode discoveryNode;
  private final Set<NodeRole> nodeRoles = new LinkedHashSet<>();

  SystemTableNode(final DiscoveryDruidNode discoveryNode)
  {
    this.discoveryNode = discoveryNode;
  }

  DiscoveryDruidNode getDiscoveryNode()
  {
    return discoveryNode;
  }

  Set<NodeRole> getNodeRoles()
  {
    return nodeRoles;
  }

  void addNodeRole(final NodeRole nodeRole)
  {
    nodeRoles.add(nodeRole);
  }
}
