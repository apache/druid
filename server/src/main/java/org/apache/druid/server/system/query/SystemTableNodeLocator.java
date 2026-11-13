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

package org.apache.druid.server.system.query;

import com.google.inject.Inject;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.server.system.table.SystemTableDescriptor;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Finds the Druid services that serve a system table: every discovered service with one of the table's node roles.
 */
public class SystemTableNodeLocator
{
  private final DruidNodeDiscoveryProvider discoveryProvider;

  @Inject
  public SystemTableNodeLocator(final DruidNodeDiscoveryProvider discoveryProvider)
  {
    this.discoveryProvider = discoveryProvider;
  }

  List<SystemTableNode> locate(final SystemTableDescriptor descriptor)
  {
    final Map<String, SystemTableNode> nodes = new LinkedHashMap<>();
    for (final NodeRole nodeRole : descriptor.getNodeRoles()) {
      for (final DiscoveryDruidNode node : discoveryProvider.getForNodeRole(nodeRole).getAllNodes()) {
        nodes.computeIfAbsent(
            node.getDruidNode().getHostAndPortToUse(),
            ignored -> new SystemTableNode(node)
        ).addNodeRole(node.getNodeRole());
      }
    }
    return new ArrayList<>(nodes.values());
  }

  static boolean sameServer(final URI first, final URI second)
  {
    return first.getScheme().equalsIgnoreCase(second.getScheme())
           && first.getHost().equalsIgnoreCase(second.getHost())
           && effectivePort(first) == effectivePort(second);
  }

  private static int effectivePort(final URI uri)
  {
    if (uri.getPort() >= 0) {
      return uri.getPort();
    }
    return "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80;
  }
}
