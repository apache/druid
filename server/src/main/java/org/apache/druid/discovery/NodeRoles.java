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

package org.apache.druid.discovery;

import com.google.common.collect.Collections2;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.Node;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class NodeRoles
{
  private static final Logger LOG = new Logger(NodeRoles.class);

  /**
   * Simulate the Guice binding of all node roles, but using just
   * the known roles. Primarily for testing.
   */
  public static Set<NodeRole> knownRoles()
  {
    return new HashSet<>(Arrays.asList(NodeRole.values()));
  }

  public static void addKnownRoles(Binder binder)
  {
    Multibinder<NodeRole> roleBinder = binder(binder);
    for (NodeRole role : NodeRole.values()) {
      roleBinder.addBinding().toInstance(role);
    }
  }

  /**
   * Add a node role for an extension service.
   */
  public static void addRole(Binder binder, NodeRole role)
  {
    LOG.debug("Adding node role: " + role.getJsonName());
    binder(binder)
               .addBinding()
               .toInstance(role);
  }

  public static Multibinder<NodeRole> binder(Binder binder)
  {
    return Multibinder.newSetBinder(binder, NodeRole.class, Global.class);
  }

  @SuppressWarnings("unchecked")
  public static Collection<Object> getNodes(
      DruidNodeDiscoveryProvider provider,
      NodeRole nodeRole,
      boolean full)
  {
    if (full) {
      return (Collection<Object>) (Collection<?>) getDiscoveryNodesForRole(provider, nodeRole);
    } else {
      return (Collection<Object>) (Collection<?>) getNodesForRole(provider, nodeRole);
    }
  }

  public static Collection<DiscoveryDruidNode> getDiscoveryNodesForRole(
      DruidNodeDiscoveryProvider provider,
      NodeRole nodeRole)
  {
    return provider
        .getForNodeRole(nodeRole)
        .getAllNodes();
  }

  public static Collection<Node> getNodesForRole(
      DruidNodeDiscoveryProvider provider,
      NodeRole nodeRole)
  {
    return Collections2.transform(
        getDiscoveryNodesForRole(provider, nodeRole),
        (discoveryDruidNode) -> Node.from(discoveryDruidNode.getDruidNode()));
  }
}
