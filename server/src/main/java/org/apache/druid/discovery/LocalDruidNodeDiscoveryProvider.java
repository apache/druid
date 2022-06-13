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

import org.apache.druid.server.DruidNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

/**
 * Local node discovery. In this mode, Druid may run a number of
 * logical servers within a single process, so that node discovery
 * is all within the same process, but matches a true distributed
 * node discovery.
 * <p>
 * Concurrency is at the level of the entire provider, which is
 * necessary to coordinate the active state with ongoing node
 * discovery and listener registrations. Such a "global" lock
 * is fine because the actions don't occur frequently, nor do the
 * actions take much time. For this same reason, we use plain old
 * Java synchronization rather than a fancier locking mechanism.
 * <p>
 * At present, this class is used in unit tests so that it is
 * possible to simulate cluster membership changes without actually running
 * ZK, etc. The class can become the primary provider if/when Druid
 * can run all services in a single process: in that case, we won't
 * need ZK to tell the in-process services about each other.
 */
public class LocalDruidNodeDiscoveryProvider extends DruidNodeDiscoveryProvider implements DruidNodeAnnouncer
{
  private class RoleEntry implements DruidNodeDiscovery
  {
    private final Map<DruidNode, DiscoveryDruidNode> nodes = new HashMap<>();
    private final List<Listener> listeners = new ArrayList<>();

    private void register(DiscoveryDruidNode node)
    {
      List<DruidNodeDiscovery.Listener> targets;
      synchronized (LocalDruidNodeDiscoveryProvider.this) {
        DiscoveryDruidNode prev = nodes.put(node.getDruidNode(), node);
        if (prev != null) {
          return;
        }
        targets = new ArrayList<>(listeners);
      }
      for (Listener listener : targets) {
        listener.nodesAdded(Collections.singletonList(node));
      }
    }

    private void deregister(DiscoveryDruidNode node)
    {
      List<DruidNodeDiscovery.Listener> targets;
      synchronized (LocalDruidNodeDiscoveryProvider.this) {
        DiscoveryDruidNode prev = nodes.remove(node.getDruidNode());
        if (prev == null) {
          return;
        }
        targets = new ArrayList<>(listeners);
      }
      for (Listener listener : targets) {
        listener.nodesRemoved(Collections.singletonList(node));
      }
    }

    @Override
    public Collection<DiscoveryDruidNode> getAllNodes()
    {
      synchronized (LocalDruidNodeDiscoveryProvider.this) {
        return new ArrayList<>(nodes.values());
      }
    }

    @Override
    public void registerListener(Listener listener)
    {
      synchronized (LocalDruidNodeDiscoveryProvider.this) {
        listeners.add(listener);
        if (!active) {
          return;
        }
      }
      listener.nodeViewInitialized();
    }

    private boolean contains(DruidNode node)
    {
      synchronized (LocalDruidNodeDiscoveryProvider.this) {
        return nodes.containsKey(node);
      }
    }
  }

  private boolean active;
  private final Map<NodeRole, RoleEntry> roles = new HashMap<>();

  public void initialized()
  {
    List<DruidNodeDiscovery.Listener> targets = new ArrayList<>();
    synchronized (this) {
      if (active) {
        return;
      }
      active = true;
      for (RoleEntry value : roles.values()) {
        targets.addAll(value.listeners);
      }
    }
    for (DruidNodeDiscovery.Listener target : targets) {
      target.nodeViewInitialized();
    }
  }

  @Override
  public BooleanSupplier getForNode(DruidNode node, NodeRole nodeRole)
  {
    return () -> entry(nodeRole).contains(node);
  }

  @Override
  public DruidNodeDiscovery getForNodeRole(NodeRole nodeRole)
  {
    return entry(nodeRole);
  }

  public RoleEntry entry(NodeRole nodeRole)
  {
    return roles.computeIfAbsent(nodeRole, role -> new RoleEntry());
  }

  @Override
  public void announce(DiscoveryDruidNode node)
  {
    entry(node.getNodeRole()).register(node);
  }

  @Override
  public void unannounce(DiscoveryDruidNode node)
  {
    entry(node.getNodeRole()).deregister(node);
  }
}
