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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provider of DruidNodeDiscovery instances.
 */
public abstract class DruidNodeDiscoveryProvider
{
  private static final Logger log = new Logger(DruidNodeDiscoveryProvider.class);

  public static final String NODE_TYPE_COORDINATOR = "coordinator";
  public static final String NODE_TYPE_HISTORICAL = "historical";
  public static final String NODE_TYPE_BROKER = "broker";
  public static final String NODE_TYPE_OVERLORD = "overlord";
  public static final String NODE_TYPE_PEON = "peon";
  public static final String NODE_TYPE_ROUTER = "router";
  public static final String NODE_TYPE_MM = "middleManager";

  public static final Set<String> ALL_NODE_TYPES = ImmutableSet.of(
      NODE_TYPE_COORDINATOR,
      NODE_TYPE_HISTORICAL,
      NODE_TYPE_BROKER,
      NODE_TYPE_OVERLORD,
      NODE_TYPE_PEON,
      NODE_TYPE_ROUTER,
      NODE_TYPE_MM
  );

  private static final Map<String, Set<String>> SERVICE_TO_NODE_TYPES = ImmutableMap.of(
      LookupNodeService.DISCOVERY_SERVICE_KEY, ImmutableSet.of(NODE_TYPE_BROKER, NODE_TYPE_HISTORICAL, NODE_TYPE_PEON),
      DataNodeService.DISCOVERY_SERVICE_KEY, ImmutableSet.of(NODE_TYPE_HISTORICAL, NODE_TYPE_PEON),
      WorkerNodeService.DISCOVERY_SERVICE_KEY, ImmutableSet.of(NODE_TYPE_MM)
  );

  private final ConcurrentHashMap<String, ServiceDruidNodeDiscovery> serviceDiscoveryMap = new ConcurrentHashMap<>(
      SERVICE_TO_NODE_TYPES.size());

  /**
   * Get DruidNodeDiscovery instance to discover nodes of given nodeType.
   */
  public abstract DruidNodeDiscovery getForNodeType(String nodeType);

  /**
   * Get DruidNodeDiscovery instance to discover nodes that announce given service in its metadata.
   */
  public DruidNodeDiscovery getForService(String serviceName)
  {
    return serviceDiscoveryMap.compute(
        serviceName,
        (k, v) -> {
          if (v != null) {
            return v;
          }

          Set<String> nodeTypesToWatch = DruidNodeDiscoveryProvider.SERVICE_TO_NODE_TYPES.get(serviceName);
          if (nodeTypesToWatch == null) {
            throw new IAE("Unknown service [%s].", serviceName);
          }

          ServiceDruidNodeDiscovery serviceDiscovery = new ServiceDruidNodeDiscovery(serviceName);
          for (String nodeType : nodeTypesToWatch) {
            getForNodeType(nodeType).registerListener(serviceDiscovery.nodeTypeListener());
          }
          return serviceDiscovery;
        }
    );
  }

  private static class ServiceDruidNodeDiscovery implements DruidNodeDiscovery
  {
    private static final Logger log = new Logger(ServiceDruidNodeDiscovery.class);

    private final String service;
    private final Map<String, DiscoveryDruidNode> nodes = new ConcurrentHashMap<>();

    private final List<Listener> listeners = new ArrayList<>();

    private final Object lock = new Object();

    private Set<NodeTypeListener> uninitializedNodeTypeListeners = new HashSet<>();

    ServiceDruidNodeDiscovery(String service)
    {
      this.service = service;
    }

    @Override
    public Collection<DiscoveryDruidNode> getAllNodes()
    {
      return Collections.unmodifiableCollection(nodes.values());
    }

    @Override
    public void registerListener(Listener listener)
    {
      synchronized (lock) {
        if (uninitializedNodeTypeListeners.isEmpty()) {
          listener.nodesAdded(ImmutableList.copyOf(nodes.values()));
        }
        listeners.add(listener);
      }
    }

    NodeTypeListener nodeTypeListener()
    {
      NodeTypeListener nodeListener = new NodeTypeListener();
      uninitializedNodeTypeListeners.add(nodeListener);
      return nodeListener;
    }

    class NodeTypeListener implements DruidNodeDiscovery.Listener
    {
      @Override
      public void nodesAdded(List<DiscoveryDruidNode> nodesDiscovered)
      {
        synchronized (lock) {
          ImmutableList.Builder<DiscoveryDruidNode> builder = ImmutableList.builder();
          for (DiscoveryDruidNode node : nodesDiscovered) {
            if (node.getServices().containsKey(service)) {
              DiscoveryDruidNode prev = nodes.putIfAbsent(node.getDruidNode().getHostAndPortToUse(), node);

              if (prev == null) {
                builder.add(node);
              } else {
                log.warn("Node[%s] discovered but already exists [%s].", node, prev);
              }
            } else {
              log.warn("Node[%s] discovered but doesn't have service[%s]. Ignored.", node, service);
            }
          }

          ImmutableList<DiscoveryDruidNode> newNodesAdded = null;
          if (uninitializedNodeTypeListeners.isEmpty()) {
            newNodesAdded = builder.build();
          } else if (uninitializedNodeTypeListeners.remove(this) && uninitializedNodeTypeListeners.isEmpty()) {
            newNodesAdded = ImmutableList.copyOf(nodes.values());
          }

          if (newNodesAdded != null) {
            for (Listener listener : listeners) {
              try {
                listener.nodesAdded(newNodesAdded);
              }
              catch (Exception ex) {
                log.error(ex, "Listener[%s].nodesAdded(%s) threw exception. Ignored.", listener, newNodesAdded);
              }
            }
          }
        }
      }

      @Override
      public void nodesRemoved(List<DiscoveryDruidNode> nodesDisappeared)
      {
        synchronized (lock) {
          ImmutableList.Builder<DiscoveryDruidNode> builder = ImmutableList.builder();
          for (DiscoveryDruidNode node : nodesDisappeared) {
            DiscoveryDruidNode prev = nodes.remove(node.getDruidNode().getHostAndPortToUse());
            if (prev != null) {
              builder.add(node);
            } else {
              log.warn("Node[%s] disappeared but was unknown for service listener [%s].", node, service);
            }
          }

          if (uninitializedNodeTypeListeners.isEmpty()) {
            ImmutableList<DiscoveryDruidNode> nodesRemoved = builder.build();
            for (Listener listener : listeners) {
              try {
                listener.nodesRemoved(nodesRemoved);
              }
              catch (Exception ex) {
                log.error(ex, "Listener[%s].nodesRemoved(%s) threw exception. Ignored.", listener, nodesRemoved);
              }
            }
          }
        }
      }
    }
  }
}
