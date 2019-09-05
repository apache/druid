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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Provider of {@link DruidNodeDiscovery} instances.
 */
public abstract class DruidNodeDiscoveryProvider
{
  private static final Map<String, Set<NodeType>> SERVICE_TO_NODE_TYPES = ImmutableMap.of(
      LookupNodeService.DISCOVERY_SERVICE_KEY, ImmutableSet.of(NodeType.BROKER, NodeType.HISTORICAL, NodeType.PEON, NodeType.INDEXER),
      DataNodeService.DISCOVERY_SERVICE_KEY, ImmutableSet.of(NodeType.HISTORICAL, NodeType.PEON, NodeType.INDEXER),
      WorkerNodeService.DISCOVERY_SERVICE_KEY, ImmutableSet.of(NodeType.MIDDLE_MANAGER, NodeType.INDEXER)
  );

  private final ConcurrentHashMap<String, ServiceDruidNodeDiscovery> serviceDiscoveryMap =
      new ConcurrentHashMap<>(SERVICE_TO_NODE_TYPES.size());

  /**
   * Get DruidNodeDiscovery instance to discover nodes of given nodeType.
   */
  public abstract DruidNodeDiscovery getForNodeType(NodeType nodeType);

  /**
   * Get DruidNodeDiscovery instance to discover nodes that announce given service in its metadata.
   */
  public DruidNodeDiscovery getForService(String serviceName)
  {
    return serviceDiscoveryMap.computeIfAbsent(
        serviceName,
        service -> {

          Set<NodeType> nodeTypesToWatch = DruidNodeDiscoveryProvider.SERVICE_TO_NODE_TYPES.get(service);
          if (nodeTypesToWatch == null) {
            throw new IAE("Unknown service [%s].", service);
          }
          ServiceDruidNodeDiscovery serviceDiscovery = new ServiceDruidNodeDiscovery(service, nodeTypesToWatch.size());
          DruidNodeDiscovery.Listener filteringGatheringUpstreamListener =
              serviceDiscovery.filteringUpstreamListener();
          for (NodeType nodeType : nodeTypesToWatch) {
            getForNodeType(nodeType).registerListener(filteringGatheringUpstreamListener);
          }
          return serviceDiscovery;
        }
    );
  }

  private static class ServiceDruidNodeDiscovery implements DruidNodeDiscovery
  {
    private static final Logger log = new Logger(ServiceDruidNodeDiscovery.class);

    private final String service;
    private final ConcurrentMap<String, DiscoveryDruidNode> nodes = new ConcurrentHashMap<>();
    private final Collection<DiscoveryDruidNode> unmodifiableNodes = Collections.unmodifiableCollection(nodes.values());

    private final List<Listener> listeners = new ArrayList<>();

    private final Object lock = new Object();

    private int uninitializedNodeTypes;

    ServiceDruidNodeDiscovery(String service, int watchedNodeTypes)
    {
      Preconditions.checkArgument(watchedNodeTypes > 0);
      this.service = service;
      this.uninitializedNodeTypes = watchedNodeTypes;
    }

    @Override
    public Collection<DiscoveryDruidNode> getAllNodes()
    {
      return unmodifiableNodes;
    }

    @Override
    public void registerListener(Listener listener)
    {
      if (listener instanceof FilteringUpstreamListener) {
        throw new IAE("FilteringUpstreamListener should not be registered with ServiceDruidNodeDiscovery itself");
      }
      synchronized (lock) {
        if (!unmodifiableNodes.isEmpty()) {
          listener.nodesAdded(unmodifiableNodes);
        }
        if (uninitializedNodeTypes == 0) {
          listener.nodeViewInitialized();
        }
        listeners.add(listener);
      }
    }

    DruidNodeDiscovery.Listener filteringUpstreamListener()
    {
      return new FilteringUpstreamListener();
    }

    /**
     * Listens for all node updates and filters them based on {@link #service}. Note: this listener is registered with
     * the objects returned from {@link #getForNodeType(NodeType)}, NOT with {@link ServiceDruidNodeDiscovery} itself.
     */
    class FilteringUpstreamListener implements DruidNodeDiscovery.Listener
    {
      @Override
      public void nodesAdded(Collection<DiscoveryDruidNode> nodesDiscovered)
      {
        synchronized (lock) {
          List<DiscoveryDruidNode> nodesAdded = new ArrayList<>();
          for (DiscoveryDruidNode node : nodesDiscovered) {
            if (node.getServices().containsKey(service)) {
              DiscoveryDruidNode prev = nodes.putIfAbsent(node.getDruidNode().getHostAndPortToUse(), node);

              if (prev == null) {
                nodesAdded.add(node);
              } else {
                log.warn("Node[%s] discovered but already exists [%s].", node, prev);
              }
            } else {
              log.warn("Node[%s] discovered but doesn't have service[%s]. Ignored.", node, service);
            }
          }

          if (nodesAdded.isEmpty()) {
            // Don't bother listeners with an empty update, it doesn't make sense.
            return;
          }

          Collection<DiscoveryDruidNode> unmodifiableNodesAdded = Collections.unmodifiableCollection(nodesAdded);
          for (Listener listener : listeners) {
            try {
              listener.nodesAdded(unmodifiableNodesAdded);
            }
            catch (Exception ex) {
              log.error(ex, "Listener[%s].nodesAdded(%s) threw exception. Ignored.", listener, nodesAdded);
            }
          }
        }
      }

      @Override
      public void nodesRemoved(Collection<DiscoveryDruidNode> nodesDisappeared)
      {
        synchronized (lock) {
          List<DiscoveryDruidNode> nodesRemoved = new ArrayList<>();
          for (DiscoveryDruidNode node : nodesDisappeared) {
            DiscoveryDruidNode prev = nodes.remove(node.getDruidNode().getHostAndPortToUse());
            if (prev != null) {
              nodesRemoved.add(node);
            } else {
              log.warn("Node[%s] disappeared but was unknown for service listener [%s].", node, service);
            }
          }

          if (nodesRemoved.isEmpty()) {
            // Don't bother listeners with an empty update, it doesn't make sense.
            return;
          }

          Collection<DiscoveryDruidNode> unmodifiableNodesRemoved = Collections.unmodifiableCollection(nodesRemoved);
          for (Listener listener : listeners) {
            try {
              listener.nodesRemoved(unmodifiableNodesRemoved);
            }
            catch (Exception ex) {
              log.error(ex, "Listener[%s].nodesRemoved(%s) threw exception. Ignored.", listener, nodesRemoved);
            }
          }
        }
      }

      @Override
      public void nodeViewInitialized()
      {
        synchronized (lock) {
          if (uninitializedNodeTypes == 0) {
            log.error("Unexpected call of nodeViewInitialized()");
            return;
          }
          uninitializedNodeTypes--;
          if (uninitializedNodeTypes == 0) {
            for (Listener listener : listeners) {
              try {
                listener.nodeViewInitialized();
              }
              catch (Exception ex) {
                log.error(ex, "Listener[%s].nodeViewInitialized() threw exception. Ignored.", listener);
              }
            }
          }
        }
      }
    }
  }
}
