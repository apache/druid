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

package io.druid.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
          DruidNodeDiscovery.Listener nodeListener = serviceDiscovery.serviceNodeListener();
          for (String nodeType : nodeTypesToWatch) {
            getForNodeType(nodeType).registerListener(nodeListener);
          }
          return serviceDiscovery;
        }
    );
  }

  private static class ServiceDruidNodeDiscovery implements DruidNodeDiscovery
  {
    private final String service;
    private final Map<String, DiscoveryDruidNode> nodes = new ConcurrentHashMap<>();

    private final List<Listener> listeners = new ArrayList<>();

    private final Object lock = new Object();

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
        for (DiscoveryDruidNode node : nodes.values()) {
          listener.nodeAdded(node);
        }
        listeners.add(listener);
      }
    }

    ServiceNodeListener serviceNodeListener()
    {
      return new ServiceNodeListener();
    }

    class ServiceNodeListener implements DruidNodeDiscovery.Listener
    {
      @Override
      public void nodeAdded(DiscoveryDruidNode node)
      {
        synchronized (lock) {
          if (node.getServices().containsKey(service)) {
            DiscoveryDruidNode prev = nodes.putIfAbsent(node.getDruidNode().getHostAndPortToUse(), node);

            if (prev == null) {
              for (Listener listener : listeners) {
                listener.nodeAdded(node);
              }
            } else {
              log.warn("Node[%s] discovered but already exists [%s].", node, prev);
            }
          } else {
            log.warn("Node[%s] discovered but doesn't have service[%s]. Ignored.", node, service);
          }
        }
      }

      @Override
      public void nodeRemoved(DiscoveryDruidNode node)
      {
        synchronized (lock) {
          DiscoveryDruidNode prev = nodes.remove(node.getDruidNode().getHostAndPortToUse());
          if (prev != null) {
            for (Listener listener : listeners) {
              listener.nodeRemoved(node);
            }
          } else {
            log.warn("Node[%s] disappeared but was unknown for service listener [%s].", node, service);
          }
        }
      }
    }
  }
}
