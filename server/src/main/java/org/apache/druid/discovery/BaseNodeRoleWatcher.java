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
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Common code used by various implementations of DruidNodeDiscovery.
 *
 * User code is supposed to arrange for following methods to be called,
 * childAdded(DiscoveryDruidNode)
 * childRemove(DiscoveryDruidNode)
 * cacheInitialized()
 * resetNodes(Map<String, DiscoveryDruidNode>)
 *
 */
public class BaseNodeRoleWatcher
{
  private static final Logger LOGGER = new Logger(BaseNodeRoleWatcher.class);

  private final NodeRole nodeRole;

  /**
   * hostAndPort -> DiscoveryDruidNode
   */
  private final ConcurrentMap<String, DiscoveryDruidNode> nodes = new ConcurrentHashMap<>();
  private final Collection<DiscoveryDruidNode> unmodifiableNodes = Collections.unmodifiableCollection(nodes.values());

  private final ExecutorService listenerExecutor;

  private final List<DruidNodeDiscovery.Listener> nodeListeners = new ArrayList<>();

  private final Object lock = new Object();

  private final CountDownLatch cacheInitialized = new CountDownLatch(1);

  public BaseNodeRoleWatcher(
      ExecutorService listenerExecutor,
      NodeRole nodeRole
  )
  {
    this.listenerExecutor = listenerExecutor;
    this.nodeRole = nodeRole;
  }

  public Collection<DiscoveryDruidNode> getAllNodes()
  {
    boolean nodeViewInitialized;
    try {
      nodeViewInitialized = cacheInitialized.await((long) 30, TimeUnit.SECONDS);
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      nodeViewInitialized = false;
    }
    if (!nodeViewInitialized) {
      LOGGER.info(
          "Cache for node role [%s] not initialized yet; getAllNodes() might not return full information.",
          nodeRole.getJsonName()
      );
    }
    return unmodifiableNodes;
  }

  public void registerListener(DruidNodeDiscovery.Listener listener)
  {
    synchronized (lock) {
      // No need to wait on CountDownLatch, because we are holding the lock under which it could only be counted down.
      if (cacheInitialized.getCount() == 0) {
        // It is important to take a snapshot here as list of nodes might change by the time listeners process
        // the changes.
        List<DiscoveryDruidNode> currNodes = Lists.newArrayList(nodes.values());
        safeSchedule(
            () -> {
              listener.nodesAdded(currNodes);
              listener.nodeViewInitialized();
            },
            "Exception occured in nodesAdded([%s]) in listener [%s].", currNodes, listener
        );
      }
      nodeListeners.add(listener);
    }
  }

  public void childAdded(DiscoveryDruidNode druidNode)
  {
    synchronized (lock) {
      if (!nodeRole.equals(druidNode.getNodeRole())) {
        LOGGER.error(
            "Node[%s] of role[%s] addition ignored due to mismatched role (expected role[%s]).",
            druidNode.getDruidNode().getUriToUse(),
            druidNode.getNodeRole().getJsonName(),
            nodeRole.getJsonName()
        );
        return;
      }

      LOGGER.info("Node[%s] of role[%s] detected.", druidNode.getDruidNode().getUriToUse(), nodeRole.getJsonName());

      addNode(druidNode);
    }
  }

  @GuardedBy("lock")
  private void addNode(DiscoveryDruidNode druidNode)
  {
    DiscoveryDruidNode prev = nodes.putIfAbsent(druidNode.getDruidNode().getHostAndPortToUse(), druidNode);
    if (prev == null) {
      // No need to wait on CountDownLatch, because we are holding the lock under which it could only be counted down.
      if (cacheInitialized.getCount() == 0) {
        List<DiscoveryDruidNode> newNode = ImmutableList.of(druidNode);
        for (DruidNodeDiscovery.Listener listener : nodeListeners) {
          safeSchedule(
              () -> listener.nodesAdded(newNode),
              "Exception occured in nodeAdded(node=[%s]) in listener [%s].",
              druidNode.getDruidNode().getHostAndPortToUse(),
              listener
          );
        }
      }
    } else {
      LOGGER.error(
          "Node[%s] of role[%s] discovered but existed already [%s].",
          druidNode.getDruidNode().getUriToUse(),
          nodeRole.getJsonName(),
          prev
      );
    }
  }

  public void childRemoved(DiscoveryDruidNode druidNode)
  {
    synchronized (lock) {
      if (!nodeRole.equals(druidNode.getNodeRole())) {
        LOGGER.error(
            "Node[%s] of role[%s] removal ignored due to mismatched role (expected role[%s]).",
            druidNode.getDruidNode().getUriToUse(),
            druidNode.getNodeRole().getJsonName(),
            nodeRole.getJsonName()
        );
        return;
      }

      LOGGER.info("Node[%s] of role[%s] went offline.", druidNode.getDruidNode().getUriToUse(), nodeRole.getJsonName());

      removeNode(druidNode);
    }
  }

  @GuardedBy("lock")
  private void removeNode(DiscoveryDruidNode druidNode)
  {
    DiscoveryDruidNode prev = nodes.remove(druidNode.getDruidNode().getHostAndPortToUse());

    if (prev == null) {
      LOGGER.error(
          "Noticed disappearance of unknown druid node [%s] of role[%s].",
          druidNode.getDruidNode().getUriToUse(),
          druidNode.getNodeRole().getJsonName()
      );
      return;
    }

    // No need to wait on CountDownLatch, because we are holding the lock under which it could only be counted down.
    if (cacheInitialized.getCount() == 0) {
      List<DiscoveryDruidNode> nodeRemoved = ImmutableList.of(druidNode);
      for (DruidNodeDiscovery.Listener listener : nodeListeners) {
        safeSchedule(
            () -> listener.nodesRemoved(nodeRemoved),
            "Exception occured in nodeRemoved(node[%s] of role[%s]) in listener [%s].",
            druidNode.getDruidNode().getUriToUse(),
            druidNode.getNodeRole().getJsonName(),
            listener
        );
      }
    }
  }

  public void cacheInitialized()
  {
    synchronized (lock) {
      // No need to wait on CountDownLatch, because we are holding the lock under which it could only be
      // counted down.
      if (cacheInitialized.getCount() == 0) {
        LOGGER.error("cache is already initialized. ignoring cache initialization event.");
        return;
      }

      LOGGER.info("Node watcher of role[%s] is now initialized.", nodeRole.getJsonName());

      for (DruidNodeDiscovery.Listener listener : nodeListeners) {
        // It is important to take a snapshot here as list of nodes might change by the time listeners process
        // the changes.
        List<DiscoveryDruidNode> currNodes = Lists.newArrayList(nodes.values());
        safeSchedule(
            () -> {
              listener.nodesAdded(currNodes);
              listener.nodeViewInitialized();
            },
            "Exception occured in nodesAdded([%s]) in listener [%s].",
            currNodes,
            listener
        );
      }

      cacheInitialized.countDown();
    }
  }

  public void resetNodes(Map<String, DiscoveryDruidNode> fullNodes)
  {
    synchronized (lock) {
      List<DiscoveryDruidNode> nodesAdded = new ArrayList<>();
      List<DiscoveryDruidNode> nodesDeleted = new ArrayList<>();

      for (Map.Entry<String, DiscoveryDruidNode> e : fullNodes.entrySet()) {
        if (!nodes.containsKey(e.getKey())) {
          nodesAdded.add(e.getValue());
        }
      }

      for (Map.Entry<String, DiscoveryDruidNode> e : nodes.entrySet()) {
        if (!fullNodes.containsKey(e.getKey())) {
          nodesDeleted.add(e.getValue());
        }
      }

      for (DiscoveryDruidNode node : nodesDeleted) {
        nodes.remove(node.getDruidNode().getHostAndPortToUse());
      }

      for (DiscoveryDruidNode node : nodesAdded) {
        nodes.put(node.getDruidNode().getHostAndPortToUse(), node);
      }

      // No need to wait on CountDownLatch, because we are holding the lock under which it could only be counted down.
      if (cacheInitialized.getCount() == 0) {
        for (DruidNodeDiscovery.Listener listener : nodeListeners) {
          safeSchedule(
              () -> {
                if (!nodesAdded.isEmpty()) {
                  listener.nodesAdded(nodesAdded);
                }

                if (!nodesDeleted.isEmpty()) {
                  listener.nodesRemoved(nodesDeleted);
                }
              },
              "Exception occured in resetNodes in listener [%s].",
              listener
          );
        }
      }
    }
  }

  private void safeSchedule(Runnable runnable, String errMsgFormat, Object... args)
  {
    listenerExecutor.submit(() -> {
      try {
        runnable.run();
      }
      catch (Exception ex) {
        LOGGER.error(errMsgFormat, args);
      }
    });
  }
}
