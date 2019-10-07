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

package org.apache.druid.curator.discovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeType;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.initialization.ZkPathsConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 */
@ManageLifecycle
public class CuratorDruidNodeDiscoveryProvider extends DruidNodeDiscoveryProvider
{
  private static final Logger log = new Logger(CuratorDruidNodeDiscoveryProvider.class);

  private final CuratorFramework curatorFramework;
  private final ZkPathsConfig config;
  private final ObjectMapper jsonMapper;

  private ExecutorService listenerExecutor;

  private final ConcurrentHashMap<NodeType, NodeTypeWatcher> nodeTypeWatchers = new ConcurrentHashMap<>();

  private final LifecycleLock lifecycleLock = new LifecycleLock();

  @Inject
  public CuratorDruidNodeDiscoveryProvider(
      CuratorFramework curatorFramework,
      ZkPathsConfig config,
      @Json ObjectMapper jsonMapper
  )
  {
    this.curatorFramework = curatorFramework;
    this.config = config;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public DruidNodeDiscovery getForNodeType(NodeType nodeType)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    return nodeTypeWatchers.computeIfAbsent(
        nodeType,
        nType -> {
          log.info("Creating NodeTypeWatcher for nodeType [%s].", nType);
          NodeTypeWatcher nodeTypeWatcher = new NodeTypeWatcher(
              listenerExecutor,
              curatorFramework,
              config.getInternalDiscoveryPath(),
              jsonMapper,
              nType
          );
          nodeTypeWatcher.start();
          log.info("Created NodeTypeWatcher for nodeType [%s].", nType);
          return nodeTypeWatcher;
        }
    );
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    try {
      log.info("starting");

      // This is single-threaded to ensure that all listener calls are executed precisely in the oder of add/remove
      // event occurences.
      listenerExecutor = Execs.singleThreaded("CuratorDruidNodeDiscoveryProvider-ListenerExecutor");

      log.info("started");

      lifecycleLock.started();
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("can't stop.");
    }

    log.info("stopping");

    for (NodeTypeWatcher watcher : nodeTypeWatchers.values()) {
      watcher.stop();
    }
    listenerExecutor.shutdownNow();

    log.info("stopped");
  }

  private static class NodeTypeWatcher implements DruidNodeDiscovery
  {
    private static final Logger log = new Logger(NodeTypeWatcher.class);

    private final CuratorFramework curatorFramework;

    private final NodeType nodeType;
    private final ObjectMapper jsonMapper;

    /** hostAndPort -> DiscoveryDruidNode */
    private final ConcurrentMap<String, DiscoveryDruidNode> nodes = new ConcurrentHashMap<>();
    private final Collection<DiscoveryDruidNode> unmodifiableNodes = Collections.unmodifiableCollection(nodes.values());

    private final PathChildrenCache cache;
    private final ExecutorService cacheExecutor;

    private final ExecutorService listenerExecutor;

    private final List<DruidNodeDiscovery.Listener> nodeListeners = new ArrayList<>();

    private final Object lock = new Object();

    private final CountDownLatch cacheInitialized = new CountDownLatch(1);

    NodeTypeWatcher(
        ExecutorService listenerExecutor,
        CuratorFramework curatorFramework,
        String basePath,
        ObjectMapper jsonMapper,
        NodeType nodeType
    )
    {
      this.listenerExecutor = listenerExecutor;
      this.curatorFramework = curatorFramework;
      this.nodeType = nodeType;
      this.jsonMapper = jsonMapper;

      // This is required to be single threaded from Docs in PathChildrenCache;
      this.cacheExecutor = Execs.singleThreaded(StringUtils.format("NodeTypeWatcher[%s]", nodeType));
      this.cache = new PathChildrenCache(
          curatorFramework,
          ZKPaths.makePath(basePath, nodeType.toString()),
          true,
          true,
          cacheExecutor
      );
    }

    @Override
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
        log.info("cache is not initialized yet. getAllNodes() might not return full information.");
      }
      return unmodifiableNodes;
    }

    @Override
    public void registerListener(DruidNodeDiscovery.Listener listener)
    {
      synchronized (lock) {
        // No need to wait on CountDownLatch, because we are holding the lock under which it could only be counted down.
        if (cacheInitialized.getCount() == 0) {
          safeSchedule(
              () -> {
                listener.nodesAdded(unmodifiableNodes);
                listener.nodeViewInitialized();
              },
              "Exception occured in nodesAdded([%s]) in listener [%s].", unmodifiableNodes, listener
          );
        }
        nodeListeners.add(listener);
      }
    }

    void handleChildEvent(PathChildrenCacheEvent event)
    {
      synchronized (lock) {
        try {
          switch (event.getType()) {
            case CHILD_ADDED: {
              final byte[] data;
              try {
                data = curatorFramework.getData().decompressed().forPath(event.getData().getPath());
              }
              catch (Exception ex) {
                log.error(
                    ex,
                    "Failed to get data for path [%s]. Ignoring event [%s].",
                    event.getData().getPath(),
                    event.getType()
                );
                return;
              }

              DiscoveryDruidNode druidNode = jsonMapper.readValue(data, DiscoveryDruidNode.class);

              if (!nodeType.equals(druidNode.getNodeType())) {
                log.warn(
                    "Node[%s:%s] add is discovered by node watcher of different node type. Ignored.",
                    druidNode.getDruidNode().getHostAndPortToUse(),
                    druidNode
                );
                return;
              }

              log.info("Node[%s:%s] appeared.", druidNode.getDruidNode().getHostAndPortToUse(), druidNode);

              addNode(druidNode);

              break;
            }
            case CHILD_REMOVED: {
              DiscoveryDruidNode druidNode = jsonMapper.readValue(event.getData().getData(), DiscoveryDruidNode.class);

              if (!nodeType.equals(druidNode.getNodeType())) {
                log.warn(
                    "Node[%s:%s] removal is discovered by node watcher of different type. Ignored.",
                    druidNode.getDruidNode().getHostAndPortToUse(),
                    druidNode
                );
                return;
              }

              log.info(
                  "Node[%s:%s] disappeared.",
                  druidNode.getDruidNode().getHostAndPortToUse(),
                  druidNode
              );

              removeNode(druidNode);

              break;
            }
            case INITIALIZED: {
              // No need to wait on CountDownLatch, because we are holding the lock under which it could only be
              // counted down.
              if (cacheInitialized.getCount() == 0) {
                log.warn("cache is already initialized. ignoring [%s] event.", event.getType());
                return;
              }

              log.info("Received INITIALIZED in node watcher.");

              for (Listener listener : nodeListeners) {
                safeSchedule(
                    () -> {
                      listener.nodesAdded(unmodifiableNodes);
                      listener.nodeViewInitialized();
                    },
                    "Exception occured in nodesAdded([%s]) in listener [%s].",
                    unmodifiableNodes,
                    listener
                );
              }

              cacheInitialized.countDown();
              break;
            }
            default: {
              log.info("Ignored event type [%s] for nodeType watcher.", event.getType());
            }
          }
        }
        catch (Exception ex) {
          log.error(ex, "unknown error in node watcher.");
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
          log.error(ex, errMsgFormat, args);
        }
      });
    }

    @GuardedBy("lock")
    private void addNode(DiscoveryDruidNode druidNode)
    {
      DiscoveryDruidNode prev = nodes.putIfAbsent(druidNode.getDruidNode().getHostAndPortToUse(), druidNode);
      if (prev == null) {
        // No need to wait on CountDownLatch, because we are holding the lock under which it could only be counted down.
        if (cacheInitialized.getCount() == 0) {
          List<DiscoveryDruidNode> newNode = ImmutableList.of(druidNode);
          for (Listener listener : nodeListeners) {
            safeSchedule(
                () -> listener.nodesAdded(newNode),
                "Exception occured in nodeAdded(node=[%s]) in listener [%s].",
                druidNode.getDruidNode().getHostAndPortToUse(),
                listener
            );
          }
        }
      } else {
        log.warn(
            "Node[%s:%s] discovered but existed already [%s].",
            druidNode.getDruidNode().getHostAndPortToUse(),
            druidNode,
            prev
        );
      }
    }

    @GuardedBy("lock")
    private void removeNode(DiscoveryDruidNode druidNode)
    {
      DiscoveryDruidNode prev = nodes.remove(druidNode.getDruidNode().getHostAndPortToUse());

      if (prev == null) {
        log.warn(
            "Noticed disappearance of unknown druid node [%s:%s].",
            druidNode.getDruidNode().getHostAndPortToUse(),
            druidNode
        );
        return;
      }

      // No need to wait on CountDownLatch, because we are holding the lock under which it could only be counted down.
      if (cacheInitialized.getCount() == 0) {
        List<DiscoveryDruidNode> nodeRemoved = ImmutableList.of(druidNode);
        for (Listener listener : nodeListeners) {
          safeSchedule(
              () -> listener.nodesRemoved(nodeRemoved),
              "Exception occured in nodeRemoved(node=[%s]) in listener [%s].",
              druidNode.getDruidNode().getHostAndPortToUse(),
              listener
          );
        }
      }
    }

    public void start()
    {
      try {
        cache.getListenable().addListener((client, event) -> handleChildEvent(event));
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
      }
      catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    public void stop()
    {
      try {
        cache.close();
        cacheExecutor.shutdownNow();
      }
      catch (Exception ex) {
        log.error(ex, "Failed to stop node watcher for type [%s].", nodeType);
      }
    }
  }
}
