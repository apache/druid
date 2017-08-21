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

package io.druid.curator.discovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.concurrent.Execs;
import io.druid.concurrent.LifecycleLock;
import io.druid.discovery.DiscoveryDruidNode;
import io.druid.discovery.DruidNodeDiscovery;
import io.druid.discovery.DruidNodeDiscoveryProvider;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.ZKPaths;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

  private final Map<String, NodeTypeWatcher> nodeTypeWatchers = new ConcurrentHashMap<>();

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
  public DruidNodeDiscovery getForNodeType(String nodeType)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    return nodeTypeWatchers.compute(
        nodeType,
        (k, v) -> {
          if (v != null) {
            return v;
          }

          log.info("Creating NodeTypeWatcher for nodeType [%s].", nodeType);
          NodeTypeWatcher nodeTypeWatcher = new NodeTypeWatcher(
              listenerExecutor,
              curatorFramework,
              config.getInternalDiscoveryPath(),
              jsonMapper,
              nodeType
          );
          nodeTypeWatcher.start();
          log.info("Created NodeTypeWatcher for nodeType [%s].", nodeType);
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

    private final String nodeType;
    private final ObjectMapper jsonMapper;

    // hostAndPort -> DiscoveryDruidNode
    private final Map<String, DiscoveryDruidNode> nodes = new ConcurrentHashMap<>();

    private final PathChildrenCache cache;
    private final ExecutorService cacheExecutor;

    private final ExecutorService listenerExecutor;

    private final List<DruidNodeDiscovery.Listener> nodeListeners = new ArrayList();

    private final Object lock = new Object();

    NodeTypeWatcher(
        ExecutorService listenerExecutor,
        CuratorFramework curatorFramework,
        String basePath,
        ObjectMapper jsonMapper,
        String nodeType
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
          ZKPaths.makePath(basePath, nodeType),
          true,
          true,
          cacheExecutor
      );
    }

    @Override
    public Collection<DiscoveryDruidNode> getAllNodes()
    {
      return Collections.unmodifiableCollection(nodes.values());
    }

    @Override
    public void registerListener(DruidNodeDiscovery.Listener listener)
    {
      synchronized (lock) {
        for (DiscoveryDruidNode node : nodes.values()) {
          listenerExecutor.submit(() -> {
            try {
              listener.nodeAdded(node);
            }
            catch (Exception ex) {
              log.error(
                  ex,
                  "Exception occured in DiscoveryDruidNode.nodeAdded(node=[%s]) in listener [%s].",
                  node,
                  listener
              );
            }
          });
        }
        nodeListeners.add(listener);
      }
    }

    public void handleChildEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
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

              DiscoveryDruidNode druidNode = jsonMapper.readValue(
                  data,
                  DiscoveryDruidNode.class
              );

              if (!nodeType.equals(druidNode.getNodeType())) {
                log.warn(
                    "Node[%s:%s] add is discovered by node watcher of nodeType [%s]. Ignored.",
                    druidNode.getNodeType(),
                    druidNode,
                    nodeType
                );
                return;
              }

              log.info("Received event [%s] for Node[%s:%s].", event.getType(), druidNode.getNodeType(), druidNode);

              addNode(druidNode);

              break;
            }
            case CHILD_REMOVED: {
              DiscoveryDruidNode druidNode = jsonMapper.readValue(event.getData().getData(), DiscoveryDruidNode.class);

              if (!nodeType.equals(druidNode.getNodeType())) {
                log.warn(
                    "Node[%s:%s] removal is discovered by node watcher of nodeType [%s]. Ignored.",
                    druidNode.getNodeType(),
                    druidNode,
                    nodeType
                );
                return;
              }

              log.info("Node[%s:%s] disappeared.", druidNode.getNodeType(), druidNode);

              removeNode(druidNode);

              break;
            }
            default: {
              log.error("Ignored event type [%s] for nodeType [%s] watcher.", event.getType(), nodeType);
            }
          }
        }
        catch (Exception ex) {
          log.error(ex, "unknown error in node watcher for type [%s].", nodeType);
        }
      }
    }

    private void addNode(DiscoveryDruidNode druidNode)
    {
      synchronized (lock) {
        DiscoveryDruidNode prev = nodes.putIfAbsent(druidNode.getDruidNode().getHostAndPortToUse(), druidNode);
        if (prev == null) {
          for (DruidNodeDiscovery.Listener l : nodeListeners) {
            listenerExecutor.submit(() -> {
              try {
                l.nodeAdded(druidNode);
              }
              catch (Exception ex) {
                log.error(
                    ex,
                    "Exception occured in DiscoveryDruidNode.nodeAdded(node=[%s]) in listener [%s].",
                    druidNode,
                    l
                );
              }
            });
          }
        } else {
          log.warn("Node[%s] discovered but existed already [%s].", druidNode, prev);
        }
      }
    }

    private void removeNode(DiscoveryDruidNode druidNode)
    {
      synchronized (lock) {
        DiscoveryDruidNode prev = nodes.remove(druidNode.getDruidNode().getHostAndPortToUse());

        if (prev == null) {
          log.warn("Noticed disappearance of unknown druid node [%s:%s].", druidNode.getNodeType(), druidNode);
          return;
        }

        for (DruidNodeDiscovery.Listener l : nodeListeners) {
          listenerExecutor.submit(() -> {
            try {
              l.nodeRemoved(druidNode);
            }
            catch (Exception ex) {
              log.error(
                  ex,
                  "Exception occured in DiscoveryDruidNode.nodeRemoved(node=[%s]) in listener [%s].",
                  druidNode,
                  l
              );
            }
          });
        }
      }
    }

    public void start()
    {
      try {
        cache.getListenable().addListener(
            (client, event) -> handleChildEvent(client, event)
        );
        cache.start();
      }
      catch (Exception ex) {
        throw Throwables.propagate(ex);
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
