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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.curator.cache.PathChildrenCacheFactory;
import org.apache.druid.discovery.BaseNodeRoleWatcher;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;

/**
 *
 */
@ManageLifecycle
public class CuratorDruidNodeDiscoveryProvider extends DruidNodeDiscoveryProvider
{
  private static final Logger log = new Logger(CuratorDruidNodeDiscoveryProvider.class);

  private final CuratorFramework curatorFramework;
  private final ZkPathsConfig config;
  private final ObjectMapper jsonMapper;

  private ExecutorService listenerExecutor;

  private final ConcurrentHashMap<NodeRole, NodeRoleWatcher> nodeRoleWatchers = new ConcurrentHashMap<>();
  private final ConcurrentLinkedQueue<NodeDiscoverer> nodeDiscoverers = new ConcurrentLinkedQueue<>();

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
  public BooleanSupplier getForNode(DruidNode node, NodeRole nodeRole)
  {
    Preconditions.checkState(lifecycleLock.isStarted());
    log.debug("Creating a NodeDiscoverer for node [%s] and role [%s]", node, nodeRole);
    NodeDiscoverer nodeDiscoverer = new NodeDiscoverer(config, jsonMapper, curatorFramework, node, nodeRole);
    nodeDiscoverers.add(nodeDiscoverer);
    return nodeDiscoverer::nodeDiscovered;
  }

  @Override
  public DruidNodeDiscovery getForNodeRole(NodeRole nodeRole)
  {
    Preconditions.checkState(lifecycleLock.isStarted());

    return nodeRoleWatchers.computeIfAbsent(
        nodeRole,
        role -> {
          log.debug("Creating NodeRoleWatcher for nodeRole [%s].", role);
          NodeRoleWatcher nodeRoleWatcher = new NodeRoleWatcher(
              listenerExecutor,
              curatorFramework,
              config.getInternalDiscoveryPath(),
              jsonMapper,
              role
          );
          log.debug("Created NodeRoleWatcher for nodeRole [%s].", role);
          return nodeRoleWatcher;
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
      // This is single-threaded to ensure that all listener calls are executed precisely in the order of add/remove
      // event occurrences.
      listenerExecutor = Execs.singleThreaded("CuratorDruidNodeDiscoveryProvider-ListenerExecutor");

      log.debug("Started.");

      lifecycleLock.started();
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @LifecycleStop
  public void stop() throws IOException
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("can't stop.");
    }

    log.debug("Stopping.");

    Closer closer = Closer.create();
    closer.registerAll(nodeRoleWatchers.values());
    closer.registerAll(nodeDiscoverers);

    CloseableUtils.closeAll(closer, listenerExecutor::shutdownNow);
  }

  private static class NodeRoleWatcher implements DruidNodeDiscovery, Closeable
  {
    private static final Logger log = new Logger(NodeRoleWatcher.class);

    private final CuratorFramework curatorFramework;

    private final NodeRole nodeRole;
    private final ObjectMapper jsonMapper;
    private final BaseNodeRoleWatcher baseNodeRoleWatcher;

    private final PathChildrenCache cache;
    private final ExecutorService cacheExecutor;

    private final Object lock = new Object();

    NodeRoleWatcher(
        ExecutorService listenerExecutor,
        CuratorFramework curatorFramework,
        String basePath,
        ObjectMapper jsonMapper,
        NodeRole nodeRole
    )
    {
      this.curatorFramework = curatorFramework;
      this.nodeRole = nodeRole;
      this.jsonMapper = jsonMapper;
      this.baseNodeRoleWatcher = new BaseNodeRoleWatcher(listenerExecutor, nodeRole);

      // This is required to be single threaded from docs in PathChildrenCache.
      this.cacheExecutor = Execs.singleThreaded(
          StringUtils.format("NodeRoleWatcher[%s]", StringUtils.encodeForFormat(nodeRole.toString()))
      );
      cache = new PathChildrenCacheFactory.Builder()
          //NOTE: cacheData is temporarily set to false and we get data directly from ZK on each event.
          //this is a workaround to solve curator's out-of-order events problem
          //https://issues.apache.org/jira/browse/CURATOR-191
          // This is also done in CuratorInventoryManager.
          .withCacheData(true)
          .withCompressed(true)
          .withExecutorService(cacheExecutor)
          .build()
          .make(curatorFramework, ZKPaths.makePath(basePath, nodeRole.toString()));

      try {
        cache.getListenable().addListener((client, event) -> handleChildEvent(event));
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
      }
      catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public void close() throws IOException
    {
      CloseableUtils.closeAll(cache, cacheExecutor::shutdownNow);
    }

    @Override
    public Collection<DiscoveryDruidNode> getAllNodes()
    {
      return baseNodeRoleWatcher.getAllNodes();
    }

    @Override
    public void registerListener(DruidNodeDiscovery.Listener listener)
    {
      baseNodeRoleWatcher.registerListener(listener);
    }

    void handleChildEvent(PathChildrenCacheEvent event)
    {
      synchronized (lock) {
        try {
          switch (event.getType()) {
            case CHILD_ADDED: {
              childAdded(event);
              break;
            }
            case CHILD_REMOVED: {
              childRemoved(event);
              break;
            }
            case INITIALIZED: {
              baseNodeRoleWatcher.cacheInitialized();
              break;
            }
            default: {
              log.warn("Ignored event type [%s] for node watcher of role [%s].", event.getType(), nodeRole.getJsonName());
            }
          }
        }
        catch (Exception ex) {
          log.error(ex, "Unknown error in node watcher of role [%s].", nodeRole.getJsonName());
        }
      }
    }

    @GuardedBy("lock")
    private void childAdded(PathChildrenCacheEvent event) throws IOException
    {
      final byte[] data = getZkDataForNode(event.getData());
      if (data == null) {
        log.error(
            "Failed to get data for path [%s]. Ignoring a child addition event.",
            event.getData().getPath()
        );
        return;
      }

      baseNodeRoleWatcher.childAdded(jsonMapper.readValue(data, DiscoveryDruidNode.class));
    }

    @GuardedBy("lock")
    private void childRemoved(PathChildrenCacheEvent event) throws IOException
    {
      final byte[] data = event.getData().getData();
      if (data == null) {
        log.error("Failed to get data for path [%s]. Ignoring a child removal event.", event.getData().getPath());
        return;
      }

      baseNodeRoleWatcher.childRemoved(jsonMapper.readValue(data, DiscoveryDruidNode.class));
    }

    /**
     * Doing this instead of a simple call to {@link ChildData#getData()} because data cache is turned off, see a
     * comment in {@link #NodeRoleWatcher}.
     */
    @Nullable
    private byte[] getZkDataForNode(ChildData child)
    {
      try {
        return curatorFramework.getData().decompressed().forPath(child.getPath());
      }
      catch (Exception ex) {
        log.error(ex, "Exception while getting data for node %s", child.getPath());
        return null;
      }
    }
  }

  private static class NodeDiscoverer implements Closeable
  {
    private final ObjectMapper jsonMapper;
    private final NodeCache nodeCache;
    private final NodeRole nodeRole;

    private NodeDiscoverer(
        ZkPathsConfig config,
        ObjectMapper jsonMapper,
        CuratorFramework curatorFramework,
        DruidNode node,
        NodeRole nodeRole
    )
    {
      this.jsonMapper = jsonMapper;
      String path = CuratorDruidNodeAnnouncer.makeNodeAnnouncementPath(config, nodeRole, node);
      nodeCache = new NodeCache(curatorFramework, path, true);
      this.nodeRole = nodeRole;

      try {
        nodeCache.start(true /* buildInitial */);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private boolean nodeDiscovered()
    {
      @Nullable ChildData currentChild = nodeCache.getCurrentData();
      if (currentChild == null) {
        // Not discovered yet.
        return false;
      }

      final byte[] data = currentChild.getData();

      DiscoveryDruidNode druidNode;
      try {
        druidNode = jsonMapper.readValue(data, DiscoveryDruidNode.class);
      }
      catch (IOException e) {
        log.error(e, "Exception occurred when reading node's value");
        return false;
      }

      if (!nodeRole.equals(druidNode.getNodeRole())) {
        log.error(
            "Node[%s] of role[%s] add is discovered by node watcher of different node role. Ignored.",
            druidNode.getDruidNode().getUriToUse(),
            druidNode.getNodeRole().getJsonName()
        );
        return false;
      }

      log.info(
          "Node[%s] of role[%s] appeared.",
          druidNode.getDruidNode().getUriToUse(),
          druidNode.getNodeRole().getJsonName()
      );
      return true;
    }

    @Override
    public void close() throws IOException
    {
      nodeCache.close();
    }
  }
}
