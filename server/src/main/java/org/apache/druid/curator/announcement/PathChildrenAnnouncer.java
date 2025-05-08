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

package org.apache.druid.curator.announcement;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorMultiTransaction;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.curator.cache.PathChildrenCacheFactory;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The {@link PathChildrenAnnouncer} class manages the announcement of a node, and watches all child
 * and sibling nodes under the specified path in a ZooKeeper ensemble. It monitors these nodes
 * to ensure their existence and manage their lifecycle collectively.
 *
 * <p>
 * This class uses Apache Curator's PathChildrenCache recipe under the hood to track all znodes
 * under the specified node's parent. See {@link NodeAnnouncer} for an announcer that
 * uses the NodeCache recipe instead.
 * </p>
 */
public class PathChildrenAnnouncer implements ServiceAnnouncer
{
  private static final Logger log = new Logger(PathChildrenAnnouncer.class);

  private final CuratorFramework curator;
  private final PathChildrenCacheFactory factory;
  private final ExecutorService pathChildrenCacheExecutor;

  @GuardedBy("toAnnounce")
  private final List<Announceable> toAnnounce = new ArrayList<>();
  @GuardedBy("toAnnounce")
  private final List<Announceable> toUpdate = new ArrayList<>();
  private final ConcurrentHashMap<String, PathChildrenCache> listeners = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, byte[]>> announcements = new ConcurrentHashMap<>();
  private final List<String> parentsIBuilt = new CopyOnWriteArrayList<>();

  // Used for testing
  private Set<String> addedChildren;

  private boolean started = false;

  public PathChildrenAnnouncer(
      CuratorFramework curator,
      ExecutorService exec
  )
  {
    this.curator = curator;
    this.pathChildrenCacheExecutor = exec;
    this.factory = new PathChildrenCacheFactory.Builder()
        .withCacheData(false)
        .withCompressed(true)
        .withExecutorService(exec)
        .withShutdownExecutorOnClose(false)
        .build();
  }

  @VisibleForTesting
  void initializeAddedChildren()
  {
    addedChildren = new HashSet<>();
  }

  @VisibleForTesting
  Set<String> getAddedChildren()
  {
    return addedChildren;
  }

  @LifecycleStart
  @Override
  public void start()
  {
    log.debug("Starting Announcer.");
    synchronized (toAnnounce) {
      if (started) {
        log.debug("Announcer has already been started by another thread, ignoring start request.");
        return;
      }

      started = true;

      for (Announceable announceable : toAnnounce) {
        announce(announceable.path, announceable.bytes, announceable.removeParentsIfCreated);
      }
      toAnnounce.clear();

      for (Announceable announceable : toUpdate) {
        update(announceable.path, announceable.bytes);
      }
      toUpdate.clear();
    }
  }

  @LifecycleStop
  @Override
  public void stop()
  {
    log.debug("Stopping Announcer.");
    synchronized (toAnnounce) {
      if (!started) {
        log.debug("Announcer has already been stopped by another thread, ignoring stop request.");
        return;
      }

      started = false;

      try {
        CloseableUtils.closeAll(listeners.values());
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      finally {
        pathChildrenCacheExecutor.shutdown();
      }

      for (Map.Entry<String, ConcurrentHashMap<String, byte[]>> entry : announcements.entrySet()) {
        String basePath = entry.getKey();

        for (String announcementPath : entry.getValue().keySet()) {
          unannounce(ZKPaths.makePath(basePath, announcementPath));
        }
      }

      if (!parentsIBuilt.isEmpty()) {
        CuratorMultiTransaction transaction = curator.transaction();

        ArrayList<CuratorOp> operations = new ArrayList<>();
        for (String parent : parentsIBuilt) {
          try {
            operations.add(curator.transactionOp().delete().forPath(parent));
          }
          catch (Exception e) {
            log.info(e, "Unable to delete parent[%s] when closing Announcer.", parent);
          }
        }

        try {
          transaction.forOperations(operations);
        }
        catch (Exception e) {
          log.info(e, "Unable to commit transaction when closing Announcer.");
        }
      }
    }
  }

  /**
   * Overload of {@link #announce(String, byte[], boolean)}, but removes parent node of path after announcement.
   */
  @Override
  public void announce(String path, byte[] bytes)
  {
    announce(path, bytes, true);
  }

  /**
   * Announces the provided bytes at the given path.
   *
   * <p>
   * Announcement using {@link PathChildrenAnnouncer} will create an ephemeral znode at the specified path, and uses its parent
   * path to watch all the siblings and children znodes of your specified path. The watched nodes will always exist
   * until it is unannounced, or until {@link #stop()} is called.
   * </p>
   *
   * @param path                  The path to announce at
   * @param bytes                 The payload to announce
   * @param removeParentIfCreated remove parent of "path" if we had created that parent during announcement
   */
  @Override
  public void announce(String path, byte[] bytes, boolean removeParentIfCreated)
  {
    synchronized (toAnnounce) {
      if (!started) {
        log.debug("Announcer has not started yet, queuing announcement for later processing...");
        toAnnounce.add(new Announceable(path, bytes, removeParentIfCreated));
        return;
      }
    }

    final ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);

    final String parentPath = pathAndNode.getPath();
    boolean buildParentPath = false;

    ConcurrentMap<String, byte[]> subPaths = announcements.get(parentPath);

    if (subPaths == null) {
      try {
        if (curator.checkExists().forPath(parentPath) == null) {
          buildParentPath = true;
        }
      }
      catch (Exception e) {
        log.debug(e, "Problem checking if the parent existed, ignoring.");
      }

      final ConcurrentHashMap<String, byte[]> finalSubPaths = announcements.computeIfAbsent(parentPath, key -> new ConcurrentHashMap<>());

      // Synchronize to make sure that I only create a listener once.
      synchronized (finalSubPaths) {
        if (!listeners.containsKey(parentPath)) {
          final PathChildrenCache cache = factory.make(curator, parentPath);
          cache.getListenable().addListener(
              new PathChildrenCacheListener()
              {
                private final AtomicReference<Set<String>> pathsLost = new AtomicReference<>(null);

                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                {
                  // NOTE: ZooKeeper does not guarantee that we will get every event, and thus PathChildrenCache doesn't
                  // as well. If one of the below events are missed, Announcer might not work properly.
                  log.debug("Path[%s] got event[%s]", parentPath, event);
                  switch (event.getType()) {
                    case CHILD_REMOVED:
                      final ChildData child = event.getData();
                      final ZKPaths.PathAndNode childPath = ZKPaths.getPathAndNode(child.getPath());
                      final byte[] value = finalSubPaths.get(childPath.getNode());
                      if (value != null) {
                        log.info("Node[%s] dropped, reinstating.", child.getPath());
                        createAnnouncement(child.getPath(), value);
                      }
                      break;
                    case CONNECTION_LOST:
                      // Lost connection, which means session is broken, take inventory of what has been seen.
                      // This is to protect from a race condition in which the ephemeral node could have been
                      // created but not actually seen by the PathChildrenCache, which means that it won't know
                      // that it disappeared and thus will not generate a CHILD_REMOVED event for us.  Under normal
                      // circumstances, this can only happen upon connection loss; but technically if you have
                      // an adversary in the system, they could also delete the ephemeral node before the cache sees
                      // it.  This does not protect from that case, so don't have adversaries.

                      Set<String> pathsToReinstate = new HashSet<>();
                      for (String node : finalSubPaths.keySet()) {
                        String path = ZKPaths.makePath(parentPath, node);
                        log.info("Node[%s] is added to reinstate.", path);
                        pathsToReinstate.add(path);
                      }

                      if (!pathsToReinstate.isEmpty() && !pathsLost.compareAndSet(null, pathsToReinstate)) {
                        log.info("Already had a pathsLost set!?[%s]", parentPath);
                      }
                      break;
                    case CONNECTION_RECONNECTED:
                      final Set<String> thePathsLost = pathsLost.getAndSet(null);

                      if (thePathsLost != null) {
                        for (String path : thePathsLost) {
                          log.info("Reinstating [%s]", path);
                          final ZKPaths.PathAndNode split = ZKPaths.getPathAndNode(path);
                          createAnnouncement(path, announcements.get(split.getPath()).get(split.getNode()));
                        }
                      }
                      break;
                    case CHILD_ADDED:
                      if (addedChildren != null) {
                        addedChildren.add(event.getData().getPath());
                      }
                      // fall through
                    case INITIALIZED:
                    case CHILD_UPDATED:
                    case CONNECTION_SUSPENDED:
                      // do nothing
                  }
                }
              }
          );

          synchronized (toAnnounce) {
            if (started) {
              if (buildParentPath) {
                createPath(parentPath, removeParentIfCreated);
              }
              startCache(cache);
              listeners.put(parentPath, cache);
            }
          }
        }
      }

      subPaths = finalSubPaths;
    }

    boolean created = false;
    synchronized (toAnnounce) {
      if (started) {
        byte[] oldBytes = subPaths.putIfAbsent(pathAndNode.getNode(), bytes);

        if (oldBytes == null) {
          created = true;
        } else if (!Arrays.equals(oldBytes, bytes)) {
          throw new IAE("Cannot reannounce different values under the same path");
        }
      }
    }

    if (created) {
      try {
        createAnnouncement(path, bytes);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void update(final String path, final byte[] bytes)
  {
    synchronized (toAnnounce) {
      if (!started) {
        toUpdate.add(new Announceable(path, bytes));
        return;
      }
    }

    final ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);

    final String parentPath = pathAndNode.getPath();
    final String nodePath = pathAndNode.getNode();

    ConcurrentMap<String, byte[]> subPaths = announcements.get(parentPath);

    if (subPaths == null || subPaths.get(nodePath) == null) {
      throw new ISE("Cannot update path[%s] that hasn't been announced!", path);
    }

    synchronized (toAnnounce) {
      try {
        byte[] oldBytes = subPaths.get(nodePath);

        if (!Arrays.equals(oldBytes, bytes)) {
          subPaths.put(nodePath, bytes);
          updateAnnouncement(path, bytes);
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void createAnnouncement(final String path, byte[] value) throws Exception
  {
    curator.create().compressed().withMode(CreateMode.EPHEMERAL).inBackground().forPath(path, value);
  }

  private void updateAnnouncement(final String path, final byte[] value) throws Exception
  {
    curator.setData().compressed().inBackground().forPath(path, value);
  }

  /**
   * Unannounces an announcement created at path.  Note that if all announcements get removed, the Announcer
   * will continue to have ZK watches on paths because clearing them out is a source of ugly race conditions.
   * <p/>
   * If you need to completely clear all the state of what is being watched and announced, stop() the Announcer.
   *
   * @param path the path to unannounce
   */
  @Override
  public void unannounce(String path)
  {
    final ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);
    final String parentPath = pathAndNode.getPath();

    final ConcurrentMap<String, byte[]> subPaths = announcements.get(parentPath);

    if (subPaths == null || subPaths.remove(pathAndNode.getNode()) == null) {
      log.debug("Path[%s] not announced, cannot unannounce.", path);
      return;
    }
    log.info("Unannouncing [%s]", path);

    try {
      CuratorOp deleteOp = curator.transactionOp().delete().forPath(path);
      curator.transaction().forOperations(deleteOp);
    }
    catch (KeeperException.NoNodeException e) {
      log.info("Unannounced node[%s] that does not exist.", path);
    }
    catch (KeeperException.NotEmptyException e) {
      log.warn("Unannouncing non-empty path[%s]", path);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void startCache(PathChildrenCache cache)
  {
    try {
      cache.start();
    }
    catch (Throwable e) {
      throw CloseableUtils.closeAndWrapInCatch(e, cache);
    }
  }

  private void createPath(String parentPath, boolean removeParentsIfCreated)
  {
    try {
      curator.create().creatingParentsIfNeeded().forPath(parentPath);
      if (removeParentsIfCreated) {
        parentsIBuilt.add(parentPath);
      }
      log.debug("Created parentPath[%s], %s remove on stop.", parentPath, removeParentsIfCreated ? "will" : "will not");
    }
    catch (KeeperException.NodeExistsException e) {
      log.info(e, "Problem creating parentPath[%s], someone else created it first?", parentPath);
    }
    catch (Exception e) {
      log.error(e, "Unhandled exception when creating parentPath[%s].", parentPath);
    }
  }
}
