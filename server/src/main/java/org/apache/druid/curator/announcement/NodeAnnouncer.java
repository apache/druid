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
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.utils.ZKPaths;
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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

/**
 * The {@link NodeAnnouncer} class is responsible for announcing a single node
 * in a ZooKeeper ensemble. It creates an ephemeral node at a specified path
 * and monitors its existence to ensure that it remains active until it is
 * explicitly unannounced or the object is closed.
 *
 * <p>
 * This class uses Apache Curator's NodeCache recipe under the hood to track a single
 * node, along with all of its parent's status. See {@link Announcer} for an announcer that
 * uses the PathChildrenCache recipe instead.
 * </p>
 */
public class NodeAnnouncer
{
  private static final Logger log = new Logger(NodeAnnouncer.class);

  private final CuratorFramework curator;
  private final ExecutorService nodeCacheExecutor;

  private final ConcurrentHashMap<String, NodeCache> listeners = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, byte[]> announcedPaths = new ConcurrentHashMap<>();

  @GuardedBy("toAnnounce")
  private boolean started = false;

  /**
   * This list holds paths that need to be announced. If a path is added to this list
   * in the {@link #announce(String, byte[], boolean)} method before the connection to ZooKeeper is established,
   * it will be stored here and announced later during the {@link #start} method.
   */
  @GuardedBy("toAnnounce")
  private final List<Announceable> toAnnounce = new ArrayList<>();

  /**
   * This list holds paths that need to be updated. If a path is added to this list
   * in the {@link #update} method before the connection to ZooKeeper is established,
   * it will be stored here and updated later during the {@link #start} method.
   */
  @GuardedBy("toAnnounce")
  private final List<Announceable> toUpdate = new ArrayList<>();

  /**
   * This list keeps track of all the paths created by this node announcer.
   * When the {@link #stop} method is called,
   * the node announcer is responsible for deleting all paths stored in this list.
   */
  @GuardedBy("toAnnounce")
  private final List<String> parentsIBuilt = new CopyOnWriteArrayList<>();

  public NodeAnnouncer(CuratorFramework curator, ExecutorService exec)
  {
    this.curator = curator;
    this.nodeCacheExecutor = exec;
  }

  @VisibleForTesting
  Set<String> getAddedPaths()
  {
    return announcedPaths.keySet();
  }

  @LifecycleStart
  public void start()
  {
    log.debug("Starting NodeAnnouncer");
    synchronized (toAnnounce) {
      if (started) {
        log.debug("NodeAnnouncer has already been started by another thread, ignoring start request.");
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
  public void stop()
  {
    log.debug("Stopping NodeAnnouncer");
    synchronized (toAnnounce) {
      if (!started) {
        log.debug("NodeAnnouncer has already been stopped by another thread, ignoring stop request.");
        return;
      }

      started = false;
      closeResources();
    }
  }

  @GuardedBy("toAnnounce")
  private void closeResources()
  {
    try {
      // Close all caches...
      CloseableUtils.closeAll(listeners.values());
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    finally {
      nodeCacheExecutor.shutdown();
    }

    for (String announcementPath : announcedPaths.keySet()) {
      unannounce(announcementPath);
    }

    if (!parentsIBuilt.isEmpty()) {
      CuratorMultiTransaction transaction = curator.transaction();

      ArrayList<CuratorOp> operations = new ArrayList<>();
      for (String parent : parentsIBuilt) {
        try {
          operations.add(curator.transactionOp().delete().forPath(parent));
        }
        catch (Exception e) {
          log.info(e, "Unable to delete parent[%s] when closing NodeAnnouncer.", parent);
        }
      }

      try {
        transaction.forOperations(operations);
      }
      catch (Exception e) {
        log.info(e, "Unable to commit transaction when closing NodeAnnouncer.");
      }
    }
  }

  /**
   * Overload of {@link #announce(String, byte[], boolean)}, but removes parent node of path after announcement.
   */
  public void announce(String path, byte[] bytes)
  {
    announce(path, bytes, true);
  }

  /**
   * Announces the provided bytes at the given path.
   *
   * <p>
   * Announcement using {@link NodeAnnouncer} will create an ephemeral znode at the specified path, and listens for
   * changes on your znode. Your znode will exist until it is unannounced, or until {@link #stop()} is called.
   * </p>
   *
   * @param path                  The path to announce at
   * @param bytes                 The payload to announce
   * @param removeParentIfCreated remove parent of "path" if we had created that parent during announcement
   */
  public void announce(String path, byte[] bytes, boolean removeParentIfCreated)
  {
    synchronized (toAnnounce) {
      if (!started) {
        log.debug("NodeAnnouncer has not started yet, queuing announcement for later processing...");
        toAnnounce.add(new Announceable(path, bytes, removeParentIfCreated));
        return;
      }
    }

    final String parentPath = ZKPaths.getPathAndNode(path).getPath();
    byte[] announcedPayload = announcedPaths.get(path);

    // If announcedPayload is null, this means that we have yet to announce this path.
    // There is a possibility that the parent paths do not exist, so we check if we need to create the parent path first.
    if (announcedPayload == null) {
      boolean buildParentPath = false;
      try {
        buildParentPath = curator.checkExists().forPath(parentPath) == null;
      }
      catch (Exception e) {
        log.warn(e, "Failed to check existence of parent path. Proceeding without creating parent path.");
      }

      // Synchronize to make sure that I only create a listener once.
      synchronized (toAnnounce) {
        if (!listeners.containsKey(path)) {
          final NodeCache cache = setupNodeCache(path);

          if (started) {
            if (buildParentPath) {
              createPath(parentPath, removeParentIfCreated);
            }
            startCache(cache);
            listeners.put(path, cache);
          }
        }
      }
    }

    final boolean readyToCreateAnnouncement = updateAnnouncedPaths(path, bytes);

    if (readyToCreateAnnouncement) {
      try {
        createAnnouncement(path, bytes);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @GuardedBy("toAnnounce")
  private NodeCache setupNodeCache(String path)
  {
    final NodeCache cache = new NodeCache(curator, path, true);
    cache.getListenable().addListener(
        () -> nodeCacheExecutor.submit(() -> {
          ChildData currentData = cache.getCurrentData();

          if (currentData == null) {
            // If currentData is null, and we know we have already announced the data,
            // this means that the ephemeral node was unexpectedly removed.
            // We will recreate the node again using the previous data.
            final byte[] previouslyAnnouncedData = announcedPaths.get(path);
            if (previouslyAnnouncedData != null) {
              log.info("Node[%s] dropped, reinstating.", path);
              try {
                createAnnouncement(path, previouslyAnnouncedData);
              }
              catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
          }
        })
    );
    return cache;
  }

  private boolean updateAnnouncedPaths(String path, byte[] bytes)
  {
    synchronized (toAnnounce) {
      if (!started) {
        return false; // Do nothing if not started
      }
    }

    final byte[] updatedAnnouncementData = announcedPaths.compute(path, (key, oldBytes) -> {
      if (oldBytes == null) {
        return bytes; // Insert the new value
      } else if (!Arrays.equals(oldBytes, bytes)) {
        throw new IAE("Cannot reannounce different values under the same path.");
      }
      return oldBytes; // No change if values are equal
    });

    // Return true if we have updated the paths.
    return Arrays.equals(updatedAnnouncementData, bytes);
  }

  public void update(final String path, final byte[] bytes)
  {
    synchronized (toAnnounce) {
      if (!started) {
        log.debug("NodeAnnouncer has not started yet, queuing updates for later processing...");
        // removeParentsIfCreated is not relevant for updates; use dummy value "false".
        toUpdate.add(new Announceable(path, bytes, false));
        return;
      }

      byte[] oldBytes = announcedPaths.get(path);

      if (oldBytes == null) {
        throw new ISE("Cannot update path[%s] that hasn't been announced!", path);
      }

      try {
        if (!Arrays.equals(oldBytes, bytes)) {
          announcedPaths.put(path, bytes);
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
  public void unannounce(String path)
  {
    synchronized (toAnnounce) {
      final byte[] value = announcedPaths.remove(path);

      if (value == null) {
        log.debug("Path[%s] not announced, cannot unannounce.", path);
        return;
      }
    }

    log.info("unannouncing [%s]", path);

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

  private void startCache(NodeCache cache)
  {
    try {
      cache.start();
    }
    catch (Throwable e) {
      throw CloseableUtils.closeAndWrapInCatch(e, cache);
    }
  }

  @GuardedBy("toAnnounce")
  private void createPath(String parentPath, boolean removeParentsIfCreated)
  {
    try {
      curator.create().creatingParentsIfNeeded().forPath(parentPath);
      if (removeParentsIfCreated) {
        // We keep track of all parents we have built, so we can delete them later on when needed.
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
