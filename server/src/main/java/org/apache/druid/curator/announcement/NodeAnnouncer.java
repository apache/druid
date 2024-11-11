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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.ZKPathsUtils;
import org.apache.druid.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The {@link NodeAnnouncer} class is responsible for announcing a single node
 * in a ZooKeeper ensemble. It creates an ephemeral node at a specified path
 * and monitors its existence to ensure that it remains active until it is
 * explicitly unannounced or the object is closed.
 *
 * <p>This class provides methods to announce and update the content of the
 * node as well as handle path creation if required.</p>
 *
 * <p>Use this class when you need to manage the lifecycle of a standalone
 * node without concerns about its children or siblings. Should your use case
 * involve the management of child nodes under a specific parent path in a
 * ZooKeeper ensemble, see {@link Announcer}.</p>
 */
public class NodeAnnouncer
{
  private static final Logger log = new Logger(NodeAnnouncer.class);

  private final CuratorFramework curator;
  private final ConcurrentMap<String, NodeCache> listeners = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, byte[]> announcedPaths = new ConcurrentHashMap<>();

  @GuardedBy("toAnnounce")
  private boolean started = false;

  /**
   * This list holds paths that need to be announced. If a path is added to this list
   * in the {@link #announce} method before the connection to ZooKeeper is established,
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
  private final List<String> pathsCreatedInThisAnnouncer = new ArrayList<>();

  public NodeAnnouncer(CuratorFramework curator)
  {
    this.curator = curator;
  }

  @VisibleForTesting
  Set<String> getAddedPaths()
  {
    return announcedPaths.keySet();
  }

  @LifecycleStart
  public void start()
  {
    log.info("Starting NodeAnnouncer");
    synchronized (toAnnounce) {
      if (started) {
        log.debug("Called start() but NodeAnnouncer have already started.");
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
    log.info("Stopping NodeAnnouncer");
    synchronized (toAnnounce) {
      if (!started) {
        log.debug("Called stop() but NodeAnnouncer have not started.");
        return;
      }

      started = false;
      closeResources();
      deletePaths();
    }
  }

  private void closeResources()
  {
    Closer closer = Closer.create();
    for (NodeCache cache : listeners.values()) {
      closer.register(cache);
    }
    for (String announcementPath : announcedPaths.keySet()) {
      closer.register(() -> unannounce(announcementPath));
    }
    CloseableUtils.closeAndWrapExceptions(closer);
  }

  private void deletePaths()
  {
    // deletePaths method is only used in stop(), which already has synchronized(toAnnounce),
    // this line is here just to prevent the static analysis from throwing
    // "Access to field 'pathsCreatedInThisAnnouncer' outside declared guards".
    synchronized (toAnnounce) {
      if (!pathsCreatedInThisAnnouncer.isEmpty()) {
        final List<CuratorOp> deleteOps = new ArrayList<>(pathsCreatedInThisAnnouncer.size());
        for (String parent : pathsCreatedInThisAnnouncer) {
          try {
            deleteOps.add(curator.transactionOp().delete().forPath(parent));
          }
          catch (Exception e) {
            log.error(e, "Unable to delete parent[%s].", parent);
          }
        }

        try {
          curator.transaction().forOperations(deleteOps);
        }
        catch (Exception e) {
          log.error(e, "Unable to commit transaction.");
        }
      }
    }
  }


  /**
   * Overload of {@link #announce(String, byte[],boolean)}, but removes parent node of path after announcement.
   */
  public void announce(String path, byte[] bytes)
  {
    announce(path, bytes, true);
  }

  /**
   * Announces the provided bytes at the given path.  Announcement means that it will create an ephemeral node
   * and monitor it to make sure that it always exists until it is unannounced or this object is closed.
   *
   * @param path                  The path to announce at
   * @param bytes                 The payload to announce
   * @param removeParentIfCreated remove parent of "path" if we had created that parent
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

    final String parentPath = ZKPathsUtils.getParentPath(path);
    boolean buildParentPath = false;

    byte[] value = announcedPaths.get(path);

    if (value == null) {
      try {
        if (curator.checkExists().forPath(parentPath) == null) {
          buildParentPath = true;
        }
      }
      catch (Exception e) {
        log.debug(e, "Problem checking if the parent path doesn't exist, ignoring.");
      }

      // Synchronize to make sure that I only create a listener once.
      synchronized (toAnnounce) {
        if (!listeners.containsKey(path)) {
          final NodeCache cache = new NodeCache(curator, path, true);
          cache.getListenable().addListener(
              () -> {
                ChildData currentData = cache.getCurrentData();
                if (currentData == null) {
                  final byte[] value1 = announcedPaths.get(path);
                  if (value1 != null) {
                    log.info("Node[%s] dropped, reinstating.", path);
                    createAnnouncement(path, value1);
                  }
                }
              }
          );

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

    boolean created = false;
    synchronized (toAnnounce) {
      if (started) {
        byte[] oldBytes = announcedPaths.putIfAbsent(path, bytes);

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

  public void update(final String path, final byte[] bytes)
  {
    synchronized (toAnnounce) {
      if (!started) {
        // removeParentsIfCreated is not relevant for updates; use dummy value "false".
        toUpdate.add(new Announceable(path, bytes, false));
        return;
      }
    }

    byte[] oldBytes = announcedPaths.get(path);

    if (oldBytes == null) {
      throw new ISE("Cannot update a path[%s] that hasn't been announced!", path);
    }

    synchronized (toAnnounce) {
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
    log.info("unannouncing [%s]", path);
    final byte[] value = announcedPaths.remove(path);

    if (value == null) {
      log.error("Path[%s] not announced, cannot unannounce.", path);
      return;
    }

    try {
      curator.transaction().forOperations(curator.transactionOp().delete().forPath(path));
    }
    catch (KeeperException.NoNodeException e) {
      log.info("node[%s] didn't exist anyway...", path);
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
    catch (Exception e) {
      throw CloseableUtils.closeInCatch(new RuntimeException(e), cache);
    }
  }

  @GuardedBy("toAnnounce")
  private void createPath(String parentPath, boolean removeParentsIfCreated)
  {
    try {
      curator.create().creatingParentsIfNeeded().forPath(parentPath);
      if (removeParentsIfCreated) {
        pathsCreatedInThisAnnouncer.add(parentPath);
      }
      log.debug(
          "Created parentPath[%s], %s remove on stop() called.",
          parentPath,
          removeParentsIfCreated ? "will" : "will not"
      );
    }
    catch (Exception e) {
      log.error(e, "Problem creating parentPath[%s], someone else created it first?", parentPath);
    }
  }
}
