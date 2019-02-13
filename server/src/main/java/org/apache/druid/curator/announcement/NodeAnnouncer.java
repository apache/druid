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
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.ZKPathsUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * NodeAnnouncer announces single node on Zookeeper and only watches this node,
 * while {@link Announcer} watches all child paths, not only this node
 */
public class NodeAnnouncer
{
  private static final Logger log = new Logger(NodeAnnouncer.class);

  private final CuratorFramework curator;

  // incase a path is added to `toAnnounce` in announce() before zk is connected,
  // should remember the path and do announce in start() later
  private final List<Announceable> toAnnounce = new ArrayList<>();
  // incase a path is added to `toUpdate` in update() before zk is connected,
  // should remember the path and do update in start() later
  private final List<Announceable> toUpdate = new ArrayList<>();
  private final ConcurrentMap<String, NodeCache> listeners = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, byte[]> announcedPaths = new ConcurrentHashMap<>();
  // only who creates the parent path can drop the parent path, so should remmeber the created parents
  private final List<String> parentsIBuilt = new CopyOnWriteArrayList<String>();

  private boolean started = false;

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
    log.info("Starting announcer");
    synchronized (toAnnounce) {
      if (started) {
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
    log.info("Stopping announcer");
    synchronized (toAnnounce) {
      if (!started) {
        return;
      }

      started = false;

      Closer closer = Closer.create();
      for (NodeCache cache : listeners.values()) {
        closer.register(cache);
      }
      CloseQuietly.close(closer);

      for (String announcementPath : announcedPaths.keySet()) {
        unannounce(announcementPath);
      }

      if (!parentsIBuilt.isEmpty()) {
        CuratorTransaction transaction = curator.inTransaction();
        for (String parent : parentsIBuilt) {
          try {
            transaction = transaction.delete().forPath(parent).and();
          }
          catch (Exception e) {
            log.error(e, "Unable to delete parent[%s].", parent);
          }
        }
        try {
          ((CuratorTransactionFinal) transaction).commit();
        }
        catch (Exception e) {
          log.error(e, "Unable to commit transaction.");
        }
      }
    }
  }

  /**
   * Like announce(path, bytes, true).
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
        log.debug(e, "Problem checking if the parent existed, ignoring.");
      }

      // Synchronize to make sure that I only create a listener once.
      synchronized (toAnnounce) {
        if (!listeners.containsKey(path)) {
          final NodeCache cache = new NodeCache(curator, path, true);
          cache.getListenable().addListener(
              new NodeCacheListener()
              {
                @Override
                public void nodeChanged() throws Exception
                {
                  ChildData currentData = cache.getCurrentData();
                  if (currentData == null) {
                    final byte[] value = announcedPaths.get(path);
                    if (value != null) {
                      log.info("Node[%s] dropped, reinstating.", path);
                      createAnnouncement(path, value);
                    }
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

  private String createAnnouncement(final String path, byte[] value) throws Exception
  {
    return curator.create().compressed().withMode(CreateMode.EPHEMERAL).inBackground().forPath(path, value);
  }

  private Stat updateAnnouncement(final String path, final byte[] value) throws Exception
  {
    return curator.setData().compressed().inBackground().forPath(path, value);
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
      curator.inTransaction().delete().forPath(path).and().commit();
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
      CloseQuietly.close(cache);
      throw new RuntimeException(e);
    }
  }

  private void createPath(String parentPath, boolean removeParentsIfCreated)
  {
    try {
      curator.create().creatingParentsIfNeeded().forPath(parentPath);
      if (removeParentsIfCreated) {
        parentsIBuilt.add(parentPath);
      }
      log.debug("Created parentPath[%s], %s remove on stop() called.", parentPath, removeParentsIfCreated ? "will" : "will not");
    }
    catch (Exception e) {
      log.error(e, "Problem creating parentPath[%s], someone else created it first?", parentPath);
    }
  }

  private static class Announceable
  {
    final String path;
    final byte[] bytes;
    final boolean removeParentsIfCreated;

    public Announceable(String path, byte[] bytes, boolean removeParentsIfCreated)
    {
      this.path = path;
      this.bytes = bytes;
      this.removeParentsIfCreated = removeParentsIfCreated;
    }
  }
}
