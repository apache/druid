package com.metamx.druid.curator.announcement;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.io.Closeables;
import com.metamx.common.IAE;
import com.metamx.common.Pair;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.curator.ShutdownNowIgnoringExecutorService;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

/**
 */
public class Announcer
{
  private static final Logger log = new Logger(Announcer.class);

  private final CuratorFramework curator;
  private final ExecutorService exec;

  private final List<Pair<String, byte[]>> toAnnounce = Lists.newArrayList();
  private final ConcurrentMap<String, PathChildrenCache> listeners = new MapMaker().makeMap();
  private final ConcurrentMap<String, ConcurrentMap<String, byte[]>> announcements = new MapMaker().makeMap();

  private boolean started = false;

  public Announcer(
      CuratorFramework curator,
      ExecutorService exec
  )
  {
    this.curator = curator;
    this.exec = new ShutdownNowIgnoringExecutorService(exec);
  }

  @LifecycleStart
  public void start()
  {
    synchronized (toAnnounce) {
      if (started) {
        return;
      }

      started = true;

      for (Pair<String, byte[]> pair : toAnnounce) {
        announce(pair.lhs, pair.rhs);
      }
      toAnnounce.clear();
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (toAnnounce) {
      if (!started) {
        return;
      }

      started = false;

      for (Map.Entry<String, PathChildrenCache> entry : listeners.entrySet()) {
        Closeables.closeQuietly(entry.getValue());
      }

      for (Map.Entry<String, ConcurrentMap<String, byte[]>> entry : announcements.entrySet()) {
        String basePath = entry.getKey();

        for (String announcementPath : entry.getValue().keySet()) {
          unannounce(ZKPaths.makePath(basePath, announcementPath));
        }
      }
    }
  }

  public void announce(String path, byte[] bytes)
  {
    synchronized (toAnnounce) {
      if (!started) {
        toAnnounce.add(Pair.of(path, bytes));
        return;
      }
    }

    final ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);

    final String parentPath = pathAndNode.getPath();

    ConcurrentMap<String, byte[]> subPaths = announcements.get(parentPath);

    if (subPaths == null) {

      // I don't have a watcher on this path yet, create a Map and start watching.
      announcements.putIfAbsent(parentPath, new MapMaker().<String, byte[]>makeMap());

      // Guaranteed to be non-null, but might be a map put in there by another thread.
      final ConcurrentMap<String, byte[]> finalSubPaths = announcements.get(parentPath);

      // Synchronize to make sure that I only create a listener once.
      synchronized (finalSubPaths) {
        if (! listeners.containsKey(parentPath)) {
          PathChildrenCache cache = new PathChildrenCache(curator, parentPath, true, false, exec);
          cache.getListenable().addListener(
              new PathChildrenCacheListener()
              {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
                {
                  switch (event.getType()) {
                    case CHILD_REMOVED:
                      final ChildData child = event.getData();
                      final ZKPaths.PathAndNode childPath = ZKPaths.getPathAndNode(child.getPath());
                      final byte[] value = finalSubPaths.get(childPath.getNode());
                      if (value != null) {
                        log.info("Node[%s] dropped, reinstating.", child.getPath());
                        createAnnouncement(child.getPath(), value);
                      }
                  }
                }
              }
          );

          try {
            synchronized (toAnnounce) {
              if (started) {
                cache.start();
                listeners.put(parentPath, cache);
              }
            }
          }
          catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      }

      subPaths = finalSubPaths;
    }

    boolean created = false;
    synchronized (toAnnounce) {
      if (started) {
        byte[] oldBytes = subPaths.putIfAbsent(pathAndNode.getNode(), bytes);

        if (oldBytes != null) {
          throw new IAE("Already announcing[%s], cannot announce it twice.", path);
        }

        created = true;
      }
    }

    if (created) {
      try {
        createAnnouncement(path, bytes);
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private String createAnnouncement(final String path, byte[] value) throws Exception
  {
    return curator.create().withMode(CreateMode.EPHEMERAL).inBackground().forPath(path, value);
  }

  /**
   * Unannounces an announcement created at path.  Note that if all announcements get removed, the Announcer
   * will continue to have ZK watches on paths because clearing them out is a source of ugly race conditions.
   *
   * If you need to completely clear all the state of what is being watched and announced, stop() the Announcer.
   *
   * @param path
   */
  public void unannounce(String path)
  {
    final ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);
    final String parentPath = pathAndNode.getPath();

    final ConcurrentMap<String, byte[]> subPaths = announcements.get(parentPath);

    if (subPaths == null) {
      throw new IAE("Path[%s] not announced, cannot unannounce.", path);
    }

    if (subPaths.remove(pathAndNode.getNode()) == null) {
      throw new IAE("Path[%s] not announced, cannot unannounce.", path);
    }
  }
}
