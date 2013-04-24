/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.curator.cache;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import java.util.Map;

/**
 * A class to abstract out workloads that just want to attach child watchers onto specific paths.  Provides all the
 * bookeeping of whether or not something is already watching (and can just have a listener added) or if it needs
 * to create a new cache.
 *
 * Also builds PathChildrenCache objects with a dependency-injected factory, so that code that just wants to watch a
 * specific path doesn't have to worry about anything other than its listener.
 */
public class PathChildrenCacheWatcher
{
  private final CuratorFramework curator;
  private final PathChildrenCacheFactory factory;

  private final Map<String, PathChildrenCache> caches;

  public PathChildrenCacheWatcher(
      CuratorFramework curator,
      PathChildrenCacheFactory factory
  )
  {
    this.curator = curator;
    this.factory = factory;

    this.caches = Maps.newConcurrentMap();
  }

  public void registerListener(String path, PathChildrenCacheListener listener)
  {
    synchronized (this) {
      final PathChildrenCache cache = getCache(path);
      cache.getListenable().addListener(listener);
    }
  }

  public void unregisterListener(String path, PathChildrenCacheListener listener)
  {
    synchronized (this) {
      if (caches.containsKey(path)) {
        final PathChildrenCache cache = getCache(path);
        cache.getListenable().removeListener(listener);

        if (cache.getListenable().size() == 0) {
          caches.remove(path);
        }
      }
    }
  }

  private PathChildrenCache getCache(String path)
  {
    PathChildrenCache cache = caches.get(path);

    if (cache == null) {
      cache = factory.make(curator, path);
      final PathChildrenCache storedCache = caches.put(path, cache);
      if (storedCache != null) {
        cache = storedCache;
      } else {
        try {
          cache.start();
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    }

    return cache;
  }
}
