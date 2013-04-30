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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;

import java.util.concurrent.ExecutorService;

/**
 */
public class SimplePathChildrenCacheFactory implements PathChildrenCacheFactory
{
  private final boolean cacheData;
  private final boolean compressed;
  private final ExecutorService exec;

  public SimplePathChildrenCacheFactory(
      boolean cacheData,
      boolean compressed,
      ExecutorService exec
  )
  {
    this.cacheData = cacheData;
    this.compressed = compressed;
    this.exec = exec;
  }

  @Override
  public PathChildrenCache make(CuratorFramework curator, String path)
  {
    return new PathChildrenCache(curator, path, cacheData, compressed, exec);
  }
}
