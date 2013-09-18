/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.curator.cache;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.ThreadUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

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

  public static class Builder
  {
    private static final ThreadFactory defaultThreadFactory = ThreadUtils.newThreadFactory("PathChildrenCache");

    private boolean cacheData;
    private boolean compressed;
    private ExecutorService exec;

    public Builder()
    {
      cacheData = true;
      compressed = false;
      exec = Executors.newSingleThreadExecutor(defaultThreadFactory);
    }

    public Builder withCacheData(boolean cacheData)
    {
      this.cacheData = cacheData;
      return this;
    }

    public Builder withCompressed(boolean compressed)
    {
      this.compressed = compressed;
      return this;
    }

    public Builder withExecutorService(ExecutorService exec)
    {
      this.exec = exec;
      return this;
    }

    public SimplePathChildrenCacheFactory build()
    {
      return new SimplePathChildrenCacheFactory(cacheData, compressed, exec);
    }
  }
}
