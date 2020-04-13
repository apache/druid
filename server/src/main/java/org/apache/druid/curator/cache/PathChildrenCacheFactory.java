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

package org.apache.druid.curator.cache;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.CloseableExecutorService;
import org.apache.curator.utils.ThreadUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 */
public class PathChildrenCacheFactory
{
  private final boolean cacheData;
  private final boolean compressed;
  private final ExecutorService exec;
  private final boolean shutdownExecutorOnClose;

  private PathChildrenCacheFactory(
      boolean cacheData,
      boolean compressed,
      ExecutorService exec,
      boolean shutdownExecutorOnClose
  )
  {
    this.cacheData = cacheData;
    this.compressed = compressed;
    this.exec = exec;
    this.shutdownExecutorOnClose = shutdownExecutorOnClose;
  }

  public PathChildrenCache make(CuratorFramework curator, String path)
  {
    return new PathChildrenCache(
        curator,
        path,
        cacheData,
        compressed,
        new CloseableExecutorService(exec, shutdownExecutorOnClose)
    );
  }

  public static class Builder
  {
    private static final ThreadFactory DEFAULT_THREAD_FACTORY = ThreadUtils.newThreadFactory("PathChildrenCache");

    private boolean cacheData;
    private boolean compressed;
    private ExecutorService exec;
    private boolean shutdownExecutorOnClose;

    public Builder()
    {
      cacheData = true;
      compressed = false;
      exec = null;
      shutdownExecutorOnClose = true;
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

    public Builder withShutdownExecutorOnClose(boolean shutdownExecutorOnClose)
    {
      this.shutdownExecutorOnClose = shutdownExecutorOnClose;
      return this;
    }

    public PathChildrenCacheFactory build()
    {
      ExecutorService exec = this.exec != null ? this.exec : createDefaultExecutor();
      return new PathChildrenCacheFactory(cacheData, compressed, exec, shutdownExecutorOnClose);
    }

    public static ExecutorService createDefaultExecutor()
    {
      return Executors.newSingleThreadExecutor(DEFAULT_THREAD_FACTORY);
    }
  }
}
