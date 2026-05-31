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

package org.apache.druid.segment.loading;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.ProvisionException;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.StorageNodeModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.external.VirtualStorageManager;

import javax.annotation.Nullable;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/**
 * Holds the thread pool used for background loading by {@link SegmentLocalCacheManager} and
 * {@link VirtualStorageManager}.
 */
public class StorageLoadingThreadPool
{
  private static final Logger log = new Logger(StorageNodeModule.class);

  private final ListeningExecutorService exec;
  private final Semaphore permits;

  public StorageLoadingThreadPool(
      @Nullable final ListeningExecutorService exec,
      @Nullable final Semaphore permits
  )
  {
    this.exec = exec;
    this.permits = permits;
  }

  public static StorageLoadingThreadPool createFromConfig(final SegmentLoaderConfig config)
  {
    final ListeningExecutorService exec;
    final Semaphore permits;

    if (config.isVirtualStorage()) {
      if (config.getVirtualStorageLoadThreads() <= 0) {
        throw new ProvisionException(
            StringUtils.format(
                "virtualStorageLoadThreads must be greater than 0, got [%d]",
                config.getVirtualStorageLoadThreads()
            )
        );
      }
      if (config.isVirtualStorageUseVirtualThreads()) {
        log.info(
            "Using virtual storage mode with virtual threads - max concurrent on demand loads: [%d].",
            config.getVirtualStorageLoadThreads()
        );
        permits = new Semaphore(config.getVirtualStorageLoadThreads());
        exec = MoreExecutors.listeningDecorator(
            Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual()
                      .name("VirtualStorageOnDemandLoadingThread-", 0)
                      .factory()
            )
        );
      } else {
        log.info(
            "Using virtual storage mode with fixed platform thread pool - on demand load threads: [%d].",
            config.getVirtualStorageLoadThreads()
        );
        permits = null;
        exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(
                config.getVirtualStorageLoadThreads(),
                Execs.makeThreadFactory("VirtualStorageOnDemandLoadingThread-%s")
            )
        );
      }
    } else {
      exec = null;
      permits = null;
    }

    return new StorageLoadingThreadPool(exec, permits);
  }

  /**
   * Returns an instance representing "no thread pool". Calling {@link #getExecutorService()} will return an error.
   */
  public static StorageLoadingThreadPool none()
  {
    return new StorageLoadingThreadPool(null, null);
  }

  public boolean isAvailable()
  {
    return exec != null;
  }

  public ListeningExecutorService getExecutorService()
  {
    if (exec == null) {
      throw DruidException.defensive("No exec set, we thought we wouldn't need this");
    }
    return exec;
  }

  /**
   * Bounds the number of in-flight on-demand loads when running with virtual threads. If null, no permits are used;
   * the natural limit of the {@link #getExecutorService()} is sufficient. If nonnull, callers must acquire a permit
   * in the tasks submitted to {@link #getExecutorService()}, before doing real work. Permits must be released before
   * the task completes.
   */
  @Nullable
  public Semaphore getPermits()
  {
    return permits;
  }

  @LifecycleStop
  public void stop()
  {
    if (exec != null) {
      exec.shutdownNow();
    }
  }
}
