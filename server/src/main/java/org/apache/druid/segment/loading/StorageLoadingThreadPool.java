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
import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.common.asyncresource.AsyncResources;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.external.VirtualStorageManager;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Holds the thread pool used for background loading by {@link SegmentLocalCacheManager} and
 * {@link VirtualStorageManager}.
 * <p>
 * <b>Submissions are not automatically concurrency-bounded.</b> The executor returned by {@link #getExecutorService()}
 * runs submitted tasks as fast as it can (in the virtual-thread mode, one virtual thread per task). Instead, the
 * number of concurrent <em>deep-storage reads</em> is bounded by a permit that callers acquire via
 * {@link #acquireLoadPermit()} around <em>only</em> the actual I/O, never around lock acquisition, reservation, or
 * deserialization. Any new load path that reads from deep storage must acquire a permit around that read, or it will
 * be unbounded.
 */
public class StorageLoadingThreadPool
{
  private static final Logger log = new Logger(StorageLoadingThreadPool.class);

  /**
   * A handle to a held on-demand-load permit. {@link #close()} releases the permit.
   *
   * @see #acquireLoadPermit()
   */
  public interface LoadPermit extends Closeable
  {
    /**
     * Releases the permit.
     */
    @Override
    void close();
  }

  /**
   * A permit handle whose {@code close()} releases nothing. Returned by {@link #acquireLoadPermit()} when there is no
   * semaphore (the fixed-thread-pool mode, where the thread count is the bound, and the "no pool" instance).
   */
  private static final LoadPermit NOOP_PERMIT = () -> {};

  @Nullable
  private final ListeningExecutorService exec;
  /**
   * Bounds concurrent on-demand deep-storage reads in the virtual-thread mode, where the executor is otherwise
   * unbounded (one virtual thread per task). Null in the fixed-thread-pool mode (the pool size is the bound) and in
   * the "no pool" instance.
   */
  @Nullable
  private final Semaphore permits;

  private StorageLoadingThreadPool(@Nullable final ListeningExecutorService exec, @Nullable final Semaphore permits)
  {
    this.exec = exec;
    this.permits = permits;
  }

  public static StorageLoadingThreadPool createFromConfig(final SegmentLoaderConfig config)
  {
    if (!config.isVirtualStorage()) {
      return new StorageLoadingThreadPool(null, null);
    }

    if (config.getVirtualStorageLoadThreads() <= 0) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "virtualStorageLoadThreads must be greater than 0, got [%d]",
                              config.getVirtualStorageLoadThreads()
                          );
    }

    final ListeningExecutorService exec;
    final Semaphore permits;
    if (config.isVirtualStorageUseVirtualThreads()) {
      log.info(
          "Using virtual storage mode with virtual threads - max concurrent on demand loads: [%d].",
          config.getVirtualStorageLoadThreads()
      );
      // Unbounded thread-per-virtual-thread executor; concurrency is bounded by the permit count, acquired by callers
      // via acquireLoadPermit() around the actual deep-storage reads.
      exec = MoreExecutors.listeningDecorator(
          Executors.newThreadPerTaskExecutor(
              Thread.ofVirtual()
                    .name("VirtualStorageOnDemandLoadingThread-", 0)
                    .factory()
          )
      );
      permits = new Semaphore(config.getVirtualStorageLoadThreads());
    } else {
      log.info(
          "Using virtual storage mode with fixed platform thread pool - on demand load threads: [%d].",
          config.getVirtualStorageLoadThreads()
      );
      // Fixed pool: the thread count is the concurrency bound, so no separate permit is needed.
      exec = MoreExecutors.listeningDecorator(
          Executors.newFixedThreadPool(
              config.getVirtualStorageLoadThreads(),
              Execs.makeThreadFactory("VirtualStorageOnDemandLoadingThread-%s")
          )
      );
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

  /**
   * Executor for on-demand load work. Not concurrency-bounded by itself. Callers doing deep-storage reads with this
   * pool must bound themselves via {@link #acquireLoadPermit()} around the read.
   */
  public ListeningExecutorService getExecutorService()
  {
    if (exec == null) {
      throw DruidException.defensive("No exec set, we thought we wouldn't need this");
    }
    return exec;
  }

  /**
   * Acquire one on-demand-load permit, returning a {@link LoadPermit} that releases it (idempotently) on close. Callers
   * must hold the permit only around the actual deep-storage read, not around lock acquisition, reservation, or
   * deserialization.
   * <p>
   * In the fixed-thread-pool mode (or the "no pool" instance) there is no semaphore and this returns a no-op handle
   * (the thread count, or nothing, is the bound). The acquire is interruptible: if the thread is interrupted while
   * waiting for a permit (e.g. a canceled load), the interrupt flag is restored and a {@link RuntimeException} is
   * thrown so the load aborts before any I/O begins.
   */
  public LoadPermit acquireLoadPermit()
  {
    final Semaphore p = permits;
    if (p == null) {
      return NOOP_PERMIT;
    }
    try {
      p.acquire();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    final AtomicBoolean released = new AtomicBoolean(false);
    return () -> {
      if (released.compareAndSet(false, true)) {
        p.release();
      }
    };
  }

  /**
   * Submit a task to the pool and hand back an {@link AsyncResource} that becomes ready when the task completes,
   * exposing the task's (non-null) result. The result is treated as a plain value with no lifecycle: closing the
   * returned resource does not close the result; closing it before completion cancels the task.
   *
   * <p>This is the unmanaged counterpart of {@link #submitCloseableAsyncResource}; use that when the task's result
   * owns a lifecycle.
   *
   * @see AsyncResources#fromFutureUnmanaged
   */
  public <T> AsyncResource<T> submitUnmanagedAsyncResource(Callable<T> task)
  {
    return AsyncResources.fromFutureUnmanaged(getExecutorService().submit(task));
  }

  /**
   * Submit a task whose result <b>owns a lifecycle</b> and hand back an {@link AsyncResource} that manages it: closing
   * the returned resource closes the result, a result produced after a cancel/close race is closed rather than leaked,
   * and closing it before completion cancels the task.
   *
   * <p>This is the managed counterpart of {@link #submitUnmanagedAsyncResource}; use that when the task's result is a
   * plain value with no lifecycle.
   *
   * @see AsyncResources#fromFutureCloseable
   */
  public <T extends Closeable> AsyncResource<T> submitCloseableAsyncResource(Callable<T> task)
  {
    return AsyncResources.fromFutureCloseable(getExecutorService().submit(task));
  }

  @LifecycleStop
  public void stop()
  {
    if (exec != null) {
      exec.shutdownNow();
    }
  }
}
