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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.common.asyncresource.AsyncResources;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.StorageNodeModule;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.PartialBundleAcquirer;
import org.apache.druid.segment.loading.external.VirtualStorageManager;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Holds the thread pool used for background loading by {@link SegmentLocalCacheManager} and
 * {@link VirtualStorageManager}.
 */
public class StorageLoadingThreadPool
{
  private static final Logger log = new Logger(StorageNodeModule.class);

  private final ListeningExecutorService exec;

  public StorageLoadingThreadPool(
      @Nullable final ListeningExecutorService exec
  )
  {
    this.exec = exec;
  }

  public static StorageLoadingThreadPool createFromConfig(final SegmentLoaderConfig config)
  {
    final ListeningExecutorService exec;

    if (config.isVirtualStorage()) {
      if (config.getVirtualStorageLoadThreads() <= 0) {
        throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                            .ofCategory(DruidException.Category.INVALID_INPUT)
                            .build(
                                "virtualStorageLoadThreads must be greater than 0, got [%d]",
                                config.getVirtualStorageLoadThreads()
                            );
      }
      if (config.isVirtualStorageUseVirtualThreads()) {
        log.info(
            "Using virtual storage mode with virtual threads - max concurrent on demand loads: [%d].",
            config.getVirtualStorageLoadThreads()
        );
        exec = new PermitBoundedListeningExecutorService(
            MoreExecutors.listeningDecorator(
                Executors.newThreadPerTaskExecutor(
                    Thread.ofVirtual()
                          .name("VirtualStorageOnDemandLoadingThread-", 0)
                          .factory()
                )
            ),
            new Semaphore(config.getVirtualStorageLoadThreads())
        );
      } else {
        log.info(
            "Using virtual storage mode with fixed platform thread pool - on demand load threads: [%d].",
            config.getVirtualStorageLoadThreads()
        );
        exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(
                config.getVirtualStorageLoadThreads(),
                Execs.makeThreadFactory("VirtualStorageOnDemandLoadingThread-%s")
            )
        );
      }
    } else {
      exec = null;
    }

    return new StorageLoadingThreadPool(exec);
  }

  /**
   * Returns an instance representing "no thread pool". Calling {@link #getExecutorService()} will return an error.
   */
  public static StorageLoadingThreadPool none()
  {
    return new StorageLoadingThreadPool(null);
  }

  public boolean isAvailable()
  {
    return exec != null;
  }

  /**
   * Executor for on-demand load work. Concurrency is bounded by the executor itself: in the virtual-thread path it is
   * a {@link PermitBoundedListeningExecutorService} wrapping an unbounded thread-per-virtual-thread executor (so the
   * permit count, not the thread count, is the bound); in the fixed-pool path the pool size is the bound. Every
   * on-demand-load submission, including those routed through {@link #submitUnmanagedAsyncResource}, is therefore
   * bounded without callers having to acquire a permit themselves.
   */
  public ListeningExecutorService getExecutorService()
  {
    if (exec == null) {
      throw DruidException.defensive("No exec set, we thought we wouldn't need this");
    }
    return exec;
  }

  /**
   * Submit a task to the pool and hand back an {@link AsyncResource} that becomes ready when the task completes,
   * exposing the task's (non-null) result. The result is treated as a plain value with no lifecycle: closing the
   * returned resource does not close the result; closing it before completion cancels the task.
   *
   * <p>This is the unmanaged counterpart of {@link #submitCloseableAsyncResource}; use that when the task's result
   * owns a lifecycle.
   *
   * @see AsyncResources#fromFutureUnmanaged(ListenableFuture)
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
   * @see AsyncResources#fromFutureCloseable(ListenableFuture)
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

  /**
   * A {@link ListeningExecutorService} that caps the number of concurrently-running submitted tasks at a semaphore's
   * permit count, acquiring a permit (on the worker thread) before each task body runs and releasing it after. Used to
   * bound concurrent virtual-storage on-demand loads when the backing executor is an unbounded thread-per-virtual-thread
   * executor: the permit count is the concurrency bound, not the thread count, and the wait for a permit parks a virtual
   * thread rather than blocking the submitter.
   * <p>
   * The permit wait is <b>interruptible</b>: a task whose future is canceled with {@code mayInterruptIfRunning} (or a
   * {@code shutdownNow}) while it is parked on the permit is interrupted out of the wait and aborts before running its
   * body. Canceling a query stops not only its queued column downloads but also those blocked on the permit,
   * before any deep-storage I/O begins. A task that has already passed the permit and started its body runs to
   * completion (aborting in-flight reads is handled separately by the task itself, not here).
   * <p>
   * Only {@code execute}/{@code submit} are bounded; those are the only submission paths on-demand load work uses
   * (including the {@link PartialBundleAcquirer#submitDownload} column-download path). {@code invokeAll}/
   * {@code invokeAny} throw {@link UnsupportedOperationException} so the concurrency bound can never be silently
   * bypassed by a future caller.
   * <p>
   * Callers must not submit a task that itself blocks on another task submitted to this executor while holding a
   * permit, or all permits could be exhausted by waiters; the on-demand load tasks here never nest submissions.
   */
  @VisibleForTesting
  static final class PermitBoundedListeningExecutorService extends ForwardingListeningExecutorService
  {
    private final ListeningExecutorService delegate;
    private final Semaphore permits;

    PermitBoundedListeningExecutorService(ListeningExecutorService delegate, Semaphore permits)
    {
      this.delegate = delegate;
      this.permits = permits;
    }

    @Override
    protected ListeningExecutorService delegate()
    {
      return delegate;
    }

    @Override
    public void execute(Runnable command)
    {
      delegate.execute(withPermit(command));
    }

    @Override
    public <T> ListenableFuture<T> submit(Callable<T> task)
    {
      return delegate.submit(withPermit(task));
    }

    @Override
    public ListenableFuture<?> submit(Runnable task)
    {
      return delegate.submit(withPermit(task));
    }

    @Override
    public <T> ListenableFuture<T> submit(Runnable task, T result)
    {
      return delegate.submit(withPermit(task), result);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
    {
      throw new UnsupportedOperationException("invokeAll is not permit-bounded; use submit");
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
    {
      throw new UnsupportedOperationException("invokeAll is not permit-bounded; use submit");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
    {
      throw new UnsupportedOperationException("invokeAny is not permit-bounded; use submit");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
    {
      throw new UnsupportedOperationException("invokeAny is not permit-bounded; use submit");
    }

    private Runnable withPermit(Runnable task)
    {
      return () -> {
        try {
          permits.acquire();
        }
        catch (InterruptedException e) {
          // Interrupted while waiting for a permit (e.g. the task's future was canceled with mayInterruptIfRunning).
          // Abort before running the body. Restore the flag and surface as a failure rather than a silent success;
          // if the future was canceled it already reports as such, so this only matters for a stray interrupt.
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
        try {
          task.run();
        }
        finally {
          permits.release();
        }
      };
    }

    private <T> Callable<T> withPermit(Callable<T> task)
    {
      return () -> {
        permits.acquire();
        try {
          return task.call();
        }
        finally {
          permits.release();
        }
      };
    }
  }
}
