/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.druid.java.util.common.lifecycle.Lifecycle;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class PrioritizedExecutorService extends AbstractExecutorService implements ListeningExecutorService
{
  public static PrioritizedExecutorService create(Lifecycle lifecycle, DruidProcessingConfig config)
  {
    final PrioritizedExecutorService service = new PrioritizedExecutorService(
        new ThreadPoolExecutor(
            config.getNumThreads(),
            config.getNumThreads(),
            0L,
            TimeUnit.MILLISECONDS,
            new PriorityBlockingQueue<Runnable>(),
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat(config.getFormatString()).build()
        ),
        config
    );

    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
          }

          @Override
          public void stop()
          {
            service.shutdownNow();
          }
        }
    );

    return service;
  }

  private final AtomicLong queuePosition = new AtomicLong(Long.MAX_VALUE);
  private final ListeningExecutorService delegate;
  private final BlockingQueue<Runnable> delegateQueue;
  private final boolean allowRegularTasks;
  private final int defaultPriority;
  private final DruidProcessingConfig config;
  final ThreadPoolExecutor threadPoolExecutor; // Used in unit tests

  public PrioritizedExecutorService(
      ThreadPoolExecutor threadPoolExecutor,
      DruidProcessingConfig config
  )
  {
    this(threadPoolExecutor, false, 0, config);
  }

  public PrioritizedExecutorService(
      ThreadPoolExecutor threadPoolExecutor,
      boolean allowRegularTasks,
      int defaultPriority,
      DruidProcessingConfig config
  )
  {
    this.threadPoolExecutor = threadPoolExecutor;
    this.delegate = MoreExecutors.listeningDecorator(Preconditions.checkNotNull(threadPoolExecutor));
    this.delegateQueue = threadPoolExecutor.getQueue();
    this.allowRegularTasks = allowRegularTasks;
    this.defaultPriority = defaultPriority;
    this.config = config;
  }

  @Override
  protected <T> PrioritizedListenableFutureTask<T> newTaskFor(Runnable runnable, T value)
  {
    Preconditions.checkArgument(
        allowRegularTasks || runnable instanceof PrioritizedRunnable,
        "task does not implement PrioritizedRunnable"
    );
    return PrioritizedListenableFutureTask.create(
        ListenableFutureTask.create(runnable, value),
        runnable instanceof PrioritizedRunnable
        ? ((PrioritizedRunnable) runnable).getPriority()
        : defaultPriority,
        config.isFifo() ? queuePosition.decrementAndGet() : 0
    );
  }

  @Override
  protected <T> PrioritizedListenableFutureTask<T> newTaskFor(Callable<T> callable)
  {
    Preconditions.checkArgument(
        allowRegularTasks || callable instanceof PrioritizedCallable,
        "task does not implement PrioritizedCallable"
    );
    return PrioritizedListenableFutureTask.create(
        ListenableFutureTask.create(callable),
        callable instanceof PrioritizedCallable
        ? ((PrioritizedCallable) callable).getPriority()
        : defaultPriority,
        config.isFifo() ? queuePosition.decrementAndGet() : 0
    );
  }

  @Override
  public ListenableFuture<?> submit(Runnable task)
  {
    return (ListenableFuture<?>) super.submit(task);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, @Nullable T result)
  {
    return (ListenableFuture<T>) super.submit(task, result);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task)
  {
    return (ListenableFuture<T>) super.submit(task);
  }

  @Override
  public void shutdown()
  {
    delegate.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow()
  {
    return delegate.shutdownNow();
  }

  @Override
  public boolean isShutdown()
  {
    return delegate.isShutdown();
  }

  @Override
  public boolean isTerminated()
  {
    return delegate.isTerminated();
  }

  @Override
  public boolean awaitTermination(long l, TimeUnit timeUnit) throws InterruptedException
  {
    return delegate.awaitTermination(l, timeUnit);
  }

  @Override
  public void execute(final Runnable runnable)
  {
    delegate.execute(runnable);
  }

  public int getQueueSize()
  {
    return delegateQueue.size();
  }
}

class PrioritizedListenableFutureTask<V> implements RunnableFuture<V>,
    ListenableFuture<V>,
    PrioritizedRunnable,
    Comparable<PrioritizedListenableFutureTask>
{
  // NOTE: For priority HIGHER numeric value means more priority. As such we swap left and right in the compares
  private static final Comparator<PrioritizedListenableFutureTask> PRIORITY_COMPARATOR = new Ordering<PrioritizedListenableFutureTask>()
  {
    @Override
    public int compare(
        PrioritizedListenableFutureTask left, PrioritizedListenableFutureTask right
    )
    {
      return Integer.compare(right.getPriority(), left.getPriority());
    }
  }.compound(
      new Ordering<PrioritizedListenableFutureTask>()
      {
        @Override
        public int compare(PrioritizedListenableFutureTask left, PrioritizedListenableFutureTask right)
        {
          return Long.compare(right.getInsertionPlace(), left.getInsertionPlace());
        }
      }
  );

  public static <V> PrioritizedListenableFutureTask<V> create(
      PrioritizedRunnable task,
      @Nullable V result,
      long position
  )
  {
    return new PrioritizedListenableFutureTask<>(
        ListenableFutureTask.create(task, result),
        task.getPriority(),
        position
    );
  }

  public static <V> PrioritizedListenableFutureTask<?> create(PrioritizedCallable<V> callable, long position)
  {
    return new PrioritizedListenableFutureTask<>(
        ListenableFutureTask.create(callable),
        callable.getPriority(),
        position
    );
  }

  public static <V> PrioritizedListenableFutureTask<V> create(ListenableFutureTask<V> task, int priority, long position)
  {
    return new PrioritizedListenableFutureTask<>(task, priority, position);
  }

  private final ListenableFutureTask<V> delegate;
  private final int priority;
  private final long insertionPlace;

  PrioritizedListenableFutureTask(ListenableFutureTask<V> delegate, int priority, long position)
  {
    this.delegate = delegate;
    this.priority = priority;
    this.insertionPlace = position; // Long.MAX_VALUE will always be "highest"
  }

  @Override
  public void run()
  {
    delegate.run();
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning)
  {
    return delegate.cancel(mayInterruptIfRunning);
  }

  @Override
  public boolean isCancelled()
  {
    return delegate.isCancelled();
  }

  @Override
  public boolean isDone()
  {
    return delegate.isDone();
  }

  @Override
  public V get() throws InterruptedException, ExecutionException
  {
    return delegate.get();
  }

  @Override
  public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
  {
    return delegate.get(timeout, unit);
  }

  @Override
  public void addListener(Runnable listener, Executor executor)
  {
    delegate.addListener(listener, executor);
  }

  @Override
  public int getPriority()
  {
    return priority;
  }

  protected long getInsertionPlace()
  {
    return insertionPlace;
  }

  @Override
  public int compareTo(PrioritizedListenableFutureTask otherTask)
  {
    return PRIORITY_COMPARATOR.compare(this, otherTask);
  }
}
