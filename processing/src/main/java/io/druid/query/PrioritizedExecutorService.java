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

package io.druid.query;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.concurrent.ExecutorServiceConfig;
import com.metamx.common.lifecycle.Lifecycle;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 */
public class PrioritizedExecutorService extends AbstractExecutorService
{
  public static PrioritizedExecutorService create(Lifecycle lifecycle, ExecutorServiceConfig config)
  {
    final PrioritizedExecutorService service = new PrioritizedExecutorService(
        new ThreadPoolExecutor(
            config.getNumThreads(),
            config.getNumThreads(),
            0L,
            TimeUnit.MILLISECONDS,
            new PriorityBlockingQueue<Runnable>(),
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat(config.getFormatString()).build()
        )
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

  private static final int DEFAULT_PRIORITY = 0;


  private final ThreadPoolExecutor threadPoolExecutor;

  public PrioritizedExecutorService(
      ThreadPoolExecutor threadPoolExecutor
  )
  {
    this.threadPoolExecutor = threadPoolExecutor;
  }

  @Override
  public void shutdown()
  {
    threadPoolExecutor.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow()
  {
    return threadPoolExecutor.shutdownNow();
  }

  @Override
  public boolean isShutdown()
  {
    return threadPoolExecutor.isShutdown();
  }

  @Override
  public boolean isTerminated()
  {
    return threadPoolExecutor.isTerminated();
  }

  @Override
  public boolean awaitTermination(long l, TimeUnit timeUnit) throws InterruptedException
  {
    return threadPoolExecutor.awaitTermination(l, timeUnit);
  }

  @Override
  public void execute(Runnable runnable)
  {
    threadPoolExecutor.execute(runnable);
  }

  @Override
  protected <T> RunnableFuture<T> newTaskFor(final Callable<T> tCallable)
  {
    Callable<T> theCallable = tCallable;
    if (!(tCallable instanceof PrioritizedCallable)) {
      theCallable = new PrioritizedCallable<T>(DEFAULT_PRIORITY)
      {
        @Override
        public T call() throws Exception
        {
          return tCallable.call();
        }
      };
    }
    return new PrioritizedFuture<T>((PrioritizedCallable) theCallable);
  }

  public int getQueueSize()
  {
    return threadPoolExecutor.getQueue().size();
  }

  private static class PrioritizedFuture<V> extends FutureTask<V> implements Comparable<PrioritizedFuture>
  {
    private final PrioritizedCallable<V> callable;

    public PrioritizedFuture(PrioritizedCallable<V> callable)
    {
      super(callable);
      this.callable = callable;
    }

    public int getPriority()
    {
      return callable.getPriority();
    }

    @Override
    public int compareTo(PrioritizedFuture future)
    {
      return -Ints.compare(getPriority(), future.getPriority());
    }
  }
}
