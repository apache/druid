/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query;

import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.Runnables;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

import javax.validation.constraints.NotNull;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.Callable;

public class MetricsEmittingExecutorService extends ForwardingListeningExecutorService
{
  private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
  private static final ThreadLocal<Long> THREAD_USER_START = new ThreadLocal<Long>()
  {
    @Override
    public Long initialValue()
    {
      return 0L;
    }
  };
  private static final Runnable PRE = THREAD_MX_BEAN.isCurrentThreadCpuTimeSupported() ? new Runnable()
  {
    @Override
    public void run()
    {
      THREAD_USER_START.set(THREAD_MX_BEAN.getCurrentThreadUserTime());
    }
  } : Runnables.doNothing();

  private final ListeningExecutorService delegate;
  private final ServiceEmitter emitter;
  private final ServiceMetricEvent.Builder metricBuilder;

  private final Runnable post = THREAD_MX_BEAN.isCurrentThreadCpuTimeSupported() ? new Runnable()
  {
    @Override
    public void run()
    {
      emitter.emit(
          metricBuilder.build("exec/cpu", THREAD_MX_BEAN.getCurrentThreadUserTime() - THREAD_USER_START.get())
      );
    }
  } : Runnables.doNothing();

  public MetricsEmittingExecutorService(
      ListeningExecutorService delegate,
      ServiceEmitter emitter,
      ServiceMetricEvent.Builder metricBuilder
  )
  {
    super();
    this.delegate = delegate;
    this.emitter = emitter;
    this.metricBuilder = metricBuilder;
  }

  @Override
  protected ListeningExecutorService delegate()
  {
    return delegate;
  }

  @Override
  public <T> ListenableFuture<T> submit(final Callable<T> tCallable)
  {
    final Callable<T> wrappedPrioritizedCallable =
        (tCallable instanceof PrioritizedCallable)
        ? PrioritizedExecutorService.wrapCallable((PrioritizedCallable<T>) tCallable, PRE, post)
        : wrapCallable(tCallable, PRE, post);
    emitMetrics();
    return super.submit(wrappedPrioritizedCallable);
  }

  @Override
  public ListenableFuture<?> submit(final Runnable runnable)
  {
    return submit(runnable, (Object) null);
  }

  @Override
  public <T> ListenableFuture<T> submit(final Runnable runnable, T result)
  {
    final Runnable wrappedPrioritizedRunnable =
        (runnable instanceof PrioritizedRunnable)
        ? PrioritizedExecutorService.wrapRunnable((PrioritizedRunnable) runnable, PRE, post)
        : wrapRunnable(runnable, PRE, post);
    emitMetrics();
    return super.submit(wrappedPrioritizedRunnable, result);
  }

  @Override
  public void execute(Runnable runnable)
  {
    submit(runnable);
  }

  private void emitMetrics()
  {
    if (delegate instanceof PrioritizedExecutorService) {
      emitter.emit(metricBuilder.build("exec/backlog", ((PrioritizedExecutorService) delegate).getQueueSize()));
    }
  }

  public static Runnable wrapRunnable(
      @NotNull final Runnable runnable,
      @NotNull final Runnable pre,
      @NotNull final Runnable post
  )
  {
    return new Runnable()
    {
      @Override
      public void run()
      {
        pre.run();
        runnable.run();
        post.run();
      }
    };
  }

  public static <V> Callable<V> wrapCallable(
      @NotNull final Callable<V> callable,
      @NotNull final Runnable pre,
      @NotNull final Runnable post
  )
  {
    return new Callable<V>()
    {
      @Override
      public V call() throws Exception
      {
        pre.run();
        final V retVal = callable.call();
        post.run();
        return retVal;
      }
    };
  }
}
