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

package org.apache.druid.query.context;

import com.google.common.util.concurrent.ForwardingFuture;
import org.apache.druid.java.util.common.concurrent.DecoratedExecutorService;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/** Immutable operation headers, restored on worker threads and cleared when the operation finishes. */
public final class RequestHeaders
{
  private static final ThreadLocal<Snapshot> CURRENT = new ThreadLocal<>();
  private static final Scope NOOP = () -> {};

  private RequestHeaders()
  {
  }

  public static Map<String, String> current()
  {
    final Snapshot snapshot = CURRENT.get();
    return snapshot == null ? Map.of() : snapshot.headers;
  }

  public static Map<String, Object> currentContext()
  {
    final Snapshot snapshot = CURRENT.get();
    return snapshot == null ? Map.of() : snapshot.context;
  }

  public static Scope open(final RequestHeaderContextConfig config, @Nullable final Map<String, Object> context)
  {
    final Map<String, String> headers = config.fromContext(context == null ? Map.of() : context);
    return restore(
        headers.isEmpty() ? null : new Snapshot(Map.copyOf(headers), Map.copyOf(config.capture(headers::get)))
    );
  }

  private static Scope restore(@Nullable final Snapshot snapshot)
  {
    final Snapshot previous = CURRENT.get();
    if (previous == snapshot) {
      return NOOP;
    }
    set(snapshot);
    return () -> set(previous);
  }

  private static void set(@Nullable final Snapshot snapshot)
  {
    if (snapshot == null) {
      CURRENT.remove();
    } else {
      CURRENT.set(snapshot);
    }
  }

  /** Bind callbacks at registration, before an asynchronous completion submits them on another thread. */
  public static Executor bind(final Executor executor)
  {
    final Snapshot snapshot = CURRENT.get();
    return command -> executor.execute(wrap(command, snapshot));
  }

  public static Runnable wrap(final Runnable runnable)
  {
    return wrap(runnable, CURRENT.get());
  }

  private static Runnable wrap(final Runnable runnable, @Nullable final Snapshot snapshot)
  {
    Objects.requireNonNull(runnable, "runnable");
    final Runnable wrapped = () -> {
      try (Scope ignored = restore(snapshot)) {
        runnable.run();
      }
    };
    // ThreadPoolExecutor.purge() must still recognize canceled futures in its queue.
    return runnable instanceof Future<?> future ? new HeaderFuture<>(future, wrapped) : wrapped;
  }

  private static class HeaderFuture<T> extends ForwardingFuture.SimpleForwardingFuture<T> implements Runnable
  {
    private final Runnable runnable;

    HeaderFuture(final Future<T> future, final Runnable runnable)
    {
      super(future);
      this.runnable = runnable;
    }

    @Override
    public void run()
    {
      runnable.run();
    }
  }

  public static <T> Callable<T> wrap(final Callable<T> callable)
  {
    Objects.requireNonNull(callable, "callable");
    final Snapshot snapshot = CURRENT.get();
    return () -> {
      try (Scope ignored = restore(snapshot)) {
        return callable.call();
      }
    };
  }

  /** Capture at submission, including empty context, so a reused worker cannot retain another operation's headers. */
  public static ExecutorService wrap(final ExecutorService executor)
  {
    return new DecoratedExecutorService(executor, new DecoratedExecutorService.Decorator()
    {
      @Override
      public <T> Callable<T> decorateCallable(final Callable<T> callable)
      {
        return RequestHeaders.wrap(callable);
      }

      @Override
      public Runnable decorateRunnable(final Runnable runnable)
      {
        return RequestHeaders.wrap(runnable);
      }
    });
  }

  public static ScheduledThreadPoolExecutor scheduled(final int threads, final ThreadFactory threadFactory)
  {
    return new HeaderScheduler(threads, threadFactory);
  }

  /** Sequences execute lazily, potentially after the servlet or task submission scope has closed. */
  public static <T> Sequence<T> wrap(final Sequence<T> sequence)
  {
    final Snapshot snapshot = CURRENT.get();
    return new Sequence<>()
    {
      @Override
      public <R> R accumulate(final R initial, final Accumulator<R, T> accumulator)
      {
        try (Scope ignored = restore(snapshot)) {
          return sequence.accumulate(initial, accumulator);
        }
      }

      @Override
      public <R> Yielder<R> toYielder(final R initial, final YieldingAccumulator<R, T> accumulator)
      {
        try (Scope ignored = restore(snapshot)) {
          return new HeaderYielder<>(sequence.toYielder(initial, accumulator), snapshot);
        }
      }
    };
  }

  private static class HeaderYielder<T> implements Yielder<T>
  {
    private Yielder<T> delegate;
    @Nullable
    private final Snapshot snapshot;

    HeaderYielder(final Yielder<T> delegate, @Nullable final Snapshot snapshot)
    {
      this.delegate = delegate;
      this.snapshot = snapshot;
    }

    @Override
    public T get()
    {
      try (Scope ignored = restore(snapshot)) {
        return delegate.get();
      }
    }

    @Override
    public Yielder<T> next(final T initial)
    {
      try (Scope ignored = restore(snapshot)) {
        delegate = delegate.next(initial);
        return this;
      }
    }

    @Override
    public boolean isDone()
    {
      try (Scope ignored = restore(snapshot)) {
        return delegate.isDone();
      }
    }

    @Override
    public void close() throws IOException
    {
      try (Scope ignored = restore(snapshot)) {
        delegate.close();
      }
    }
  }

  public interface Scope extends AutoCloseable
  {
    @Override
    void close();
  }

  private record Snapshot(Map<String, String> headers, Map<String, Object> context)
  {
  }

  private static class HeaderScheduler extends ScheduledThreadPoolExecutor
  {
    HeaderScheduler(final int threads, final ThreadFactory threadFactory)
    {
      super(threads, threadFactory);
    }

    @Override
    public ScheduledFuture<?> schedule(final Runnable command, final long delay, final TimeUnit unit)
    {
      return super.schedule(RequestHeaders.wrap(command), delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(final Callable<V> callable, final long delay, final TimeUnit unit)
    {
      return super.schedule(RequestHeaders.wrap(callable), delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
        final Runnable command,
        final long initialDelay,
        final long period,
        final TimeUnit unit
    )
    {
      return super.scheduleAtFixedRate(RequestHeaders.wrap(command), initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
        final Runnable command,
        final long initialDelay,
        final long delay,
        final TimeUnit unit
    )
    {
      return super.scheduleWithFixedDelay(RequestHeaders.wrap(command), initialDelay, delay, unit);
    }
  }
}
