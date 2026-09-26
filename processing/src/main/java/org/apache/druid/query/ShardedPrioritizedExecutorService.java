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

package org.apache.druid.query;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A composite of several independent {@link PrioritizedExecutorService} pools ("shards"). Each shard has its own
 * {@link java.util.concurrent.PriorityBlockingQueue} and its own queue lock, so with {@code N} shards each lock only
 * sees ~1/N of the submit/take traffic and ~1/N of the worker threads. This exists to relieve contention on the single
 * queue lock that a lone {@link PrioritizedExecutorService} exhibits under very high task rates (e.g. one task per
 * segment scan on a historical).
 *
 * <p>Per-task work ({@link #execute}, {@link #submit}) is routed to a shard chosen by {@link ThreadLocalRandom} — no
 * shared counter, so routing adds no contention of its own — and the chosen shard does all the priority wrapping and
 * ordering. The trade-off is that priority ordering is <em>per shard</em> rather than global; for the high-throughput,
 * effectively-single-priority workload this pool serves, that is an acceptable exchange for the reduced contention.
 *
 * <p>Lifecycle calls fan out to every shard; {@link #getQueueSize()} and {@link #getActiveTasks()} are summed across
 * shards so the emitted {@code segment/scan/pending} / {@code segment/scan/active} metrics reflect the whole pool.
 */
public class ShardedPrioritizedExecutorService implements ListeningExecutorService, ProcessingPoolStats
{
  private static final Logger log = new Logger(ShardedPrioritizedExecutorService.class);

  /**
   * Builds {@code config.getNumThreadPools()} shards, splitting {@code config.getNumThreads()} threads across them
   * (the remainder is handed to the first shards). All shards share one {@link ThreadFactory} so worker thread names
   * stay unique across shards.
   */
  public static ShardedPrioritizedExecutorService create(Lifecycle lifecycle, DruidProcessingConfig config)
  {
    final int numPools = Math.max(1, config.getNumThreadPools());
    final int totalThreads = config.getNumThreads();
    final ThreadFactory threadFactory =
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat(config.getFormatString()).build();

    final PrioritizedExecutorService[] shards = new PrioritizedExecutorService[numPools];
    final int[] threadsPerPool = new int[numPools];
    for (int p = 0; p < numPools; p++) {
      final int threadsForPool = totalThreads / numPools + (p < (totalThreads % numPools) ? 1 : 0);
      threadsPerPool[p] = threadsForPool;
      final ThreadPoolExecutor pool = PrioritizedExecutorService.makeThreadPoolExecutor(threadsForPool, threadFactory);
      shards[p] = new PrioritizedExecutorService(pool, config);
    }

    log.info(
        "Creating sharded processing pool with [%d] pools (druid.processing.numThreadPools) splitting [%d] "
        + "threads as %s.",
        numPools,
        totalThreads,
        Arrays.toString(threadsPerPool)
    );

    final ShardedPrioritizedExecutorService service = new ShardedPrioritizedExecutorService(shards);

    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start()
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

  private final PrioritizedExecutorService[] shards;

  public ShardedPrioritizedExecutorService(PrioritizedExecutorService[] shards)
  {
    Preconditions.checkArgument(shards != null && shards.length > 0, "need at least one shard");
    this.shards = shards;
  }

  /**
   * Picks a shard for a single task. {@link ThreadLocalRandom} is contention-free (no shared cache line) and spreads
   * homogeneous high-rate tasks evenly enough that per-shard queue depths stay balanced.
   */
  private PrioritizedExecutorService pickShard()
  {
    return shards[shards.length == 1 ? 0 : ThreadLocalRandom.current().nextInt(shards.length)];
  }

  @Override
  public void execute(Runnable command)
  {
    pickShard().execute(command);
  }

  @Override
  public ListenableFuture<?> submit(Runnable task)
  {
    return pickShard().submit(task);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result)
  {
    return pickShard().submit(task, result);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task)
  {
    return pickShard().submit(task);
  }

  // invokeAll/invokeAny submit a whole batch and block for it; they are not on the per-segment hot path. Sending the
  // batch to a single (randomly chosen) shard keeps the priority/ordering semantics of that shard intact and is
  // simpler than striping a batch across shards.

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
  {
    return pickShard().invokeAll(tasks);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException
  {
    return pickShard().invokeAll(tasks, timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
  {
    return pickShard().invokeAny(tasks);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException
  {
    return pickShard().invokeAny(tasks, timeout, unit);
  }

  @Override
  public void shutdown()
  {
    for (PrioritizedExecutorService shard : shards) {
      shard.shutdown();
    }
  }

  @Override
  public List<Runnable> shutdownNow()
  {
    final List<Runnable> pending = new ArrayList<>();
    for (PrioritizedExecutorService shard : shards) {
      pending.addAll(shard.shutdownNow());
    }
    return pending;
  }

  @Override
  public boolean isShutdown()
  {
    for (PrioritizedExecutorService shard : shards) {
      if (!shard.isShutdown()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isTerminated()
  {
    for (PrioritizedExecutorService shard : shards) {
      if (!shard.isTerminated()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
  {
    // Wait for all shards to terminate, but bound the total wait by a single deadline rather than `timeout` per shard.
    final long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
    for (PrioritizedExecutorService shard : shards) {
      final long remainingNanos = deadlineNanos - System.nanoTime();
      if (!shard.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Total number of queued (not-yet-running) tasks summed across all shards. Called only by the periodic metrics
   * monitor, never on the task hot path.
   */
  @Override
  public int getQueueSize()
  {
    int total = 0;
    for (PrioritizedExecutorService shard : shards) {
      total += shard.getQueueSize();
    }
    return total;
  }

  /**
   * Approximate number of tasks currently running, summed across all shards.
   */
  @Override
  public int getActiveTasks()
  {
    int total = 0;
    for (PrioritizedExecutorService shard : shards) {
      total += shard.getActiveTasks();
    }
    return total;
  }

  /**
   * The configured thread count of each shard, in shard order. Used only by tests to assert that
   * {@code numThreads} is split across the shards as evenly as possible.
   */
  @VisibleForTesting
  int[] shardThreadCounts()
  {
    final int[] counts = new int[shards.length];
    for (int i = 0; i < shards.length; i++) {
      counts[i] = shards[i].threadPoolExecutor.getMaximumPoolSize();
    }
    return counts;
  }
}
