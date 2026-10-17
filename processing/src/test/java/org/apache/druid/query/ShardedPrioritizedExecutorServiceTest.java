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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class ShardedPrioritizedExecutorServiceTest
{
  /**
   * Verifies the composite (sharded) pool: with several shards, tasks are routed across all of them, all tasks still
   * run to completion, and {@link ShardedPrioritizedExecutorService#getQueueSize()} /
   * {@link ShardedPrioritizedExecutorService#getActiveTasks()} aggregate correctly across every shard.
   */
  @Test
  public void testShardedExecutionAndAggregatedMetrics() throws Exception
  {
    final int numPools = 4;
    final int numThreads = 8; // 2 threads per pool
    final ShardedPrioritizedExecutorService sharded = ShardedPrioritizedExecutorService.create(
        new Lifecycle(),
        new DruidProcessingConfig()
        {
          @Override
          public String getFormatString()
          {
            return "sharded-test";
          }

          @Override
          public int getNumThreads()
          {
            return numThreads;
          }

          @Override
          public int getNumThreadPools()
          {
            return numPools;
          }
        }
    );

    try {
      final int numTasks = 400;
      final CountDownLatch gate = new CountDownLatch(1);
      final AtomicInteger completed = new AtomicInteger(0);
      final List<ListenableFuture<?>> futures = Lists.newArrayListWithExpectedSize(numTasks);

      for (int i = 0; i < numTasks; i++) {
        futures.add(
            sharded.submit(
                new PrioritizedRunnable()
                {
                  @Override
                  public int getPriority()
                  {
                    return 0;
                  }

                  @Override
                  public void run()
                  {
                    try {
                      gate.await();
                    }
                    catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw new RuntimeException(e);
                    }
                    completed.incrementAndGet();
                  }
                }
            )
        );
      }

      // Wait until every worker thread across all shards has picked up a (blocked) task. Reaching numThreads active
      // proves tasks were routed across all shards (each shard has only numThreads/numPools threads), and exercises
      // getActiveTasks() aggregation.
      final long deadlineMs = System.currentTimeMillis() + 30_000L;
      while (sharded.getActiveTasks() < numThreads && System.currentTimeMillis() < deadlineMs) {
        Thread.sleep(10);
      }
      Assert.assertEquals(
          "all worker threads across shards should be busy",
          numThreads,
          sharded.getActiveTasks()
      );

      // With numThreads tasks running (blocked) and none finished, the remaining tasks must be queued across shards.
      Assert.assertEquals(
          "queued tasks should be summed across all shards",
          numTasks - numThreads,
          sharded.getQueueSize()
      );

      // Release the gate; every task must complete regardless of which shard ran it.
      gate.countDown();
      for (ListenableFuture<?> future : futures) {
        future.get();
      }
      Assert.assertEquals(numTasks, completed.get());

      // Queues drain to empty across all shards once everything has run.
      Assert.assertEquals(0, sharded.getQueueSize());
    }
    finally {
      sharded.shutdownNow();
    }
  }

  /**
   * A single-shard composite must still route and run every task, and its aggregated counters must match a plain
   * single pool.
   */
  @Test
  public void testSingleShardBehavesLikeOnePool() throws ExecutionException, InterruptedException
  {
    final ShardedPrioritizedExecutorService sharded = ShardedPrioritizedExecutorService.create(
        new Lifecycle(),
        new DruidProcessingConfig()
        {
          @Override
          public String getFormatString()
          {
            return "single-shard-test";
          }

          @Override
          public int getNumThreads()
          {
            return 2;
          }

          @Override
          public int getNumThreadPools()
          {
            return 1;
          }
        }
    );

    try {
      final AtomicInteger completed = new AtomicInteger(0);
      final List<ListenableFuture<?>> futures = Lists.newArrayListWithExpectedSize(50);
      for (int i = 0; i < 50; i++) {
        futures.add(
            sharded.submit(
                new PrioritizedRunnable()
                {
                  @Override
                  public int getPriority()
                  {
                    return 0;
                  }

                  @Override
                  public void run()
                  {
                    completed.incrementAndGet();
                  }
                }
            )
        );
      }
      for (ListenableFuture<?> future : futures) {
        future.get();
      }
      Assert.assertEquals(50, completed.get());
      Assert.assertEquals(0, sharded.getQueueSize());
    }
    finally {
      sharded.shutdownNow();
    }
  }

  /**
   * numThreads must be split across the shards as evenly as possible, with the remainder handed to the first shards,
   * and the per-shard counts must sum back to numThreads.
   */
  @Test
  public void testThreadsSplitEvenlyAcrossShards()
  {
    final ShardedPrioritizedExecutorService sharded = ShardedPrioritizedExecutorService.create(
        new Lifecycle(),
        new DruidProcessingConfig()
        {
          @Override
          public String getFormatString()
          {
            return "split-test";
          }

          @Override
          public int getNumThreads()
          {
            return 10;
          }

          @Override
          public int getNumThreadPools()
          {
            return 4;
          }
        }
    );

    try {
      // 10 threads over 4 shards -> base 2 each, remainder 2 handed to the first two shards.
      Assert.assertArrayEquals(new int[]{3, 3, 2, 2}, sharded.shardThreadCounts());
      Assert.assertEquals(10, IntStream.of(sharded.shardThreadCounts()).sum());
    }
    finally {
      sharded.shutdownNow();
    }
  }
}
