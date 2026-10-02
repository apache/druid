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

import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;

@SuppressWarnings("DoNotMock")
public class MetricsEmittingQueryProcessingPoolTest
{
  private ScheduledExecutorService timeoutService;

  @BeforeEach
  public void setUp()
  {
    timeoutService = Mockito.mock(ScheduledExecutorService.class);
  }

  @Test
  public void testPrioritizedExecutorDelegate()
  {
    PrioritizedExecutorService service = Mockito.mock(PrioritizedExecutorService.class);
    ScheduledExecutorService timeoutService = Mockito.mock(ScheduledExecutorService.class);
    Mockito.when(service.getQueueSize()).thenReturn(10);
    Mockito.when(service.getActiveTasks()).thenReturn(2);
    ExecutorServiceMonitor monitor = new ExecutorServiceMonitor();
    MetricsEmittingQueryProcessingPool processingPool = new MetricsEmittingQueryProcessingPool(
        service,
        timeoutService,
        monitor
    );
    Assertions.assertSame(service, processingPool.delegate());

    final StubServiceEmitter serviceEmitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(serviceEmitter);

    serviceEmitter.verifyValue("segment/scan/pending", 10);
    serviceEmitter.verifyValue("segment/scan/active", 2);
  }

  @Test
  public void testNonPrioritizedExecutorDelegate()
  {
    ListeningExecutorService service = Mockito.mock(ListeningExecutorService.class);
    ExecutorServiceMonitor monitor = new ExecutorServiceMonitor();
    MetricsEmittingQueryProcessingPool processingPool = new MetricsEmittingQueryProcessingPool(
        service,
        timeoutService,
        monitor
    );
    Assertions.assertSame(service, processingPool.delegate());

    ServiceEmitter serviceEmitter = Mockito.mock(ServiceEmitter.class);
    monitor.doMonitor(serviceEmitter);
    Mockito.verifyNoInteractions(serviceEmitter);
  }

  /**
   * End-to-end: a real {@link ShardedPrioritizedExecutorService} is a {@link ProcessingPoolStats}, so the emitter
   * picks it up and the emitted counters are the totals summed across all shards.
   */
  @Test
  public void testShardedExecutorDelegateAggregatesAcrossShards() throws InterruptedException
  {
    final int numPools = 4;
    final int numThreads = 8; // 2 threads per shard
    final int numTasks = 400;
    final ShardedPrioritizedExecutorService sharded = ShardedPrioritizedExecutorService.create(
        new Lifecycle(),
        new DruidProcessingConfig()
        {
          @Override
          public String getFormatString()
          {
            return "metrics-sharded-test";
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

    final CountDownLatch gate = new CountDownLatch(1);
    try {
      for (int i = 0; i < numTasks; i++) {
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
              }
            }
        );
      }

      // Wait until every worker thread across all shards is busy (proves work spread across shards).
      final long deadlineMs = System.currentTimeMillis() + 30_000L;
      while (sharded.getActiveTasks() < numThreads && System.currentTimeMillis() < deadlineMs) {
        Thread.sleep(10);
      }

      final ExecutorServiceMonitor monitor = new ExecutorServiceMonitor();
      // Registers itself with the monitor.
      new MetricsEmittingQueryProcessingPool(sharded, timeoutService, monitor);
      final StubServiceEmitter serviceEmitter = new StubServiceEmitter("service", "host");
      monitor.doMonitor(serviceEmitter);

      // active == numThreads and pending == the rest, both summed across the four shards.
      serviceEmitter.verifyValue("segment/scan/active", numThreads);
      serviceEmitter.verifyValue("segment/scan/pending", numTasks - numThreads);
    }
    finally {
      gate.countDown();
      sharded.shutdownNow();
    }
  }
}
