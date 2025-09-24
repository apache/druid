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

package org.apache.druid.server.metrics;

import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class QueryCountStatsMonitorTest
{
  private QueryCountStatsProvider queryCountStatsProvider;
  private BlockingPool<ByteBuffer> mergeBufferPool;
  private MonitorsConfig monitorsConfig;
  private ExecutorService executorService;

  @Before
  public void setUp()
  {
    monitorsConfig = new MonitorsConfig(
        new ArrayList<>(
            Arrays.asList(
                GroupByStatsMonitor.class.getName(),
                QueryCountStatsMonitor.class.getName()
            )
        )
    );

    queryCountStatsProvider = new QueryCountStatsProvider()
    {
      private final AtomicLong successEmitCount = new AtomicLong(0);
      private final AtomicLong failedEmitCount = new AtomicLong(0);
      private final AtomicLong interruptedEmitCount = new AtomicLong(0);
      private final AtomicLong timedOutEmitCount = new AtomicLong(0);

      @Override
      public long getSuccessfulQueryCount()
      {
        return successEmitCount.get();
      }

      @Override
      public long getFailedQueryCount()
      {
        return failedEmitCount.get();
      }

      @Override
      public long getInterruptedQueryCount()
      {
        return interruptedEmitCount.get();
      }

      @Override
      public long getTimedOutQueryCount()
      {
        return timedOutEmitCount.get();
      }

      @Override
      public void incrementSuccess()
      {
        successEmitCount.incrementAndGet();
      }

      @Override
      public void incrementFailed()
      {
        failedEmitCount.incrementAndGet();
      }

      @Override
      public void incrementInterrupted()
      {
        interruptedEmitCount.incrementAndGet();
      }

      @Override
      public void incrementTimedOut()
      {
        timedOutEmitCount.incrementAndGet();
      }
    };

    mergeBufferPool = new DefaultBlockingPool(() -> ByteBuffer.allocate(1024), 5);
    executorService = Executors.newSingleThreadExecutor();
  }

  @After
  public void tearDown()
  {
    executorService.shutdown();
  }

  @Test
  public void testMonitor()
  {
    final QueryCountStatsMonitor monitor =
        new QueryCountStatsMonitor(queryCountStatsProvider, monitorsConfig, mergeBufferPool);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);

    // Mock metrics emission
    queryCountStatsProvider.incrementSuccess();
    queryCountStatsProvider.incrementFailed();
    queryCountStatsProvider.incrementFailed();
    queryCountStatsProvider.incrementInterrupted();
    queryCountStatsProvider.incrementTimedOut();

    emitter.flush();
    // Trigger metric emission
    monitor.doMonitor(emitter);
    Assert.assertEquals(5, emitter.getNumEmittedEvents());
    emitter.verifyValue("query/success/count", 1L);
    emitter.verifyValue("query/failed/count", 2L);
    emitter.verifyValue("query/interrupted/count", 1L);
    emitter.verifyValue("query/timeout/count", 1L);
    emitter.verifyValue("query/count", 5L);
  }

  @Test
  public void testMonitor_emitPendingRequests()
  {
    monitorsConfig = new MonitorsConfig(Collections.singletonList(QueryCountStatsMonitor.class.getName()));

    final QueryCountStatsMonitor monitor =
        new QueryCountStatsMonitor(queryCountStatsProvider, monitorsConfig, mergeBufferPool);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);

    // Mock metrics emission
    queryCountStatsProvider.incrementSuccess();
    queryCountStatsProvider.incrementFailed();
    queryCountStatsProvider.incrementFailed();
    queryCountStatsProvider.incrementInterrupted();
    queryCountStatsProvider.incrementTimedOut();

    emitter.flush();
    // Trigger metric emission
    monitor.doMonitor(emitter);

    Assert.assertEquals(6, emitter.getNumEmittedEvents());
    emitter.verifyValue("mergeBuffer/pendingRequests", 0L);
    emitter.verifyValue("query/success/count", 1L);
    emitter.verifyValue("query/failed/count", 2L);
    emitter.verifyValue("query/interrupted/count", 1L);
    emitter.verifyValue("query/timeout/count", 1L);
    emitter.verifyValue("query/count", 5L);
  }
}
