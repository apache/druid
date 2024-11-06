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
import org.apache.druid.query.groupby.GroupByStatsProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class GroupByStatsMonitorTest
{
  private GroupByStatsProvider groupByStatsProvider;
  private BlockingPool<ByteBuffer> mergeBufferPool;
  private ExecutorService executorService;

  @Before
  public void setUp()
  {
    groupByStatsProvider = new GroupByStatsProvider()
    {
      @Override
      public synchronized AggregateStats getStatsSince()
      {
        return new AggregateStats(
                1L,
                100L,
                2L,
                200L,
                300L
            );
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
    final GroupByStatsMonitor monitor =
        new GroupByStatsMonitor(groupByStatsProvider, mergeBufferPool);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);
    emitter.flush();
    // Trigger metric emission
    monitor.doMonitor(emitter);
    Map<String, Long> resultMap = emitter.getEvents()
                                         .stream()
                                         .collect(Collectors.toMap(
                                             event -> (String) event.toMap().get("metric"),
                                             event -> (Long) event.toMap().get("value")
                                         ));
    Assert.assertEquals(7, resultMap.size());
    Assert.assertEquals(0, (long) resultMap.get("mergeBuffer/pendingRequests"));
    Assert.assertEquals(0, (long) resultMap.get("mergeBuffer/used"));
    Assert.assertEquals(1, (long) resultMap.get("mergeBuffer/queries"));
    Assert.assertEquals(100, (long) resultMap.get("mergeBuffer/acquisitionTimeNs"));
    Assert.assertEquals(2, (long) resultMap.get("groupBy/spilledQueries"));
    Assert.assertEquals(200, (long) resultMap.get("groupBy/spilledBytes"));
    Assert.assertEquals(300, (long) resultMap.get("groupBy/mergeDictionarySize"));
  }

  @Test
  public void testMonitoringMergeBuffer_acquiredCount()
      throws ExecutionException, InterruptedException, TimeoutException
  {
    executorService.submit(() -> {
      mergeBufferPool.takeBatch(4);
    }).get(20, TimeUnit.SECONDS);

    final GroupByStatsMonitor monitor =
        new GroupByStatsMonitor(groupByStatsProvider, mergeBufferPool);
    final StubServiceEmitter emitter = new StubServiceEmitter("DummyService", "DummyHost");
    boolean ret = monitor.doMonitor(emitter);
    Assert.assertTrue(ret);

    List<Number> numbers = emitter.getMetricValues("mergeBuffer/used", Collections.emptyMap());
    Assert.assertEquals(1, numbers.size());
    Assert.assertEquals(4, numbers.get(0).intValue());
  }

  @Test(timeout = 2_000L)
  public void testMonitoringMergeBuffer_pendingRequests()
  {
    executorService.submit(() -> {
      mergeBufferPool.takeBatch(10);
    });

    int count = 0;
    try {
      // wait at most 20 secs for the executor thread to block
      while (mergeBufferPool.getPendingRequests() == 0) {
        Thread.sleep(100);
        count++;
        if (count >= 20) {
          break;
        }
      }

      final GroupByStatsMonitor monitor =
          new GroupByStatsMonitor(groupByStatsProvider, mergeBufferPool);
      final StubServiceEmitter emitter = new StubServiceEmitter("DummyService", "DummyHost");
      boolean ret = monitor.doMonitor(emitter);
      Assert.assertTrue(ret);

      List<Number> numbers = emitter.getMetricValues("mergeBuffer/pendingRequests", Collections.emptyMap());
      Assert.assertEquals(1, numbers.size());
      Assert.assertEquals(1, numbers.get(0).intValue());
    }
    catch (InterruptedException e) {
      // do nothing
    }
  }
}
