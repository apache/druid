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
import org.apache.druid.java.util.emitter.core.EventMap;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.groupby.GroupByStatsProvider;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
    emitter.start();
    monitor.doMonitor(emitter);
    emitter.flush();
    // Trigger metric emission
    monitor.doMonitor(emitter);

    Assert.assertEquals(7, emitter.getNumEmittedEvents());
    emitter.verifyValue("mergeBuffer/pendingRequests", 0L);
    emitter.verifyValue("mergeBuffer/used", 0L);
    emitter.verifyValue("mergeBuffer/queries", 1L);
    emitter.verifyValue("mergeBuffer/acquisitionTimeNs", 100L);
    emitter.verifyValue("groupBy/spilledQueries", 2L);
    emitter.verifyValue("groupBy/spilledBytes", 200L);
    emitter.verifyValue("groupBy/mergeDictionarySize", 300L);
  }

  @Test
  public void testMonitorWithServiceDimensions()
  {
    final String dataSource = "fooDs";
    final String taskId = "taskId1";
    final String groupId = "test_groupid";
    final String taskType = "test_tasktype";
    final GroupByStatsMonitor monitor = new GroupByStatsMonitor(
        groupByStatsProvider,
        mergeBufferPool
    );
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host", new TestTaskHolder(dataSource, taskId, taskType, groupId));
    emitter.start();
    monitor.doMonitor(emitter);
    emitter.flush();
    // Trigger metric emission
    monitor.doMonitor(emitter);

    final Map<String, Object> dimFilters = Map.of(DruidMetrics.DATASOURCE, dataSource, DruidMetrics.TASK_ID, taskId,
        DruidMetrics.ID, taskId, DruidMetrics.TASK_TYPE, taskType, DruidMetrics.GROUP_ID, groupId
    );

    verifyTaskServiceDimensions(emitter, "mergeBuffer/pendingRequests", dimFilters, 0L);
    verifyTaskServiceDimensions(emitter, "mergeBuffer/used", dimFilters, 0L);
    verifyTaskServiceDimensions(emitter, "mergeBuffer/queries", dimFilters, 1L);
    verifyTaskServiceDimensions(emitter, "mergeBuffer/acquisitionTimeNs", dimFilters, 100L);
    verifyTaskServiceDimensions(emitter, "groupBy/spilledQueries", dimFilters, 2L);
    verifyTaskServiceDimensions(emitter, "groupBy/spilledBytes", dimFilters, 200L);
    verifyTaskServiceDimensions(emitter, "groupBy/mergeDictionarySize", dimFilters, 300L);
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


  private void verifyTaskServiceDimensions(StubServiceEmitter emitter, String metricName, Map<String, Object> dimFilters, Number expectedValue)
  {
    final List<ServiceMetricEvent> observedMetricEvents = emitter.getMetricEvents(metricName);
    Assert.assertEquals(1, observedMetricEvents.size());
    final ServiceMetricEvent event = observedMetricEvents.get(0);
    final EventMap map = event.toMap();
    final boolean matchesDims = dimFilters.entrySet().stream()
                                          .allMatch(e -> Objects.equals(e.getValue(), map.get(e.getKey())));
    Assert.assertTrue(matchesDims);
    Assert.assertEquals(expectedValue, event.getValue());
  }
}
