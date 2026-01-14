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
import org.apache.druid.query.QueryResourceId;
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
            200L,
            100L,
            200L,
            2L,
            200L,
            200L,
            300L,
            300L
        );
      }
    };

    mergeBufferPool = new DefaultBlockingPool<>(() -> ByteBuffer.allocate(1024), 5);
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
    final GroupByStatsMonitor monitor = new GroupByStatsMonitor(groupByStatsProvider, mergeBufferPool);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    emitter.start();
    monitor.doMonitor(emitter);
    emitter.flush();
    // Trigger metric emission
    monitor.doMonitor(emitter);

    Assert.assertEquals(12, emitter.getNumEmittedEvents());
    emitter.verifyValue("mergeBuffer/pendingRequests", 0L);
    emitter.verifyValue("mergeBuffer/used", 0L);
    emitter.verifyValue("mergeBuffer/queries", 1L);
    emitter.verifyValue("mergeBuffer/acquisitionTimeNs", 100L);
    emitter.verifyValue("mergeBuffer/bytesUsed", 200L);
    emitter.verifyValue("mergeBuffer/maxAcquisitionTimeNs", 100L);
    emitter.verifyValue("mergeBuffer/maxBytesUsed", 200L);
    emitter.verifyValue("groupBy/spilledQueries", 2L);
    emitter.verifyValue("groupBy/spilledBytes", 200L);
    emitter.verifyValue("groupBy/maxSpilledBytes", 200L);
    emitter.verifyValue("groupBy/mergeDictionarySize", 300L);
    emitter.verifyValue("groupBy/maxMergeDictionarySize", 300L);
  }

  @Test
  public void testMonitorWithServiceDimensions()
  {
    final String dataSource = "fooDs";
    final String taskId = "taskId1";
    final String groupId = "test_groupid";
    final String taskType = "test_tasktype";
    final GroupByStatsMonitor monitor = new GroupByStatsMonitor(groupByStatsProvider, mergeBufferPool);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host", new TestTaskHolder(dataSource, taskId, taskType, groupId));
    emitter.start();
    monitor.doMonitor(emitter);
    emitter.flush();
    monitor.doMonitor(emitter);

    final Map<String, Object> dimFilters = Map.of(
        DruidMetrics.DATASOURCE, dataSource,
        DruidMetrics.TASK_ID, taskId,
        DruidMetrics.ID, taskId,
        DruidMetrics.TASK_TYPE,
        taskType, DruidMetrics.GROUP_ID, groupId
    );

    verifyMetricValue(emitter, "mergeBuffer/pendingRequests", dimFilters, 0L);
    verifyMetricValue(emitter, "mergeBuffer/used", dimFilters, 0L);
    verifyMetricValue(emitter, "mergeBuffer/queries", dimFilters, 1L);
    verifyMetricValue(emitter, "mergeBuffer/acquisitionTimeNs", dimFilters, 100L);
    verifyMetricValue(emitter, "mergeBuffer/bytesUsed", dimFilters, 200L);
    verifyMetricValue(emitter, "mergeBuffer/maxAcquisitionTimeNs", dimFilters, 100L);
    verifyMetricValue(emitter, "mergeBuffer/maxBytesUsed", dimFilters, 200L);
    verifyMetricValue(emitter, "groupBy/spilledQueries", dimFilters, 2L);
    verifyMetricValue(emitter, "groupBy/spilledBytes", dimFilters, 200L);
    verifyMetricValue(emitter, "groupBy/maxSpilledBytes", dimFilters, 200L);
    verifyMetricValue(emitter, "groupBy/mergeDictionarySize", dimFilters, 300L);
    verifyMetricValue(emitter, "groupBy/maxMergeDictionarySize", dimFilters, 300L);
  }

  @Test
  public void testMonitoringMergeBuffer_acquiredCount()
      throws ExecutionException, InterruptedException, TimeoutException
  {
    executorService.submit(() -> {
      mergeBufferPool.takeBatch(4);
    }).get(20, TimeUnit.SECONDS);

    final GroupByStatsMonitor monitor = new GroupByStatsMonitor(groupByStatsProvider, mergeBufferPool);
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

      final GroupByStatsMonitor monitor = new GroupByStatsMonitor(groupByStatsProvider, mergeBufferPool);
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

  @Test
  public void testMonitoringWithMultipleResources()
  {
    GroupByStatsProvider statsProvider = new GroupByStatsProvider();

    QueryResourceId r1 = new QueryResourceId("r1");
    GroupByStatsProvider.PerQueryStats stats1 = statsProvider.getPerQueryStatsContainer(r1);
    stats1.mergeBufferAcquisitionTime(100);
    stats1.mergeBufferTotalUsedBytes(50);
    stats1.spilledBytes(200);
    stats1.dictionarySize(100);

    QueryResourceId r2 = new QueryResourceId("r2");
    GroupByStatsProvider.PerQueryStats stats2 = statsProvider.getPerQueryStatsContainer(r2);
    stats2.mergeBufferAcquisitionTime(500);
    stats2.mergeBufferTotalUsedBytes(30);
    stats2.spilledBytes(100);
    stats2.dictionarySize(300);

    QueryResourceId r3 = new QueryResourceId("r3");
    GroupByStatsProvider.PerQueryStats stats3 = statsProvider.getPerQueryStatsContainer(r3);
    stats3.mergeBufferAcquisitionTime(200);
    stats3.mergeBufferTotalUsedBytes(150);
    stats3.spilledBytes(800);
    stats3.dictionarySize(200);

    // Close all queries to aggregate stats (mimics GroupByMergingQueryRunner behavior)
    statsProvider.closeQuery(r1);
    statsProvider.closeQuery(r2);
    statsProvider.closeQuery(r3);

    final GroupByStatsMonitor monitor = new GroupByStatsMonitor(statsProvider, mergeBufferPool);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    emitter.start();
    monitor.doMonitor(emitter);

    emitter.verifyValue("mergeBuffer/queries", 3L);
    emitter.verifyValue("mergeBuffer/acquisitionTimeNs", 800L);
    emitter.verifyValue("mergeBuffer/bytesUsed", 230L);
    emitter.verifyValue("groupBy/spilledQueries", 3L);
    emitter.verifyValue("groupBy/spilledBytes", 1100L);
    emitter.verifyValue("groupBy/mergeDictionarySize", 600L);

    emitter.verifyValue("mergeBuffer/maxAcquisitionTimeNs", 500L);
    emitter.verifyValue("mergeBuffer/maxBytesUsed", 150L);
    emitter.verifyValue("groupBy/maxSpilledBytes", 800L);
    emitter.verifyValue("groupBy/maxMergeDictionarySize", 300L);
  }

  private void verifyMetricValue(StubServiceEmitter emitter, String metricName, Map<String, Object> dimFilters, Number expectedValue)
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
