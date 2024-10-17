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

package org.apache.druid.indexing.batch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import org.apache.calcite.avatica.SqlType;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.sql.client.BrokerClient;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlParameter;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.sql.http.SqlTaskStatus;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ScheduledBatchSchedulerTest
{
  private TaskMaster taskMaster;
  private BlockingExecutorService executor;
  private BrokerClient brokerClient;
  private StubServiceEmitter serviceEmitter;
  private SqlQuery query;

  private ScheduledBatchScheduler scheduler;

  @Before
  public void setUp()
  {
    brokerClient = Mockito.mock(BrokerClient.class);
    taskMaster = new TaskMaster(null, null);
    taskMaster.becomeLeader(Mockito.mock(TaskRunner.class), Mockito.mock(TaskQueue.class));
    executor = new BlockingExecutorService("test");
    serviceEmitter = new StubServiceEmitter();
    query = new SqlQuery(
        "REPLACE INTO foo OVERWRITE ALL SELECT * FROM bar PARTITIONED BY ALL",
        ResultFormat.ARRAY,
        true,
        true,
        true,
        ImmutableMap.of("useCache", false),
        ImmutableList.of(new SqlParameter(SqlType.INTEGER, 1))
    );
    initScheduler();
  }

  private void initScheduler()
  {
    scheduler = new ScheduledBatchScheduler(
        taskMaster,
        (nameFormat, numThreads) -> new WrappingScheduledExecutorService("test", executor, false),
        serviceEmitter,
        brokerClient
    );
  }

  @Test
  public void testStartStopSchedulingSupervisor()
  {
    scheduler.start();
    scheduler.startScheduledIngestion("foo", new QuartzCronSchedulerConfig("*/30 * * * * ?"), query);
    ScheduledBatchSupervisorSnapshot snapshot = scheduler.getSchedulerSnapshot("foo");
    assertNotNull(snapshot);
    assertEquals("foo", snapshot.getSupervisorId());
    assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    assertEquals(ImmutableMap.of(), snapshot.getCompletedTasks());
    assertEquals(ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING, snapshot.getStatus());
    assertNull(snapshot.getLastTaskSubmittedTime());

    scheduler.stopScheduledIngestion("foo");
    snapshot = scheduler.getSchedulerSnapshot("foo");
    assertNotNull(snapshot);
    assertEquals("foo", snapshot.getSupervisorId());
    assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    assertEquals(ImmutableMap.of(), snapshot.getCompletedTasks());
    assertEquals(ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_SHUTDOWN, snapshot.getStatus());

    scheduler.startScheduledIngestion("foo", new QuartzCronSchedulerConfig("*/10 * * * * ?"), query);
    snapshot = scheduler.getSchedulerSnapshot("foo");
    assertNotNull(snapshot);
    assertEquals("foo", snapshot.getSupervisorId());
    assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    assertEquals(ImmutableMap.of(), snapshot.getCompletedTasks());
    assertEquals(ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING, snapshot.getStatus());

    scheduler.stop();
    assertNull(scheduler.getSchedulerSnapshot("foo"));
  }

  @Test
  public void testStartStopSchedulingSupervisorWhenResponseIsNull() throws Exception
  {
    Mockito.when(brokerClient.submitTask(query))
           .thenReturn(Futures.immediateFuture(new SqlTaskStatus("taskId1", TaskState.SUCCESS, null)));

    final SqlQuery sqlQuery = new SqlQuery(
        "REPLACE",
        ResultFormat.ARRAY,
        true,
        true,
        true,
        ImmutableMap.of("useCache", false),
        ImmutableList.of(new SqlParameter(SqlType.INTEGER, 1))
    );

    scheduler.start();
    scheduler.startScheduledIngestion("foo", new QuartzCronSchedulerConfig("* * * * * ?"), sqlQuery);
    ScheduledBatchSupervisorSnapshot snapshot = scheduler.getSchedulerSnapshot("foo");
    assertNotNull(snapshot);
    assertEquals("foo", snapshot.getSupervisorId());
    assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    assertEquals(ImmutableMap.of(), snapshot.getCompletedTasks());
    assertEquals(ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING, snapshot.getStatus());
    assertNull(snapshot.getLastTaskSubmittedTime());

    Thread.sleep(1200);

    scheduler.stopScheduledIngestion("foo");
    snapshot = scheduler.getSchedulerSnapshot("foo");
    assertNotNull(snapshot);
    assertEquals("foo", snapshot.getSupervisorId());
    assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    assertEquals(ImmutableMap.of(), snapshot.getCompletedTasks());
    assertEquals(ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_SHUTDOWN, snapshot.getStatus());

    scheduler.stop();
    assertNull(scheduler.getSchedulerSnapshot("foo"));
  }

  @Test
  public void testStartStopSchedulingSupervisorWhenResponseIsValid() throws Exception
  {
    final SqlTaskStatus expectedTaskStatus = new SqlTaskStatus("taskId1", TaskState.SUCCESS, null);
    Mockito.when(brokerClient.submitTask(query))
           .thenReturn(Futures.immediateFuture(expectedTaskStatus));

    scheduler.start();
    scheduler.startScheduledIngestion("foo", new QuartzCronSchedulerConfig("* * * * * ?"), query);
    ScheduledBatchSupervisorSnapshot snapshot = scheduler.getSchedulerSnapshot("foo");
    assertNotNull(snapshot);
    assertEquals("foo", snapshot.getSupervisorId());
    assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    assertEquals(ImmutableMap.of(), snapshot.getCompletedTasks());
    assertEquals(ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING, snapshot.getStatus());
    assertNull(snapshot.getLastTaskSubmittedTime());

    Thread.sleep(1200);
    executor.finishNextPendingTask();
    snapshot = scheduler.getSchedulerSnapshot("foo");
    assertNotNull(snapshot);
    assertEquals("foo", snapshot.getSupervisorId());
    assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    assertEquals(ImmutableMap.of(expectedTaskStatus.getTaskId(), TaskStatus.success(expectedTaskStatus.getTaskId())), snapshot.getCompletedTasks());
    assertEquals(ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING, snapshot.getStatus());
    assertNotNull(snapshot.getLastTaskSubmittedTime());

    scheduler.stopScheduledIngestion("foo");

    snapshot = scheduler.getSchedulerSnapshot("foo");
    assertNotNull(snapshot);
    assertEquals("foo", snapshot.getSupervisorId());
    assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    assertEquals(ImmutableMap.of(expectedTaskStatus.getTaskId(), TaskStatus.success(expectedTaskStatus.getTaskId())), snapshot.getCompletedTasks());
    assertEquals(ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_SHUTDOWN, snapshot.getStatus());

    scheduler.stop();
    assertNull(scheduler.getSchedulerSnapshot("foo"));
  }

  @Test
  public void testStartStopSchedulingMultipleSupervisors() throws Exception
  {
    final SqlTaskStatus expectedTask1 = new SqlTaskStatus("taskId1", TaskState.SUCCESS, null);
    final SqlTaskStatus expectedTask2 = new SqlTaskStatus("taskId2", TaskState.RUNNING, null);
    Mockito.when(brokerClient.submitTask(query))
           .thenReturn(Futures.immediateFuture(expectedTask1))
           .thenReturn(Futures.immediateFuture(expectedTask2));

    scheduler.start();
    scheduler.startScheduledIngestion("foo", new QuartzCronSchedulerConfig("* * * * * ?"), query);
    ScheduledBatchSupervisorSnapshot snapshot = scheduler.getSchedulerSnapshot("foo");
    assertNotNull(snapshot);
    assertEquals("foo", snapshot.getSupervisorId());
    assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    assertEquals(ImmutableMap.of(), snapshot.getCompletedTasks());
    assertEquals(ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING, snapshot.getStatus());
    assertNull(snapshot.getLastTaskSubmittedTime());

    Thread.sleep(1200);
    executor.finishNextPendingTask();
    snapshot = scheduler.getSchedulerSnapshot("foo");
    assertNotNull(snapshot);
    assertEquals("foo", snapshot.getSupervisorId());
    assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    assertEquals(ImmutableMap.of(expectedTask1.getTaskId(), TaskStatus.success(expectedTask1.getTaskId())), snapshot.getCompletedTasks());
    assertEquals(ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING, snapshot.getStatus());
    assertNotNull(snapshot.getLastTaskSubmittedTime());

    Thread.sleep(1200);
    executor.finishNextPendingTask();
    scheduler.stopScheduledIngestion("foo");

    snapshot = scheduler.getSchedulerSnapshot("foo");
    assertNotNull(snapshot);
    assertEquals("foo", snapshot.getSupervisorId());
    assertEquals(ImmutableMap.of(expectedTask2.getTaskId(), TaskStatus.running(expectedTask2.getTaskId())), snapshot.getActiveTasks());
    assertEquals(ImmutableMap.of(expectedTask1.getTaskId(), TaskStatus.success(expectedTask1.getTaskId())), snapshot.getCompletedTasks());
    assertEquals(ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_SHUTDOWN, snapshot.getStatus());

    scheduler.stop();
    assertNull(scheduler.getSchedulerSnapshot("foo"));
  }

  @Test
  public void testStopScheduling()
  {
    scheduler.start();
    scheduler.startScheduledIngestion("foo", new QuartzCronSchedulerConfig("*/30 * * * * ?"), query);
    final ScheduledBatchSupervisorSnapshot state = scheduler.getSchedulerSnapshot("foo");
    assertNotNull(state);
  }
}
