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

package org.apache.druid.indexing.scheduledbatch;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.sql.client.BrokerClient;
import org.apache.druid.sql.http.ResultFormat;
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
  private ScheduledBatchStatusTracker statusTracker;
  private SqlQuery query;
  private SqlQuery query2;
  private ScheduledBatchScheduler scheduler;

  @Before
  public void setUp() {
    brokerClient = Mockito.mock(BrokerClient.class);
    taskMaster = new TaskMaster(null, null);
    executor = new BlockingExecutorService("test");
    statusTracker = new ScheduledBatchStatusTracker();
    serviceEmitter = new StubServiceEmitter();
    query = createSqlQuery("REPLACE INTO foo OVERWRITE ALL SELECT * FROM bar PARTITIONED BY ALL");
    query2 = createSqlQuery("REPLACE INTO foo OVERWRITE ALL SELECT * FROM bar PARTITIONED BY DAY");
    initScheduler();
  }

  private SqlQuery createSqlQuery(final String query) {
    return new SqlQuery(
        query,
        ResultFormat.ARRAY,
        true,
        true,
        true,
        null,
        null
    );
  }

  private void initScheduler() {
    scheduler = new ScheduledBatchScheduler(
        taskMaster,
        (nameFormat, numThreads) -> new WrappingScheduledExecutorService("test", executor, false),
        brokerClient,
        serviceEmitter,
        statusTracker
    );
  }

  @Test
  public void testStartStopSchedulingSupervisor() {
    scheduler.start();
    startSupervisor("foo", new QuartzCronSchedulerConfig("*/30 * * * * ?"), query);
    verifySchedulerSnapshot("foo", ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING);

    scheduler.stopScheduledIngestion("foo");
    verifySchedulerSnapshot("foo", ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_SHUTDOWN);

    startSupervisor("foo", new QuartzCronSchedulerConfig("*/10 * * * * ?"), query);
    verifySchedulerSnapshot("foo", ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING);

    scheduler.stop();
    assertNull(scheduler.getSchedulerSnapshot("foo"));
    serviceEmitter.verifyNotEmitted("batchSupervisor/tasks/submit/success");
  }

  @Test
  public void testStartStopSchedulingSupervisorWhenResponseIsNull() throws Exception {
    Mockito.when(brokerClient.submitSqlTask(query))
           .thenReturn(Futures.immediateFuture(new SqlTaskStatus("taskId1", TaskState.SUCCESS, null)));

    scheduler.start();
    startSupervisor("foo", new QuartzCronSchedulerConfig("* * * * * ?"), query);
    verifySchedulerSnapshot("foo", ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING);

    Thread.sleep(1200);
    scheduler.stopScheduledIngestion("foo");
    verifySchedulerSnapshot("foo", ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_SHUTDOWN);

    scheduler.stop();
    assertNull(scheduler.getSchedulerSnapshot("foo"));
    serviceEmitter.verifyNotEmitted("batchSupervisor/tasks/submit/success");
  }

  @Test
  public void testStartStopSchedulingSupervisorWhenResponseIsValid() throws Exception {
    SqlTaskStatus expectedTaskStatus = new SqlTaskStatus("taskId1", TaskState.SUCCESS, null);
    Mockito.when(brokerClient.submitSqlTask(query))
           .thenReturn(Futures.immediateFuture(expectedTaskStatus));

    scheduler.start();
    startSupervisor("foo", new QuartzCronSchedulerConfig("* * * * * ?"), query);
    verifySchedulerSnapshot("foo", ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING);

    Thread.sleep(1200);
    executor.finishNextPendingTask();
    verifySchedulerSnapshotWithTasks("foo", ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING,
                                     ImmutableMap.of(expectedTaskStatus.getTaskId(), TaskStatus.success(expectedTaskStatus.getTaskId())));

    scheduler.stopScheduledIngestion("foo");
    verifySchedulerSnapshotWithTasks("foo", ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_SHUTDOWN,
                                     ImmutableMap.of(expectedTaskStatus.getTaskId(), TaskStatus.success(expectedTaskStatus.getTaskId())));

    scheduler.stop();
    assertNull(scheduler.getSchedulerSnapshot("foo"));
    serviceEmitter.verifyEmitted("batchSupervisor/tasks/submit/success", 1);
  }

  @Test
  public void testStartStopSchedulingMultipleSupervisors() throws Exception {
    SqlTaskStatus expectedFooTask1 = new SqlTaskStatus("fooTaskId1", TaskState.SUCCESS, null);
    SqlTaskStatus expectedFooTask2 = new SqlTaskStatus("fooTaskId2", TaskState.RUNNING, null);
    SqlTaskStatus expectedBarTask1 = new SqlTaskStatus("barTaskId1", TaskState.FAILED, new ErrorResponse(
        InvalidInput.exception("some exception")));
    SqlTaskStatus expectedBarTask2 = new SqlTaskStatus("barTaskId2", TaskState.SUCCESS, null);

    // Mocking the task submission responses for "foo" and "bar" supervisors
    Mockito.when(brokerClient.submitSqlTask(query))
           .thenReturn(Futures.immediateFuture(expectedFooTask1))
           .thenReturn(Futures.immediateFuture(expectedFooTask2));
    Mockito.when(brokerClient.submitSqlTask(query2))
           .thenReturn(Futures.immediateFuture(expectedBarTask1))
           .thenReturn(Futures.immediateFuture(expectedBarTask2));

    scheduler.start();
    startSupervisor("foo", new QuartzCronSchedulerConfig("* * * * * ?"), query);
    startSupervisor("bar", new QuartzCronSchedulerConfig("* * * * * ?"), query2);

    // Verify initial state after starting the supervisors
    verifySchedulerSnapshot("foo", ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING);
    verifySchedulerSnapshot("bar", ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING);

    // Simulate task execution and completion
    Thread.sleep(1200);
    executor.finishAllPendingTasks();

    // Verify state after finishing the first task for "foo"
    verifySchedulerSnapshotWithTasks("foo", ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING,
                                     ImmutableMap.of(expectedFooTask1.getTaskId(), TaskStatus.success(expectedFooTask1.getTaskId())));

    // Stop "foo" supervisor and verify its state after stopping
    Thread.sleep(1200);
    executor.finishAllPendingTasks();
    scheduler.stopScheduledIngestion("foo");
    verifySchedulerSnapshotWithTasks("foo", ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_SHUTDOWN,
                                     ImmutableMap.of(expectedFooTask2.getTaskId(), TaskStatus.running(expectedFooTask2.getTaskId())),
                                     ImmutableMap.of(expectedFooTask1.getTaskId(), TaskStatus.success(expectedFooTask1.getTaskId())));

    // Verify state after finishing tasks for "bar"
    verifySchedulerSnapshotWithTasks("bar", ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING,
                                     ImmutableMap.of(
                                         expectedBarTask2.getTaskId(), TaskStatus.success(expectedBarTask2.getTaskId()),
                                         expectedBarTask1.getTaskId(), TaskStatus.failure(expectedBarTask1.getTaskId(), null)
                                     )
    );

    // Stop all supervisors
    scheduler.stop();
    assertNull(scheduler.getSchedulerSnapshot("foo"));
    assertNull(scheduler.getSchedulerSnapshot("bar"));
    serviceEmitter.verifyEmitted("batchSupervisor/tasks/submit/success", 4);
  }

  private void startSupervisor(final String supervisorId, final CronSchedulerConfig quartzSchedule, final SqlQuery sqlQuery)
  {
    scheduler.startScheduledIngestion(supervisorId, quartzSchedule, sqlQuery);
  }

  private void verifySchedulerSnapshot(
      final String supervisorId,
      final ScheduledBatchSupervisorPayload.BatchSupervisorStatus expectedStatus
  )
  {
    verifySchedulerSnapshotWithTasks(supervisorId, expectedStatus, ImmutableMap.of(), ImmutableMap.of());
  }

  private void verifySchedulerSnapshotWithTasks(
      final String supervisorId,
      final ScheduledBatchSupervisorPayload.BatchSupervisorStatus expectedStatus,
      final ImmutableMap<String, TaskStatus> expectedCompletedTasks
  )
  {
    verifySchedulerSnapshotWithTasks(supervisorId, expectedStatus, ImmutableMap.of(), expectedCompletedTasks);
  }

  private void verifySchedulerSnapshotWithTasks(
      final String supervisorId,
      final ScheduledBatchSupervisorPayload.BatchSupervisorStatus expectedStatus,
      final ImmutableMap<String, TaskStatus> expectedActiveTasks,
      final ImmutableMap<String, TaskStatus> expectedCompletedTasks
  )
  {
    final ScheduledBatchSupervisorSnapshot snapshot = scheduler.getSchedulerSnapshot(supervisorId);
    assertNotNull(snapshot);
    assertEquals(supervisorId, snapshot.getSupervisorId());
    assertEquals(expectedActiveTasks, snapshot.getActiveTasks());
    assertEquals(expectedCompletedTasks, snapshot.getCompletedTasks());
    assertEquals(expectedStatus, snapshot.getStatus());
  }
}
