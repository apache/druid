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
  private static final String SUPERVISOR_ID_FOO = "foo";
  private static final String SUPERVISOR_ID_BAR = "bar";

  private static final String TASK_ID_FOO1 = "fooTaskId1";
  private static final String TASK_ID_FOO2 = "fooTaskId2";
  private static final String TASK_ID_BAR1 = "barTaskId1";
  private static final String TASK_ID_BAR2 = "barTaskId2";

  private static final QuartzCronSchedulerConfig CRON_SCHEDULE_1_SECOND = new QuartzCronSchedulerConfig("* * * * * ?");
  private static final QuartzCronSchedulerConfig CRON_SCHEDULE_10_SECONDS = new QuartzCronSchedulerConfig("*/10 * * * * ?");
  private static final QuartzCronSchedulerConfig CRON_SCHEDULE_30_SECONDS = new QuartzCronSchedulerConfig("*/30 * * * * ?");

  private TaskMaster taskMaster;
  private BlockingExecutorService executor;
  private BrokerClient brokerClient;
  private StubServiceEmitter serviceEmitter;
  private ScheduledBatchStatusTracker statusTracker;
  private SqlQuery query;
  private SqlQuery query2;
  private ScheduledBatchScheduler scheduler;

  @Before
  public void setUp()
  {
    brokerClient = Mockito.mock(BrokerClient.class);
    taskMaster = new TaskMaster(null, null);
    executor = new BlockingExecutorService("test");
    statusTracker = new ScheduledBatchStatusTracker();
    serviceEmitter = new StubServiceEmitter();
    query = createSqlQuery("REPLACE INTO foo OVERWRITE ALL SELECT * FROM bar PARTITIONED BY ALL");
    query2 = createSqlQuery("REPLACE INTO foo OVERWRITE ALL SELECT * FROM bar PARTITIONED BY DAY");
    initScheduler();
  }

  private SqlQuery createSqlQuery(final String query)
  {
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

  private void initScheduler()
  {
    scheduler = new ScheduledBatchScheduler(
        taskMaster,
        (nameFormat, numThreads) -> new WrappingScheduledExecutorService("test", executor, false),
        brokerClient,
        serviceEmitter,
        statusTracker
    );
  }

  @Test
  public void testStartStopSchedulingSupervisor()
  {
    scheduler.start();
    scheduler.startScheduledIngestion(SUPERVISOR_ID_FOO, CRON_SCHEDULE_30_SECONDS, query);
    verifySchedulerSnapshot(SUPERVISOR_ID_FOO, ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING);

    scheduler.stopScheduledIngestion(SUPERVISOR_ID_FOO);
    verifySchedulerSnapshot(SUPERVISOR_ID_FOO, ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_SHUTDOWN);

    scheduler.startScheduledIngestion(SUPERVISOR_ID_FOO, CRON_SCHEDULE_10_SECONDS, query);
    verifySchedulerSnapshot(SUPERVISOR_ID_FOO, ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING);

    scheduler.stop();
    assertNull(scheduler.getSchedulerSnapshot(SUPERVISOR_ID_FOO));
    serviceEmitter.verifyNotEmitted("batchSupervisor/tasks/submit/success");
  }

  @Test
  public void testStartStopSchedulingSupervisorWhenResponseIsNull() throws Exception
  {
    Mockito.when(brokerClient.submitSqlTask(query))
           .thenReturn(Futures.immediateFuture(new SqlTaskStatus(TASK_ID_FOO1, TaskState.SUCCESS, null)));

    scheduler.start();
    scheduler.startScheduledIngestion(SUPERVISOR_ID_FOO, CRON_SCHEDULE_1_SECOND, query);
    verifySchedulerSnapshot(SUPERVISOR_ID_FOO, ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING);

    Thread.sleep(1200);
    scheduler.stopScheduledIngestion(SUPERVISOR_ID_FOO);
    verifySchedulerSnapshot(SUPERVISOR_ID_FOO, ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_SHUTDOWN);

    scheduler.stop();
    assertNull(scheduler.getSchedulerSnapshot(SUPERVISOR_ID_FOO));
    serviceEmitter.verifyNotEmitted("batchSupervisor/tasks/submit/success");
  }

  @Test
  public void testStartStopSchedulingSupervisorWhenResponseIsValid() throws Exception
  {
    SqlTaskStatus expectedTaskStatus = new SqlTaskStatus(TASK_ID_FOO1, TaskState.SUCCESS, null);
    Mockito.when(brokerClient.submitSqlTask(query))
           .thenReturn(Futures.immediateFuture(expectedTaskStatus));

    scheduler.start();
    scheduler.startScheduledIngestion(SUPERVISOR_ID_FOO, CRON_SCHEDULE_1_SECOND, query);
    verifySchedulerSnapshot(SUPERVISOR_ID_FOO, ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING);

    Thread.sleep(1200);
    executor.finishNextPendingTask();
    verifySchedulerSnapshotWithTasks(
        SUPERVISOR_ID_FOO,
        ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING,
        ImmutableMap.of(),
        ImmutableMap.of(expectedTaskStatus.getTaskId(), TaskStatus.success(expectedTaskStatus.getTaskId()))
    );

    scheduler.stopScheduledIngestion(SUPERVISOR_ID_FOO);
    verifySchedulerSnapshotWithTasks(
        SUPERVISOR_ID_FOO,
        ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_SHUTDOWN,
        ImmutableMap.of(),
        ImmutableMap.of(expectedTaskStatus.getTaskId(), TaskStatus.success(expectedTaskStatus.getTaskId()))
    );

    scheduler.stop();
    assertNull(scheduler.getSchedulerSnapshot(SUPERVISOR_ID_FOO));
    serviceEmitter.verifyEmitted("batchSupervisor/tasks/submit/success", 1);
  }

  @Test
  public void testStartStopSchedulingMultipleSupervisors() throws Exception
  {
    SqlTaskStatus expectedFooTask1 = new SqlTaskStatus(TASK_ID_FOO1, TaskState.SUCCESS, null);
    SqlTaskStatus expectedFooTask2 = new SqlTaskStatus(TASK_ID_FOO2, TaskState.RUNNING, null);
    SqlTaskStatus expectedBarTask1 = new SqlTaskStatus(TASK_ID_BAR1, TaskState.FAILED, new ErrorResponse(
        InvalidInput.exception("some exception")));
    SqlTaskStatus expectedBarTask2 = new SqlTaskStatus(TASK_ID_BAR2, TaskState.SUCCESS, null);

    Mockito.when(brokerClient.submitSqlTask(query))
           .thenReturn(Futures.immediateFuture(expectedFooTask1))
           .thenReturn(Futures.immediateFuture(expectedFooTask2));
    Mockito.when(brokerClient.submitSqlTask(query2))
           .thenReturn(Futures.immediateFuture(expectedBarTask1))
           .thenReturn(Futures.immediateFuture(expectedBarTask2));

    scheduler.start();
    scheduler.startScheduledIngestion(SUPERVISOR_ID_FOO, CRON_SCHEDULE_1_SECOND, query);
    scheduler.startScheduledIngestion(SUPERVISOR_ID_BAR, CRON_SCHEDULE_1_SECOND, query2);

    verifySchedulerSnapshot(SUPERVISOR_ID_FOO, ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING);
    verifySchedulerSnapshot(SUPERVISOR_ID_BAR, ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING);

    Thread.sleep(1200);
    executor.finishAllPendingTasks();

    verifySchedulerSnapshotWithTasks(
        SUPERVISOR_ID_FOO,
        ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING,
        ImmutableMap.of(),
        ImmutableMap.of(expectedFooTask1.getTaskId(), TaskStatus.success(expectedFooTask1.getTaskId()))
    );

    Thread.sleep(1200);
    executor.finishAllPendingTasks();
    scheduler.stopScheduledIngestion(SUPERVISOR_ID_FOO);
    verifySchedulerSnapshotWithTasks(
        SUPERVISOR_ID_FOO,
        ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_SHUTDOWN,
        ImmutableMap.of(expectedFooTask2.getTaskId(), TaskStatus.running(expectedFooTask2.getTaskId())),
        ImmutableMap.of(expectedFooTask1.getTaskId(), TaskStatus.success(expectedFooTask1.getTaskId()))
    );

    verifySchedulerSnapshotWithTasks(
        SUPERVISOR_ID_BAR,
        ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING,
        ImmutableMap.of(),
        ImmutableMap.of(
            expectedBarTask2.getTaskId(), TaskStatus.success(expectedBarTask2.getTaskId()),
            expectedBarTask1.getTaskId(), TaskStatus.failure(expectedBarTask1.getTaskId(), null)
        )
    );

    scheduler.stop();
    assertNull(scheduler.getSchedulerSnapshot(SUPERVISOR_ID_FOO));
    assertNull(scheduler.getSchedulerSnapshot(SUPERVISOR_ID_BAR));
    serviceEmitter.verifyEmitted("batchSupervisor/tasks/submit/success", 4);
  }

  private void verifySchedulerSnapshot(final String supervisorId, final ScheduledBatchSupervisorPayload.BatchSupervisorStatus expectedStatus)
  {
    verifySchedulerSnapshotWithTasks(supervisorId, expectedStatus, ImmutableMap.of(), ImmutableMap.of());
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
