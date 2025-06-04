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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ScheduledBatchTaskManagerTest
{
  private static final String SUPERVISOR_ID_FOO = "foo";
  private static final String SUPERVISOR_ID_BAR = "bar";

  private static final String DATASOURCE = "ds";

  private static final String TASK_ID_FOO1 = "fooTaskId1";
  private static final String TASK_ID_FOO2 = "fooTaskId2";
  private static final String TASK_ID_BAR1 = "barTaskId1";
  private static final String TASK_ID_BAR2 = "barTaskId2";

  private static final CronSchedulerConfig IMMEDIATE_SCHEDULER_CONFIG = new CronSchedulerConfig()
  {
    @Override
    public DateTime getNextTaskStartTimeAfter(final DateTime referenceTime)
    {
      return referenceTime;
    }

    @Override
    public Duration getDurationUntilNextTaskStartTimeAfter(final DateTime referenceTime)
    {
      return Duration.ZERO;
    }
  };

  private BlockingExecutorService executor;
  private BrokerClient brokerClient;
  private StubServiceEmitter serviceEmitter;

  private ClientSqlQuery query1;
  private ClientSqlQuery query2;

  private ScheduledBatchTaskManager scheduler;

  @Before
  public void setUp()
  {
    brokerClient = Mockito.mock(BrokerClient.class);
    executor = new BlockingExecutorService("test");
    serviceEmitter = new StubServiceEmitter();
    query1 = createSqlQuery("REPLACE INTO foo OVERWRITE ALL SELECT * FROM bar PARTITIONED BY ALL");
    query2 = createSqlQuery("REPLACE INTO foo OVERWRITE ALL SELECT * FROM bar PARTITIONED BY DAY");
    scheduler = new ScheduledBatchTaskManager(
        new TaskMaster(null, null),
        (nameFormat, numThreads) -> new WrappingScheduledExecutorService("test", executor, false),
        brokerClient,
        serviceEmitter,
        new ScheduledBatchStatusTracker()
    );
  }

  private static ClientSqlQuery createSqlQuery(final String query)
  {
    return new ClientSqlQuery(
        query,
        null,
        true,
        true,
        true,
        null,
        null
    );
  }

  @Test
  public void testSchedulerWithSuccessfulTaskSubmission()
  {
    final SqlTaskStatus expectedTaskStatus = new SqlTaskStatus(TASK_ID_FOO1, TaskState.SUCCESS, null);
    Mockito.when(brokerClient.submitSqlTask(query1))
           .thenReturn(Futures.immediateFuture(expectedTaskStatus));

    scheduler.start();
    scheduler.startScheduledIngestion(SUPERVISOR_ID_FOO, DATASOURCE, IMMEDIATE_SCHEDULER_CONFIG, query1);
    verifySchedulerState(SUPERVISOR_ID_FOO, ScheduledBatchSupervisor.State.RUNNING);

    executor.finishNextPendingTasks(1);
    verifySchedulerStateWithTasks(
        SUPERVISOR_ID_FOO,
        ScheduledBatchSupervisor.State.RUNNING,
        ImmutableList.of(TaskStatus.success(expectedTaskStatus.getTaskId()))
    );

    scheduler.stopScheduledIngestion(SUPERVISOR_ID_FOO);
    verifySchedulerStateWithTasks(
        SUPERVISOR_ID_FOO,
        ScheduledBatchSupervisor.State.SUSPENDED,
        ImmutableList.of(TaskStatus.success(expectedTaskStatus.getTaskId()))
    );

    scheduler.stop();
    assertNull(scheduler.getSupervisorStatus(SUPERVISOR_ID_FOO));
    serviceEmitter.verifyEmitted(
        "task/scheduledBatch/submit/success",
        ImmutableMap.of(DruidMetrics.ID, SUPERVISOR_ID_FOO),
        1
    );
  }

  @Test
  public void testSchedulerWithFailedTaskSubmission()
  {
    Mockito.when(brokerClient.submitSqlTask(query1))
           .thenReturn(
               Futures.immediateFailedFuture(
                   new HttpResponseException(
                       new StringFullResponseHolder(
                           new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR),
                           StandardCharsets.UTF_8
                       )
                   )
               ));

    scheduler.start();
    scheduler.startScheduledIngestion(SUPERVISOR_ID_FOO, DATASOURCE, IMMEDIATE_SCHEDULER_CONFIG, query1);
    verifySchedulerState(SUPERVISOR_ID_FOO, ScheduledBatchSupervisor.State.RUNNING);

    executor.finishNextPendingTasks(1);
    scheduler.stopScheduledIngestion(SUPERVISOR_ID_FOO);
    verifySchedulerState(
        SUPERVISOR_ID_FOO,
        ScheduledBatchSupervisor.State.SUSPENDED
    );

    scheduler.stop();
    assertNull(scheduler.getSupervisorStatus(SUPERVISOR_ID_FOO));
    serviceEmitter.verifyEmitted(
        "task/scheduledBatch/submit/failed",
        ImmutableMap.of(DruidMetrics.ID, SUPERVISOR_ID_FOO),
        1
    );
  }

  @Test
  public void testSchedulerRestartAfterSchedulerIsSuspended()
  {
    final SqlTaskStatus expectedTaskStatus = new SqlTaskStatus(TASK_ID_FOO1, TaskState.SUCCESS, null);
    Mockito.when(brokerClient.submitSqlTask(query1))
           .thenReturn(Futures.immediateFuture(expectedTaskStatus));

    scheduler.start();
    scheduler.startScheduledIngestion(SUPERVISOR_ID_FOO, DATASOURCE, IMMEDIATE_SCHEDULER_CONFIG, query1);
    verifySchedulerState(SUPERVISOR_ID_FOO, ScheduledBatchSupervisor.State.RUNNING);

    executor.finishNextPendingTasks(1);
    verifySchedulerStateWithTasks(
        SUPERVISOR_ID_FOO,
        ScheduledBatchSupervisor.State.RUNNING,
        ImmutableList.of(TaskStatus.success(expectedTaskStatus.getTaskId()))
    );

    scheduler.stopScheduledIngestion(SUPERVISOR_ID_FOO);
    executor.finishNextPendingTask();
    verifySchedulerStateWithTasks(
        SUPERVISOR_ID_FOO,
        ScheduledBatchSupervisor.State.SUSPENDED,
        ImmutableList.of(TaskStatus.success(expectedTaskStatus.getTaskId()))
    );
    assertFalse(executor.hasPendingTasks());

    scheduler.startScheduledIngestion(SUPERVISOR_ID_FOO, DATASOURCE, IMMEDIATE_SCHEDULER_CONFIG, query1);
    verifySchedulerStateWithTasks(
        SUPERVISOR_ID_FOO,
        ScheduledBatchSupervisor.State.RUNNING,
        ImmutableList.of(TaskStatus.success(expectedTaskStatus.getTaskId()))
    );
    executor.finishNextPendingTasks(1);

    scheduler.stop();
    assertNull(scheduler.getSupervisorStatus(SUPERVISOR_ID_FOO));
    serviceEmitter.verifyEmitted(
        "task/scheduledBatch/submit/success",
        ImmutableMap.of(DruidMetrics.ID, SUPERVISOR_ID_FOO),
        2
    );
  }

  @Test
  public void testSupervisorStopsSubmittingJobsWhenSuspended()
  {
    final SqlTaskStatus expectedTaskStatus = new SqlTaskStatus(TASK_ID_FOO1, TaskState.SUCCESS, null);
    Mockito.when(brokerClient.submitSqlTask(query1))
           .thenReturn(Futures.immediateFuture(expectedTaskStatus));

    scheduler.start();
    scheduler.startScheduledIngestion(SUPERVISOR_ID_FOO, DATASOURCE, IMMEDIATE_SCHEDULER_CONFIG, query1);
    verifySchedulerState(SUPERVISOR_ID_FOO, ScheduledBatchSupervisor.State.RUNNING);

    executor.finishNextPendingTasks(1);
    verifySchedulerStateWithTasks(
        SUPERVISOR_ID_FOO,
        ScheduledBatchSupervisor.State.RUNNING,
        ImmutableList.of(TaskStatus.success(expectedTaskStatus.getTaskId()))
    );

    scheduler.stopScheduledIngestion(SUPERVISOR_ID_FOO);
    executor.finishNextPendingTask();
    verifySchedulerStateWithTasks(
        SUPERVISOR_ID_FOO,
        ScheduledBatchSupervisor.State.SUSPENDED,
        ImmutableList.of(TaskStatus.success(expectedTaskStatus.getTaskId()))
    );
    assertFalse(executor.hasPendingTasks());

    scheduler.stop();
    assertNull(scheduler.getSupervisorStatus(SUPERVISOR_ID_FOO));
    serviceEmitter.verifyEmitted(
        "task/scheduledBatch/submit/success",
        ImmutableMap.of(DruidMetrics.ID, SUPERVISOR_ID_FOO),
        1
    );
  }

  @Test
  public void testSchedulerWithMultipleSupervisors()
  {
    final SqlTaskStatus expectedFooTask1 = new SqlTaskStatus(TASK_ID_FOO1, TaskState.SUCCESS, null);
    final SqlTaskStatus expectedFooTask2 = new SqlTaskStatus(TASK_ID_FOO2, TaskState.RUNNING, null);
    final SqlTaskStatus expectedBarTask1 = new SqlTaskStatus(TASK_ID_BAR1, TaskState.FAILED, new ErrorResponse(
        InvalidInput.exception("some exception")));
    final SqlTaskStatus expectedBarTask2 = new SqlTaskStatus(TASK_ID_BAR2, TaskState.SUCCESS, null);

    Mockito.when(brokerClient.submitSqlTask(query1))
           .thenReturn(Futures.immediateFuture(expectedFooTask1))
           .thenReturn(Futures.immediateFuture(expectedFooTask2));
    Mockito.when(brokerClient.submitSqlTask(query2))
           .thenReturn(Futures.immediateFuture(expectedBarTask1))
           .thenReturn(Futures.immediateFuture(expectedBarTask2));

    assertFalse(executor.hasPendingTasks());

    scheduler.start();
    scheduler.startScheduledIngestion(SUPERVISOR_ID_FOO, DATASOURCE, IMMEDIATE_SCHEDULER_CONFIG, query1);
    scheduler.startScheduledIngestion(SUPERVISOR_ID_BAR, DATASOURCE, IMMEDIATE_SCHEDULER_CONFIG, query2);

    verifySchedulerState(SUPERVISOR_ID_FOO, ScheduledBatchSupervisor.State.RUNNING);
    verifySchedulerState(SUPERVISOR_ID_BAR, ScheduledBatchSupervisor.State.RUNNING);

    executor.finishNextPendingTasks(2);

    verifySchedulerStateWithTasks(
        SUPERVISOR_ID_FOO,
        ScheduledBatchSupervisor.State.RUNNING,
        ImmutableList.of(TaskStatus.success(TASK_ID_FOO1))
    );

    executor.finishNextPendingTasks(2);
    scheduler.stopScheduledIngestion(SUPERVISOR_ID_FOO);
    verifySchedulerStateWithTasks(
        SUPERVISOR_ID_FOO,
        ScheduledBatchSupervisor.State.SUSPENDED,
        ImmutableList.of(TaskStatus.success(TASK_ID_FOO1), TaskStatus.running(TASK_ID_FOO2))
    );

    verifySchedulerStateWithTasks(
        SUPERVISOR_ID_BAR,
        ScheduledBatchSupervisor.State.RUNNING,
        ImmutableList.of(TaskStatus.failure(TASK_ID_BAR1, null), TaskStatus.success(TASK_ID_BAR2))
    );

    scheduler.stop();
    assertNull(scheduler.getSupervisorStatus(SUPERVISOR_ID_FOO));
    assertNull(scheduler.getSupervisorStatus(SUPERVISOR_ID_BAR));
    serviceEmitter.verifyEmitted(
        "task/scheduledBatch/submit/success",
        ImmutableMap.of(DruidMetrics.ID, SUPERVISOR_ID_FOO),
        2
    );

    serviceEmitter.verifyEmitted(
        "task/scheduledBatch/submit/success",
        ImmutableMap.of(DruidMetrics.ID, SUPERVISOR_ID_BAR),
        2
    );
  }

  private void verifySchedulerState(
      final String supervisorId,
      final ScheduledBatchSupervisor.State expectedSupervisorState
  )
  {
    verifySchedulerStateWithTasks(
        supervisorId,
        expectedSupervisorState,
        ImmutableList.of()
    );
  }

  private void verifySchedulerStateWithTasks(
      final String supervisorId,
      final ScheduledBatchSupervisor.State expectedSupervisorState,
      final List<TaskStatus> expectedRecentTasks
  )
  {
    final ScheduledBatchSupervisorStatus taskStatus = scheduler.getSupervisorStatus(supervisorId);
    assertNotNull(taskStatus);
    assertEquals(supervisorId, taskStatus.getSupervisorId());
    assertEquals(expectedSupervisorState, taskStatus.getState());

    BatchSupervisorTaskReport taskReport = taskStatus.getTaskReport();
    verifyTaskStatuses(taskReport.getRecentTasks(), expectedRecentTasks);
  }

  private void verifyTaskStatuses(List<BatchSupervisorTaskStatus> actualStatuses, List<TaskStatus> expectedStatuses)
  {
    assertEquals(expectedStatuses.size(), actualStatuses.size());
    for (int i = 0; i < expectedStatuses.size(); i++) {
      assertEquals(expectedStatuses.get(i), actualStatuses.get(i).getTaskStatus());
    }
  }
}
