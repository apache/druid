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
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.query.http.SqlTaskStatus;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
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
    verifySchedulerSnapshot(SUPERVISOR_ID_FOO, ScheduledBatchSupervisorSnapshot.BatchSupervisorStatus.SCHEDULER_RUNNING);

    executor.finishNextPendingTasks(1);
    verifySchedulerSnapshotWithTasks(
        SUPERVISOR_ID_FOO,
        ScheduledBatchSupervisorSnapshot.BatchSupervisorStatus.SCHEDULER_RUNNING,
        ImmutableMap.of(),
        ImmutableMap.of(expectedTaskStatus.getTaskId(), TaskStatus.success(expectedTaskStatus.getTaskId()))
    );

    scheduler.stopScheduledIngestion(SUPERVISOR_ID_FOO);
    verifySchedulerSnapshotWithTasks(
        SUPERVISOR_ID_FOO,
        ScheduledBatchSupervisorSnapshot.BatchSupervisorStatus.SCHEDULER_SHUTDOWN,
        ImmutableMap.of(),
        ImmutableMap.of(expectedTaskStatus.getTaskId(), TaskStatus.success(expectedTaskStatus.getTaskId()))
    );

    scheduler.stop();
    assertNull(scheduler.getSchedulerSnapshot(SUPERVISOR_ID_FOO));
    serviceEmitter.verifyEmitted(
        "batchSupervisor/tasks/submit/success",
        ImmutableMap.of("supervisorId", SUPERVISOR_ID_FOO),
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
    verifySchedulerSnapshot(SUPERVISOR_ID_FOO, ScheduledBatchSupervisorSnapshot.BatchSupervisorStatus.SCHEDULER_RUNNING);

    executor.finishNextPendingTasks(1);
    scheduler.stopScheduledIngestion(SUPERVISOR_ID_FOO);
    verifySchedulerSnapshot(
        SUPERVISOR_ID_FOO,
        ScheduledBatchSupervisorSnapshot.BatchSupervisorStatus.SCHEDULER_SHUTDOWN
    );

    scheduler.stop();
    assertNull(scheduler.getSchedulerSnapshot(SUPERVISOR_ID_FOO));
    serviceEmitter.verifyEmitted(
        "batchSupervisor/tasks/submit/failed",
        ImmutableMap.of("supervisorId", SUPERVISOR_ID_FOO),
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

    scheduler.start();
    scheduler.startScheduledIngestion(SUPERVISOR_ID_FOO, DATASOURCE, IMMEDIATE_SCHEDULER_CONFIG, query1);
    scheduler.startScheduledIngestion(SUPERVISOR_ID_BAR, DATASOURCE, IMMEDIATE_SCHEDULER_CONFIG, query2);

    verifySchedulerSnapshot(SUPERVISOR_ID_FOO, ScheduledBatchSupervisorSnapshot.BatchSupervisorStatus.SCHEDULER_RUNNING);
    verifySchedulerSnapshot(SUPERVISOR_ID_BAR, ScheduledBatchSupervisorSnapshot.BatchSupervisorStatus.SCHEDULER_RUNNING);

    executor.finishNextPendingTasks(2);

    verifySchedulerSnapshotWithTasks(
        SUPERVISOR_ID_FOO,
        ScheduledBatchSupervisorSnapshot.BatchSupervisorStatus.SCHEDULER_RUNNING,
        ImmutableMap.of(),
        ImmutableMap.of(expectedFooTask1.getTaskId(), TaskStatus.success(expectedFooTask1.getTaskId()))
    );

    executor.finishNextPendingTasks(4);
    scheduler.stopScheduledIngestion(SUPERVISOR_ID_FOO);
    verifySchedulerSnapshotWithTasks(
        SUPERVISOR_ID_FOO,
        ScheduledBatchSupervisorSnapshot.BatchSupervisorStatus.SCHEDULER_SHUTDOWN,
        ImmutableMap.of(expectedFooTask2.getTaskId(), TaskStatus.running(expectedFooTask2.getTaskId())),
        ImmutableMap.of(expectedFooTask1.getTaskId(), TaskStatus.success(expectedFooTask1.getTaskId()))
    );

    verifySchedulerSnapshotWithTasks(
        SUPERVISOR_ID_BAR,
        ScheduledBatchSupervisorSnapshot.BatchSupervisorStatus.SCHEDULER_RUNNING,
        ImmutableMap.of(),
        ImmutableMap.of(
            TASK_ID_BAR2, TaskStatus.success(TASK_ID_BAR2),
            TASK_ID_BAR1, TaskStatus.failure(TASK_ID_BAR1, null)
        )
    );

    scheduler.stop();
    assertNull(scheduler.getSchedulerSnapshot(SUPERVISOR_ID_FOO));
    assertNull(scheduler.getSchedulerSnapshot(SUPERVISOR_ID_BAR));
    serviceEmitter.verifyEmitted(
        "batchSupervisor/tasks/submit/success",
        ImmutableMap.of("supervisorId", SUPERVISOR_ID_FOO),
        3
    );
    serviceEmitter.verifyEmitted(
        "batchSupervisor/tasks/submit/success",
        ImmutableMap.of("supervisorId", SUPERVISOR_ID_BAR),
        3
    );
  }

  private void verifySchedulerSnapshot(
      final String supervisorId,
      final ScheduledBatchSupervisorSnapshot.BatchSupervisorStatus expectedSupervisorStatus
  )
  {
    verifySchedulerSnapshotWithTasks(supervisorId, expectedSupervisorStatus, ImmutableMap.of(), ImmutableMap.of());
  }

  private void verifySchedulerSnapshotWithTasks(
      final String supervisorId,
      final ScheduledBatchSupervisorSnapshot.BatchSupervisorStatus expectedSupervisorStatus,
      final ImmutableMap<String, TaskStatus> expectedActiveTasks,
      final ImmutableMap<String, TaskStatus> expectedCompletedTasks
  )
  {
    final ScheduledBatchSupervisorSnapshot snapshot = scheduler.getSchedulerSnapshot(supervisorId);
    assertNotNull(snapshot);
    assertEquals(supervisorId, snapshot.getSupervisorId());
    assertEquals(expectedActiveTasks, snapshot.getActiveTasks());
    assertEquals(expectedCompletedTasks, snapshot.getCompletedTasks());
    assertEquals(expectedSupervisorStatus, snapshot.getStatus());
  }
}
