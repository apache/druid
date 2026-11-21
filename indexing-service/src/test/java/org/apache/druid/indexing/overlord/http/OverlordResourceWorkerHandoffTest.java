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

package org.apache.druid.indexing.overlord.http;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.indexing.overlord.DruidOverlord;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageAdapter;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueryTool;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.WorkerTaskRunner;
import org.apache.druid.indexing.overlord.WorkerTaskRunnerQueryAdapter;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.overlord.supervisor.WorkerHandoffResult;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.Response;
import java.util.Collections;

public class OverlordResourceWorkerHandoffTest
{
  private static final String HOST = "mm-0.svc.ns.svc.cluster.local:8091";

  private TaskMaster taskMaster;
  private WorkerTaskRunner taskRunner;
  private TaskQueue taskQueue;
  private SupervisorManager supervisorManager;
  private OverlordResource resource;

  @BeforeEach
  public void setUp()
  {
    taskMaster = EasyMock.mock(TaskMaster.class);
    taskRunner = EasyMock.mock(WorkerTaskRunner.class);
    taskQueue = EasyMock.mock(TaskQueue.class);
    supervisorManager = EasyMock.mock(SupervisorManager.class);
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();

    resource = new OverlordResource(
        EasyMock.mock(DruidOverlord.class),
        taskMaster,
        EasyMock.mock(TaskQueryTool.class),
        EasyMock.mock(IndexerMetadataStorageAdapter.class),
        null,
        EasyMock.mock(JacksonConfigManager.class),
        EasyMock.mock(AuditManager.class),
        new AuthorizerMapper(Collections.emptyMap()),
        EasyMock.mock(WorkerTaskRunnerQueryAdapter.class),
        EasyMock.mock(AuthConfig.class),
        TestHelper.JSON_MAPPER
    );
  }

  @Test
  public void testHandoffReturnsAcceptedWithGroupedSupervisors()
  {
    EasyMock.expect(taskRunner.getWorkers()).andReturn(ImmutableList.of(workerInfo(ImmutableSet.of("primary", "ha"))));
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue));
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.submitHandoffNotices(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(new WorkerHandoffResult(
                ImmutableMap.of(
                    "KubeState", ImmutableList.of(0),
                    "KubeState_gcp_moncevents", ImmutableList.of(1)
                ),
                Collections.emptyList()
            ));
    EasyMock.replay(taskMaster, taskRunner, taskQueue, supervisorManager);

    final Response response = resource.handoffWorkerTasks(HOST);
    Assertions.assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
    final WorkerHandoffResponse entity = (WorkerHandoffResponse) response.getEntity();
    Assertions.assertEquals(HOST, entity.host());
    Assertions.assertEquals(ImmutableList.of(0), entity.taskGroups().get("KubeState"));
    Assertions.assertEquals(ImmutableList.of(1), entity.taskGroups().get("KubeState_gcp_moncevents"));
    Assertions.assertEquals(Collections.emptyList(), entity.skipped());
    EasyMock.verify(taskMaster, taskRunner, taskQueue, supervisorManager);
  }

  @Test
  public void testHandoffEmptyWorkerIsAccepted()
  {
    EasyMock.expect(taskRunner.getWorkers()).andReturn(ImmutableList.of(workerInfo(ImmutableSet.of())));
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue));
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.submitHandoffNotices(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(new WorkerHandoffResult(Collections.emptyMap(), Collections.emptyList()));
    EasyMock.replay(taskMaster, taskRunner, taskQueue, supervisorManager);

    final Response response = resource.handoffWorkerTasks(HOST);
    Assertions.assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
    final WorkerHandoffResponse entity = (WorkerHandoffResponse) response.getEntity();
    Assertions.assertEquals(Collections.emptyMap(), entity.taskGroups());
    Assertions.assertEquals(Collections.emptyList(), entity.skipped());
    EasyMock.verify(taskMaster, taskRunner, taskQueue, supervisorManager);
  }

  @Test
  public void testHandoffUnknownHostIsNotFound()
  {
    EasyMock.expect(taskRunner.getWorkers()).andReturn(ImmutableList.of(workerInfo(ImmutableSet.of("t1"))));
    EasyMock.replay(taskMaster, taskRunner, taskQueue, supervisorManager);

    final Response response = resource.handoffWorkerTasks("missing:8091");
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    EasyMock.verify(taskMaster, taskRunner);
  }

  @Test
  public void testHandoffSkipsNonStreamingTasks()
  {
    EasyMock.expect(taskRunner.getWorkers()).andReturn(ImmutableList.of(workerInfo(ImmutableSet.of("noop-task"))));
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue));
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.submitHandoffNotices(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(new WorkerHandoffResult(Collections.emptyMap(), Collections.emptyList()));
    EasyMock.replay(taskMaster, taskRunner, taskQueue, supervisorManager);

    final Response response = resource.handoffWorkerTasks(HOST);
    Assertions.assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
    final WorkerHandoffResponse entity = (WorkerHandoffResponse) response.getEntity();
    Assertions.assertEquals(Collections.emptyMap(), entity.taskGroups());
    Assertions.assertEquals(Collections.emptyList(), entity.skipped());
  }

  @Test
  public void testHandoffReportsNullTaskGroupIdAsSkipped()
  {
    EasyMock.expect(taskRunner.getWorkers())
            .andReturn(ImmutableList.of(workerInfo(ImmutableSet.of("queued", "index_kafka_legacy_abc"))));
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue));
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.submitHandoffNotices(EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(new WorkerHandoffResult(
                ImmutableMap.of("wikipedia", ImmutableList.of(0)),
                ImmutableList.of(
                    new WorkerHandoffResult.SkippedTask(
                        "index_kafka_legacy_abc",
                        WorkerHandoffResult.REASON_MISSING_TASK_GROUP_ID
                    )
                )
            ));
    EasyMock.replay(taskMaster, taskRunner, taskQueue, supervisorManager);

    final Response response = resource.handoffWorkerTasks(HOST);
    Assertions.assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
    final WorkerHandoffResponse entity = (WorkerHandoffResponse) response.getEntity();
    Assertions.assertEquals(ImmutableList.of(0), entity.taskGroups().get("wikipedia"));
    Assertions.assertEquals(
        ImmutableList.of(
            new WorkerHandoffResult.SkippedTask(
                "index_kafka_legacy_abc",
                WorkerHandoffResult.REASON_MISSING_TASK_GROUP_ID
            )
        ),
        entity.skipped()
    );
  }

  @Test
  public void testHandoffResponseJsonUsesIdAndReason() throws Exception
  {
    final WorkerHandoffResponse body = new WorkerHandoffResponse(
        HOST,
        ImmutableMap.of("wikipedia", ImmutableList.of(0, 2)),
        ImmutableList.of(
            new WorkerHandoffResult.SkippedTask(
                "index_kafka_legacy_abc",
                WorkerHandoffResult.REASON_MISSING_TASK_GROUP_ID
            ),
            new WorkerHandoffResult.SkippedTask(
                "index_kafka_manual_xyz",
                WorkerHandoffResult.REASON_MISSING_SUPERVISOR_ID
            )
        )
    );
    final String json = TestHelper.JSON_MAPPER.writeValueAsString(body);
    Assertions.assertTrue(json.contains("\"id\":\"index_kafka_legacy_abc\""));
    Assertions.assertTrue(json.contains("\"reason\":\"missing_taskGroupId\""));
    Assertions.assertTrue(json.contains("\"id\":\"index_kafka_manual_xyz\""));
    Assertions.assertTrue(json.contains("\"reason\":\"missing_supervisorId\""));
  }

  @Test
  public void testHandoffMissingTaskQueueIsServiceUnavailable()
  {
    EasyMock.expect(taskRunner.getWorkers()).andReturn(ImmutableList.of(workerInfo(ImmutableSet.of("t1"))));
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.absent());
    EasyMock.replay(taskMaster, taskRunner, taskQueue, supervisorManager);

    final Response response = resource.handoffWorkerTasks(HOST);
    Assertions.assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
    EasyMock.verify(taskMaster, taskRunner, taskQueue, supervisorManager);
  }

  @Test
  public void testHandoffNotLeaderIsServiceUnavailable()
  {
    final TaskMaster absentMaster = EasyMock.mock(TaskMaster.class);
    EasyMock.expect(absentMaster.getTaskRunner()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(absentMaster);
    final OverlordResource notLeader = new OverlordResource(
        EasyMock.mock(DruidOverlord.class),
        absentMaster,
        EasyMock.mock(TaskQueryTool.class),
        EasyMock.mock(IndexerMetadataStorageAdapter.class),
        null,
        EasyMock.mock(JacksonConfigManager.class),
        EasyMock.mock(AuditManager.class),
        new AuthorizerMapper(Collections.emptyMap()),
        EasyMock.mock(WorkerTaskRunnerQueryAdapter.class),
        EasyMock.mock(AuthConfig.class),
        TestHelper.JSON_MAPPER
    );
    final Response response = notLeader.handoffWorkerTasks(HOST);
    Assertions.assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
  }

  private static ImmutableWorkerInfo workerInfo(java.util.Set<String> runningTasks)
  {
    return new ImmutableWorkerInfo(
        new Worker("http", HOST, "127.0.0.1", 4, "v1", "default"),
        runningTasks.size(),
        ImmutableSet.of(),
        runningTasks,
        DateTimes.nowUtc()
    );
  }
}
