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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TimeChunkLock;
import org.apache.druid.indexing.common.task.KillUnusedSegmentsTask;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.DruidOverlord;
import org.apache.druid.indexing.overlord.GlobalTaskLockbox;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageAdapter;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueryTool;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.WorkerTaskRunnerQueryAdapter;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.metadata.TaskLookup;
import org.apache.druid.metadata.TaskLookup.ActiveTaskLookup;
import org.apache.druid.metadata.TaskLookup.CompleteTaskLookup;
import org.apache.druid.metadata.TaskLookup.TaskLookupType;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class OverlordResourceTest
{
  private OverlordResource overlordResource;
  private DruidOverlord overlord;
  private TaskMaster taskMaster;
  private TaskStorage taskStorage;
  private GlobalTaskLockbox taskLockbox;
  private JacksonConfigManager configManager;
  private AuthConfig authConfig;
  private TaskQueryTool taskQueryTool;
  private IndexerMetadataStorageAdapter indexerMetadataStorageAdapter;
  private HttpServletRequest req;
  private TaskRunner taskRunner;
  private TaskQueue taskQueue;
  private WorkerTaskRunnerQueryAdapter workerTaskRunnerQueryAdapter;
  private AuditManager auditManager;

  @BeforeEach
  public void setUp()
  {
    taskRunner = EasyMock.createMock(TaskRunner.class);
    taskQueue = EasyMock.createStrictMock(TaskQueue.class);
    configManager = EasyMock.createMock(JacksonConfigManager.class);
    authConfig = EasyMock.createMock(AuthConfig.class);
    overlord = EasyMock.createStrictMock(DruidOverlord.class);
    taskMaster = EasyMock.createStrictMock(TaskMaster.class);
    taskStorage = EasyMock.createStrictMock(TaskStorage.class);
    taskLockbox = EasyMock.createStrictMock(GlobalTaskLockbox.class);
    taskQueryTool = new TaskQueryTool(
        taskStorage,
        taskLockbox,
        taskMaster,
        () -> configManager.watch(WorkerBehaviorConfig.CONFIG_KEY, WorkerBehaviorConfig.class).get()
    );
    indexerMetadataStorageAdapter = EasyMock.createStrictMock(IndexerMetadataStorageAdapter.class);
    req = EasyMock.createStrictMock(HttpServletRequest.class);
    workerTaskRunnerQueryAdapter = EasyMock.createStrictMock(WorkerTaskRunnerQueryAdapter.class);
    auditManager = EasyMock.createMock(AuditManager.class);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(
        Optional.of(taskRunner)
    ).anyTimes();

    AuthorizerMapper authMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return new Authorizer()
        {
          @Override
          public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
          {
            final String username = authenticationResult.getIdentity();
            switch (resource.getName()) {
              case "allow":
                return new Access(true);
              case Datasources.WIKIPEDIA:
                // Only "Wiki Reader" can read "wikipedia"
                return new Access(
                    action == Action.READ && Users.WIKI_READER.equals(username)
                );
              case Datasources.BUZZFEED:
                // Only "Buzz Reader" can read "buzzfeed"
                return new Access(
                    action == Action.READ && Users.BUZZ_READER.equals(username)
                );
              default:
                return new Access(false);
            }
          }

        };
      }
    };

    overlordResource = new OverlordResource(
        overlord,
        taskMaster,
        taskQueryTool,
        indexerMetadataStorageAdapter,
        null,
        configManager,
        auditManager,
        authMapper,
        workerTaskRunnerQueryAdapter,
        authConfig
    );
  }

  @AfterEach
  public void tearDown()
  {
    EasyMock.verify(
        taskRunner,
        taskMaster,
        taskStorage,
        taskLockbox,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter,
        authConfig
    );
  }

  private void replayAll()
  {
    EasyMock.replay(
        overlord,
        taskRunner,
        taskQueue,
        taskMaster,
        taskStorage,
        taskLockbox,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter,
        authConfig,
        configManager,
        auditManager
    );
  }

  @Test
  public void testLeader()
  {
    EasyMock.expect(overlord.getCurrentLeader()).andReturn("boz").once();
    replayAll();

    final Response response = overlordResource.getLeader();
    Assertions.assertEquals("boz", response.getEntity());
    Assertions.assertEquals(200, response.getStatus());
  }

  @Test
  public void testIsLeader()
  {
    EasyMock.expect(overlord.isLeader()).andReturn(true).once();
    EasyMock.expect(overlord.isLeader()).andReturn(false).once();
    replayAll();

    // true
    final Response response1 = overlordResource.isLeader();
    Assertions.assertEquals(ImmutableMap.of("leader", true), response1.getEntity());
    Assertions.assertEquals(200, response1.getStatus());

    // false
    final Response response2 = overlordResource.isLeader();
    Assertions.assertEquals(ImmutableMap.of("leader", false), response2.getEntity());
    Assertions.assertEquals(404, response2.getStatus());
  }

  @Test
  public void testSecuredGetWaitingTask()
  {
    expectAuthorizationTokenCheck();
    EasyMock.expect(
        taskStorage.getTaskStatusPlusList(
            ImmutableMap.of(TaskLookupType.ACTIVE, ActiveTaskLookup.getInstance()),
            null
        )
    ).andStubReturn(
        ImmutableList.of(
            createTaskStatusPlus("id_1", TaskState.RUNNING, "allow"),
            createTaskStatusPlus("id_2", TaskState.RUNNING, "allow"),
            createTaskStatusPlus("id_3", TaskState.RUNNING, "deny"),
            createTaskStatusPlus("id_4", TaskState.RUNNING, "deny")
        )
    );

    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getKnownTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_1"),
            new MockTaskRunnerWorkItem("id_4")
        )
    );

    replayAll();

    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource.getWaitingTasks(req)
                                                                                  .getEntity();
    Assertions.assertEquals(1, responseObjects.size());
    Assertions.assertEquals("id_2", responseObjects.get(0).getId());
  }

  @Test
  public void testSecuredGetCompleteTasks()
  {
    expectAuthorizationTokenCheck();
    List<String> tasksIds = ImmutableList.of("id_1", "id_2", "id_3");

    EasyMock.expect(
        taskStorage.getTaskStatusPlusList(
            ImmutableMap.of(TaskLookupType.COMPLETE, CompleteTaskLookup.of(null, (Duration) null)), null)
    ).andStubReturn(
        ImmutableList.of(
            createTaskStatusPlus("id_1", TaskState.SUCCESS, "deny"),
            createTaskStatusPlus("id_2", TaskState.SUCCESS, "allow"),
            createTaskStatusPlus("id_3", TaskState.SUCCESS, "allow")
        )
    );
    replayAll();

    List<TaskStatusPlus> responseObjects = (List) overlordResource
        .getCompleteTasks(null, req).getEntity();

    Assertions.assertEquals(2, responseObjects.size());
    Assertions.assertEquals(tasksIds.get(1), responseObjects.get(0).getId());
    Assertions.assertEquals(tasksIds.get(2), responseObjects.get(1).getId());
  }

  @Test
  public void testSecuredGetRunningTasks()
  {
    expectAuthorizationTokenCheck();
    List<String> tasksIds = ImmutableList.of("id_1", "id_2");
    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getRunningTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem(tasksIds.get(0)),
            new MockTaskRunnerWorkItem(tasksIds.get(1))
        )
    );
    EasyMock.expect(
        taskStorage.getTaskStatusPlusList(
            ImmutableMap.of(TaskLookupType.ACTIVE, ActiveTaskLookup.getInstance()),
            null
        )
    ).andStubReturn(
        ImmutableList.of(
            createTaskStatusPlus("id_1", TaskState.RUNNING, "deny"),
            createTaskStatusPlus("id_2", TaskState.RUNNING, "allow")
        )
    );
    EasyMock.expect(taskRunner.getRunnerTaskState("id_1")).andStubReturn(RunnerTaskState.RUNNING);
    EasyMock.expect(taskRunner.getRunnerTaskState("id_2")).andStubReturn(RunnerTaskState.RUNNING);

    replayAll();

    List<TaskStatusPlus> responseObjects = (List) overlordResource.getRunningTasks(null, req)
                                                                  .getEntity();

    Assertions.assertEquals(1, responseObjects.size());
    Assertions.assertEquals(tasksIds.get(1), responseObjects.get(0).getId());
  }

  @Test
  public void test_getAllActiveTasks_withTaskQueryTool_returnsRunningTasksOnly()
  {
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue));
    EasyMock.expect(taskQueue.getTaskInfos()).andReturn(
        List.of(
            new TaskInfo(DateTimes.nowUtc(), TaskStatus.success("s"), new NoopTask("s", null, null, 1L, 0L, null)),
            new TaskInfo(DateTimes.nowUtc(), TaskStatus.failure("f", ""), new NoopTask("f", null, null, 1L, 0L, null)),
            new TaskInfo(DateTimes.nowUtc(), TaskStatus.running("r1"), new NoopTask("r1", null, null, 1L, 0L, null)),
            new TaskInfo(DateTimes.nowUtc(), TaskStatus.running("r2"), new NoopTask("r2", null, null, 1L, 0L, null))
        )
    );

    replayAll();

    final List<TaskStatusPlus> activeTasks = taskQueryTool.getAllActiveTasks();
    Assertions.assertEquals(2, activeTasks.size());
    Assertions.assertTrue(activeTasks.stream().allMatch(status -> status.getStatusCode().equals(TaskState.RUNNING)));
  }

  @Test
  public void testGetTasks()
  {
    expectAuthorizationTokenCheck();
    EasyMock.expect(
        taskStorage.getTaskStatusPlusList(
            ImmutableMap.of(
                TaskLookupType.ACTIVE,
                ActiveTaskLookup.getInstance(),
                TaskLookupType.COMPLETE,
                CompleteTaskLookup.of(null, null)
            ),
            null
        )
    ).andStubReturn(
        ImmutableList.of(
            createTaskStatusPlus("id_5", TaskState.SUCCESS, "deny"),
            createTaskStatusPlus("id_6", TaskState.SUCCESS, "allow"),
            createTaskStatusPlus("id_7", TaskState.SUCCESS, "allow"),
            createTaskStatusPlus("id_5", TaskState.SUCCESS, "deny"),
            createTaskStatusPlus("id_6", TaskState.SUCCESS, "allow"),
            createTaskStatusPlus("id_7", TaskState.SUCCESS, "allow")
        )
    );

    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getKnownTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_1"),
            new MockTaskRunnerWorkItem("id_4")
        )
    ).atLeastOnce();

    replayAll();

    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks(null, null, null, null, null, req)
        .getEntity();
    Assertions.assertEquals(4, responseObjects.size());
  }

  @Test
  public void testGetTasksFilterDataSource()
  {
    expectAuthorizationTokenCheck();
    //completed tasks
    EasyMock.expect(
        taskStorage.getTaskStatusPlusList(
            ImmutableMap.of(
                TaskLookupType.COMPLETE,
                CompleteTaskLookup.of(null, null),
                TaskLookupType.ACTIVE,
                ActiveTaskLookup.getInstance()
            ),
            "allow"
        )
    ).andStubReturn(
            ImmutableList.of(
                createTaskStatusPlus("id_5", TaskState.SUCCESS, "allow"),
                createTaskStatusPlus("id_6", TaskState.SUCCESS, "allow"),
                createTaskStatusPlus("id_7", TaskState.SUCCESS, "allow"),
                createTaskStatusPlus("id_1", TaskState.RUNNING, "allow"),
                createTaskStatusPlus("id_2", TaskState.SUCCESS, "allow"),
                createTaskStatusPlus("id_3", TaskState.SUCCESS, "allow"),
                createTaskStatusPlus("id_4", TaskState.SUCCESS, "allow")
            )
    );
    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getKnownTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_1"),
            new MockTaskRunnerWorkItem("id_4")
        )
    ).atLeastOnce();
    replayAll();

    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks(null, "allow", null, null, null, req)
        .getEntity();
    Assertions.assertEquals(7, responseObjects.size());
    Assertions.assertEquals("id_5", responseObjects.get(0).getId());
    Assertions.assertEquals("allow", responseObjects.get(0).getDataSource(), "DataSource Check");
  }

  @Test
  public void testGetTasksFilterWaitingState()
  {
    expectAuthorizationTokenCheck();
    //active tasks
    EasyMock.expect(
        taskStorage.getTaskStatusPlusList(
            ImmutableMap.of(
                TaskLookupType.ACTIVE,
                ActiveTaskLookup.getInstance()
            ),
            null
        )
    ).andStubReturn(
        ImmutableList.of(
            createTaskStatusPlus("id_1", TaskState.RUNNING, "allow"),
            createTaskStatusPlus("id_2", TaskState.RUNNING, "allow"),
            createTaskStatusPlus("id_3", TaskState.RUNNING, "deny"),
            createTaskStatusPlus("id_4", TaskState.RUNNING, "deny")
        )
    );

    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getKnownTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_1"),
            new MockTaskRunnerWorkItem("id_4")
        )
    );

    replayAll();

    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks(
            "waiting",
            null,
            null,
            null,
            null,
            req
        ).getEntity();
    Assertions.assertEquals(1, responseObjects.size());
    Assertions.assertEquals("id_2", responseObjects.get(0).getId());
  }

  @Test
  public void testGetTasksFilterRunningState()
  {
    expectAuthorizationTokenCheck();
    EasyMock.expect(
        taskStorage.getTaskStatusPlusList(
            ImmutableMap.of(
                TaskLookupType.ACTIVE,
                ActiveTaskLookup.getInstance()
            ),
            "allow"
        )
    ).andStubReturn(
        ImmutableList.of(
            createTaskStatusPlus("id_1", TaskState.RUNNING, "allow"),
            createTaskStatusPlus("id_2", TaskState.RUNNING, "allow"),
            createTaskStatusPlus("id_3", TaskState.RUNNING, "allow"),
            createTaskStatusPlus("id_4", TaskState.RUNNING, "deny")
        )
    );

    List<String> tasksIds = ImmutableList.of("id_1", "id_2");
    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getRunningTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem(tasksIds.get(0), "allow", "test"),
            new MockTaskRunnerWorkItem(tasksIds.get(1), "allow", "test")
        )
    );
    EasyMock.expect(taskRunner.getRunnerTaskState("id_1")).andReturn(RunnerTaskState.RUNNING);
    EasyMock.expect(taskRunner.getRunnerTaskState("id_2")).andReturn(RunnerTaskState.RUNNING);

    replayAll();

    List<TaskStatusPlus> responseObjects = (List) overlordResource
        .getTasks("running", "allow", null, null, null, req)
        .getEntity();

    Assertions.assertEquals(2, responseObjects.size());
    Assertions.assertEquals(tasksIds.get(0), responseObjects.get(0).getId());
    Assertions.assertEquals("allow", responseObjects.get(0).getDataSource(), "DataSource Check");
  }

  @Test
  public void testGetTasksFilterPendingState()
  {
    expectAuthorizationTokenCheck();

    List<String> tasksIds = ImmutableList.of("id_1", "id_2");
    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getPendingTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem(tasksIds.get(0)),
            new MockTaskRunnerWorkItem(tasksIds.get(1))
        )
    );
    EasyMock.expect(
        taskStorage.getTaskStatusPlusList(
            ImmutableMap.of(TaskLookupType.ACTIVE, ActiveTaskLookup.getInstance()),
            null
        )
    ).andStubReturn(
        ImmutableList.of(
            createTaskStatusPlus("id_1", TaskState.RUNNING, "deny"),
            createTaskStatusPlus("id_2", TaskState.RUNNING, "allow"),
            createTaskStatusPlus("id_3", TaskState.RUNNING, "allow"),
            createTaskStatusPlus("id_4", TaskState.RUNNING, "deny")
        )
    );

    EasyMock.expect(taskRunner.getRunnerTaskState("id_1")).andStubReturn(RunnerTaskState.PENDING);
    EasyMock.expect(taskRunner.getRunnerTaskState("id_2")).andStubReturn(RunnerTaskState.PENDING);
    EasyMock.expect(taskRunner.getRunnerTaskState("id_3")).andStubReturn(RunnerTaskState.RUNNING);
    EasyMock.expect(taskRunner.getRunnerTaskState("id_4")).andStubReturn(RunnerTaskState.RUNNING);

    replayAll();

    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks("pending", null, null, null, null, req)
        .getEntity();

    Assertions.assertEquals(1, responseObjects.size());
    Assertions.assertEquals(tasksIds.get(1), responseObjects.get(0).getId());
    Assertions.assertEquals("allow", responseObjects.get(0).getDataSource(), "DataSource Check");
  }

  @Test
  public void testGetTasksFilterCompleteState()
  {
    expectAuthorizationTokenCheck();
    EasyMock.expect(
        taskStorage.getTaskStatusPlusList(
            ImmutableMap.of(TaskLookupType.COMPLETE, CompleteTaskLookup.of(null, (Duration) null)),
            null
        )
    ).andStubReturn(
        ImmutableList.of(
            createTaskStatusPlus("id_1", TaskState.SUCCESS, "allow"),
            createTaskStatusPlus("id_2", TaskState.SUCCESS, "deny"),
            createTaskStatusPlus("id_3", TaskState.SUCCESS, "allow")
        )
    );
    replayAll();

    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks("complete", null, null, null, null, req)
        .getEntity();
    Assertions.assertEquals(2, responseObjects.size());
    Assertions.assertEquals("id_1", responseObjects.get(0).getId());
    Assertions.assertTrue("allow".equals(responseObjects.get(0).getDataSource()), "DataSource Check");
  }

  @Test
  public void testGetTasksFilterCompleteStateWithInterval()
  {
    expectAuthorizationTokenCheck();
    Duration duration = new Period("PT86400S").toStandardDuration();
    EasyMock.expect(
        taskStorage.getTaskStatusPlusList(
            EasyMock.anyObject(),
            EasyMock.anyObject()
        )
    ).andStubReturn(
            ImmutableList.of(
                createTaskStatusPlus("id_1", TaskState.SUCCESS, "deny"),
                createTaskStatusPlus("id_2", TaskState.SUCCESS, "allow"),
                createTaskStatusPlus("id_3", TaskState.SUCCESS, "allow")
            )
    );

    replayAll();

    String interval = "2010-01-01_P1D";
    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks("complete", null, interval, null, null, req)
        .getEntity();
    Assertions.assertEquals(2, responseObjects.size());
    Assertions.assertEquals("id_2", responseObjects.get(0).getId());
    Assertions.assertEquals("allow", responseObjects.get(0).getDataSource(), "DataSource Check");
  }

  @Test
  public void testGetTasksRequiresDatasourceRead()
  {
    // Setup mocks for a user who has read access to "wikipedia"
    // and no access to "buzzfeed"
    expectAuthorizationTokenCheck(Users.WIKI_READER);

    // Setup mocks to return completed, active, known, pending and running tasks
    EasyMock.expect(
        taskStorage.getTaskStatusPlusList(
            ImmutableMap.of(
                TaskLookupType.COMPLETE,
                CompleteTaskLookup.of(null, null),
                TaskLookupType.ACTIVE,
                ActiveTaskLookup.getInstance()
            ),
            null
        )
    ).andStubReturn(
        ImmutableList.of(
            createTaskStatusPlus("id_5", TaskState.SUCCESS, Datasources.WIKIPEDIA),
            createTaskStatusPlus("id_6", TaskState.SUCCESS, Datasources.BUZZFEED),
            createTaskStatusPlus("id_1", TaskState.RUNNING, Datasources.WIKIPEDIA),
            createTaskStatusPlus("id_4", TaskState.RUNNING, Datasources.BUZZFEED)
        )
    );

    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getKnownTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_1", Datasources.WIKIPEDIA, "test"),
            new MockTaskRunnerWorkItem("id_4", Datasources.BUZZFEED, "test")
        )
    ).atLeastOnce();

    EasyMock.expect(taskRunner.getRunnerTaskState("id_4")).andReturn(RunnerTaskState.PENDING);
    EasyMock.expect(taskRunner.getRunnerTaskState("id_1")).andReturn(RunnerTaskState.RUNNING);

    replayAll();

    // Verify that only the tasks of read access datasource are returned
    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks(null, null, null, null, null, req)
        .getEntity();
    Assertions.assertEquals(2, responseObjects.size());
    for (TaskStatusPlus taskStatus : responseObjects) {
      Assertions.assertEquals(Datasources.WIKIPEDIA, taskStatus.getDataSource());
    }
  }

  @Test
  public void testGetTasksFilterByTaskTypeRequiresDatasourceRead()
  {
    // Setup mocks for a user who has read access to "wikipedia"
    // and no access to "buzzfeed"
    expectAuthorizationTokenCheck(Users.WIKI_READER);

    // Setup mocks to return completed, active, known, pending and running tasks
    EasyMock.expect(
        taskStorage.getTaskStatusPlusList(
            ImmutableMap.of(
                TaskLookupType.COMPLETE,
                CompleteTaskLookup.of(null, null),
                TaskLookupType.ACTIVE,
                ActiveTaskLookup.getInstance()
            ),
            null
        )
    ).andStubReturn(
        ImmutableList.of(
            createTaskStatusPlus("id_5", TaskState.SUCCESS, Datasources.WIKIPEDIA),
            createTaskStatusPlus("id_6", TaskState.SUCCESS, Datasources.BUZZFEED),
            createTaskStatusPlus("id_1", TaskState.RUNNING, Datasources.WIKIPEDIA, "to-return"),
            createTaskStatusPlus("id_4", TaskState.RUNNING, Datasources.BUZZFEED)
        )
    );

    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getKnownTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_1", Datasources.WIKIPEDIA, "to-return"),
            new MockTaskRunnerWorkItem("id_4", Datasources.WIKIPEDIA, "test")
        )
    ).atLeastOnce();

    EasyMock.expect(taskRunner.getRunnerTaskState("id_1")).andReturn(RunnerTaskState.RUNNING);

    replayAll();

    // Verify that only the tasks of read access datasource are returned
    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks(null, null, null, null, "to-return", req)
        .getEntity();
    Assertions.assertEquals(1, responseObjects.size());
    for (TaskStatusPlus taskStatus : responseObjects) {
      Assertions.assertEquals("to-return", taskStatus.getType());
    }
  }

  @Test
  public void testGetTasksFilterByDatasourceRequiresReadAccess()
  {
    // Setup mocks for a user who has read access to "wikipedia"
    // and no access to "buzzfeed"
    expectAuthorizationTokenCheck(Users.WIKI_READER);

    replayAll();

    // Verify that only the tasks of read access datasource are returned
    Assertions.assertThrows(
        WebApplicationException.class,
        () -> overlordResource.getTasks(null, Datasources.BUZZFEED, null, null, null, req)
    );
  }

  @Test
  public void testGetCompleteTasksOfAllDatasources()
  {
    expectAuthorizationTokenCheck();
    EasyMock.expect(
        taskStorage.getTaskStatusPlusList(
            ImmutableMap.of(
                TaskLookupType.COMPLETE,
                CompleteTaskLookup.of(null, null)
            ),
            null
        )
    ).andStubReturn(
        ImmutableList.of(
            createTaskStatusPlus("id_1", TaskState.SUCCESS, "allow"),
            createTaskStatusPlus("id_2", TaskState.SUCCESS, "deny"),
            createTaskStatusPlus("id_3", TaskState.SUCCESS, "allow")
        )
    );
    replayAll();

    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks("complete", null, null, null, null, req)
        .getEntity();
    Assertions.assertEquals(2, responseObjects.size());
    Assertions.assertEquals("id_1", responseObjects.get(0).getId());
    Assertions.assertEquals("allow", responseObjects.get(0).getDataSource(), "DataSource Check");
  }

  @Test
  public void testGetTasksNegativeState()
  {
    replayAll();

    Object responseObject = overlordResource
        .getTasks("blah", "ds_test", null, null, null, req)
        .getEntity();
    Assertions.assertEquals(
        "Invalid task state[blah]. Must be one of [pending, waiting, running, complete].",
        responseObject.toString()
    );
  }

  @Test
  public void testSecuredTaskPost()
  {
    expectAuthorizationTokenCheck();
    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(false);

    replayAll();

    final Task task = NoopTask.create();
    Assertions.assertThrows(ForbiddenException.class, () -> overlordResource.taskPost(task, req));
  }

  @Test
  public void testKillTaskIsAudited()
  {
    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(false);

    final String username = Users.DRUID;
    expectAuthorizationTokenCheck(username);
    EasyMock.expect(req.getMethod()).andReturn("POST").once();
    EasyMock.expect(req.getRequestURI()).andReturn("/indexer/v2/task").once();
    EasyMock.expect(req.getQueryString()).andReturn("").once();
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn(username).once();
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn("killing segments").once();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(new AuthenticationResult(username, "druid", null, null))
            .once();
    EasyMock.expect(req.getRemoteAddr()).andReturn("127.0.0.1").once();

    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).once();

    final Capture<AuditEntry> auditEntryCapture = EasyMock.newCapture();
    auditManager.doAudit(EasyMock.capture(auditEntryCapture));
    EasyMock.expectLastCall().once();

    replayAll();

    Task task = new KillUnusedSegmentsTask("kill_all", "allow", Intervals.ETERNITY, null, null, 10, null, null);
    overlordResource.taskPost(task, req);

    Assertions.assertTrue(auditEntryCapture.hasCaptured());
    AuditEntry auditEntry = auditEntryCapture.getValue();
    Assertions.assertEquals(username, auditEntry.getAuditInfo().getAuthor());
    Assertions.assertEquals("killing segments", auditEntry.getAuditInfo().getComment());
    Assertions.assertEquals("druid", auditEntry.getAuditInfo().getIdentity());
    Assertions.assertEquals("127.0.0.1", auditEntry.getAuditInfo().getIp());
  }

  @Test
  public void testTaskPostDeniesDatasourceReadUser()
  {
    expectAuthorizationTokenCheck(Users.WIKI_READER);
    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(false);

    replayAll();

    // Verify that taskPost fails for user who has only datasource read access
    final Task task = NoopTask.forDatasource(Datasources.WIKIPEDIA);
    Assertions.assertThrows(ForbiddenException.class, () -> overlordResource.taskPost(task, req));
  }

  @Test
  public void testKillPendingSegments()
  {
    expectAuthorizationTokenCheck();

    EasyMock.expect(overlord.isLeader()).andReturn(true);
    EasyMock
        .expect(
            indexerMetadataStorageAdapter.deletePendingSegments(
                EasyMock.eq("allow"),
                EasyMock.anyObject(Interval.class)
            )
        )
        .andReturn(2);

    replayAll();

    Response response = overlordResource
        .killPendingSegments("allow", new Interval(DateTimes.MIN, DateTimes.nowUtc()).toString(), req);
    Assertions.assertEquals(200, response.getStatus());
    Assertions.assertEquals(ImmutableMap.of("numDeleted", 2), response.getEntity());
  }

  @Test
  public void testKillPendingSegmentsThrowsInvalidInputDruidException()
  {
    expectAuthorizationTokenCheck();

    EasyMock.expect(overlord.isLeader()).andReturn(true);
    final String exceptionMsg = "Some exception msg";
    EasyMock
        .expect(
            indexerMetadataStorageAdapter.deletePendingSegments(
                EasyMock.eq("allow"),
                EasyMock.anyObject(Interval.class)
            )
        )
        .andThrow(InvalidInput.exception(exceptionMsg))
        .once();

    replayAll();

    Response response = overlordResource
        .killPendingSegments("allow", new Interval(DateTimes.MIN, DateTimes.nowUtc()).toString(), req);

    Assertions.assertEquals(400, response.getStatus());
    Assertions.assertEquals(ImmutableMap.of("error", exceptionMsg), response.getEntity());
  }

  @Test
  public void testKillPendingSegmentsThrowsDefensiveDruidException()
  {
    expectAuthorizationTokenCheck();

    EasyMock.expect(overlord.isLeader()).andReturn(true);
    final String exceptionMsg = "An internal defensive exception";
    EasyMock
        .expect(
            indexerMetadataStorageAdapter.deletePendingSegments(
                EasyMock.eq("allow"),
                EasyMock.anyObject(Interval.class)
            )
        )
        .andThrow(DruidException.defensive(exceptionMsg))
        .once();

    replayAll();

    Response response = overlordResource
        .killPendingSegments("allow", new Interval(DateTimes.MIN, DateTimes.nowUtc()).toString(), req);

    Assertions.assertEquals(500, response.getStatus());
    Assertions.assertEquals(ImmutableMap.of("error", exceptionMsg), response.getEntity());
  }

  @Test
  public void testKillPendingSegmentsThrowsArbitraryException()
  {
    expectAuthorizationTokenCheck();

    EasyMock.expect(overlord.isLeader()).andReturn(true);
    final String exceptionMsg = "An unexpected illegal state exception";
    EasyMock
        .expect(
            indexerMetadataStorageAdapter.deletePendingSegments(
                EasyMock.eq("allow"),
                EasyMock.anyObject(Interval.class)
            )
        )
        .andThrow(new IllegalStateException(exceptionMsg))
        .once();

    replayAll();

    Response response = overlordResource
        .killPendingSegments("allow", new Interval(DateTimes.MIN, DateTimes.nowUtc()).toString(), req);

    Assertions.assertEquals(500, response.getStatus());
    Assertions.assertEquals(ImmutableMap.of("error", exceptionMsg), response.getEntity());
  }

  @Test
  public void testKillPendingSegmentsToNonLeader()
  {
    expectAuthorizationTokenCheck();

    EasyMock.expect(overlord.isLeader()).andReturn(false);

    replayAll();

    Response response = overlordResource
        .killPendingSegments("allow", new Interval(DateTimes.MIN, DateTimes.nowUtc()).toString(), req);

    Assertions.assertEquals(503, response.getStatus());
    Assertions.assertEquals(ImmutableMap.of("error", "overlord is not the leader or not initialized yet"), response.getEntity());
  }

  @Test
  public void testGetTaskPayload() throws Exception
  {
    // This is disabled since OverlordResource.getTaskStatus() is annotated with TaskResourceFilter which is supposed to
    // set authorization token properly, but isn't called in this test.
    // This should be fixed in https://github.com/apache/druid/issues/6685.
    // expectAuthorizationTokenCheck();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.absent()).anyTimes();

    final NoopTask task = NoopTask.create();
    EasyMock.expect(taskStorage.getTask("mytask"))
            .andReturn(Optional.of(task));

    EasyMock.expect(taskStorage.getTask("othertask"))
            .andReturn(Optional.absent());

    replayAll();

    final Response response1 = overlordResource.getTaskPayload("mytask");
    final TaskPayloadResponse taskPayloadResponse1 = TestHelper.makeJsonMapper().readValue(
        TestHelper.makeJsonMapper().writeValueAsString(response1.getEntity()),
        TaskPayloadResponse.class
    );
    Assertions.assertEquals(new TaskPayloadResponse("mytask", task), taskPayloadResponse1);

    final Response response2 = overlordResource.getTaskPayload("othertask");
    final TaskPayloadResponse taskPayloadResponse2 = TestHelper.makeJsonMapper().readValue(
        TestHelper.makeJsonMapper().writeValueAsString(response2.getEntity()),
        TaskPayloadResponse.class
    );
    Assertions.assertEquals(new TaskPayloadResponse("othertask", null), taskPayloadResponse2);
  }

  @Test
  public void testGetTaskStatus() throws Exception
  {
    // This is disabled since OverlordResource.getTaskStatus() is annotated with TaskResourceFilter which is supposed to
    // set authorization token properly, but isn't called in this test.
    // This should be fixed in https://github.com/apache/druid/issues/6685.
    // expectAuthorizationTokenCheck();
    final Task task = NoopTask.create();
    final String taskId = task.getId();
    final TaskStatus status = TaskStatus.running(taskId);
    final TaskInfo taskInfo = new TaskInfo(
        DateTimes.of("2018-01-01"),
        status,
        task
    );

    // For noop, simulate in-memory hit
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).once();
    EasyMock.expect(taskQueue.getActiveTaskInfo(taskId)).andReturn(Optional.of(taskInfo)).once();

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).once();
    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getKnownTasks())
            .andReturn(ImmutableList.of(new MockTaskRunnerWorkItem(taskId))).anyTimes();
    EasyMock.expect(taskRunner.getRunnerTaskState(taskId)).andReturn(RunnerTaskState.RUNNING).once();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).once();

    // For "othertask", simulate in-memory miss, then task storage read
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).once();
    EasyMock.expect(taskQueue.getActiveTaskInfo("othertask")).andReturn(Optional.absent()).once();
    EasyMock.expect(taskStorage.getTaskInfo("othertask")).andReturn(null).once();

    replayAll();

    final Response response1 = overlordResource.getTaskStatus(taskId);
    final TaskStatusResponse taskStatusResponse1 = TestHelper.makeJsonMapper().readValue(
        TestHelper.makeJsonMapper().writeValueAsString(response1.getEntity()),
        TaskStatusResponse.class
    );
    Assertions.assertEquals(
        new TaskStatusResponse(
            taskId,
            new TaskStatusPlus(
                task.getId(),
                task.getGroupId(),
                "noop",
                DateTimes.of("2018-01-01"),
                DateTimes.EPOCH,
                TaskState.RUNNING,
                RunnerTaskState.RUNNING,
                -1L,
                TaskLocation.unknown(),
                task.getDataSource(),
                null
            )
        ),
        taskStatusResponse1
    );

    final Response response2 = overlordResource.getTaskStatus("othertask");
    final TaskStatusResponse taskStatusResponse2 = TestHelper.makeJsonMapper().readValue(
        TestHelper.makeJsonMapper().writeValueAsString(response2.getEntity()),
        TaskStatusResponse.class
    );
    Assertions.assertEquals(new TaskStatusResponse("othertask", null), taskStatusResponse2);
  }

  @Test
  public void testGetLockedIntervals() throws Exception
  {
    final List<LockFilterPolicy> lockFilterPolicies = ImmutableList.of(
        new LockFilterPolicy("ds1", 25, null, null)
    );
    final Map<String, List<Interval>> expectedIntervals = Collections.singletonMap(
        "ds1",
        Arrays.asList(
            Intervals.of("2012-01-01/2012-01-02"),
            Intervals.of("2012-01-01/2012-01-02")
        )
    );

    EasyMock.expect(taskLockbox.getLockedIntervals(lockFilterPolicies))
            .andReturn(expectedIntervals);
    replayAll();

    final Response response = overlordResource.getDatasourceLockedIntervals(lockFilterPolicies);
    Assertions.assertEquals(200, response.getStatus());

    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    Map<String, List<Interval>> observedIntervals = jsonMapper.readValue(
        jsonMapper.writeValueAsString(response.getEntity()),
        new TypeReference<>() {}
    );

    Assertions.assertEquals(expectedIntervals, observedIntervals);
  }

  @Test
  public void testGetLockedIntervalsWithEmptyBody()
  {
    replayAll();

    Response response = overlordResource.getDatasourceLockedIntervals(null);
    Assertions.assertEquals(400, response.getStatus());

    response = overlordResource.getDatasourceLockedIntervals(Collections.emptyList());
    Assertions.assertEquals(400, response.getStatus());
  }

  @Test
  public void testGetActiveLocks() throws Exception
  {
    final List<LockFilterPolicy> lockFilterPolicies = ImmutableList.of(
        new LockFilterPolicy("ds1", 25, null, null)
    );
    final Map<String, List<TaskLock>> expectedLocks = Collections.singletonMap(
        "ds1",
        Arrays.asList(
            new TimeChunkLock(
                TaskLockType.REPLACE,
                "groupId",
                "datasource",
                Intervals.of("2012-01-01/2012-01-02"),
                "version",
                25
            ),
            new TimeChunkLock(
                TaskLockType.EXCLUSIVE,
                "groupId",
                "datasource",
                Intervals.of("2012-01-02/2012-01-03"),
                "version",
                75
                )
        )
    );

    EasyMock.expect(taskLockbox.getActiveLocks(lockFilterPolicies))
            .andReturn(expectedLocks);
    replayAll();

    final Response response = overlordResource.getActiveLocks(lockFilterPolicies);
    Assertions.assertEquals(200, response.getStatus());

    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    Map<String, List<TaskLock>> observedLocks = jsonMapper.readValue(
        jsonMapper.writeValueAsString(response.getEntity()),
        new TypeReference<TaskLockResponse>()
        {
        }
    ).getDatasourceToLocks();

    Assertions.assertEquals(expectedLocks, observedLocks);
  }

  @Test
  public void testGetActiveLocksWithEmptyBody()
  {
    replayAll();

    Response response = overlordResource.getActiveLocks(null);
    Assertions.assertEquals(400, response.getStatus());

    response = overlordResource.getActiveLocks(Collections.emptyList());
    Assertions.assertEquals(400, response.getStatus());
  }

  @Test
  public void testShutdownTask()
  {
    // This is disabled since OverlordResource.doShutdown is annotated with TaskResourceFilter
    // This should be fixed in https://github.com/apache/druid/issues/6685.
    // expectAuthorizationTokenCheck();
    TaskQueue mockQueue = EasyMock.createMock(TaskQueue.class);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(
        Optional.of(mockQueue)
    ).once();
    mockQueue.shutdown("id_1", "Shutdown request from user");
    EasyMock.expectLastCall();

    replayAll();
    EasyMock.replay(mockQueue);

    final Map<String, Integer> response = (Map<String, Integer>) overlordResource
        .doShutdown("id_1")
        .getEntity();
    Assertions.assertEquals("id_1", response.get("task"));
  }

  @Test
  public void testShutdownAllTasks()
  {
    // This is disabled since OverlordResource.shutdownTasksForDataSource is annotated with DatasourceResourceFilter
    // This should be fixed in https://github.com/apache/druid/issues/6685.
    // expectAuthorizationTokenCheck();
    TaskQueue mockQueue = EasyMock.createMock(TaskQueue.class);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(
        Optional.of(mockQueue)
    ).anyTimes();
    EasyMock.expect(
        taskStorage.getTaskInfos(TaskLookup.activeTasksOnly(), "datasource")
    ).andStubReturn(ImmutableList.of(
        new TaskInfo(
            DateTime.now(ISOChronology.getInstanceUTC()),
            TaskStatus.success("id_1"),
            new NoopTask("id_1", null, "datasource", 1L, 0L, null)
        ),
        new TaskInfo(
            DateTime.now(ISOChronology.getInstanceUTC()),
            TaskStatus.success("id_2"),
            new NoopTask("id_2", null, "datasource", 1L, 0L, null)
        )
    ));
    mockQueue.shutdown("id_1", "Shutdown request from user");
    EasyMock.expectLastCall();
    mockQueue.shutdown("id_2", "Shutdown request from user");
    EasyMock.expectLastCall();

    replayAll();
    EasyMock.replay(mockQueue);

    final Map<String, String> response = (Map<String, String>) overlordResource
        .shutdownTasksForDataSource("datasource")
        .getEntity();
    Assertions.assertEquals("datasource", response.get("dataSource"));
  }

  @Test
  public void testShutdownAllTasksForNonExistingDataSource()
  {
    final TaskQueue taskQueue = EasyMock.createMock(TaskQueue.class);
    EasyMock.expect(overlord.isLeader()).andReturn(true).anyTimes();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskStorage.getTaskInfos(EasyMock.anyObject(TaskLookup.class), EasyMock.anyString()))
            .andReturn(Collections.emptyList());
    replayAll();

    final Response response = overlordResource.shutdownTasksForDataSource("notExisting");
    Assertions.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void testEnableWorker()
  {
    final String host = "worker-host";

    workerTaskRunnerQueryAdapter.enableWorker(host);
    EasyMock.expectLastCall().once();

    replayAll();

    final Response response = overlordResource.enableWorker(host);

    Assertions.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatus());
    Assertions.assertEquals(ImmutableMap.of(host, "enabled"), response.getEntity());
  }

  @Test
  public void testDisableWorker()
  {
    final String host = "worker-host";

    workerTaskRunnerQueryAdapter.disableWorker(host);
    EasyMock.expectLastCall().once();

    replayAll();

    final Response response = overlordResource.disableWorker(host);

    Assertions.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatus());
    Assertions.assertEquals(ImmutableMap.of(host, "disabled"), response.getEntity());
  }

  @Test
  public void testEnableWorkerWhenWorkerAPIRaisesError()
  {
    final String host = "worker-host";

    workerTaskRunnerQueryAdapter.enableWorker(host);
    EasyMock.expectLastCall().andThrow(new RE("Worker API returns error!")).once();

    replayAll();

    final Response response = overlordResource.enableWorker(host);

    Assertions.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode(), response.getStatus());
    Assertions.assertEquals(ImmutableMap.of("error", "Worker API returns error!"), response.getEntity());
  }

  @Test
  public void testDisableWorkerWhenWorkerAPIRaisesError()
  {
    final String host = "worker-host";

    workerTaskRunnerQueryAdapter.disableWorker(host);
    EasyMock.expectLastCall().andThrow(new RE("Worker API returns error!")).once();

    replayAll();

    final Response response = overlordResource.disableWorker(host);

    Assertions.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode(), response.getStatus());
    Assertions.assertEquals(ImmutableMap.of("error", "Worker API returns error!"), response.getEntity());
  }

  @Test
  public void testGetTotalWorkerCapacityNotLeader()
  {
    EasyMock.expect(overlord.isLeader()).andReturn(false);
    replayAll();
    final Response response = overlordResource.getTotalWorkerCapacity();
    Assertions.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.getCode(), response.getStatus());
  }

  @Test
  public void testGetTotalWorkerCapacityWithUnknown()
  {
    WorkerBehaviorConfig workerBehaviorConfig = EasyMock.createMock(WorkerBehaviorConfig.class);
    AtomicReference<WorkerBehaviorConfig> workerBehaviorConfigAtomicReference
        = new AtomicReference<>(workerBehaviorConfig);
    EasyMock.expect(configManager.watch(WorkerBehaviorConfig.CONFIG_KEY, WorkerBehaviorConfig.class))
            .andReturn(workerBehaviorConfigAtomicReference);
    EasyMock.expect(taskRunner.getTotalCapacity()).andReturn(-1);
    EasyMock.expect(taskRunner.getUsedCapacity()).andReturn(-1);
    EasyMock.expect(taskRunner.getMaximumCapacityWithAutoscale()).andReturn(-1);
    EasyMock.expect(overlord.isLeader()).andReturn(true);
    replayAll();

    final Response response = overlordResource.getTotalWorkerCapacity();
    Assertions.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatus());
    Assertions.assertEquals(-1, ((TotalWorkerCapacityResponse) response.getEntity()).getCurrentClusterCapacity());
    Assertions.assertEquals(-1, ((TotalWorkerCapacityResponse) response.getEntity()).getUsedClusterCapacity());
    Assertions.assertEquals(-1, ((TotalWorkerCapacityResponse) response.getEntity()).getMaximumCapacityWithAutoScale());
  }

  @Test
  public void testGetTotalWorkerCapacityWithMaximumCapacity()
  {
    int expectedWorkerCapacity = 3;
    int expectedWorkerCapacityWithAutoscale = 10;
    WorkerBehaviorConfig workerBehaviorConfig = EasyMock.createMock(WorkerBehaviorConfig.class);
    AtomicReference<WorkerBehaviorConfig> workerBehaviorConfigAtomicReference
        = new AtomicReference<>(workerBehaviorConfig);
    EasyMock.expect(configManager.watch(WorkerBehaviorConfig.CONFIG_KEY, WorkerBehaviorConfig.class))
        .andReturn(workerBehaviorConfigAtomicReference);
    EasyMock.expect(taskRunner.getTotalCapacity()).andReturn(expectedWorkerCapacity);
    EasyMock.expect(taskRunner.getUsedCapacity()).andReturn(expectedWorkerCapacity);
    EasyMock.expect(taskRunner.getMaximumCapacityWithAutoscale()).andReturn(expectedWorkerCapacityWithAutoscale);
    EasyMock.expect(overlord.isLeader()).andReturn(true);
    replayAll();

    final Response response = overlordResource.getTotalWorkerCapacity();
    Assertions.assertEquals(HttpResponseStatus.OK.getCode(), response.getStatus());
    Assertions.assertEquals(expectedWorkerCapacity, ((TotalWorkerCapacityResponse) response.getEntity()).getCurrentClusterCapacity());
    Assertions.assertEquals(expectedWorkerCapacity, ((TotalWorkerCapacityResponse) response.getEntity()).getUsedClusterCapacity());
    Assertions.assertEquals(expectedWorkerCapacityWithAutoscale, ((TotalWorkerCapacityResponse) response.getEntity()).getMaximumCapacityWithAutoScale());
  }

  @Test
  public void testResourceActionsForTaskWithInputTypeAndInputSecurityEnabled()
  {

    final String dataSource = "dataSourceTest";
    final String inputSourceType = "local";
    Task task = EasyMock.createMock(Task.class);

    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(true);
    EasyMock.expect(task.getDataSource()).andReturn(dataSource);
    EasyMock.expect(task.getDestinationResource()).andReturn(java.util.Optional.of(new Resource(dataSource, ResourceType.DATASOURCE)));
    EasyMock.expect(task.getInputSourceResources())
            .andReturn(ImmutableSet.of(new ResourceAction(
                new Resource(inputSourceType, ResourceType.EXTERNAL),
                Action.READ
            )));

    EasyMock.replay(task);
    replayAll();

    Set<ResourceAction> expectedResourceActions = ImmutableSet.of(
        new ResourceAction(new Resource(dataSource, ResourceType.DATASOURCE), Action.WRITE),
        new ResourceAction(new Resource(inputSourceType, ResourceType.EXTERNAL), Action.READ)
    );
    Set<ResourceAction> resourceActions = overlordResource.getNeededResourceActionsForTask(task);
    Assertions.assertEquals(expectedResourceActions, resourceActions);
  }

  @Test
  public void testResourceActionsForTaskWithInvalidSecurityAndInputSecurityEnabled()
  {
    final String dataSource = "dataSourceTest";
    final UOE expectedException = new UOE("unsupported");
    Task task = EasyMock.createMock(Task.class);

    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(true);
    EasyMock.expect(task.getId()).andReturn("taskId");
    EasyMock.expect(task.getDataSource()).andReturn(dataSource);
    EasyMock.expect(task.getDestinationResource()).andReturn(java.util.Optional.of(new Resource(dataSource, ResourceType.DATASOURCE)));
    EasyMock.expect(task.getInputSourceResources()).andThrow(expectedException);

    EasyMock.replay(task);
    replayAll();

    final UOE e = Assertions.assertThrows(
        UOE.class,
        () -> overlordResource.getNeededResourceActionsForTask(task)
    );

    Assertions.assertEquals(expectedException, e);
  }

  @Test
  public void testResourceActionsForTaskWithInputTypeAndInputSecurityDisabled()
  {
    final String dataSource = "dataSourceTest";
    final String inputSourceType = "local";
    Task task = EasyMock.createMock(Task.class);

    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(false);
    EasyMock.expect(task.getDataSource()).andReturn(dataSource);
    EasyMock.expect(task.getDestinationResource()).andReturn(java.util.Optional.of(new Resource(dataSource, ResourceType.DATASOURCE)));
    EasyMock.expect(task.getInputSourceResources())
            .andReturn(ImmutableSet.of(new ResourceAction(
                new Resource(inputSourceType, ResourceType.EXTERNAL),
                Action.READ
            )));

    EasyMock.replay(task);
    replayAll();

    Set<ResourceAction> expectedResourceActions = ImmutableSet.of(
        new ResourceAction(new Resource(dataSource, ResourceType.DATASOURCE), Action.WRITE)
    );
    Set<ResourceAction> resourceActions = overlordResource.getNeededResourceActionsForTask(task);
    Assertions.assertEquals(expectedResourceActions, resourceActions);
  }

  @Test
  public void testGetMultipleTaskStatuses_presentTaskQueue()
  {
    EasyMock.expect(taskMaster.getTaskQueue())
            .andReturn(Optional.of(taskQueue));
    EasyMock.expect(taskQueue.getTaskStatus("task"))
            .andReturn(Optional.of(TaskStatus.running("task")));
    replayAll();

    final Object response = overlordResource.getMultipleTaskStatuses(ImmutableSet.of("task"))
                                            .getEntity();
    Assertions.assertEquals(ImmutableMap.of("task", TaskStatus.running("task")), response);
  }

  @Test
  public void testGetMultipleTaskStatuses_absentTaskQueue()
  {
    EasyMock.expect(taskStorage.getStatus("task"))
            .andReturn(Optional.of(TaskStatus.running("task")));
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.absent());
    replayAll();

    final Object response = overlordResource.getMultipleTaskStatuses(ImmutableSet.of("task"))
                                            .getEntity();
    Assertions.assertEquals(ImmutableMap.of("task", TaskStatus.running("task")), response);
  }

  @Test
  public void testGetTaskSegmentsReturns404()
  {
    replayAll();
    OverlordResource overlordResource =
        new OverlordResource(null, null, null, null, null, null, null, null, null, null);
    final Response response = overlordResource.getTaskSegments("taskId");
    Assertions.assertEquals(404, response.getStatus());
    Assertions.assertEquals(
        Collections.singletonMap(
            "error",
            "Segment IDs committed by a task action are not persisted anymore."
            + " Use the metric 'segment/added/bytes' to identify the segments created by a task."
        ),
        response.getEntity()
    );
  }

  private void expectAuthorizationTokenCheck()
  {
    expectAuthorizationTokenCheck(Users.DRUID);
  }

  private void expectAuthorizationTokenCheck(String username)
  {
    AuthenticationResult authenticationResult = new AuthenticationResult(username, "druid", null, null);
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(authenticationResult)
            .atLeastOnce();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();

    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, false);
    EasyMock.expectLastCall().anyTimes();

    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
  }

  private TaskStatusPlus createTaskStatusPlus(String taskId, TaskState taskState, String datasource)
  {
    return createTaskStatusPlus(taskId, taskState, datasource, "test");
  }

  private TaskStatusPlus createTaskStatusPlus(String taskId, TaskState taskState, String datasource, String taskType)
  {
    return new TaskStatusPlus(
        taskId,
        null,
        taskType,
        DateTime.now(ISOChronology.getInstanceUTC()),
        DateTimes.EPOCH,
        taskState,
        taskState.isComplete() ? RunnerTaskState.NONE : RunnerTaskState.WAITING,
        100L,
        TaskLocation.unknown(),
        datasource,
        null
    );
  }

  /**
   * Usernames to use in the tests.
   */
  private static class Users
  {
    private static final String DRUID = "druid";
    private static final String WIKI_READER = "Wiki Reader";
    private static final String BUZZ_READER = "Buzz Reader";
  }

  /**
   * Datasource names to use in the tests.
   */
  private static class Datasources
  {
    private static final String WIKIPEDIA = "wikipedia";
    private static final String BUZZFEED = "buzzfeed";
  }

  private static class MockTaskRunnerWorkItem extends TaskRunnerWorkItem
  {
    private final String dataSource;
    private final String type;

    public MockTaskRunnerWorkItem(String taskId)
    {
      this(taskId, "ds_test", "test");
    }

    public MockTaskRunnerWorkItem(
        String taskId,
        String dataSource,
        String type
    )
    {
      super(taskId, null);
      this.dataSource = dataSource;
      this.type = type;
    }

    @Override
    public TaskLocation getLocation()
    {
      return TaskLocation.unknown();
    }

    @Override
    public String getTaskType()
    {
      return type;
    }

    @Override
    public String getDataSource()
    {
      return dataSource;
    }

  }
}
