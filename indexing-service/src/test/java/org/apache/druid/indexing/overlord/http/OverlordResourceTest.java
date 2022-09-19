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
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageAdapter;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskStorageQueryAdapter;
import org.apache.druid.indexing.overlord.WorkerTaskRunner;
import org.apache.druid.indexing.overlord.WorkerTaskRunnerQueryAdapter;
import org.apache.druid.indexing.overlord.autoscaling.AutoScaler;
import org.apache.druid.indexing.overlord.autoscaling.ProvisioningStrategy;
import org.apache.druid.indexing.overlord.setup.DefaultWorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.RE;
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
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class OverlordResourceTest
{
  private OverlordResource overlordResource;
  private TaskMaster taskMaster;
  private JacksonConfigManager configManager;
  private ProvisioningStrategy provisioningStrategy;
  private TaskStorageQueryAdapter taskStorageQueryAdapter;
  private IndexerMetadataStorageAdapter indexerMetadataStorageAdapter;
  private HttpServletRequest req;
  private TaskRunner taskRunner;
  private WorkerTaskRunnerQueryAdapter workerTaskRunnerQueryAdapter;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp()
  {
    taskRunner = EasyMock.createMock(TaskRunner.class);
    configManager = EasyMock.createMock(JacksonConfigManager.class);
    provisioningStrategy = EasyMock.createMock(ProvisioningStrategy.class);
    taskMaster = EasyMock.createStrictMock(TaskMaster.class);
    taskStorageQueryAdapter = EasyMock.createStrictMock(TaskStorageQueryAdapter.class);
    indexerMetadataStorageAdapter = EasyMock.createStrictMock(IndexerMetadataStorageAdapter.class);
    req = EasyMock.createStrictMock(HttpServletRequest.class);
    workerTaskRunnerQueryAdapter = EasyMock.createStrictMock(WorkerTaskRunnerQueryAdapter.class);

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
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        null,
        configManager,
        null,
        authMapper,
        workerTaskRunnerQueryAdapter,
        provisioningStrategy
    );
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );
  }

  @Test
  public void testLeader()
  {
    EasyMock.expect(taskMaster.getCurrentLeader()).andReturn("boz").once();
    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    final Response response = overlordResource.getLeader();
    Assert.assertEquals("boz", response.getEntity());
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testIsLeader()
  {
    EasyMock.expect(taskMaster.isLeader()).andReturn(true).once();
    EasyMock.expect(taskMaster.isLeader()).andReturn(false).once();
    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    // true
    final Response response1 = overlordResource.isLeader();
    Assert.assertEquals(ImmutableMap.of("leader", true), response1.getEntity());
    Assert.assertEquals(200, response1.getStatus());

    // false
    final Response response2 = overlordResource.isLeader();
    Assert.assertEquals(ImmutableMap.of("leader", false), response2.getEntity());
    Assert.assertEquals(404, response2.getStatus());
  }

  @Test
  public void testSecuredGetWaitingTask()
  {
    expectAuthorizationTokenCheck();
    EasyMock.expect(
        taskStorageQueryAdapter.getTaskInfos(
            ImmutableMap.of(TaskLookupType.ACTIVE, ActiveTaskLookup.getInstance()),
            null
        )
    ).andStubReturn(
        ImmutableList.of(
            new TaskInfo<>(
                "id_1",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.running("id_1"),
                "allow",
                getTaskWithIdAndDatasource("id_1", "allow")
            ),
            new TaskInfo<>(
                "id_2",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.running("id_2"),
                "allow",
                getTaskWithIdAndDatasource("id_2", "allow")
            ),
            new TaskInfo<>(
                "id_3",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.running("id_3"),
                "deny",
                getTaskWithIdAndDatasource("id_3", "deny")
            ),
            new TaskInfo<>(
                "id_4",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.running("id_4"),
                "deny",
                getTaskWithIdAndDatasource("id_4", "deny")
            )
        )
    );

    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getKnownTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_1"),
            new MockTaskRunnerWorkItem("id_4")
        )
    );

    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource.getWaitingTasks(req)
                                                                                  .getEntity();
    Assert.assertEquals(1, responseObjects.size());
    Assert.assertEquals("id_2", responseObjects.get(0).getId());
  }

  @Test
  public void testSecuredGetCompleteTasks()
  {
    expectAuthorizationTokenCheck();
    List<String> tasksIds = ImmutableList.of("id_1", "id_2", "id_3");

    EasyMock.expect(
        taskStorageQueryAdapter.getTaskInfos(
            ImmutableMap.of(TaskLookupType.COMPLETE, CompleteTaskLookup.of(null, (Duration) null)), null)
    ).andStubReturn(
        ImmutableList.of(
            new TaskInfo<>(
                "id_1",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_1"),
                "deny",
                getTaskWithIdAndDatasource("id_1", "deny")
            ),
            new TaskInfo<>(
                "id_2",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_2"),
                "allow",
                getTaskWithIdAndDatasource("id_2", "allow")
            ),
            new TaskInfo<>(
                "id_3",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_3"),
                "allow",
                getTaskWithIdAndDatasource("id_3", "allow")
            )
        )
    );
    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );
    List<TaskStatusPlus> responseObjects = (List) overlordResource
        .getCompleteTasks(null, req).getEntity();

    Assert.assertEquals(2, responseObjects.size());
    Assert.assertEquals(tasksIds.get(1), responseObjects.get(0).getId());
    Assert.assertEquals(tasksIds.get(2), responseObjects.get(1).getId());
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
        taskStorageQueryAdapter.getTaskInfos(
            ImmutableMap.of(TaskLookupType.ACTIVE, ActiveTaskLookup.getInstance()),
            null
        )
    ).andStubReturn(
        ImmutableList.of(
            new TaskInfo<>(
                "id_1",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.running("id_1"),
                "deny",
                getTaskWithIdAndDatasource("id_1", "deny")
            ),
            new TaskInfo<>(
                "id_2",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.running("id_2"),
                "allow",
                getTaskWithIdAndDatasource("id_2", "allow")
            )
        )
    );
    EasyMock.expect(taskRunner.getRunnerTaskState("id_1")).andStubReturn(RunnerTaskState.RUNNING);
    EasyMock.expect(taskRunner.getRunnerTaskState("id_2")).andStubReturn(RunnerTaskState.RUNNING);

    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    List<TaskStatusPlus> responseObjects = (List) overlordResource.getRunningTasks(null, req)
                                                                  .getEntity();

    Assert.assertEquals(1, responseObjects.size());
    Assert.assertEquals(tasksIds.get(1), responseObjects.get(0).getId());
  }

  @Test
  public void testGetTasks()
  {
    expectAuthorizationTokenCheck();
    EasyMock.expect(
        taskStorageQueryAdapter.getTaskInfos(
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
            new TaskInfo<>(
                "id_5",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_5"),
                "deny",
                getTaskWithIdAndDatasource("id_5", "deny")
            ),
            new TaskInfo<>(
                "id_6",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_6"),
                "allow",
                getTaskWithIdAndDatasource("id_6", "allow")
            ),
            new TaskInfo<>(
                "id_7",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_7"),
                "allow",
                getTaskWithIdAndDatasource("id_7", "allow")
            ),
            new TaskInfo<>(
                "id_5",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_5"),
                "deny",
                getTaskWithIdAndDatasource("id_5", "deny")
            ),
            new TaskInfo<>(
                "id_6",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_6"),
                "allow",
                getTaskWithIdAndDatasource("id_6", "allow")
            ),
            new TaskInfo<>(
                "id_7",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_7"),
                "allow",
                getTaskWithIdAndDatasource("id_7", "allow")
            )
        )
    );

    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getKnownTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_1"),
            new MockTaskRunnerWorkItem("id_4")
        )
    ).atLeastOnce();

    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );
    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks(null, null, null, null, null, req)
        .getEntity();
    Assert.assertEquals(4, responseObjects.size());
  }

  @Test
  public void testGetTasksFilterDataSource()
  {
    expectAuthorizationTokenCheck();
    //completed tasks
    EasyMock.expect(
        taskStorageQueryAdapter.getTaskInfos(
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
                new TaskInfo<>(
                    "id_5",
                    DateTime.now(ISOChronology.getInstanceUTC()),
                    TaskStatus.success("id_5"),
                    "allow",
                    getTaskWithIdAndDatasource("id_5", "allow")
                ),
                new TaskInfo<>(
                    "id_6",
                    DateTime.now(ISOChronology.getInstanceUTC()),
                    TaskStatus.success("id_6"),
                    "allow",
                    getTaskWithIdAndDatasource("id_6", "allow")
                ),
                new TaskInfo<>(
                    "id_7",
                    DateTime.now(ISOChronology.getInstanceUTC()),
                    TaskStatus.success("id_7"),
                    "allow",
                    getTaskWithIdAndDatasource("id_7", "allow")
                ),
                new TaskInfo<>(
                    "id_1",
                    DateTime.now(ISOChronology.getInstanceUTC()),
                    TaskStatus.running("id_1"),
                    "allow",
                    getTaskWithIdAndDatasource("id_1", "allow")
                ),
                new TaskInfo<>(
                    "id_2",
                    DateTime.now(ISOChronology.getInstanceUTC()),
                    TaskStatus.success("id_2"),
                    "allow",
                    getTaskWithIdAndDatasource("id_2", "allow")
                ),
                new TaskInfo<>(
                    "id_3",
                    DateTime.now(ISOChronology.getInstanceUTC()),
                    TaskStatus.success("id_3"),
                    "allow",
                    getTaskWithIdAndDatasource("id_3", "allow")
                ),
                new TaskInfo<>(
                    "id_4",
                    DateTime.now(ISOChronology.getInstanceUTC()),
                    TaskStatus.success("id_4"),
                    "allow",
                    getTaskWithIdAndDatasource("id_4", "allow")
                )
            )
    );
    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getKnownTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_1"),
            new MockTaskRunnerWorkItem("id_4")
        )
    ).atLeastOnce();
    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks(null, "allow", null, null, null, req)
        .getEntity();
    Assert.assertEquals(7, responseObjects.size());
    Assert.assertEquals("id_5", responseObjects.get(0).getId());
    Assert.assertEquals("DataSource Check", "allow", responseObjects.get(0).getDataSource());
  }

  @Test
  public void testGetTasksFilterWaitingState()
  {
    expectAuthorizationTokenCheck();
    //active tasks
    EasyMock.expect(
        taskStorageQueryAdapter.getTaskInfos(
            ImmutableMap.of(
                TaskLookupType.ACTIVE,
                ActiveTaskLookup.getInstance()
            ),
            null
        )
    ).andStubReturn(
        ImmutableList.of(
            new TaskInfo<>(
                "id_1",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.running("id_1"),
                "allow",
                getTaskWithIdAndDatasource("id_1", "allow")
            ),
            new TaskInfo<>(
                "id_2",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.running("id_2"),
                "allow",
                getTaskWithIdAndDatasource("id_2", "allow")
            ),
            new TaskInfo<>(
                "id_3",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.running("id_3"),
                "deny",
                getTaskWithIdAndDatasource("id_3", "deny")
            ),
            new TaskInfo<>(
                "id_4",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.running("id_4"),
                "deny",
                getTaskWithIdAndDatasource("id_4", "deny")
            )
        )
    );

    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getKnownTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_1"),
            new MockTaskRunnerWorkItem("id_4")
        )
    );

    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );
    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks(
            "waiting",
            null,
            null,
            null,
            null,
            req
        ).getEntity();
    Assert.assertEquals(1, responseObjects.size());
    Assert.assertEquals("id_2", responseObjects.get(0).getId());
  }

  @Test
  public void testGetTasksFilterRunningState()
  {
    expectAuthorizationTokenCheck();
    EasyMock.expect(
        taskStorageQueryAdapter.getTaskInfos(
            ImmutableMap.of(
                TaskLookupType.ACTIVE,
                ActiveTaskLookup.getInstance()
            ),
            "allow"
        )
    ).andStubReturn(
        ImmutableList.of(
            new TaskInfo<>(
                "id_1",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.running("id_1"),
                "allow",
                getTaskWithIdAndDatasource("id_1", "allow")
            ),
            new TaskInfo<>(
                "id_2",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.running("id_2"),
                "allow",
                getTaskWithIdAndDatasource("id_2", "allow")
            ),
            new TaskInfo<>(
                "id_3",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.running("id_3"),
                "allow",
                getTaskWithIdAndDatasource("id_3", "allow")
            ),
            new TaskInfo<>(
                "id_4",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.running("id_4"),
                "deny",
                getTaskWithIdAndDatasource("id_4", "deny")
            )
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

    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    List<TaskStatusPlus> responseObjects = (List) overlordResource
        .getTasks("running", "allow", null, null, null, req)
        .getEntity();

    Assert.assertEquals(2, responseObjects.size());
    Assert.assertEquals(tasksIds.get(0), responseObjects.get(0).getId());
    Assert.assertEquals("DataSource Check", "allow", responseObjects.get(0).getDataSource());
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
        taskStorageQueryAdapter.getTaskInfos(
            ImmutableMap.of(TaskLookupType.ACTIVE, ActiveTaskLookup.getInstance()),
            null
        )
    ).andStubReturn(
        ImmutableList.of(
            new TaskInfo<>(
                "id_1",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.running("id_1"),
                "deny",
                getTaskWithIdAndDatasource("id_1", "deny")
            ),
            new TaskInfo<>(
                "id_2",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.running("id_2"),
                "allow",
                getTaskWithIdAndDatasource("id_2", "allow")
            ),
            new TaskInfo<>(
                "id_3",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.running("id_3"),
                "allow",
                getTaskWithIdAndDatasource("id_3", "allow")
            ),
            new TaskInfo<>(
                "id_4",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.running("id_4"),
                "deny",
                getTaskWithIdAndDatasource("id_4", "deny")
            )
        )
    );

    EasyMock.expect(taskRunner.getRunnerTaskState("id_1")).andStubReturn(RunnerTaskState.PENDING);
    EasyMock.expect(taskRunner.getRunnerTaskState("id_2")).andStubReturn(RunnerTaskState.PENDING);
    EasyMock.expect(taskRunner.getRunnerTaskState("id_3")).andStubReturn(RunnerTaskState.RUNNING);
    EasyMock.expect(taskRunner.getRunnerTaskState("id_4")).andStubReturn(RunnerTaskState.RUNNING);

    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks("pending", null, null, null, null, req)
        .getEntity();

    Assert.assertEquals(1, responseObjects.size());
    Assert.assertEquals(tasksIds.get(1), responseObjects.get(0).getId());
    Assert.assertEquals("DataSource Check", "allow", responseObjects.get(0).getDataSource());
  }

  @Test
  public void testGetTasksFilterCompleteState()
  {
    expectAuthorizationTokenCheck();
    EasyMock.expect(
        taskStorageQueryAdapter.getTaskInfos(
            ImmutableMap.of(TaskLookupType.COMPLETE, CompleteTaskLookup.of(null, (Duration) null)),
            null
        )
    ).andStubReturn(
        ImmutableList.of(
            new TaskInfo<>(
                "id_1",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_1"),
                "allow",
                getTaskWithIdAndDatasource("id_1", "allow")
            ),
            new TaskInfo<>(
                "id_2",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_2"),
                "deny",
                getTaskWithIdAndDatasource("id_2", "deny")
            ),
            new TaskInfo<>(
                "id_3",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_3"),
                "allow",
                getTaskWithIdAndDatasource("id_3", "allow")
            )
        )
    );
    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );
    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks("complete", null, null, null, null, req)
        .getEntity();
    Assert.assertEquals(2, responseObjects.size());
    Assert.assertEquals("id_1", responseObjects.get(0).getId());
    Assert.assertTrue("DataSource Check", "allow".equals(responseObjects.get(0).getDataSource()));
  }

  @Test
  public void testGetTasksFilterCompleteStateWithInterval()
  {
    expectAuthorizationTokenCheck();
    Duration duration = new Period("PT86400S").toStandardDuration();
    EasyMock.expect(
        taskStorageQueryAdapter.getTaskInfos(
            EasyMock.anyObject(),
            EasyMock.anyObject()
        )
    ).andStubReturn(
            ImmutableList.of(
                new TaskInfo<>(
                    "id_1",
                    DateTime.now(ISOChronology.getInstanceUTC()),
                    TaskStatus.success("id_1"),
                    "deny",
                    getTaskWithIdAndDatasource("id_1", "deny")
                ),
                new TaskInfo<>(
                    "id_2",
                    DateTime.now(ISOChronology.getInstanceUTC()),
                    TaskStatus.success("id_2"),
                    "allow",
                    getTaskWithIdAndDatasource("id_2", "allow")
                ),
                new TaskInfo<>(
                    "id_3",
                    DateTime.now(ISOChronology.getInstanceUTC()),
                    TaskStatus.success("id_3"),
                    "allow",
                    getTaskWithIdAndDatasource("id_3", "allow")
                )
            )
    );

    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );
    String interval = "2010-01-01_P1D";
    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks("complete", null, interval, null, null, req)
        .getEntity();
    Assert.assertEquals(2, responseObjects.size());
    Assert.assertEquals("id_2", responseObjects.get(0).getId());
    Assert.assertTrue("DataSource Check", "allow".equals(responseObjects.get(0).getDataSource()));
  }

  @Test
  public void testGetTasksRequiresDatasourceRead()
  {
    // Setup mocks for a user who has read access to "wikipedia"
    // and no access to "buzzfeed"
    expectAuthorizationTokenCheck(Users.WIKI_READER);

    // Setup mocks to return completed, active, known, pending and running tasks
    EasyMock.expect(
        taskStorageQueryAdapter.getTaskInfos(
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
            createTaskInfo("id_5", Datasources.WIKIPEDIA),
            createTaskInfo("id_6", Datasources.BUZZFEED),
            createTaskInfo("id_1", Datasources.WIKIPEDIA, TaskState.RUNNING, "test"),
            createTaskInfo("id_4", Datasources.BUZZFEED, TaskState.RUNNING, "test")
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

    // Replay all mocks
    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    // Verify that only the tasks of read access datasource are returned
    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks(null, null, null, null, null, req)
        .getEntity();
    Assert.assertEquals(2, responseObjects.size());
    for (TaskStatusPlus taskStatus : responseObjects) {
      Assert.assertEquals(Datasources.WIKIPEDIA, taskStatus.getDataSource());
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
        taskStorageQueryAdapter.getTaskInfos(
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
            createTaskInfo("id_5", Datasources.WIKIPEDIA),
            createTaskInfo("id_6", Datasources.BUZZFEED),
            createTaskInfo("id_1", Datasources.WIKIPEDIA, TaskState.RUNNING, "to-return"),
            createTaskInfo("id_4", Datasources.WIKIPEDIA, TaskState.RUNNING, "test")
        )
    );

    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getKnownTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_1", Datasources.WIKIPEDIA, "to-return"),
            new MockTaskRunnerWorkItem("id_4", Datasources.WIKIPEDIA, "test")
        )
    ).atLeastOnce();

    EasyMock.expect(taskRunner.getRunnerTaskState("id_1")).andReturn(RunnerTaskState.RUNNING);

    // Replay all mocks
    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    // Verify that only the tasks of read access datasource are returned
    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks(null, null, null, null, "to-return", req)
        .getEntity();
    Assert.assertEquals(1, responseObjects.size());
    for (TaskStatusPlus taskStatus : responseObjects) {
      Assert.assertEquals("to-return", taskStatus.getType());
    }
  }

  @Test
  public void testGetTasksFilterByDatasourceRequiresReadAccess()
  {
    // Setup mocks for a user who has read access to "wikipedia"
    // and no access to "buzzfeed"
    expectAuthorizationTokenCheck(Users.WIKI_READER);

    // Replay all mocks
    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    // Verify that only the tasks of read access datasource are returned
    expectedException.expect(WebApplicationException.class);
    overlordResource.getTasks(null, Datasources.BUZZFEED, null, null, null, req);
  }

  @Test
  public void testGetNullCompleteTask()
  {
    expectAuthorizationTokenCheck();
    EasyMock.expect(
        taskStorageQueryAdapter.getTaskInfos(
            ImmutableMap.of(
                TaskLookupType.COMPLETE,
                CompleteTaskLookup.of(null, null)
            ),
            null
        )
    ).andStubReturn(
        ImmutableList.of(
            new TaskInfo<>(
                "id_1",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_1"),
                "allow",
                null
            ),
            new TaskInfo<>(
                "id_2",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_2"),
                "deny",
                getTaskWithIdAndDatasource("id_2", "deny")
            ),
            new TaskInfo<>(
                "id_3",
                DateTime.now(ISOChronology.getInstanceUTC()),
                TaskStatus.success("id_3"),
                "allow",
                getTaskWithIdAndDatasource("id_3", "allow")
            )
        )
    );
    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );
    List<TaskStatusPlus> responseObjects = (List<TaskStatusPlus>) overlordResource
        .getTasks("complete", null, null, null, null, req)
        .getEntity();
    Assert.assertEquals(2, responseObjects.size());
    Assert.assertEquals("id_1", responseObjects.get(0).getId());
    TaskStatusPlus tsp = responseObjects.get(0);
    Assert.assertEquals(null, tsp.getType());
    Assert.assertTrue("DataSource Check", "allow".equals(responseObjects.get(0).getDataSource()));
  }

  @Test
  public void testGetTasksNegativeState()
  {
    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );
    Object responseObject = overlordResource
        .getTasks("blah", "ds_test", null, null, null, req)
        .getEntity();
    Assert.assertEquals(
        "Invalid state : blah, valid values are: [pending, waiting, running, complete]",
        responseObject.toString()
    );
  }

  @Test
  public void testSecuredTaskPost()
  {
    expectedException.expect(ForbiddenException.class);
    expectAuthorizationTokenCheck();

    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );
    Task task = NoopTask.create();
    overlordResource.taskPost(task, req);
  }

  @Test
  public void testTaskPostDeniesDatasourceReadUser()
  {
    expectAuthorizationTokenCheck(Users.WIKI_READER);

    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    // Verify that taskPost fails for user who has only datasource read access
    Task task = NoopTask.create(Datasources.WIKIPEDIA);
    expectedException.expect(ForbiddenException.class);
    expectedException.expect(ForbiddenException.class);
    overlordResource.taskPost(task, req);
  }

  @Test
  public void testKillPendingSegments()
  {
    expectAuthorizationTokenCheck();

    EasyMock.expect(taskMaster.isLeader()).andReturn(true);
    EasyMock
        .expect(
            indexerMetadataStorageAdapter.deletePendingSegments(
                EasyMock.eq("allow"),
                EasyMock.anyObject(Interval.class)
            )
        )
        .andReturn(2);

    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    final Map<String, Integer> response = (Map<String, Integer>) overlordResource
        .killPendingSegments("allow", new Interval(DateTimes.MIN, DateTimes.nowUtc()).toString(), req)
        .getEntity();
    Assert.assertEquals(2, response.get("numDeleted").intValue());
  }

  @Test
  public void testGetTaskPayload() throws Exception
  {
    // This is disabled since OverlordResource.getTaskStatus() is annotated with TaskResourceFilter which is supposed to
    // set authorization token properly, but isn't called in this test.
    // This should be fixed in https://github.com/apache/druid/issues/6685.
    // expectAuthorizationTokenCheck();
    final NoopTask task = NoopTask.create("mydatasource");
    EasyMock.expect(taskStorageQueryAdapter.getTask("mytask"))
            .andReturn(Optional.of(task));

    EasyMock.expect(taskStorageQueryAdapter.getTask("othertask"))
            .andReturn(Optional.absent());

    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    final Response response1 = overlordResource.getTaskPayload("mytask");
    final TaskPayloadResponse taskPayloadResponse1 = TestHelper.makeJsonMapper().readValue(
        TestHelper.makeJsonMapper().writeValueAsString(response1.getEntity()),
        TaskPayloadResponse.class
    );
    Assert.assertEquals(new TaskPayloadResponse("mytask", task), taskPayloadResponse1);

    final Response response2 = overlordResource.getTaskPayload("othertask");
    final TaskPayloadResponse taskPayloadResponse2 = TestHelper.makeJsonMapper().readValue(
        TestHelper.makeJsonMapper().writeValueAsString(response2.getEntity()),
        TaskPayloadResponse.class
    );
    Assert.assertEquals(new TaskPayloadResponse("othertask", null), taskPayloadResponse2);
  }

  @Test
  public void testGetTaskStatus() throws Exception
  {
    // This is disabled since OverlordResource.getTaskStatus() is annotated with TaskResourceFilter which is supposed to
    // set authorization token properly, but isn't called in this test.
    // This should be fixed in https://github.com/apache/druid/issues/6685.
    // expectAuthorizationTokenCheck();
    final Task task = NoopTask.create("mytask", 0);
    final TaskStatus status = TaskStatus.running("mytask");

    EasyMock.expect(taskStorageQueryAdapter.getTaskInfo("mytask"))
            .andReturn(new TaskInfo(
                task.getId(),
                DateTimes.of("2018-01-01"),
                status,
                task.getDataSource(),
                task
            ));

    EasyMock.expect(taskStorageQueryAdapter.getTaskInfo("othertask"))
            .andReturn(null);

    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getKnownTasks())
        .andReturn(ImmutableList.of());

    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    final Response response1 = overlordResource.getTaskStatus("mytask");
    final TaskStatusResponse taskStatusResponse1 = TestHelper.makeJsonMapper().readValue(
        TestHelper.makeJsonMapper().writeValueAsString(response1.getEntity()),
        TaskStatusResponse.class
    );
    TaskStatusPlus tsp = taskStatusResponse1.getStatus();
    Assert.assertEquals(tsp.getStatusCode(), tsp.getStatus());
    Assert.assertEquals(
        new TaskStatusResponse(
            "mytask",
            new TaskStatusPlus(
                "mytask",
                "mytask",
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
    Assert.assertEquals(new TaskStatusResponse("othertask", null), taskStatusResponse2);
  }

  @Test
  public void testGetLockedIntervals() throws Exception
  {
    final Map<String, Integer> minTaskPriority = Collections.singletonMap("ds1", 0);
    final Map<String, List<Interval>> expectedLockedIntervals = Collections.singletonMap(
        "ds1",
        Arrays.asList(
            Intervals.of("2012-01-01/2012-01-02"),
            Intervals.of("2012-01-02/2012-01-03")
        )
    );

    EasyMock.expect(taskStorageQueryAdapter.getLockedIntervals(minTaskPriority))
            .andReturn(expectedLockedIntervals);
    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    final Response response = overlordResource.getDatasourceLockedIntervals(minTaskPriority);
    Assert.assertEquals(200, response.getStatus());

    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    Map<String, List<Interval>> observedLockedIntervals = jsonMapper.readValue(
        jsonMapper.writeValueAsString(response.getEntity()),
        new TypeReference<Map<String, List<Interval>>>()
        {
        }
    );

    Assert.assertEquals(expectedLockedIntervals, observedLockedIntervals);
  }

  @Test
  public void testGetLockedIntervalsWithEmptyBody()
  {
    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    Response response = overlordResource.getDatasourceLockedIntervals(null);
    Assert.assertEquals(400, response.getStatus());

    response = overlordResource.getDatasourceLockedIntervals(Collections.emptyMap());
    Assert.assertEquals(400, response.getStatus());
  }

  @Test
  public void testShutdownTask()
  {
    // This is disabled since OverlordResource.doShutdown is annotated with TaskResourceFilter
    // This should be fixed in https://github.com/apache/druid/issues/6685.
    // expectAuthorizationTokenCheck();
    TaskQueue mockQueue = EasyMock.createMock(TaskQueue.class);
    EasyMock.expect(taskMaster.isLeader()).andReturn(true).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(
        Optional.of(taskRunner)
    ).anyTimes();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(
        Optional.of(mockQueue)
    ).anyTimes();
    mockQueue.shutdown("id_1", "Shutdown request from user");
    EasyMock.expectLastCall();

    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        mockQueue,
        workerTaskRunnerQueryAdapter
    );

    final Map<String, Integer> response = (Map<String, Integer>) overlordResource
        .doShutdown("id_1")
        .getEntity();
    Assert.assertEquals("id_1", response.get("task"));
  }

  @Test
  public void testShutdownAllTasks()
  {
    // This is disabled since OverlordResource.shutdownTasksForDataSource is annotated with DatasourceResourceFilter
    // This should be fixed in https://github.com/apache/druid/issues/6685.
    // expectAuthorizationTokenCheck();
    TaskQueue mockQueue = EasyMock.createMock(TaskQueue.class);
    EasyMock.expect(taskMaster.isLeader()).andReturn(true).anyTimes();
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(
        Optional.of(taskRunner)
    ).anyTimes();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(
        Optional.of(mockQueue)
    ).anyTimes();
    EasyMock.expect(taskStorageQueryAdapter.getActiveTaskInfo("datasource")).andStubReturn(ImmutableList.of(
        new TaskInfo(
            "id_1",
            DateTime.now(ISOChronology.getInstanceUTC()),
            TaskStatus.success("id_1"),
            "datasource",
            getTaskWithIdAndDatasource("id_1", "datasource")
        ),
        new TaskInfo(
            "id_2",
            DateTime.now(ISOChronology.getInstanceUTC()),
            TaskStatus.success("id_2"),
            "datasource",
            getTaskWithIdAndDatasource("id_2", "datasource")
        )
    ));
    mockQueue.shutdown("id_1", "Shutdown request from user");
    EasyMock.expectLastCall();
    mockQueue.shutdown("id_2", "Shutdown request from user");
    EasyMock.expectLastCall();

    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        mockQueue,
        workerTaskRunnerQueryAdapter
    );

    final Map<String, Integer> response = (Map<String, Integer>) overlordResource
        .shutdownTasksForDataSource("datasource")
        .getEntity();
    Assert.assertEquals("datasource", response.get("dataSource"));
  }

  @Test
  public void testShutdownAllTasksForNonExistingDataSource()
  {
    final TaskQueue taskQueue = EasyMock.createMock(TaskQueue.class);
    EasyMock.expect(taskMaster.isLeader()).andReturn(true).anyTimes();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskStorageQueryAdapter.getActiveTaskInfo(EasyMock.anyString())).andReturn(Collections.emptyList());
    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    final Response response = overlordResource.shutdownTasksForDataSource("notExisting");
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void testEnableWorker()
  {
    final String host = "worker-host";

    workerTaskRunnerQueryAdapter.enableWorker(host);
    EasyMock.expectLastCall().once();

    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    final Response response = overlordResource.enableWorker(host);

    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatus());
    Assert.assertEquals(ImmutableMap.of(host, "enabled"), response.getEntity());
  }

  @Test
  public void testDisableWorker()
  {
    final String host = "worker-host";

    workerTaskRunnerQueryAdapter.disableWorker(host);
    EasyMock.expectLastCall().once();

    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    final Response response = overlordResource.disableWorker(host);

    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatus());
    Assert.assertEquals(ImmutableMap.of(host, "disabled"), response.getEntity());
  }

  @Test
  public void testEnableWorkerWhenWorkerAPIRaisesError()
  {
    final String host = "worker-host";

    workerTaskRunnerQueryAdapter.enableWorker(host);
    EasyMock.expectLastCall().andThrow(new RE("Worker API returns error!")).once();

    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    final Response response = overlordResource.enableWorker(host);

    Assert.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", "Worker API returns error!"), response.getEntity());
  }

  @Test
  public void testDisableWorkerWhenWorkerAPIRaisesError()
  {
    final String host = "worker-host";

    workerTaskRunnerQueryAdapter.disableWorker(host);
    EasyMock.expectLastCall().andThrow(new RE("Worker API returns error!")).once();

    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter
    );

    final Response response = overlordResource.disableWorker(host);

    Assert.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", "Worker API returns error!"), response.getEntity());
  }

  @Test
  public void testGetTotalWorkerCapacityNotLeader()
  {
    EasyMock.reset(taskMaster);
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(
        Optional.absent()
    ).anyTimes();
    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter,
        configManager
    );
    final Response response = overlordResource.getTotalWorkerCapacity();
    Assert.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE.code(), response.getStatus());
  }

  @Test
  public void testGetTotalWorkerCapacityWithUnknown()
  {
    WorkerBehaviorConfig workerBehaviorConfig = EasyMock.createMock(WorkerBehaviorConfig.class);
    AtomicReference<WorkerBehaviorConfig> workerBehaviorConfigAtomicReference = new AtomicReference<>(workerBehaviorConfig);
    EasyMock.expect(configManager.watch(WorkerBehaviorConfig.CONFIG_KEY, WorkerBehaviorConfig.class)).andReturn(workerBehaviorConfigAtomicReference);
    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter,
        configManager
    );
    final Response response = overlordResource.getTotalWorkerCapacity();
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatus());
    Assert.assertEquals(-1, ((TotalWorkerCapacityResponse) response.getEntity()).getCurrentClusterCapacity());
    Assert.assertEquals(-1, ((TotalWorkerCapacityResponse) response.getEntity()).getMaximumCapacityWithAutoScale());
  }

  @Test
  public void testGetTotalWorkerCapacityWithWorkerTaskRunnerButWorkerBehaviorConfigNotConfigured()
  {
    AtomicReference<WorkerBehaviorConfig> workerBehaviorConfigAtomicReference = new AtomicReference<>(null);
    EasyMock.expect(configManager.watch(WorkerBehaviorConfig.CONFIG_KEY, WorkerBehaviorConfig.class)).andReturn(workerBehaviorConfigAtomicReference);
    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter,
        configManager
    );
    final Response response = overlordResource.getTotalWorkerCapacity();
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatus());
    Assert.assertEquals(-1, ((TotalWorkerCapacityResponse) response.getEntity()).getCurrentClusterCapacity());
    Assert.assertEquals(-1, ((TotalWorkerCapacityResponse) response.getEntity()).getMaximumCapacityWithAutoScale());
  }

  @Test
  public void testGetTotalWorkerCapacityWithWorkerTaskRunnerButAutoScaleNotConfigured()
  {
    DefaultWorkerBehaviorConfig workerBehaviorConfig = new DefaultWorkerBehaviorConfig(null, null);
    AtomicReference<WorkerBehaviorConfig> workerBehaviorConfigAtomicReference = new AtomicReference<>(workerBehaviorConfig);
    EasyMock.expect(configManager.watch(WorkerBehaviorConfig.CONFIG_KEY, WorkerBehaviorConfig.class)).andReturn(workerBehaviorConfigAtomicReference);
    EasyMock.replay(
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter,
        configManager
    );
    final Response response = overlordResource.getTotalWorkerCapacity();
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatus());
    Assert.assertEquals(-1, ((TotalWorkerCapacityResponse) response.getEntity()).getCurrentClusterCapacity());
    Assert.assertEquals(-1, ((TotalWorkerCapacityResponse) response.getEntity()).getMaximumCapacityWithAutoScale());
  }

  @Test
  public void testGetTotalWorkerCapacityWithAutoScaleConfiguredAndProvisioningStrategySupportExpectedWorkerCapacity()
  {
    int expectedWorkerCapacity = 3;
    int maxNumWorkers = 2;
    WorkerTaskRunner workerTaskRunner = EasyMock.createMock(WorkerTaskRunner.class);
    Collection<ImmutableWorkerInfo> workerInfos = ImmutableList.of(
        new ImmutableWorkerInfo(
            new Worker(
                "http", "testWorker", "192.0.0.1", expectedWorkerCapacity, "v1", WorkerConfig.DEFAULT_CATEGORY
            ),
            2,
            ImmutableSet.of("grp1", "grp2"),
            ImmutableSet.of("task1", "task2"),
            DateTimes.of("2015-01-01T01:01:01Z")
        )
    );
    EasyMock.expect(workerTaskRunner.getWorkers()).andReturn(workerInfos);
    EasyMock.reset(taskMaster);
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(
        Optional.of(workerTaskRunner)
    ).anyTimes();
    EasyMock.expect(provisioningStrategy.getExpectedWorkerCapacity(workerInfos)).andReturn(expectedWorkerCapacity).anyTimes();
    AutoScaler autoScaler = EasyMock.createMock(AutoScaler.class);
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(maxNumWorkers);
    DefaultWorkerBehaviorConfig workerBehaviorConfig = new DefaultWorkerBehaviorConfig(null, autoScaler);
    AtomicReference<WorkerBehaviorConfig> workerBehaviorConfigAtomicReference = new AtomicReference<>(workerBehaviorConfig);
    EasyMock.expect(configManager.watch(WorkerBehaviorConfig.CONFIG_KEY, WorkerBehaviorConfig.class)).andReturn(workerBehaviorConfigAtomicReference);
    EasyMock.replay(
        workerTaskRunner,
        autoScaler,
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter,
        configManager,
        provisioningStrategy
    );
    final Response response = overlordResource.getTotalWorkerCapacity();
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatus());
    Assert.assertEquals(expectedWorkerCapacity, ((TotalWorkerCapacityResponse) response.getEntity()).getCurrentClusterCapacity());
    Assert.assertEquals(expectedWorkerCapacity * maxNumWorkers, ((TotalWorkerCapacityResponse) response.getEntity()).getMaximumCapacityWithAutoScale());
  }

  @Test
  public void testGetTotalWorkerCapacityWithAutoScaleConfiguredAndProvisioningStrategyNotSupportExpectedWorkerCapacity()
  {
    int invalidExpectedCapacity = -1;
    int maxNumWorkers = 2;
    WorkerTaskRunner workerTaskRunner = EasyMock.createMock(WorkerTaskRunner.class);
    Collection<ImmutableWorkerInfo> workerInfos = ImmutableList.of(
        new ImmutableWorkerInfo(
            new Worker(
                "http", "testWorker", "192.0.0.1", 3, "v1", WorkerConfig.DEFAULT_CATEGORY
            ),
            2,
            ImmutableSet.of("grp1", "grp2"),
            ImmutableSet.of("task1", "task2"),
            DateTimes.of("2015-01-01T01:01:01Z")
        )
    );
    EasyMock.expect(workerTaskRunner.getWorkers()).andReturn(workerInfos);
    EasyMock.reset(taskMaster);
    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(
        Optional.of(workerTaskRunner)
    ).anyTimes();
    EasyMock.expect(provisioningStrategy.getExpectedWorkerCapacity(workerInfos)).andReturn(invalidExpectedCapacity).anyTimes();
    AutoScaler autoScaler = EasyMock.createMock(AutoScaler.class);
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(0);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(maxNumWorkers);
    DefaultWorkerBehaviorConfig workerBehaviorConfig = new DefaultWorkerBehaviorConfig(null, autoScaler);
    AtomicReference<WorkerBehaviorConfig> workerBehaviorConfigAtomicReference = new AtomicReference<>(workerBehaviorConfig);
    EasyMock.expect(configManager.watch(WorkerBehaviorConfig.CONFIG_KEY, WorkerBehaviorConfig.class)).andReturn(workerBehaviorConfigAtomicReference);
    EasyMock.replay(
        workerTaskRunner,
        autoScaler,
        taskRunner,
        taskMaster,
        taskStorageQueryAdapter,
        indexerMetadataStorageAdapter,
        req,
        workerTaskRunnerQueryAdapter,
        configManager,
        provisioningStrategy
    );
    final Response response = overlordResource.getTotalWorkerCapacity();
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatus());
    Assert.assertEquals(workerInfos.stream().findFirst().get().getWorker().getCapacity(), ((TotalWorkerCapacityResponse) response.getEntity()).getCurrentClusterCapacity());
    Assert.assertEquals(invalidExpectedCapacity, ((TotalWorkerCapacityResponse) response.getEntity()).getMaximumCapacityWithAutoScale());
  }

  private void expectAuthorizationTokenCheck()
  {
    expectAuthorizationTokenCheck(Users.DRUID);
  }

  private void expectAuthorizationTokenCheck(String username)
  {
    AuthenticationResult authenticationResult = new AuthenticationResult(username, "druid", null, null);
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(authenticationResult)
            .atLeastOnce();

    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, false);
    EasyMock.expectLastCall().anyTimes();

    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
  }

  private Task getTaskWithIdAndDatasource(String id, String datasource)
  {
    return getTaskWithIdAndDatasource(id, datasource, "test");
  }

  private Task getTaskWithIdAndDatasource(String id, String datasource, String taskType)
  {
    return new AbstractTask(id, datasource, null)
    {
      @Override
      public String getType()
      {
        return taskType;
      }

      @Override
      public boolean isReady(TaskActionClient taskActionClient)
      {
        return false;
      }

      @Override
      public void stopGracefully(TaskConfig taskConfig)
      {
      }

      @Override
      public TaskStatus run(TaskToolbox toolbox)
      {
        return null;
      }
    };
  }

  private TaskInfo<Task, TaskStatus> createTaskInfo(
      String taskId,
      String datasource
  )
  {
    return createTaskInfo(taskId, datasource, TaskState.SUCCESS, "test");
  }

  private TaskInfo<Task, TaskStatus> createTaskInfo(
      String taskId,
      String datasource,
      TaskState state,
      String taskType
  )
  {
    return new TaskInfo<>(
        taskId,
        DateTime.now(ISOChronology.getInstanceUTC()),
        TaskStatus.fromCode(taskId, state),
        datasource,
        getTaskWithIdAndDatasource(taskId, datasource, taskType)
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
