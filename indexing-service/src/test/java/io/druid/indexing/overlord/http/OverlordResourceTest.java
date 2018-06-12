/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.overlord.http;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.indexer.TaskLocation;
import io.druid.indexer.TaskStatusPlus;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.AbstractTask;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.IndexerMetadataStorageAdapter;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TaskRunnerWorkItem;
import io.druid.indexing.overlord.TaskStorageQueryAdapter;
import io.druid.java.util.common.DateTimes;
import io.druid.segment.TestHelper;
import io.druid.java.util.common.Pair;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.Authorizer;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.ForbiddenException;
import io.druid.server.security.Resource;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class OverlordResourceTest
{
  private OverlordResource overlordResource;
  private TaskMaster taskMaster;
  private TaskStorageQueryAdapter taskStorageQueryAdapter;
  private IndexerMetadataStorageAdapter indexerMetadataStorageAdapter;
  private HttpServletRequest req;
  private TaskRunner taskRunner;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp()
  {
    taskRunner = EasyMock.createMock(TaskRunner.class);
    taskMaster = EasyMock.createStrictMock(TaskMaster.class);
    taskStorageQueryAdapter = EasyMock.createStrictMock(TaskStorageQueryAdapter.class);
    indexerMetadataStorageAdapter = EasyMock.createStrictMock(IndexerMetadataStorageAdapter.class);
    req = EasyMock.createStrictMock(HttpServletRequest.class);

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
            if (resource.getName().equals("allow")) {
              return new Access(true);
            } else {
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
        null,
        null,
        authMapper
    );
  }

  public void expectAuthorizationTokenCheck()
  {
    AuthenticationResult authenticationResult = new AuthenticationResult("druid", "druid", null, null);
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(authenticationResult)
            .anyTimes();

    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, false);
    EasyMock.expectLastCall().anyTimes();

    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
  }

  @Test
  public void testLeader()
  {
    EasyMock.expect(taskMaster.getCurrentLeader()).andReturn("boz").once();
    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);

    final Response response = overlordResource.getLeader();
    Assert.assertEquals("boz", response.getEntity());
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testIsLeader()
  {
    EasyMock.expect(taskMaster.isLeader()).andReturn(true).once();
    EasyMock.expect(taskMaster.isLeader()).andReturn(false).once();
    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);

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

    EasyMock.expect(taskStorageQueryAdapter.getActiveTasks()).andReturn(
        ImmutableList.of(
            getTaskWithIdAndDatasource("id_1", "allow"),
            getTaskWithIdAndDatasource("id_2", "allow"),
            getTaskWithIdAndDatasource("id_3", "deny"),
            getTaskWithIdAndDatasource("id_4", "deny")
        )
    ).once();

    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getKnownTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem("id_1", null),
            new MockTaskRunnerWorkItem("id_4", null)
        )
    );

    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);

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
    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getRunningTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem(tasksIds.get(0), null),
            new MockTaskRunnerWorkItem(tasksIds.get(1), null),
            new MockTaskRunnerWorkItem(tasksIds.get(2), null)));

    EasyMock.expect(taskStorageQueryAdapter.getTask(tasksIds.get(0))).andStubReturn(
        Optional.of(getTaskWithIdAndDatasource(tasksIds.get(0), "deny")));
    EasyMock.expect(taskStorageQueryAdapter.getTask(tasksIds.get(1))).andStubReturn(
        Optional.of(getTaskWithIdAndDatasource(tasksIds.get(1), "allow")));
    EasyMock.expect(taskStorageQueryAdapter.getTask(tasksIds.get(2))).andStubReturn(
        Optional.of(getTaskWithIdAndDatasource(tasksIds.get(2), "allow")));
    EasyMock.expect(taskStorageQueryAdapter.getCreatedTime(tasksIds.get(0)))
        .andStubReturn(DateTimes.EPOCH);
    EasyMock.expect(taskStorageQueryAdapter.getCreatedTime(tasksIds.get(1)))
        .andStubReturn(DateTimes.EPOCH);
    EasyMock.expect(taskStorageQueryAdapter.getCreatedTime(tasksIds.get(2)))
      .andStubReturn(DateTimes.EPOCH);
    EasyMock.expect(taskStorageQueryAdapter.getCreatedDateAndDataSource(tasksIds.get(0)))
        .andStubReturn(new Pair<>(DateTime.now(ISOChronology.getInstanceUTC()), "ds_test"));
    EasyMock.expect(taskStorageQueryAdapter.getCreatedDateAndDataSource(tasksIds.get(1)))
        .andStubReturn(new Pair<>(DateTime.now(ISOChronology.getInstanceUTC()), "ds_test"));
    EasyMock.expect(taskStorageQueryAdapter.getCreatedDateAndDataSource(tasksIds.get(2)))
      .andStubReturn(new Pair<>(DateTime.now(ISOChronology.getInstanceUTC()), "ds_test"));

    EasyMock.expect(taskStorageQueryAdapter.getRecentlyFinishedTaskStatuses(null)).andStubReturn(
        Lists.transform(
            tasksIds,
            new Function<String, TaskStatus>()
            {
              @Override
              public TaskStatus apply(String input)
              {
                return TaskStatus.success(input);
              }
            }));

    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);
    Assert.assertTrue(taskStorageQueryAdapter.getRecentlyFinishedTaskStatuses(null).size() == 3);
    Assert.assertTrue(taskRunner.getRunningTasks().size() == 3);
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
            new MockTaskRunnerWorkItem(tasksIds.get(0), null),
            new MockTaskRunnerWorkItem(tasksIds.get(1), null)
        )
    );
    EasyMock.expect(taskStorageQueryAdapter.getTask(tasksIds.get(0))).andReturn(
        Optional.of(getTaskWithIdAndDatasource(tasksIds.get(0), "deny"))
    ).once();
    EasyMock.expect(taskStorageQueryAdapter.getTask(tasksIds.get(1))).andReturn(
        Optional.of(getTaskWithIdAndDatasource(tasksIds.get(1), "allow"))
    ).once();

    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);

    List<TaskStatusPlus> responseObjects = (List) overlordResource.getRunningTasks(null, req)
                                                                  .getEntity();

    Assert.assertEquals(1, responseObjects.size());
    Assert.assertEquals(tasksIds.get(1), responseObjects.get(0).getId());
  }

  @Test
  public void testSecuredTaskPost()
  {
    expectedException.expect(ForbiddenException.class);
    expectAuthorizationTokenCheck();

    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);
    Task task = NoopTask.create();
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

    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);

    final Map<String, Integer> response = (Map<String, Integer>) overlordResource
        .killPendingSegments("allow", new Interval(DateTimes.MIN, DateTimes.nowUtc()).toString(), req)
        .getEntity();
    Assert.assertEquals(2, response.get("numDeleted").intValue());
  }

  @Test
  public void testGetTaskPayload() throws Exception
  {
    expectAuthorizationTokenCheck();
    final NoopTask task = NoopTask.create("mydatasource");
    EasyMock.expect(taskStorageQueryAdapter.getTask("mytask"))
            .andReturn(Optional.of(task));

    EasyMock.expect(taskStorageQueryAdapter.getTask("othertask"))
            .andReturn(Optional.absent());

    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);

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
    expectAuthorizationTokenCheck();
    EasyMock.expect(taskStorageQueryAdapter.getStatus("mytask"))
            .andReturn(Optional.of(TaskStatus.success("mytask")));

    EasyMock.expect(taskStorageQueryAdapter.getStatus("othertask"))
            .andReturn(Optional.absent());

    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);

    final Response response1 = overlordResource.getTaskStatus("mytask");
    final TaskStatusResponse taskStatusResponse1 = TestHelper.makeJsonMapper().readValue(
        TestHelper.makeJsonMapper().writeValueAsString(response1.getEntity()),
        TaskStatusResponse.class
    );
    Assert.assertEquals(new TaskStatusResponse("mytask", TaskStatus.success("mytask")), taskStatusResponse1);

    final Response response2 = overlordResource.getTaskStatus("othertask");
    final TaskStatusResponse taskStatusResponse2 = TestHelper.makeJsonMapper().readValue(
        TestHelper.makeJsonMapper().writeValueAsString(response2.getEntity()),
        TaskStatusResponse.class
    );
    Assert.assertEquals(new TaskStatusResponse("othertask", null), taskStatusResponse2);
  }

  @Test
  public void testGetRunningTasksByDataSource()
  {

    List<String> tasksIds = ImmutableList.of("id_1", "id_2");
    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getRunningTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem(tasksIds.get(0), null),
            new MockTaskRunnerWorkItem(tasksIds.get(1), null)));
    EasyMock.expect(taskStorageQueryAdapter.getTask(tasksIds.get(0))).andReturn(
        Optional.of(getTaskWithIdAndDatasource(tasksIds.get(0), "deny"))).once();
    EasyMock.expect(taskStorageQueryAdapter.getTask(tasksIds.get(1))).andReturn(
        Optional.of(getTaskWithIdAndDatasource(tasksIds.get(1), "allow"))).once();

    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);
    List<TaskRunnerWorkItem> responseObjects = (List) overlordResource.getRunningTasksByDataSource("ds_test", req)
        .getEntity();

    Assert.assertEquals(2, responseObjects.size());
    Assert.assertEquals(taskStorageQueryAdapter.getTask("id_1").get().getId(), responseObjects.get(0).getTaskId());
    Assert.assertEquals(taskStorageQueryAdapter.getTask("id_2").get().getId(), responseObjects.get(1).getTaskId());
    Assert.assertTrue("DataSource Check", "ds_test".equals(responseObjects.get(0).getDataSource()));
  }

  @Test
  public void testGetRunningTasksByDataSourceNeg()
  {
    expectAuthorizationTokenCheck();

    List<String> tasksIds = ImmutableList.of("id_1", "id_2");
    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getRunningTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem(tasksIds.get(0), null),
            new MockTaskRunnerWorkItem(tasksIds.get(1), null)));
    EasyMock.expect(taskStorageQueryAdapter.getTask(tasksIds.get(0))).andReturn(
        Optional.of(getTaskWithIdAndDatasource(tasksIds.get(0), "deny"))).once();
    EasyMock.expect(taskStorageQueryAdapter.getTask(tasksIds.get(1))).andReturn(
        Optional.of(getTaskWithIdAndDatasource(tasksIds.get(1), "allow"))).once();

    EasyMock.replay(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);
    Assert.assertTrue(taskStorageQueryAdapter.getTask("id_1").isPresent());
    Assert.assertTrue(taskStorageQueryAdapter.getTask("id_2").isPresent());
    List<TaskRunnerWorkItem> responseObjects = (List) overlordResource.getRunningTasksByDataSource("ds_NA", req)
        .getEntity();

    Assert.assertEquals(0, responseObjects.size());
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(taskRunner, taskMaster, taskStorageQueryAdapter, indexerMetadataStorageAdapter, req);
  }

  private Task getTaskWithIdAndDatasource(String id, String datasource)
  {
    return new AbstractTask(id, datasource, null)
    {
      @Override
      public String getType()
      {
        return "test";
      }

      @Override
      public boolean isReady(TaskActionClient taskActionClient)
      {
        return false;
      }

      @Override
      public TaskStatus run(TaskToolbox toolbox)
      {
        return null;
      }
    };
  }

  private static class MockTaskRunnerWorkItem extends TaskRunnerWorkItem
  {
    public MockTaskRunnerWorkItem(
        String taskId,
        ListenableFuture<TaskStatus> result
    )
    {
      super(taskId, result);
    }

    @Override
    public TaskLocation getLocation()
    {
      return TaskLocation.unknown();
    }

    @Override
    public String getTaskType()
    {
      return "test";
    }

    @Override
    public String getDataSource()
    {
      return "ds_test";
    }
  }

}
