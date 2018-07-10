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
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.indexing.common.TaskLocation;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.AbstractTask;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TaskRunnerWorkItem;
import io.druid.indexing.overlord.TaskStorageQueryAdapter;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizationInfo;
import io.druid.server.security.Resource;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.List;

public class OverlordResourceTest
{
  private OverlordResource overlordResource;
  private TaskMaster taskMaster;
  private TaskStorageQueryAdapter tsqa;
  private HttpServletRequest req;
  private TaskRunner taskRunner;

  @Before
  public void setUp() throws Exception
  {
    taskRunner = EasyMock.createMock(TaskRunner.class);
    taskMaster = EasyMock.createStrictMock(TaskMaster.class);
    tsqa = EasyMock.createStrictMock(TaskStorageQueryAdapter.class);
    req = EasyMock.createStrictMock(HttpServletRequest.class);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(
        Optional.of(taskRunner)
    ).anyTimes();

    overlordResource = new OverlordResource(
        taskMaster,
        tsqa,
        null,
        null,
        null,
        new AuthConfig(true)
    );

    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTH_TOKEN)).andReturn(
        new AuthorizationInfo()
        {
          @Override
          public Access isAuthorized(
              Resource resource, Action action
          )
          {
            if (resource.getName().equals("allow")) {
              return new Access(true);
            } else {
              return new Access(false);
            }
          }
        }
    );
  }

  @Test
  public void testSecuredGetWaitingTask() throws Exception
  {
    EasyMock.expect(tsqa.getActiveTasks()).andReturn(
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

    EasyMock.replay(taskRunner, taskMaster, tsqa, req);

    List<OverlordResource.TaskResponseObject> responseObjects = (List) overlordResource.getWaitingTasks(req)
                                                                                       .getEntity();
    Assert.assertEquals(1, responseObjects.size());
    Assert.assertEquals("id_2", responseObjects.get(0).toJson().get("id"));
  }

  @Test
  public void testSecuredGetCompleteTasks()
  {
    List<String> tasksIds = ImmutableList.of("id_1", "id_2", "id_3");
    EasyMock.expect(tsqa.getRecentlyFinishedTaskStatuses()).andReturn(
        Lists.transform(
            tasksIds,
            new Function<String, TaskStatus>()
            {
              @Override
              public TaskStatus apply(String input)
              {
                return TaskStatus.success(input);
              }
            }
        )
    ).once();

    EasyMock.expect(tsqa.getTask(tasksIds.get(0))).andReturn(
        Optional.of(getTaskWithIdAndDatasource(tasksIds.get(0), "deny"))
    ).once();
    EasyMock.expect(tsqa.getTask(tasksIds.get(1))).andReturn(
        Optional.of(getTaskWithIdAndDatasource(tasksIds.get(1), "allow"))
    ).once();
    EasyMock.expect(tsqa.getTask(tasksIds.get(2))).andReturn(
        Optional.of(getTaskWithIdAndDatasource(tasksIds.get(2), "allow"))
    ).once();
    EasyMock.replay(taskRunner, taskMaster, tsqa, req);

    List<OverlordResource.TaskResponseObject> responseObjects = (List) overlordResource.getCompleteTasks(req)
                                                                                       .getEntity();

    Assert.assertEquals(2, responseObjects.size());
    Assert.assertEquals(tasksIds.get(1), responseObjects.get(0).toJson().get("id"));
    Assert.assertEquals(tasksIds.get(2), responseObjects.get(1).toJson().get("id"));
  }

  @Test
  public void testSecuredGetRunningTasks()
  {
    List<String> tasksIds = ImmutableList.of("id_1", "id_2");
    EasyMock.<Collection<? extends TaskRunnerWorkItem>>expect(taskRunner.getRunningTasks()).andReturn(
        ImmutableList.of(
            new MockTaskRunnerWorkItem(tasksIds.get(0), null),
            new MockTaskRunnerWorkItem(tasksIds.get(1), null)
        )
    );
    EasyMock.expect(tsqa.getTask(tasksIds.get(0))).andReturn(
        Optional.of(getTaskWithIdAndDatasource(tasksIds.get(0), "deny"))
    ).once();
    EasyMock.expect(tsqa.getTask(tasksIds.get(1))).andReturn(
        Optional.of(getTaskWithIdAndDatasource(tasksIds.get(1), "allow"))
    ).once();

    EasyMock.replay(taskRunner, taskMaster, tsqa, req);

    List<OverlordResource.TaskResponseObject> responseObjects = (List) overlordResource.getRunningTasks(req)
                                                                                       .getEntity();

    Assert.assertEquals(1, responseObjects.size());
    Assert.assertEquals(tasksIds.get(1), responseObjects.get(0).toJson().get("id"));
  }

  @Test
  public void testSecuredTaskPost()
  {
    EasyMock.replay(taskRunner, taskMaster, tsqa, req);
    Task task = NoopTask.create();
    Response response = overlordResource.taskPost(task, req);
    Assert.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(taskRunner, taskMaster, tsqa, req);
  }

  private Task getTaskWithIdAndDatasource(String id, String datasource)
  {
    return new AbstractTask(id, datasource, null)
    {
      @Override
      public String getType()
      {
        return null;
      }

      @Override
      public boolean isReady(TaskActionClient taskActionClient) throws Exception
      {
        return false;
      }

      @Override
      public TaskStatus run(TaskToolbox toolbox) throws Exception
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
      return null;
    }
  }

}
