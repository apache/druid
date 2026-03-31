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
import org.apache.druid.audit.AuditManager;
import org.apache.druid.auth.TaskAuthContext;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.overlord.DruidOverlord;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageAdapter;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueryTool;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.WorkerTaskRunnerQueryAdapter;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.util.Map;

public class OverlordResourceAuthContextTest
{
  private OverlordResource overlordResource;
  private TaskMaster taskMaster;
  private TaskQueue taskQueue;
  private TaskRunner taskRunner;
  private HttpServletRequest req;
  private AuthConfig authConfig;
  private DruidOverlord overlord;
  private JacksonConfigManager configManager;
  private AuditManager auditManager;
  private WorkerTaskRunnerQueryAdapter workerTaskRunnerQueryAdapter;

  @Before
  public void setUp()
  {
    taskRunner = EasyMock.createMock(TaskRunner.class);
    taskQueue = EasyMock.createStrictMock(TaskQueue.class);
    taskMaster = EasyMock.createStrictMock(TaskMaster.class);
    overlord = EasyMock.createStrictMock(DruidOverlord.class);
    configManager = EasyMock.createMock(JacksonConfigManager.class);
    auditManager = EasyMock.createMock(AuditManager.class);
    authConfig = EasyMock.createMock(AuthConfig.class);
    req = EasyMock.createStrictMock(HttpServletRequest.class);
    workerTaskRunnerQueryAdapter = EasyMock.createStrictMock(WorkerTaskRunnerQueryAdapter.class);

    EasyMock.expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();

    AuthorizerMapper authMapper = new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return (AuthenticationResult authenticationResult, Resource resource, Action action) ->
            Access.allow();
      }
    };

    overlordResource = new OverlordResource(
        overlord,
        taskMaster,
        new TaskQueryTool(
            EasyMock.createMock(org.apache.druid.indexing.overlord.TaskStorage.class),
            EasyMock.createMock(org.apache.druid.indexing.overlord.GlobalTaskLockbox.class),
            taskMaster,
            () -> null
        ),
        EasyMock.createMock(IndexerMetadataStorageAdapter.class),
        null,
        configManager,
        auditManager,
        authMapper,
        workerTaskRunnerQueryAdapter,
        authConfig
    );
  }

  @After
  public void tearDown()
  {
  }

  @Test
  public void testTaskPostWithAuthContextProvider()
  {
    final TestTaskAuthContext expectedAuthContext = new TestTaskAuthContext(
        "testUser",
        Map.of("token", "secret-token")
    );
    overlordResource.setTaskAuthContextProvider(
        (authResult, task) -> expectedAuthContext
    );

    AuthenticationResult authResult = expectAuthorizationTokenCheck("testUser");
    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(false);
    // Auth context injection calls authenticationResultFromRequest again
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(authResult).once();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).once();

    replayAll();

    NoopTask task = NoopTask.forDatasource("allow");
    overlordResource.taskPost(task, req);

    Assert.assertNotNull(task.getTaskAuthContext());
    Assert.assertEquals("testUser", task.getTaskAuthContext().getIdentity());
    Assert.assertEquals("secret-token", task.getTaskAuthContext().getCredentials().get("token"));
  }

  @Test
  public void testTaskPostWithoutAuthContextProvider()
  {
    expectAuthorizationTokenCheck("testUser");
    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(false);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).once();

    replayAll();

    NoopTask task = NoopTask.forDatasource("allow");
    overlordResource.taskPost(task, req);

    Assert.assertNull(task.getTaskAuthContext());
  }

  @Test
  public void testTaskPostWithProviderReturningNull()
  {
    overlordResource.setTaskAuthContextProvider((ar, t) -> null);

    AuthenticationResult authResult = expectAuthorizationTokenCheck("testUser");
    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(false);
    // Auth context injection calls authenticationResultFromRequest again
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(authResult).once();
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    EasyMock.expect(taskQueue.add(EasyMock.anyObject())).andReturn(true).once();

    replayAll();

    NoopTask task = NoopTask.forDatasource("allow");
    overlordResource.taskPost(task, req);

    Assert.assertNull(task.getTaskAuthContext());
  }

  private AuthenticationResult expectAuthorizationTokenCheck(String username)
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
    return authenticationResult;
  }

  private void replayAll()
  {
    EasyMock.replay(
        taskRunner,
        taskQueue,
        taskMaster,
        overlord,
        req,
        authConfig,
        configManager,
        auditManager,
        workerTaskRunnerQueryAdapter
    );
  }

  private static class TestTaskAuthContext implements TaskAuthContext
  {
    private final String identity;
    private final Map<String, String> credentials;

    TestTaskAuthContext(String identity, Map<String, String> credentials)
    {
      this.identity = identity;
      this.credentials = credentials;
    }

    @Override
    public String getIdentity()
    {
      return identity;
    }

    @Override
    @Nullable
    public Map<String, String> getCredentials()
    {
      return credentials;
    }
  }
}
