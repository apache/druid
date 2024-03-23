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

package org.apache.druid.indexing.overlord.supervisor;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoScaler;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.TestSeekableStreamDataSourceMetadata;
import org.apache.druid.java.util.common.DateTimes;
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
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(EasyMockRunner.class)
public class SupervisorResourceTest extends EasyMockSupport
{
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();
  private static final TestSupervisorSpec SPEC1 = new TestSupervisorSpec(
      "id1",
      null,
      Collections.singletonList("datasource1")
  );

  private static final TestSupervisorSpec SPEC2 = new TestSupervisorSpec(
      "id2",
      null,
      Collections.singletonList("datasource2")
  );

  private static final Set<String> SUPERVISOR_IDS = ImmutableSet.of(SPEC1.getId(), SPEC2.getId());

  @Mock
  private TaskMaster taskMaster;

  @Mock
  private SupervisorManager supervisorManager;

  @Mock
  private HttpServletRequest request;

  @Mock
  private AuthConfig authConfig;

  @Mock
  private AuditManager auditManager;

  private SupervisorResource supervisorResource;

  @Before
  public void setUp()
  {
    supervisorResource = new SupervisorResource(
        taskMaster,
        new AuthorizerMapper(null)
        {
          @Override
          public Authorizer getAuthorizer(String name)
          {
            return (authenticationResult, resource, action) -> {
              if (authenticationResult.getIdentity().equals("druid")) {
                return Access.OK;
              } else {
                if (resource.getType().equals(ResourceType.DATASOURCE)) {
                  if (resource.getName().equals("datasource2")) {
                    return new Access(false, "not authorized.");
                  } else {
                    return Access.OK;
                  }
                } else if (resource.getType().equals(ResourceType.EXTERNAL)) {
                  if (resource.getName().equals("test")) {
                    return new Access(false, "not authorized.");
                  } else {
                    return Access.OK;
                  }
                }
                return Access.OK;
              }
            };
          }
        },
        OBJECT_MAPPER,
        authConfig,
        auditManager
    );
  }

  @Test
  public void testSpecPost()
  {
    SupervisorSpec spec = new TestSupervisorSpec("my-id", null, null)
    {

      @Override
      public List<String> getDataSources()
      {
        return Collections.singletonList("datasource1");
      }
    };

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.createOrUpdateAndStartSupervisor(spec)).andReturn(true);

    setupMockRequest();
    setupMockRequestForAudit();

    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(true);
    auditManager.doAudit(EasyMock.anyObject());
    EasyMock.expectLastCall().once();

    replayAll();

    Response response = supervisorResource.specPost(spec, request);
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("id", "my-id"), response.getEntity());
    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent());
    replayAll();

    response = supervisorResource.specPost(spec, request);
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSpecPostWithInputSourceSecurityEnabledAuthorized()
  {
    SupervisorSpec spec = new TestSupervisorSpec("my-id", null, null)
    {

      @Override
      public List<String> getDataSources()
      {
        return Collections.singletonList("datasource1");
      }
    };

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.createOrUpdateAndStartSupervisor(spec)).andReturn(true);
    setupMockRequest();
    setupMockRequestForAudit();

    auditManager.doAudit(EasyMock.anyObject());
    EasyMock.expectLastCall().once();

    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(true);
    replayAll();

    Response response = supervisorResource.specPost(spec, request);
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("id", "my-id"), response.getEntity());
    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent());
    replayAll();

    response = supervisorResource.specPost(spec, request);
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSpecPostWithInputSourceSecurityEnabledUnauthorized()
  {
    SupervisorSpec spec = new TestSupervisorSpec("my-id", null, null)
    {

      @Override
      public List<String> getDataSources()
      {
        return Collections.singletonList("datasource1");
      }
    };

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("notdruid", "druid", null, null)
    ).atLeastOnce();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, false);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(true);
    replayAll();

    Assert.assertThrows(ForbiddenException.class, () -> supervisorResource.specPost(spec, request));
    verifyAll();
  }

  @Test
  public void testSpecGetAll()
  {
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.getSupervisorIds()).andReturn(SUPERVISOR_IDS).atLeastOnce();
    EasyMock.expect(supervisorManager.getSupervisorSpec(SPEC1.getId())).andReturn(Optional.of(SPEC1));
    EasyMock.expect(supervisorManager.getSupervisorSpec(SPEC2.getId())).andReturn(Optional.of(SPEC2));
    setupMockRequest();
    replayAll();

    Response response = supervisorResource.specGetAll(null, null, null, request);
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(SUPERVISOR_IDS, response.getEntity());
    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent());
    replayAll();

    response = supervisorResource.specGetAll(null, null, null, request);
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSpecGetAllFull()
  {
    SupervisorStateManager.State state1 = SupervisorStateManager.BasicState.RUNNING;
    SupervisorStateManager.State state2 = SupervisorStateManager.BasicState.SUSPENDED;

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.getSupervisorIds()).andReturn(SUPERVISOR_IDS).atLeastOnce();
    EasyMock.expect(supervisorManager.getSupervisorSpec("id1")).andReturn(Optional.of(SPEC1)).anyTimes();
    EasyMock.expect(supervisorManager.getSupervisorSpec("id2")).andReturn(Optional.of(SPEC2)).anyTimes();
    EasyMock.expect(supervisorManager.getSupervisorState("id1")).andReturn(Optional.of(state1)).anyTimes();
    EasyMock.expect(supervisorManager.getSupervisorState("id2")).andReturn(Optional.of(state2)).anyTimes();
    setupMockRequest();
    replayAll();

    Response response = supervisorResource.specGetAll("", null, null, request);
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
    List<SupervisorStatus> specs = (List<SupervisorStatus>) response.getEntity();
    Assert.assertTrue(
        specs.stream()
             .allMatch(spec ->
                           ("id1".equals(spec.getId()) && SPEC1.equals(spec.getSpec())) ||
                           ("id2".equals(spec.getId()) && SPEC2.equals(spec.getSpec()))
             )
    );
  }

  @Test
  public void testSpecGetAllSystem()
  {
    SupervisorStateManager.State state1 = SupervisorStateManager.BasicState.RUNNING;
    SupervisorStateManager.State state2 = SupervisorStateManager.BasicState.SUSPENDED;

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.getSupervisorIds()).andReturn(SUPERVISOR_IDS).atLeastOnce();
    EasyMock.expect(supervisorManager.getSupervisorSpec("id1")).andReturn(Optional.of(SPEC1)).anyTimes();
    EasyMock.expect(supervisorManager.getSupervisorSpec("id2")).andReturn(Optional.of(SPEC2)).anyTimes();
    EasyMock.expect(supervisorManager.getSupervisorState("id1")).andReturn(Optional.of(state1)).anyTimes();
    EasyMock.expect(supervisorManager.getSupervisorState("id2")).andReturn(Optional.of(state2)).anyTimes();
    setupMockRequest();
    replayAll();

    Response response = supervisorResource.specGetAll(null, null, "", request);
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
    List<SupervisorStatus> specs = (List<SupervisorStatus>) response.getEntity();
    specs.sort(Comparator.comparing(SupervisorStatus::getId));
    Assert.assertEquals(2, specs.size());
    SupervisorStatus spec = specs.get(0);
    Assert.assertEquals("id1", spec.getId());
    Assert.assertEquals("RUNNING", spec.getState());
    Assert.assertEquals("RUNNING", spec.getDetailedState());
    Assert.assertEquals(true, spec.isHealthy());
    Assert.assertEquals("{\"type\":\"SupervisorResourceTest$TestSupervisorSpec\"}", spec.getSpecString());
    Assert.assertEquals("test", spec.getType());
    Assert.assertEquals("dummy", spec.getSource());
    Assert.assertEquals(false, spec.isSuspended());
  }

  @Test
  public void testSpecGetState()
  {
    SupervisorStateManager.State state1 = SupervisorStateManager.BasicState.RUNNING;
    SupervisorStateManager.State state2 = SupervisorStateManager.BasicState.SUSPENDED;

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.getSupervisorIds()).andReturn(SUPERVISOR_IDS).atLeastOnce();
    EasyMock.expect(supervisorManager.getSupervisorSpec("id1")).andReturn(Optional.of(SPEC1)).times(1);
    EasyMock.expect(supervisorManager.getSupervisorSpec("id2")).andReturn(Optional.of(SPEC2)).times(1);
    EasyMock.expect(supervisorManager.getSupervisorState("id1")).andReturn(Optional.of(state1)).times(1);
    EasyMock.expect(supervisorManager.getSupervisorState("id2")).andReturn(Optional.of(state2)).times(1);
    setupMockRequest();
    replayAll();

    Response response = supervisorResource.specGetAll(null, true, null, request);
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
    List<SupervisorStatus> states = (List<SupervisorStatus>) response.getEntity();
    Assert.assertTrue(
        states.stream()
              .allMatch(state -> {
                final String id = (String) state.getId();
                if ("id1".equals(id)) {
                  return state1.toString().equals(state.getState())
                         && state1.toString().equals(state.getDetailedState())
                         && (Boolean) state.isHealthy() == state1.isHealthy();
                } else if ("id2".equals(id)) {
                  return state2.toString().equals(state.getState())
                         && state2.toString().equals(state.getDetailedState())
                         && (Boolean) state.isHealthy() == state2.isHealthy();
                }
                return false;
              })
    );
  }

  @Test
  public void testSpecGet()
  {
    SupervisorSpec spec = new TestSupervisorSpec("my-id", null, null);

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.getSupervisorSpec("my-id")).andReturn(Optional.of(spec));
    EasyMock.expect(supervisorManager.getSupervisorSpec("my-id-2")).andReturn(Optional.absent());
    replayAll();

    Response response = supervisorResource.specGet("my-id");

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(spec, response.getEntity());

    response = supervisorResource.specGet("my-id-2");

    Assert.assertEquals(404, response.getStatus());
    verifyAll();

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent());
    replayAll();

    response = supervisorResource.specGet("my-id");
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSpecGetStatus()
  {
    SupervisorReport<Void> report = new SupervisorReport<>("id", DateTimes.nowUtc(), null);

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.getSupervisorStatus("my-id")).andReturn(Optional.of(report));
    EasyMock.expect(supervisorManager.getSupervisorStatus("my-id-2")).andReturn(Optional.absent());
    replayAll();

    Response response = supervisorResource.specGetStatus("my-id");

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(report, response.getEntity());

    response = supervisorResource.specGetStatus("my-id-2");

    Assert.assertEquals(404, response.getStatus());
    verifyAll();

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent());
    replayAll();

    response = supervisorResource.specGetStatus("my-id");
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSpecGetHealth()
  {
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(3);
    EasyMock.expect(supervisorManager.isSupervisorHealthy("my-id")).andReturn(Optional.of(true));
    EasyMock.expect(supervisorManager.isSupervisorHealthy("my-id-2")).andReturn(Optional.of(false));
    EasyMock.expect(supervisorManager.isSupervisorHealthy("my-id-3")).andReturn(Optional.absent());
    replayAll();

    Response response = supervisorResource.specGetHealth("my-id");

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("healthy", true), response.getEntity());

    response = supervisorResource.specGetHealth("my-id-2");

    Assert.assertEquals(503, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("healthy", false), response.getEntity());

    response = supervisorResource.specGetHealth("my-id-3");

    Assert.assertEquals(404, response.getStatus());
    Assert.assertEquals(
        ImmutableMap.of("error", "[my-id-3] does not exist or health check not implemented"),
        response.getEntity()
    );

    verifyAll();
  }

  @Test
  public void testSpecSuspend()
  {
    TestSupervisorSpec suspended = new TestSupervisorSpec("my-id", null, null, true)
    {
      @Override
      public List<String> getDataSources()
      {
        return Collections.singletonList("datasource1");
      }
    };

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.suspendOrResumeSupervisor("my-id", true)).andReturn(true);
    EasyMock.expect(supervisorManager.getSupervisorSpec("my-id")).andReturn(Optional.of(suspended));
    replayAll();

    Response response = supervisorResource.specSuspend("my-id");
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
    TestSupervisorSpec responseSpec = (TestSupervisorSpec) response.getEntity();
    Assert.assertEquals(suspended.id, responseSpec.id);
    Assert.assertEquals(suspended.suspended, responseSpec.suspended);
    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.suspendOrResumeSupervisor("my-id", true)).andReturn(false);
    EasyMock.expect(supervisorManager.getSupervisorSpec("my-id")).andReturn(Optional.of(suspended));
    replayAll();

    response = supervisorResource.specSuspend("my-id");
    verifyAll();

    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", "[my-id] is already suspended"), response.getEntity());
  }

  @Test
  public void testSpecResume()
  {
    TestSupervisorSpec running = new TestSupervisorSpec("my-id", null, null, false)
    {
      @Override
      public List<String> getDataSources()
      {
        return Collections.singletonList("datasource1");
      }
    };

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.suspendOrResumeSupervisor("my-id", false)).andReturn(true);
    EasyMock.expect(supervisorManager.getSupervisorSpec("my-id")).andReturn(Optional.of(running));
    replayAll();

    Response response = supervisorResource.specResume("my-id");
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
    TestSupervisorSpec responseSpec = (TestSupervisorSpec) response.getEntity();
    Assert.assertEquals(running.id, responseSpec.id);
    Assert.assertEquals(running.suspended, responseSpec.suspended);
    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.suspendOrResumeSupervisor("my-id", false)).andReturn(false);
    EasyMock.expect(supervisorManager.getSupervisorSpec("my-id")).andReturn(Optional.of(running));
    replayAll();

    response = supervisorResource.specResume("my-id");
    verifyAll();

    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", "[my-id] is already running"), response.getEntity());
  }

  @Test
  public void testTerminate()
  {
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.stopAndRemoveSupervisor("my-id")).andReturn(true);
    EasyMock.expect(supervisorManager.stopAndRemoveSupervisor("my-id-2")).andReturn(false);
    replayAll();

    Response response = supervisorResource.terminate("my-id");

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("id", "my-id"), response.getEntity());

    response = supervisorResource.terminate("my-id-2");

    Assert.assertEquals(404, response.getStatus());
    verifyAll();

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent());
    replayAll();

    response = supervisorResource.terminate("my-id");
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSuspendAll()
  {
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.getSupervisorIds()).andReturn(SUPERVISOR_IDS).atLeastOnce();
    EasyMock.expect(supervisorManager.getSupervisorSpec(SPEC1.getId())).andReturn(Optional.of(SPEC1));
    EasyMock.expect(supervisorManager.getSupervisorSpec(SPEC2.getId())).andReturn(Optional.of(SPEC2));
    EasyMock.expect(supervisorManager.suspendOrResumeSupervisor(SPEC1.getId(), true)).andReturn(true);
    EasyMock.expect(supervisorManager.suspendOrResumeSupervisor(SPEC2.getId(), true)).andReturn(true);

    setupMockRequest();
    replayAll();

    Response response = supervisorResource.suspendAll(request);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("status", "success"), response.getEntity());
    verifyAll();
  }

  @Test
  public void testSuspendAllWithPartialAuthorization()
  {
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.getSupervisorIds()).andReturn(SUPERVISOR_IDS).atLeastOnce();
    EasyMock.expect(supervisorManager.getSupervisorSpec(SPEC1.getId())).andReturn(Optional.of(SPEC1));
    EasyMock.expect(supervisorManager.getSupervisorSpec(SPEC2.getId())).andReturn(Optional.of(SPEC2));
    EasyMock.expect(supervisorManager.suspendOrResumeSupervisor(SPEC1.getId(), true)).andReturn(true);

    setupMockRequestForUser("notDruid");
    replayAll();

    Response response = supervisorResource.suspendAll(request);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("status", "success"), response.getEntity());
    verifyAll();
  }

  @Test
  public void testResumeAll()
  {
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.getSupervisorIds()).andReturn(SUPERVISOR_IDS).atLeastOnce();
    EasyMock.expect(supervisorManager.getSupervisorSpec(SPEC1.getId())).andReturn(Optional.of(SPEC1));
    EasyMock.expect(supervisorManager.getSupervisorSpec(SPEC2.getId())).andReturn(Optional.of(SPEC2));
    EasyMock.expect(supervisorManager.suspendOrResumeSupervisor(SPEC1.getId(), false)).andReturn(true);
    EasyMock.expect(supervisorManager.suspendOrResumeSupervisor(SPEC2.getId(), false)).andReturn(true);

    setupMockRequest();
    replayAll();

    Response response = supervisorResource.resumeAll(request);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("status", "success"), response.getEntity());
    verifyAll();
  }

  @Test
  public void testResumeAllWithPartialAuthorization()
  {
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.getSupervisorIds()).andReturn(SUPERVISOR_IDS).atLeastOnce();
    EasyMock.expect(supervisorManager.getSupervisorSpec(SPEC1.getId())).andReturn(Optional.of(SPEC1));
    EasyMock.expect(supervisorManager.getSupervisorSpec(SPEC2.getId())).andReturn(Optional.of(SPEC2));
    EasyMock.expect(supervisorManager.suspendOrResumeSupervisor(SPEC1.getId(), false)).andReturn(true);

    setupMockRequestForUser("notDruid");
    replayAll();

    Response response = supervisorResource.resumeAll(request);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("status", "success"), response.getEntity());
    verifyAll();
  }

  @Test
  public void testTerminateAll()
  {
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.getSupervisorIds()).andReturn(SUPERVISOR_IDS).atLeastOnce();
    EasyMock.expect(supervisorManager.getSupervisorSpec(SPEC1.getId())).andReturn(Optional.of(SPEC1));
    EasyMock.expect(supervisorManager.getSupervisorSpec(SPEC2.getId())).andReturn(Optional.of(SPEC2));
    EasyMock.expect(supervisorManager.stopAndRemoveSupervisor(SPEC1.getId())).andReturn(true);
    EasyMock.expect(supervisorManager.stopAndRemoveSupervisor(SPEC2.getId())).andReturn(true);

    setupMockRequest();
    replayAll();

    Response response = supervisorResource.terminateAll(request);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("status", "success"), response.getEntity());
    verifyAll();
  }

  @Test
  public void testTerminateAllWithPartialAuthorization()
  {
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.getSupervisorIds()).andReturn(SUPERVISOR_IDS).atLeastOnce();
    EasyMock.expect(supervisorManager.getSupervisorSpec(SPEC1.getId())).andReturn(Optional.of(SPEC1));
    EasyMock.expect(supervisorManager.getSupervisorSpec(SPEC2.getId())).andReturn(Optional.of(SPEC2));
    EasyMock.expect(supervisorManager.stopAndRemoveSupervisor(SPEC1.getId())).andReturn(true);

    setupMockRequestForUser("notDruid");
    replayAll();

    Response response = supervisorResource.terminateAll(request);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("status", "success"), response.getEntity());
    verifyAll();
  }

  @Test
  public void testSpecGetAllHistory()
  {
    List<VersionedSupervisorSpec> versions1 = ImmutableList.of(
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id1", null, Collections.singletonList("datasource1")),
            "v1"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id1", null, Collections.singletonList("datasource1")),
            "v2"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, Collections.singletonList("datasource1")),
            "tombstone"
        )
    );
    List<VersionedSupervisorSpec> versions2 = ImmutableList.of(
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource2")),
            "v1"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource2")),
            "v2"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, Collections.singletonList("datasource2")),
            "tombstone"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource2")),
            "v3"
        )
    );
    List<VersionedSupervisorSpec> versions3 = ImmutableList.of(
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource3")),
            "v1"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, null),
            "tombstone"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource3")),
            "v2"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, null),
            "tombstone"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource3")),
            "v3"
        )
    );
    Map<String, List<VersionedSupervisorSpec>> history = new HashMap<>();
    history.put("id1", versions1);
    history.put("id2", versions2);
    history.put("id3", versions3);

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.getSupervisorHistory()).andReturn(history);
    EasyMock.expect(supervisorManager.getSupervisorSpec("id1")).andReturn(Optional.of(SPEC1)).atLeastOnce();
    EasyMock.expect(supervisorManager.getSupervisorSpec("id2")).andReturn(Optional.of(SPEC2)).atLeastOnce();
    setupMockRequest();
    replayAll();

    Response response = supervisorResource.specGetAllHistory(request);

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(history, response.getEntity());

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent());
    replayAll();

    response = supervisorResource.specGetAllHistory(request);
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSpecGetAllHistoryWithPartialAuthorization()
  {
    List<VersionedSupervisorSpec> versions1 = ImmutableList.of(
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id1", null, Collections.singletonList("datasource1")),
            "v1"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id1", null, Collections.singletonList("datasource1")),
            "v2"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, Collections.singletonList("datasource1")),
            "tombstone"
        )
    );
    List<VersionedSupervisorSpec> versions2 = ImmutableList.of(
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource2")),
            "v1"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource2")),
            "v2"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, Collections.singletonList("datasource2")),
            "tombstone"
        )
    );
    List<VersionedSupervisorSpec> versions3 = ImmutableList.of(
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource2")),
            "v1"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource2")),
            "v2"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, Collections.singletonList("datasource2")),
            "tombstone"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id3", null, Collections.singletonList("datasource3")),
            "v1"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, Collections.singletonList("datasource3")),
            "tombstone"
        )
    );
    List<VersionedSupervisorSpec> versions4 = ImmutableList.of(
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource2")),
            "v1"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, null),
            "tombstone"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource2")),
            "v2"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, null),
            "tombstone"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource2")),
            "v3"
        )
    );

    Map<String, List<VersionedSupervisorSpec>> history = new HashMap<>();
    history.put("id1", versions1);
    history.put("id2", versions2);
    history.put("id3", versions3);
    history.put("id4", versions4);

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.getSupervisorHistory()).andReturn(history);
    EasyMock.expect(supervisorManager.getSupervisorSpec("id1")).andReturn(Optional.of(SPEC1)).atLeastOnce();
    EasyMock.expect(supervisorManager.getSupervisorSpec("id2")).andReturn(Optional.of(SPEC2)).atLeastOnce();
    setupMockRequestForUser("wronguser");
    replayAll();

    Response response = supervisorResource.specGetAllHistory(request);

    Map<String, List<VersionedSupervisorSpec>> filteredHistory = new HashMap<>();
    filteredHistory.put("id1", versions1);
    filteredHistory.put(
        "id3",
        ImmutableList.of(
            new VersionedSupervisorSpec(
                new TestSupervisorSpec("id3", null, Collections.singletonList("datasource3")),
                "v1"
            ),
            new VersionedSupervisorSpec(
                new NoopSupervisorSpec(null, Collections.singletonList("datasource3")),
                "tombstone"
            )
        )
    );
    filteredHistory.put(
        "id4",
        ImmutableList.of(
            new VersionedSupervisorSpec(
                new NoopSupervisorSpec(null, null),
                "tombstone"
            ),
            new VersionedSupervisorSpec(
                new NoopSupervisorSpec(null, null),
                "tombstone"
            )
        )
    );

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(filteredHistory, response.getEntity());

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent());
    replayAll();

    response = supervisorResource.specGetAllHistory(request);
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSpecGetHistory()
  {
    List<VersionedSupervisorSpec> versions1 = ImmutableList.of(
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id1", null, Collections.singletonList("datasource1")),
            "v1"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, Collections.singletonList("datasource1")),
            "tombstone"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id1", null, Collections.singletonList("datasource1")),
            "v2"
        )
    );
    List<VersionedSupervisorSpec> versions2 = ImmutableList.of(
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource2")),
            "v1"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, Collections.singletonList("datasource2")),
            "tombstone"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource2")),
            "v2"
        )
    );

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(3);
    EasyMock.expect(supervisorManager.getSupervisorHistoryForId("id1")).andReturn(versions1).times(1);
    EasyMock.expect(supervisorManager.getSupervisorHistoryForId("id2")).andReturn(versions2).times(1);
    EasyMock.expect(supervisorManager.getSupervisorHistoryForId("id3")).andReturn(Collections.emptyList()).times(1);
    setupMockRequest();
    replayAll();

    Response response = supervisorResource.specGetHistory(request, "id1");

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(versions1, response.getEntity());

    response = supervisorResource.specGetHistory(request, "id2");

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(versions2, response.getEntity());

    response = supervisorResource.specGetHistory(request, "id3");

    Assert.assertEquals(404, response.getStatus());

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent());
    replayAll();

    response = supervisorResource.specGetHistory(request, "id1");
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSpecGetHistoryWithAuthFailure()
  {
    List<VersionedSupervisorSpec> versions1 = ImmutableList.of(
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id1", null, Collections.singletonList("datasource1")),
            "v1"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, Collections.singletonList("datasource3")),
            "tombstone"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id1", null, Collections.singletonList("datasource1")),
            "v2"
        )
    );
    List<VersionedSupervisorSpec> versions2 = ImmutableList.of(
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource2")),
            "v1"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, Collections.singletonList("datasource2")),
            "tombstone"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource2")),
            "v2"
        )
    );
    List<VersionedSupervisorSpec> versions3 = ImmutableList.of(
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id3", null, Collections.singletonList("datasource3")),
            "v1"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, null),
            "tombstone"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id3", null, Collections.singletonList("datasource2")),
            "v2"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, null),
            "tombstone"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id3", null, Collections.singletonList("datasource3")),
            "v2"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, Collections.singletonList("datasource3")),
            "tombstone"
        )
    );

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(4);
    EasyMock.expect(supervisorManager.getSupervisorHistoryForId("id1")).andReturn(versions1).times(1);
    EasyMock.expect(supervisorManager.getSupervisorHistoryForId("id2")).andReturn(versions2).times(1);
    EasyMock.expect(supervisorManager.getSupervisorHistoryForId("id3")).andReturn(versions3).times(1);
    EasyMock.expect(supervisorManager.getSupervisorHistoryForId("id4")).andReturn(Collections.emptyList()).times(1);
    setupMockRequestForUser("notdruid");
    replayAll();

    Response response = supervisorResource.specGetHistory(request, "id1");

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(versions1, response.getEntity());

    response = supervisorResource.specGetHistory(request, "id2");

    // user is not authorized to access datasource2
    Assert.assertEquals(404, response.getStatus());

    response = supervisorResource.specGetHistory(request, "id3");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        ImmutableList.of(
            new VersionedSupervisorSpec(
                new TestSupervisorSpec("id3", null, Collections.singletonList("datasource3")),
                "v1"
            ),
            new VersionedSupervisorSpec(
                new NoopSupervisorSpec(null, null),
                "tombstone"
            ),
            new VersionedSupervisorSpec(
                new NoopSupervisorSpec(null, null),
                "tombstone"
            ),
            new VersionedSupervisorSpec(
                new TestSupervisorSpec("id3", null, Collections.singletonList("datasource3")),
                "v2"
            ),
            new VersionedSupervisorSpec(
                new NoopSupervisorSpec(null, Collections.singletonList("datasource3")),
                "tombstone"
            )
        ),
        response.getEntity()
    );

    response = supervisorResource.specGetHistory(request, "id4");
    Assert.assertEquals(404, response.getStatus());


    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent());
    replayAll();

    response = supervisorResource.specGetHistory(request, "id1");
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testReset()
  {
    Capture<String> id1 = Capture.newInstance();
    Capture<String> id2 = Capture.newInstance();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.resetSupervisor(
        EasyMock.capture(id1),
        EasyMock.anyObject(DataSourceMetadata.class)
    )).andReturn(true);
    EasyMock.expect(supervisorManager.resetSupervisor(
        EasyMock.capture(id2),
        EasyMock.anyObject(DataSourceMetadata.class)
    )).andReturn(false);
    replayAll();

    Response response = supervisorResource.reset("my-id");

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("id", "my-id"), response.getEntity());

    response = supervisorResource.reset("my-id-2");

    Assert.assertEquals(404, response.getStatus());
    Assert.assertEquals("my-id", id1.getValue());
    Assert.assertEquals("my-id-2", id2.getValue());
    verifyAll();

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent());
    replayAll();

    response = supervisorResource.terminate("my-id");

    Assert.assertEquals(503, response.getStatus());
    verifyAll();
  }

  @Test
  public void testResetOffsets()
  {
    Capture<String> id1 = Capture.newInstance();
    Capture<String> id2 = Capture.newInstance();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.resetSupervisor(
        EasyMock.capture(id1),
        EasyMock.anyObject(DataSourceMetadata.class)
    )).andReturn(true);
    EasyMock.expect(supervisorManager.resetSupervisor(
        EasyMock.capture(id2),
        EasyMock.anyObject(DataSourceMetadata.class)
    )).andReturn(false);
    replayAll();

    DataSourceMetadata datasourceMetadata = new TestSeekableStreamDataSourceMetadata(
        new SeekableStreamStartSequenceNumbers<>(
            "topic",
            ImmutableMap.of("0", "10", "1", "20", "2", "30"),
            ImmutableSet.of()
        )
    );

    Response response = supervisorResource.resetOffsets("my-id", datasourceMetadata);

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("id", "my-id"), response.getEntity());

    response = supervisorResource.resetOffsets("my-id-2", datasourceMetadata);

    Assert.assertEquals(404, response.getStatus());
    Assert.assertEquals("my-id", id1.getValue());
    Assert.assertEquals("my-id-2", id2.getValue());
    verifyAll();

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent());
    replayAll();

    response = supervisorResource.terminate("my-id");

    Assert.assertEquals(503, response.getStatus());
    verifyAll();
  }

  @Test
  public void testNoopSupervisorSpecSerde() throws Exception
  {
    ObjectMapper mapper = new ObjectMapper();
    String oldSpec = "{\"type\":\"NoopSupervisorSpec\",\"id\":null,\"dataSources\":null}";
    NoopSupervisorSpec expectedSpec = new NoopSupervisorSpec(null, null);
    NoopSupervisorSpec deserializedSpec = mapper.readValue(oldSpec, NoopSupervisorSpec.class);
    Assert.assertEquals(expectedSpec, deserializedSpec);

    NoopSupervisorSpec spec = new NoopSupervisorSpec("abcd", Collections.singletonList("defg"));
    NoopSupervisorSpec specRoundTrip = mapper.readValue(mapper.writeValueAsBytes(spec), NoopSupervisorSpec.class);
    Assert.assertEquals(spec, specRoundTrip);
  }

  private void setupMockRequest()
  {
    setupMockRequestForUser("druid");
  }

  private void setupMockRequestForUser(String user)
  {
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(new AuthenticationResult(user, "druid", null, null))
            .atLeastOnce();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
  }

  private void setupMockRequestForAudit()
  {
    EasyMock.expect(request.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn("author").once();
    EasyMock.expect(request.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn("comment").once();

    EasyMock.expect(request.getRemoteAddr()).andReturn("127.0.0.1").once();
    EasyMock.expect(request.getMethod()).andReturn("POST").once();
    EasyMock.expect(request.getRequestURI()).andReturn("supes").once();
    EasyMock.expect(request.getQueryString()).andReturn("a=b").once();
  }

  private static class TestSupervisorSpec implements SupervisorSpec
  {
    protected final String id;
    protected final Supervisor supervisor;
    protected final List<String> datasources;
    boolean suspended;

    public TestSupervisorSpec(String id, Supervisor supervisor, List<String> datasources)
    {
      this.id = id;
      this.supervisor = supervisor;
      this.datasources = datasources;
    }

    public TestSupervisorSpec(String id, Supervisor supervisor, List<String> datasources, boolean suspended)
    {
      this(id, supervisor, datasources);
      this.suspended = suspended;
    }

    @Override
    public String getId()
    {
      return id;
    }

    @Override
    public Supervisor createSupervisor()
    {
      return supervisor;
    }

    @Override
    public SupervisorTaskAutoScaler createAutoscaler(Supervisor supervisor)
    {
      return null;
    }

    @Override
    public List<String> getDataSources()
    {
      return datasources;
    }


    @Override
    public SupervisorSpec createSuspendedSpec()
    {
      return new TestSupervisorSpec(id, supervisor, datasources, true);
    }

    @Override
    public SupervisorSpec createRunningSpec()
    {
      return new TestSupervisorSpec(id, supervisor, datasources, false);
    }

    @Override
    public boolean isSuspended()
    {
      return suspended;
    }

    @Override
    public String getType()
    {
      return "test";
    }

    @JsonIgnore
    @Nonnull
    @Override
    public Set<ResourceAction> getInputSourceResources() throws UnsupportedOperationException
    {
      return Collections.singleton(new ResourceAction(new Resource("test", ResourceType.EXTERNAL), Action.READ));
    }

    @Override
    public String getSource()
    {
      return "dummy";
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TestSupervisorSpec that = (TestSupervisorSpec) o;

      if (getId() != null ? !getId().equals(that.getId()) : that.getId() != null) {
        return false;
      }
      if (supervisor != null ? !supervisor.equals(that.supervisor) : that.supervisor != null) {
        return false;
      }
      if (datasources != null ? !datasources.equals(that.datasources) : that.datasources != null) {
        return false;
      }
      return isSuspended() == that.isSuspended();

    }

    @Override
    public int hashCode()
    {
      int result = getId() != null ? getId().hashCode() : 0;
      result = 31 * result + (supervisor != null ? supervisor.hashCode() : 0);
      result = 31 * result + (datasources != null ? datasources.hashCode() : 0);
      return result;
    }
  }
}
