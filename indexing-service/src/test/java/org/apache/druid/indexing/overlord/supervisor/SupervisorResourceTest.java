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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(EasyMockRunner.class)
public class SupervisorResourceTest extends EasyMockSupport
{
  @Mock
  private TaskMaster taskMaster;

  @Mock
  private SupervisorManager supervisorManager;

  @Mock
  private HttpServletRequest request;

  private SupervisorResource supervisorResource;

  @Before
  public void setUp()
  {
    supervisorResource = new SupervisorResource(
        taskMaster,
        new AuthorizerMapper(null) {
          @Override
          public Authorizer getAuthorizer(String name)
          {
            return (authenticationResult, resource, action) -> {
              if (authenticationResult.getIdentity().equals("druid")) {
                return Access.OK;
              } else {
                if (resource.getName().equals("datasource2")) {
                  return new Access(false, "not authorized.");
                } else {
                  return Access.OK;
                }
              }
            };
          }
        }
    );
  }

  @Test
  public void testSpecPost()
  {
    SupervisorSpec spec = new TestSupervisorSpec("my-id", null, null) {

      @Override
      public List<String> getDataSources()
      {
        return Collections.singletonList("datasource1");
      }
    };

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.createOrUpdateAndStartSupervisor(spec)).andReturn(true);
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("druid", "druid", null, null)
    ).atLeastOnce();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
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
  public void testSpecGetAll()
  {
    Set<String> supervisorIds = ImmutableSet.of("id1", "id2");
    SupervisorSpec spec1 = new TestSupervisorSpec("id1", null, null) {

      @Override
      public List<String> getDataSources()
      {
        return Collections.singletonList("datasource1");
      }
    };
    SupervisorSpec spec2 = new TestSupervisorSpec("id2", null, null) {

      @Override
      public List<String> getDataSources()
      {
        return Collections.singletonList("datasource2");
      }
    };

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.getSupervisorIds()).andReturn(supervisorIds).atLeastOnce();
    EasyMock.expect(supervisorManager.getSupervisorSpec("id1")).andReturn(Optional.of(spec1));
    EasyMock.expect(supervisorManager.getSupervisorSpec("id2")).andReturn(Optional.of(spec2));
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("druid", "druid", null, null)
    ).atLeastOnce();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
    replayAll();

    Response response = supervisorResource.specGetAll(null, request);
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(supervisorIds, response.getEntity());
    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent());
    replayAll();

    response = supervisorResource.specGetAll(null, request);
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSpecGetAllFull()
  {
    Set<String> supervisorIds = ImmutableSet.of("id1", "id2");

    SupervisorSpec spec1 = new TestSupervisorSpec("id1", null, null) {

      @Override
      public List<String> getDataSources()
      {
        return Collections.singletonList("datasource1");
      }
    };
    SupervisorSpec spec2 = new TestSupervisorSpec("id2", null, null) {

      @Override
      public List<String> getDataSources()
      {
        return Collections.singletonList("datasource2");
      }
    };

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.getSupervisorIds()).andReturn(supervisorIds).atLeastOnce();
    EasyMock.expect(supervisorManager.getSupervisorSpec("id1")).andReturn(Optional.of(spec1)).times(2);
    EasyMock.expect(supervisorManager.getSupervisorSpec("id2")).andReturn(Optional.of(spec2)).times(2);
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("druid", "druid", null, null)
    ).atLeastOnce();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
    replayAll();

    Response response = supervisorResource.specGetAll("", request);
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
    List<Map<String, Object>> specs = (List<Map<String, Object>>) response.getEntity();
    Assert.assertTrue(
        specs.stream()
             .allMatch(spec ->
                           ("id1".equals(spec.get("id")) && spec1.equals(spec.get("spec"))) ||
                           ("id2".equals(spec.get("id")) && spec2.equals(spec.get("spec")))
             )
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
  public void testSpecSuspend()
  {

    TestSuspendableSupervisorSpec running = new TestSuspendableSupervisorSpec("my-id", null, null, false) {
      @Override
      public List<String> getDataSources()
      {
        return Collections.singletonList("datasource1");
      }
    };
    TestSuspendableSupervisorSpec suspended = new TestSuspendableSupervisorSpec("my-id", null, null, true) {
      @Override
      public List<String> getDataSources()
      {
        return Collections.singletonList("datasource1");
      }
    };

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.getSupervisorSpec("my-id"))
            .andReturn(Optional.of(running)).times(1)
            .andReturn(Optional.of(suspended)).times(1);
    EasyMock.expect(supervisorManager.suspendOrResumeSupervisor("my-id", true)).andReturn(true);
    EasyMock.expectLastCall().anyTimes();
    replayAll();

    Response response = supervisorResource.specSuspend("my-id");
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
    TestSuspendableSupervisorSpec responseSpec = (TestSuspendableSupervisorSpec) response.getEntity();
    Assert.assertEquals(suspended.id, responseSpec.id);
    Assert.assertEquals(suspended.suspended, responseSpec.suspended);
    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.getSupervisorSpec("my-id")).andReturn(Optional.of(suspended)).atLeastOnce();
    replayAll();

    response = supervisorResource.specSuspend("my-id");
    verifyAll();

    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", "[my-id] is already suspended"), response.getEntity());
  }



  @Test
  public void testSpecResume()
  {
    TestSuspendableSupervisorSpec suspended = new TestSuspendableSupervisorSpec("my-id", null, null, true) {
      @Override
      public List<String> getDataSources()
      {
        return Collections.singletonList("datasource1");
      }
    };
    TestSuspendableSupervisorSpec running = new TestSuspendableSupervisorSpec("my-id", null, null, false) {
      @Override
      public List<String> getDataSources()
      {
        return Collections.singletonList("datasource1");
      }
    };

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.getSupervisorSpec("my-id"))
            .andReturn(Optional.of(suspended)).times(1)
            .andReturn(Optional.of(running)).times(1);
    EasyMock.expect(supervisorManager.suspendOrResumeSupervisor("my-id", false)).andReturn(true);
    EasyMock.expectLastCall().anyTimes();
    replayAll();

    Response response = supervisorResource.specResume("my-id");
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
    TestSuspendableSupervisorSpec responseSpec = (TestSuspendableSupervisorSpec) response.getEntity();
    Assert.assertEquals(running.id, responseSpec.id);
    Assert.assertEquals(running.suspended, responseSpec.suspended);
    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.getSupervisorSpec("my-id")).andReturn(Optional.of(running)).atLeastOnce();
    replayAll();

    response = supervisorResource.specResume("my-id");
    verifyAll();

    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("error", "[my-id] is already running"), response.getEntity());
  }

  @Test
  public void testShutdown()
  {
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.stopAndRemoveSupervisor("my-id")).andReturn(true);
    EasyMock.expect(supervisorManager.stopAndRemoveSupervisor("my-id-2")).andReturn(false);
    replayAll();

    Response response = supervisorResource.shutdown("my-id");

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("id", "my-id"), response.getEntity());

    response = supervisorResource.shutdown("my-id-2");

    Assert.assertEquals(404, response.getStatus());
    verifyAll();

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent());
    replayAll();

    response = supervisorResource.shutdown("my-id");
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
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
    Map<String, List<VersionedSupervisorSpec>> history = Maps.newHashMap();
    history.put("id1", versions1);
    history.put("id2", versions2);
    history.put("id3", versions3);

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.getSupervisorHistory()).andReturn(history);
    SupervisorSpec spec1 = new TestSupervisorSpec("id1", null, Collections.singletonList("datasource1"));
    SupervisorSpec spec2 = new TestSupervisorSpec("id2", null, Collections.singletonList("datasource2"));
    EasyMock.expect(supervisorManager.getSupervisorSpec("id1")).andReturn(Optional.of(spec1)).atLeastOnce();
    EasyMock.expect(supervisorManager.getSupervisorSpec("id2")).andReturn(Optional.of(spec2)).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("druid", "druid", null, null)
    ).atLeastOnce();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
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
  public void testSpecGetAllHistoryWithAuthFailureFiltering()
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

    Map<String, List<VersionedSupervisorSpec>> history = Maps.newHashMap();
    history.put("id1", versions1);
    history.put("id2", versions2);
    history.put("id3", versions3);
    history.put("id4", versions4);

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.getSupervisorHistory()).andReturn(history);
    SupervisorSpec spec1 = new TestSupervisorSpec("id1", null, Collections.singletonList("datasource1"));
    SupervisorSpec spec2 = new TestSupervisorSpec("id2", null, Collections.singletonList("datasource2"));
    EasyMock.expect(supervisorManager.getSupervisorSpec("id1")).andReturn(Optional.of(spec1)).atLeastOnce();
    EasyMock.expect(supervisorManager.getSupervisorSpec("id2")).andReturn(Optional.of(spec2)).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("wronguser", "druid", null, null)
    ).atLeastOnce();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
    replayAll();

    Response response = supervisorResource.specGetAllHistory(request);

    Map<String, List<VersionedSupervisorSpec>> filteredHistory = Maps.newHashMap();
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
    Map<String, List<VersionedSupervisorSpec>> history = Maps.newHashMap();
    history.put("id1", versions1);
    history.put("id2", versions2);

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(3);
    EasyMock.expect(supervisorManager.getSupervisorHistory()).andReturn(history).times(3);
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("druid", "druid", null, null)
    ).atLeastOnce();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
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
  public void testSpecGetHistoryWithAuthFailure() throws Exception
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
    Map<String, List<VersionedSupervisorSpec>> history = Maps.newHashMap();
    history.put("id1", versions1);
    history.put("id2", versions2);
    history.put("id3", versions3);

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(4);
    EasyMock.expect(supervisorManager.getSupervisorHistory()).andReturn(history).times(4);
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("notdruid", "druid", null, null)
    ).atLeastOnce();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
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
    EasyMock.expect(supervisorManager.resetSupervisor(EasyMock.capture(id1), EasyMock.anyObject(DataSourceMetadata.class))).andReturn(true);
    EasyMock.expect(supervisorManager.resetSupervisor(EasyMock.capture(id2), EasyMock.anyObject(DataSourceMetadata.class))).andReturn(false);
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

    response = supervisorResource.shutdown("my-id");

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

    NoopSupervisorSpec spec1 = new NoopSupervisorSpec("abcd", Collections.singletonList("defg"));
    NoopSupervisorSpec spec2 = mapper.readValue(
        mapper.writeValueAsBytes(spec1),
        NoopSupervisorSpec.class
    );
    Assert.assertEquals(spec1, spec2);
  }

  private static class TestSupervisorSpec implements SupervisorSpec
  {
    protected final String id;
    protected final Supervisor supervisor;
    protected final List<String> datasources;

    public TestSupervisorSpec(String id, Supervisor supervisor, List<String> datasources)
    {
      this.id = id;
      this.supervisor = supervisor;
      this.datasources = datasources;
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
    public List<String> getDataSources()
    {
      return datasources;
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
      return datasources != null ? datasources.equals(that.datasources) : that.datasources == null;

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

  private static class TestSuspendableSupervisorSpec extends TestSupervisorSpec implements SuspendableSupervisorSpec
  {
    boolean suspended;
    public TestSuspendableSupervisorSpec(String id, Supervisor supervisor, List<String> datasources, boolean suspended)
    {
      super(id, supervisor, datasources);
      this.suspended = suspended;
    }

    @Override
    public SuspendableSupervisorSpec createSuspendedSpec()
    {
      return new TestSuspendableSupervisorSpec(id, supervisor, datasources, true);
    }

    @Override
    public SuspendableSupervisorSpec createRunningSpec()
    {
      return new TestSuspendableSupervisorSpec(id, supervisor, datasources, false);
    }

    @Override
    public boolean isSuspended()
    {
      return suspended;
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

      TestSuspendableSupervisorSpec that = (TestSuspendableSupervisorSpec) o;

      return super.equals(o) || (isSuspended() == that.isSuspended());
    }
  }
}
