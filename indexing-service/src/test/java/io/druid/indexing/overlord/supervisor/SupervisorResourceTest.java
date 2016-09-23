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

package io.druid.indexing.overlord.supervisor;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.druid.indexing.overlord.TaskMaster;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.ws.rs.core.Response;
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

  private SupervisorResource supervisorResource;

  @Before
  public void setUp() throws Exception
  {
    supervisorResource = new SupervisorResource(taskMaster);
  }

  @Test
  public void testSpecPost() throws Exception
  {
    SupervisorSpec spec = new TestSupervisorSpec("my-id", null);

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.createOrUpdateAndStartSupervisor(spec)).andReturn(true);
    replayAll();

    Response response = supervisorResource.specPost(spec);
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("id", "my-id"), response.getEntity());
    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.<SupervisorManager>absent());
    replayAll();

    response = supervisorResource.specPost(spec);
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSpecGetAll() throws Exception
  {
    Set<String> supervisorIds = ImmutableSet.of("id1", "id2");

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.getSupervisorIds()).andReturn(supervisorIds);
    replayAll();

    Response response = supervisorResource.specGetAll();
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(supervisorIds, response.getEntity());
    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.<SupervisorManager>absent());
    replayAll();

    response = supervisorResource.specGetAll();
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSpecGet() throws Exception
  {
    SupervisorSpec spec = new TestSupervisorSpec("my-id", null);

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.getSupervisorSpec("my-id")).andReturn(Optional.of(spec));
    EasyMock.expect(supervisorManager.getSupervisorSpec("my-id-2")).andReturn(Optional.<SupervisorSpec>absent());
    replayAll();

    Response response = supervisorResource.specGet("my-id");

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(spec, response.getEntity());

    response = supervisorResource.specGet("my-id-2");

    Assert.assertEquals(404, response.getStatus());
    verifyAll();

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.<SupervisorManager>absent());
    replayAll();

    response = supervisorResource.specGet("my-id");
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSpecGetStatus() throws Exception
  {
    SupervisorReport report = new SupervisorReport("id", DateTime.now())
    {
      @Override
      public Object getPayload()
      {
        return null;
      }
    };

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.getSupervisorStatus("my-id")).andReturn(Optional.of(report));
    EasyMock.expect(supervisorManager.getSupervisorStatus("my-id-2")).andReturn(Optional.<SupervisorReport>absent());
    replayAll();

    Response response = supervisorResource.specGetStatus("my-id");

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(report, response.getEntity());

    response = supervisorResource.specGetStatus("my-id-2");

    Assert.assertEquals(404, response.getStatus());
    verifyAll();

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.<SupervisorManager>absent());
    replayAll();

    response = supervisorResource.specGetStatus("my-id");
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testShutdown() throws Exception
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

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.<SupervisorManager>absent());
    replayAll();

    response = supervisorResource.shutdown("my-id");
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSpecGetAllHistory() throws Exception
  {
    Map<String, List<VersionedSupervisorSpec>> history = Maps.newHashMap();
    history.put("id1", null);
    history.put("id2", null);

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.getSupervisorHistory()).andReturn(history);
    replayAll();

    Response response = supervisorResource.specGetAllHistory();

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(history, response.getEntity());

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.<SupervisorManager>absent());
    replayAll();

    response = supervisorResource.specGetAllHistory();
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSpecGetHistory() throws Exception
  {
    List<VersionedSupervisorSpec> versions = ImmutableList.of(
        new VersionedSupervisorSpec(null, "v1"),
        new VersionedSupervisorSpec(null, "v2")
    );
    Map<String, List<VersionedSupervisorSpec>> history = Maps.newHashMap();
    history.put("id1", versions);
    history.put("id2", null);

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.getSupervisorHistory()).andReturn(history).times(2);
    replayAll();

    Response response = supervisorResource.specGetHistory("id1");

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(versions, response.getEntity());

    response = supervisorResource.specGetHistory("id3");

    Assert.assertEquals(404, response.getStatus());

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.<SupervisorManager>absent());
    replayAll();

    response = supervisorResource.specGetHistory("id1");
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testReset() throws Exception
  {
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.resetSupervisor("my-id")).andReturn(true);
    EasyMock.expect(supervisorManager.resetSupervisor("my-id-2")).andReturn(false);
    replayAll();

    Response response = supervisorResource.reset("my-id");

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("id", "my-id"), response.getEntity());

    response = supervisorResource.reset("my-id-2");

    Assert.assertEquals(404, response.getStatus());
    verifyAll();

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.<SupervisorManager>absent());
    replayAll();

    response = supervisorResource.shutdown("my-id");

    Assert.assertEquals(503, response.getStatus());
    verifyAll();
  }

  private class TestSupervisorSpec implements SupervisorSpec
  {
    private final String id;
    private final Supervisor supervisor;

    public TestSupervisorSpec(String id, Supervisor supervisor)
    {
      this.id = id;
      this.supervisor = supervisor;
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
  }
}
