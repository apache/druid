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

package io.druid.indexing.overlord.http.security;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Injector;
import com.sun.jersey.spi.container.ResourceFilter;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TaskStorageQueryAdapter;
import io.druid.indexing.overlord.http.OverlordResource;
import io.druid.indexing.overlord.supervisor.Supervisor;
import io.druid.indexing.overlord.supervisor.SupervisorManager;
import io.druid.indexing.overlord.supervisor.SupervisorResource;
import io.druid.indexing.overlord.supervisor.SupervisorSpec;
import io.druid.indexing.worker.http.WorkerResource;
import io.druid.server.http.security.AbstractResourceFilter;
import io.druid.server.http.security.ResourceFilterTestHelper;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class SecurityResourceFilterTest extends ResourceFilterTestHelper
{

  @Parameterized.Parameters(name = "{index}: requestPath={0}, requestMethod={1}, resourceFilter={2}")
  public static Collection<Object[]> data()
  {
    return ImmutableList.copyOf(
        Iterables.concat(
            getRequestPaths(OverlordResource.class, ImmutableList.<Class<?>>of(TaskStorageQueryAdapter.class)),
            getRequestPaths(WorkerResource.class),
            getRequestPaths(SupervisorResource.class, ImmutableList.<Class<?>>of(SupervisorManager.class))
        )
    );
  }

  private final String requestPath;
  private final String requestMethod;
  private final ResourceFilter resourceFilter;
  private final Injector injector;
  private final Task noopTask = new NoopTask(null, 0, 0, null, null, null);

  private static boolean mockedOnceTsqa;
  private static boolean mockedOnceSM;
  private TaskStorageQueryAdapter tsqa;
  private SupervisorManager supervisorManager;

  public SecurityResourceFilterTest(
      String requestPath,
      String requestMethod,
      ResourceFilter resourceFilter,
      Injector injector
  )
  {
    this.requestPath = requestPath;
    this.requestMethod = requestMethod;
    this.resourceFilter = resourceFilter;
    this.injector = injector;
  }

  @Before
  public void setUp() throws Exception
  {
    if (resourceFilter instanceof TaskResourceFilter && !mockedOnceTsqa) {
      // Since we are creating the mocked tsqa object only once and getting that object from Guice here therefore
      // if the mockedOnce check is not done then we will call EasyMock.expect and EasyMock.replay on the mocked object
      // multiple times and it will throw exceptions
      tsqa = injector.getInstance(TaskStorageQueryAdapter.class);
      EasyMock.expect(tsqa.getTask(EasyMock.anyString())).andReturn(Optional.of(noopTask)).anyTimes();
      EasyMock.replay(tsqa);
      mockedOnceTsqa = true;
    }
    if (resourceFilter instanceof SupervisorResourceFilter && !mockedOnceSM) {
      supervisorManager = injector.getInstance(SupervisorManager.class);
      SupervisorSpec supervisorSpec = new SupervisorSpec()
      {
        @Override
        public String getId()
        {
          return "id";
        }

        @Override
        public Supervisor createSupervisor()
        {
          return null;
        }

        @Override
        public List<String> getDataSources()
        {
          return ImmutableList.of("test");
        }
      };
      EasyMock.expect(supervisorManager.getSupervisorSpec(EasyMock.anyString()))
              .andReturn(Optional.of(supervisorSpec))
              .anyTimes();
      EasyMock.replay(supervisorManager);
      mockedOnceSM = true;
    }
    setUp(resourceFilter);
  }

  @Test
  public void testResourcesFilteringAccess()
  {
    setUpMockExpectations(requestPath, true, requestMethod);
    EasyMock.expect(request.getEntity(Task.class)).andReturn(noopTask).anyTimes();
    // As request object is a strict mock the ordering of expected calls matters
    // therefore adding the expectation below again as getEntity is called before getMethod
    EasyMock.expect(request.getMethod()).andReturn(requestMethod).anyTimes();
    EasyMock.replay(req, request, authorizationInfo);
    resourceFilter.getRequestFilter().filter(request);
    Assert.assertTrue(((AbstractResourceFilter) resourceFilter.getRequestFilter()).isApplicable(requestPath));
  }

  @Test(expected = WebApplicationException.class)
  public void testDatasourcesResourcesFilteringNoAccess()
  {
    setUpMockExpectations(requestPath, false, requestMethod);
    EasyMock.expect(request.getEntity(Task.class)).andReturn(noopTask).anyTimes();
    EasyMock.replay(req, request, authorizationInfo);
    Assert.assertTrue(((AbstractResourceFilter) resourceFilter.getRequestFilter()).isApplicable(requestPath));
    try {
      resourceFilter.getRequestFilter().filter(request);
    }
    catch (WebApplicationException e) {
      Assert.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), e.getResponse().getStatus());
      throw e;
    }
  }

  @Test
  public void testDatasourcesResourcesFilteringBadPath()
  {
    final String badRequestPath = requestPath.replaceAll("\\w+", "droid");
    EasyMock.expect(request.getPath()).andReturn(badRequestPath).anyTimes();
    EasyMock.replay(req, request, authorizationInfo);
    Assert.assertFalse(((AbstractResourceFilter) resourceFilter.getRequestFilter()).isApplicable(badRequestPath));
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(req, request, authorizationInfo);
    if (tsqa != null) {
      EasyMock.verify(tsqa);
    }
    if (supervisorManager != null) {
      EasyMock.verify(supervisorManager);
    }
  }

}
