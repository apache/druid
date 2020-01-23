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

package org.apache.druid.indexing.overlord.http.security;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Injector;
import com.sun.jersey.spi.container.ResourceFilter;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskStorageQueryAdapter;
import org.apache.druid.indexing.overlord.http.OverlordResource;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.overlord.supervisor.SupervisorResource;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.worker.http.WorkerResource;
import org.apache.druid.server.http.security.ResourceFilterTestHelper;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

@RunWith(Parameterized.class)
public class OverlordSecurityResourceFilterTest extends ResourceFilterTestHelper
{
  private static final Pattern WORD = Pattern.compile("\\w+");

  @Parameterized.Parameters(name = "{index}: requestPath={0}, requestMethod={1}, resourceFilter={2}")
  public static Collection<Object[]> data()
  {
    return ImmutableList.copyOf(
        Iterables.concat(
            getRequestPaths(OverlordResource.class, ImmutableList.of(
                TaskStorageQueryAdapter.class,
                AuthorizerMapper.class
                            )
            ),
            getRequestPaths(WorkerResource.class, ImmutableList.of(
                AuthorizerMapper.class
            )),
            getRequestPaths(SupervisorResource.class, ImmutableList.of(
                SupervisorManager.class,
                AuthorizerMapper.class
                            )
            )
        )
    );
  }

  private final String requestPath;
  private final String requestMethod;
  private final ResourceFilter resourceFilter;
  private final Injector injector;
  private final Task noopTask = NoopTask.create();

  private static boolean mockedOnceTsqa;
  private static boolean mockedOnceSM;
  private TaskStorageQueryAdapter tsqa;
  private SupervisorManager supervisorManager;

  public OverlordSecurityResourceFilterTest(
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
  public void setUp()
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

        @Override
        public SupervisorSpec createSuspendedSpec()
        {
          return null;
        }

        @Override
        public SupervisorSpec createRunningSpec()
        {
          return null;
        }

        @Override
        public boolean isSuspended()
        {
          return false;
        }

        @Override
        public String getType()
        {
          return null;
        }

        @Override
        public String getSource()
        {
          return null;
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
    EasyMock.replay(req, request, authorizerMapper);
    resourceFilter.getRequestFilter().filter(request);
  }

  @Test(expected = ForbiddenException.class)
  public void testDatasourcesResourcesFilteringNoAccess()
  {
    setUpMockExpectations(requestPath, false, requestMethod);
    EasyMock.expect(request.getEntity(Task.class)).andReturn(noopTask).anyTimes();
    EasyMock.replay(req, request, authorizerMapper);
    try {
      resourceFilter.getRequestFilter().filter(request);
    }
    catch (ForbiddenException e) {
      throw e;
    }
  }

  @Test
  public void testDatasourcesResourcesFilteringBadPath()
  {
    final String badRequestPath = WORD.matcher(requestPath).replaceAll("droid");
    EasyMock.expect(request.getPath()).andReturn(badRequestPath).anyTimes();
    EasyMock.replay(req, request, authorizerMapper);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(req, request, authorizerMapper);
    if (tsqa != null) {
      EasyMock.verify(tsqa);
    }
    if (supervisorManager != null) {
      EasyMock.verify(supervisorManager);
    }
  }

}
