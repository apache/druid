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
import com.sun.jersey.spi.container.ContainerRequest;
import org.apache.druid.indexing.overlord.TaskStorageQueryAdapter;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.server.security.AuthorizerMapper;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.expect;

public class TaskResourceFilterTest
{
  private AuthorizerMapper authorizerMapper;
  private TaskStorageQueryAdapter taskStorageQueryAdapter;
  private ContainerRequest containerRequest;
  private TaskResourceFilter resourceFilter;

  @Before
  public void setup()
  {
    authorizerMapper = EasyMock.createMock(AuthorizerMapper.class);
    taskStorageQueryAdapter = EasyMock.createMock(TaskStorageQueryAdapter.class);
    containerRequest = EasyMock.createMock(ContainerRequest.class);
    resourceFilter = new TaskResourceFilter(taskStorageQueryAdapter, authorizerMapper);
  }

  @Test
  public void testTaskNotFound()
  {
    String taskId = "not_exist_task_id";
    expect(containerRequest.getPathSegments())
        .andReturn(getPathSegments("/task/" + taskId))
        .anyTimes();
    expect(containerRequest.getMethod()).andReturn("POST").anyTimes();

    SupervisorSpec supervisorSpec = EasyMock.createMock(SupervisorSpec.class);
    expect(supervisorSpec.getDataSources())
        .andReturn(Collections.singletonList(taskId))
        .anyTimes();
    expect(taskStorageQueryAdapter.getTask(taskId))
        .andReturn(Optional.absent())
        .atLeastOnce();
    EasyMock.replay(containerRequest);
    EasyMock.replay(taskStorageQueryAdapter);

    WebApplicationException expected = null;
    try {
      resourceFilter.filter(containerRequest);
    }
    catch (WebApplicationException e) {
      expected = e;
    }
    Assert.assertNotNull(expected);
    Assert.assertEquals(expected.getResponse().getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    EasyMock.verify(containerRequest);
    EasyMock.verify(taskStorageQueryAdapter);
  }

  private List<PathSegment> getPathSegments(String path)
  {
    String[] segments = path.split("/");

    List<PathSegment> pathSegments = new ArrayList<>();
    for (final String segment : segments) {
      pathSegments.add(new PathSegment()
      {
        @Override
        public String getPath()
        {
          return segment;
        }

        @Override
        public MultivaluedMap<String, String> getMatrixParameters()
        {
          return null;
        }
      });
    }
    return pathSegments;
  }
}
