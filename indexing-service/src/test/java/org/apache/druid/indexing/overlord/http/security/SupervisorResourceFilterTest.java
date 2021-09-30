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
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceType;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.PathSegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;

public class SupervisorResourceFilterTest
{

  private AuthorizerMapper authorizerMapper;
  private SupervisorManager supervisorManager;
  private SupervisorResourceFilter resourceFilter;

  private ContainerRequest containerRequest;

  private List<Object> mocksToVerify;

  @Before
  public void setup()
  {
    supervisorManager = EasyMock.createMock(SupervisorManager.class);
    authorizerMapper = EasyMock.createMock(AuthorizerMapper.class);
    resourceFilter = new SupervisorResourceFilter(authorizerMapper, supervisorManager);

    containerRequest = EasyMock.createMock(ContainerRequest.class);
    mocksToVerify = new ArrayList<>();
  }

  @Test
  public void testGetWhenUserHasWriteAccess()
  {
    setExpectations("/druid/indexer/v1/supervisor/datasource1", "GET", "datasource1", Action.WRITE, true);
    ContainerRequest filteredRequest = resourceFilter.filter(containerRequest);
    Assert.assertNotNull(filteredRequest);
    verifyMocks();
  }

  @Test
  public void testGetWhenUserHasNoWriteAccess()
  {
    setExpectations("/druid/indexer/v1/supervisor/datasource1", "GET", "datasource1", Action.WRITE, false);

    ForbiddenException expected = null;
    try {
      resourceFilter.filter(containerRequest);
    }
    catch (ForbiddenException e) {
      expected = e;
    }
    Assert.assertNotNull(expected);
    verifyMocks();
  }

  @Test
  public void testPostWhenUserHasWriteAccess()
  {
    setExpectations("/druid/indexer/v1/supervisor/datasource1", "POST", "datasource1", Action.WRITE, true);
    ContainerRequest filteredRequest = resourceFilter.filter(containerRequest);
    Assert.assertNotNull(filteredRequest);
    verifyMocks();
  }

  @Test
  public void testPostWhenUserHasNoWriteAccess()
  {
    setExpectations("/druid/indexer/v1/supervisor/datasource1", "POST", "datasource1", Action.WRITE, false);

    ForbiddenException expected = null;
    try {
      resourceFilter.filter(containerRequest);
    }
    catch (ForbiddenException e) {
      expected = e;
    }
    Assert.assertNotNull(expected);
    verifyMocks();
  }

  private void setExpectations(
      String path,
      String requestMethod,
      String datasource,
      Action expectedAction,
      boolean userHasAccess
  )
  {
    expect(containerRequest.getPathSegments())
        .andReturn(getPathSegments("/druid/indexer/v1/supervisor/datasource1"))
        .anyTimes();
    expect(containerRequest.getMethod()).andReturn(requestMethod).anyTimes();

    SupervisorSpec supervisorSpec = EasyMock.createMock(SupervisorSpec.class);
    expect(supervisorSpec.getDataSources())
        .andReturn(Collections.singletonList(datasource))
        .anyTimes();
    expect(supervisorManager.getSupervisorSpec(datasource))
        .andReturn(Optional.of(supervisorSpec))
        .atLeastOnce();

    HttpServletRequest servletRequest = EasyMock.createMock(HttpServletRequest.class);
    expect(servletRequest.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH))
        .andReturn(null).anyTimes();
    expect(servletRequest.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED))
        .andReturn(null).anyTimes();
    servletRequest.setAttribute(isA(String.class), anyObject());

    final String authorizerName = "authorizer";
    AuthenticationResult authResult = EasyMock.createMock(AuthenticationResult.class);
    expect(authResult.getAuthorizerName()).andReturn(authorizerName).anyTimes();

    Authorizer authorizer = EasyMock.createMock(Authorizer.class);
    expect(
        authorizer.authorize(
            authResult,
            new Resource(datasource, ResourceType.DATASOURCE),
            expectedAction
        )
    ).andReturn(new Access(userHasAccess)).anyTimes();

    expect(authorizerMapper.getAuthorizer(authorizerName))
        .andReturn(authorizer)
        .atLeastOnce();

    expect(servletRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
        .andReturn(authResult)
        .atLeastOnce();
    resourceFilter.setReq(servletRequest);

    mocksToVerify = Arrays.asList(
        authorizerMapper,
        supervisorSpec,
        supervisorManager,
        servletRequest,
        authorizer,
        authResult,
        containerRequest
    );

    replayMocks();
  }

  private void replayMocks()
  {
    for (Object mock : mocksToVerify) {
      EasyMock.replay(mock);
    }
  }

  private void verifyMocks()
  {
    for (Object mock : mocksToVerify) {
      EasyMock.verify(mock);
    }
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
