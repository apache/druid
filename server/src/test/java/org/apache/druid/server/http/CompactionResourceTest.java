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

package org.apache.druid.server.http;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.DruidCoordinator;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Map;

@RunWith(Parameterized.class)
public class CompactionResourceTest
{
  private DruidCoordinator mock;
  private String dataSourceName = "datasource_1";
  private AutoCompactionSnapshot expectedSnapshot = new AutoCompactionSnapshot(
      dataSourceName,
      AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1
  );
  private AuthorizerMapper authorizerMapper;
  private HttpServletRequest request;

  @Parameterized.Parameters(name = "{index}: authVersion={0}")
  public static Iterable<String> data()
  {
    return Arrays.asList(AuthConfig.AUTH_VERSION_1, AuthConfig.AUTH_VERSION_2);
  }

  public CompactionResourceTest(String authVersion)
  {
    authorizerMapper = new AuthorizerMapper(
        ImmutableMap.of("auth1",
            new Authorizer()
            {
              @Override
              public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
              {
                return new Access(true);
              }

              @Override
              public Access authorizeV2(AuthenticationResult authenticationResult, Resource resource, Action action)
              {
                return new Access(true);
              }
            }
        ),
        authVersion
    );
    request = EasyMock.createMock(HttpServletRequest.class);
    if (authVersion.equals(AuthConfig.AUTH_VERSION_2)) {
      EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).once();
      EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).once();
      EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
          .andReturn(new AuthenticationResult("", "auth1", "", null)).once();
      request.setAttribute(EasyMock.anyString(), EasyMock.anyObject());
      EasyMock.expectLastCall().once();
    }
    EasyMock.replay(request);
  }

  @Before
  public void setUp()
  {
    mock = EasyMock.createStrictMock(DruidCoordinator.class);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(mock, request);
  }

  @Test
  public void testGetCompactionSnapshotForDataSourceWithEmptyQueryParameter()
  {
    Map<String, AutoCompactionSnapshot> expected = ImmutableMap.of(
        dataSourceName,
        expectedSnapshot
    );

    EasyMock.expect(mock.getAutoCompactionSnapshot()).andReturn(expected).once();
    EasyMock.replay(mock);

    final Response response = new CompactionResource(mock, authorizerMapper).getCompactionSnapshotForDataSource("", request);
    Assert.assertEquals(ImmutableMap.of("latestStatus", expected.values()), response.getEntity());
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testGetCompactionSnapshotForDataSourceWithNullQueryParameter()
  {
    String dataSourceName = "datasource_1";
    Map<String, AutoCompactionSnapshot> expected = ImmutableMap.of(
        dataSourceName,
        expectedSnapshot
    );

    EasyMock.expect(mock.getAutoCompactionSnapshot()).andReturn(expected).once();
    EasyMock.replay(mock);

    final Response response = new CompactionResource(mock, authorizerMapper).getCompactionSnapshotForDataSource(null, request);
    Assert.assertEquals(ImmutableMap.of("latestStatus", expected.values()), response.getEntity());
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testGetCompactionSnapshotForDataSourceWithValidQueryParameter()
  {
    String dataSourceName = "datasource_1";

    EasyMock.expect(mock.getAutoCompactionSnapshotForDataSource(dataSourceName)).andReturn(expectedSnapshot).once();
    EasyMock.replay(mock);

    final Response response = new CompactionResource(mock, authorizerMapper).getCompactionSnapshotForDataSource(dataSourceName, request);
    Assert.assertEquals(ImmutableMap.of("latestStatus", ImmutableList.of(expectedSnapshot)), response.getEntity());
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testGetCompactionSnapshotForDataSourceWithInvalidQueryParameter()
  {
    String dataSourceName = "invalid_datasource";

    EasyMock.expect(mock.getAutoCompactionSnapshotForDataSource(dataSourceName)).andReturn(null).once();
    EasyMock.replay(mock);

    final Response response = new CompactionResource(mock, authorizerMapper).getCompactionSnapshotForDataSource(dataSourceName, request);
    Assert.assertEquals(400, response.getStatus());
  }
}
