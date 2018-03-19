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

package io.druid.server.http.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Injector;
import com.sun.jersey.spi.container.ResourceFilter;
import io.druid.server.BrokerQueryResource;
import io.druid.server.ClientInfoResource;
import io.druid.server.QueryResource;
import io.druid.server.StatusResource;
import io.druid.server.http.BrokerResource;
import io.druid.server.http.CoordinatorDynamicConfigsResource;
import io.druid.server.http.CoordinatorResource;
import io.druid.server.http.DatasourcesResource;
import io.druid.server.http.HistoricalResource;
import io.druid.server.http.IntervalsResource;
import io.druid.server.http.MetadataResource;
import io.druid.server.http.RulesResource;
import io.druid.server.http.ServersResource;
import io.druid.server.http.TiersResource;
import io.druid.server.security.ForbiddenException;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

@RunWith(Parameterized.class)
public class SecurityResourceFilterTest extends ResourceFilterTestHelper
{
  @Parameterized.Parameters
  public static Collection<Object[]> data()
  {
    return ImmutableList.copyOf(
        Iterables.concat(
            getRequestPathsWithAuthorizer(CoordinatorResource.class),
            getRequestPathsWithAuthorizer(DatasourcesResource.class),
            getRequestPathsWithAuthorizer(BrokerResource.class),
            getRequestPathsWithAuthorizer(HistoricalResource.class),
            getRequestPathsWithAuthorizer(IntervalsResource.class),
            getRequestPathsWithAuthorizer(MetadataResource.class),
            getRequestPathsWithAuthorizer(RulesResource.class),
            getRequestPathsWithAuthorizer(ServersResource.class),
            getRequestPathsWithAuthorizer(TiersResource.class),
            getRequestPathsWithAuthorizer(ClientInfoResource.class),
            getRequestPathsWithAuthorizer(CoordinatorDynamicConfigsResource.class),
            getRequestPathsWithAuthorizer(QueryResource.class),
            getRequestPathsWithAuthorizer(StatusResource.class),
            getRequestPathsWithAuthorizer(BrokerQueryResource.class)
        )
    );
  }

  private final String requestPath;
  private final String requestMethod;
  private final ResourceFilter resourceFilter;
  private final Injector injector;

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
  public void setUp()
  {
    setUp(resourceFilter);
  }

  @Test
  public void testResourcesFilteringAccess()
  {
    setUpMockExpectations(requestPath, true, requestMethod);
    EasyMock.replay(req, request, authorizerMapper);
    Assert.assertTrue(((AbstractResourceFilter) resourceFilter.getRequestFilter()).isApplicable(requestPath));
    resourceFilter.getRequestFilter().filter(request);
    EasyMock.verify(req, request, authorizerMapper);
  }

  @Test(expected = ForbiddenException.class)
  public void testResourcesFilteringNoAccess()
  {
    setUpMockExpectations(requestPath, false, requestMethod);
    EasyMock.replay(req, request, authorizerMapper);
    Assert.assertTrue(((AbstractResourceFilter) resourceFilter.getRequestFilter()).isApplicable(requestPath));
    try {
      resourceFilter.getRequestFilter().filter(request);
      Assert.fail();
    }
    catch (ForbiddenException e) {
      throw e;
    }
    EasyMock.verify(req, request, authorizerMapper);
  }

  @Test
  public void testResourcesFilteringBadPath()
  {
    EasyMock.replay(req, request, authorizerMapper);
    final String badRequestPath = requestPath.replaceAll("\\w+", "droid");
    Assert.assertFalse(((AbstractResourceFilter) resourceFilter.getRequestFilter()).isApplicable(badRequestPath));
    EasyMock.verify(req, request, authorizerMapper);
  }
}
