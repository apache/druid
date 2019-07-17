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

package org.apache.druid.server.http.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Injector;
import com.sun.jersey.spi.container.ResourceFilter;
import org.apache.druid.server.BrokerQueryResource;
import org.apache.druid.server.ClientInfoResource;
import org.apache.druid.server.QueryResource;
import org.apache.druid.server.StatusResource;
import org.apache.druid.server.http.BrokerResource;
import org.apache.druid.server.http.CoordinatorDynamicConfigsResource;
import org.apache.druid.server.http.CoordinatorResource;
import org.apache.druid.server.http.DataSourcesResource;
import org.apache.druid.server.http.HistoricalResource;
import org.apache.druid.server.http.IntervalsResource;
import org.apache.druid.server.http.MetadataResource;
import org.apache.druid.server.http.RouterResource;
import org.apache.druid.server.http.RulesResource;
import org.apache.druid.server.http.ServersResource;
import org.apache.druid.server.http.TiersResource;
import org.apache.druid.server.security.ForbiddenException;
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

  @Parameterized.Parameters(name = "{index}: requestPath={0}, requestMethod={1}, resourceFilter={2}")
  public static Collection<Object[]> data()
  {
    return ImmutableList.copyOf(
        Iterables.concat(
            getRequestPathsWithAuthorizer(CoordinatorResource.class),
            getRequestPathsWithAuthorizer(DataSourcesResource.class),
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
            getRequestPathsWithAuthorizer(BrokerQueryResource.class),
            getRequestPathsWithAuthorizer(RouterResource.class)
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
    resourceFilter.getRequestFilter().filter(request);
    EasyMock.verify(req, request, authorizerMapper);
  }

  @Test(expected = ForbiddenException.class)
  public void testResourcesFilteringNoAccess()
  {
    setUpMockExpectations(requestPath, false, requestMethod);
    EasyMock.replay(req, request, authorizerMapper);
    try {
      resourceFilter.getRequestFilter().filter(request);
      Assert.fail();
    }
    catch (ForbiddenException e) {
      EasyMock.verify(req, request, authorizerMapper);
      throw e;
    }
  }
}
