/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord.config;

import io.druid.audit.AuditInfo;
import io.druid.common.config.JacksonConfigManager;
import io.druid.indexing.overlord.routing.TierRouteConfig;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.concurrent.atomic.AtomicReference;

public class TierConfigResourceTest
{
  private static final String HOST = "localhost";
  private static final String AUTHOR = "some_author";
  private static final String COMMENT = "some_comment";


  private final AtomicReference<TierRouteConfig> configRef = new AtomicReference<>(null);
  private JacksonConfigManager configManager;
  private HttpServletRequest request;
  private TierConfigResource resource;

  @Before
  public void setUp()
  {
    configManager = prepareManager(configRef);
    request = prepareRequest();
    EasyMock.replay(request, configManager);
    resource = new TierConfigResource(configManager);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(request, configManager);
  }

  @Test
  public void testGetNoConfig()
  {
    Assert.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resource.getConfig(request).getStatus());
  }

  @Test
  public void testGetWithConfig()
  {
    final TierRouteConfig tierRouteConfig = new TierRouteConfig();
    configRef.set(tierRouteConfig);
    Assert.assertEquals(Response.Status.OK.getStatusCode(), resource.getConfig(request).getStatus());
    Assert.assertEquals(tierRouteConfig, resource.getConfig(request).getEntity());
  }

  @Test
  public void testSetConfigSuccess()
  {
    final TierRouteConfig otherTierRouteConfig = new TierRouteConfig();
    EasyMock.reset(configManager);
    EasyMock.expect(configManager.set(
        EasyMock.eq(TierConfigModule.ROUTE_CONFIG_KEY),
        EasyMock.eq(otherTierRouteConfig),
        EasyMock.eq(new AuditInfo(AUTHOR, COMMENT, request.getRemoteHost()))
    )).andReturn(true).once();
    EasyMock.replay(configManager);

    final Response response = resource.updateConfig(otherTierRouteConfig, "some_author", "some_comment", request);
    Assert.assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
  }

  @Test
  public void testSetConfigFailure()
  {
    final TierRouteConfig otherTierRouteConfig = new TierRouteConfig();
    EasyMock.reset(configManager);
    EasyMock.expect(configManager.set(
        EasyMock.eq(TierConfigModule.ROUTE_CONFIG_KEY),
        EasyMock.eq(otherTierRouteConfig),
        EasyMock.eq(new AuditInfo(AUTHOR, COMMENT, request.getRemoteHost()))
    )).andReturn(false).once();
    EasyMock.replay(configManager);

    final Response response = resource.updateConfig(otherTierRouteConfig, "some_author", "some_comment", request);
    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
  }

  private JacksonConfigManager prepareManager(AtomicReference<TierRouteConfig> configRef)
  {
    final JacksonConfigManager configManager = EasyMock.createStrictMock(JacksonConfigManager.class);
    EasyMock.expect(configManager.watch(
        EasyMock.eq(TierConfigModule.ROUTE_CONFIG_KEY),
        EasyMock.eq(TierRouteConfig.class),
        EasyMock.<TierRouteConfig>isNull()
    )).andReturn(configRef).once();
    return configManager;
  }

  private HttpServletRequest prepareRequest()
  {
    final HttpServletRequest request = EasyMock.createStrictMock(HttpServletRequest.class);
    EasyMock.expect(request.getRemoteHost()).andReturn("localhost").anyTimes();
    return request;
  }
}