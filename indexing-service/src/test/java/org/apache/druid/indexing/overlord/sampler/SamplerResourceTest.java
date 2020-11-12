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

package org.apache.druid.indexing.overlord.sampler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SamplerResourceTest
{
  private AuthorizerMapper authorizerMapper;
  private List<ResourceAction> actualResourceActions;
  private SamplerResource samplerResource;
  private IndexTaskSamplerSpec samplerSpec;
  private HttpServletRequest request;

  @Before
  public void setUp()
  {
    actualResourceActions = new ArrayList<>();
    authorizerMapper = new AuthorizerMapper(
        ImmutableMap.of(
            "auth1",
            new Authorizer()
            {
              @Override
              public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
              {
                throw new ISE("Unexpected call");
              }

              @Override
              public Access authorizeV2(AuthenticationResult authenticationResult, Resource resource, Action action)
              {
                actualResourceActions.add(new ResourceAction(resource, action));
                return new Access(true);
              }
            }
        ), AuthConfig.AUTH_VERSION_2);
    samplerResource = new SamplerResource(authorizerMapper);

    request = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
        .andReturn(new AuthenticationResult("", "auth1", "", null)).once();
    request.setAttribute(EasyMock.anyString(), EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    EasyMock.replay(request);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(samplerSpec, request);
  }

  @Test
  public void testPost()
  {
    samplerSpec = EasyMock.createMock(IndexTaskSamplerSpec.class);
    EasyMock.expect(samplerSpec.getInputSource()).andReturn(null).once();
    EasyMock.expect(samplerSpec.sample()).andReturn(new SamplerResponse(0, 0, null)).once();
    EasyMock.replay(samplerSpec);
    List<ResourceAction> expectedResourceActions = Collections
        .singletonList(new ResourceAction(Resource.SERVER_USER_RESOURCE, Action.WRITE));
    samplerResource.post(samplerSpec, request);
    Assert.assertEquals(expectedResourceActions, actualResourceActions);
  }

  @Test
  public void testPostReindexSampler()
  {
    samplerSpec = EasyMock.createMock(IndexTaskSamplerSpec.class);
    DruidInputSource inputSource = EasyMock.createMock(DruidInputSource.class);
    EasyMock.expect(samplerSpec.getInputSource()).andReturn(inputSource).anyTimes();
    EasyMock.expect(inputSource.getDataSource()).andReturn("source").once();
    EasyMock.expect(samplerSpec.sample()).andReturn(new SamplerResponse(0, 0, null)).once();
    EasyMock.replay(samplerSpec, inputSource);
    List<ResourceAction> expectedResourceActions = Lists.newArrayList(
        new ResourceAction(Resource.SERVER_USER_RESOURCE, Action.WRITE),
        new ResourceAction(new Resource("source", ResourceType.DATASOURCE), Action.READ)
    );
    samplerResource.post(samplerSpec, request);
    Assert.assertEquals(expectedResourceActions, actualResourceActions);
    EasyMock.verify(inputSource);
  }
}
