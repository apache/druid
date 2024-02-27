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

import org.apache.druid.client.indexing.SamplerSpec;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import java.util.Collections;

public class SamplerResourceTest
{
  private HttpServletRequest req;
  private AuthConfig authConfig;

  private SamplerSpec samplerSpec;
  private SamplerResource samplerResource;

  private static class Users
  {
    private static final String INPUT_SOURCE_ALLOWED = "inputSourceAllowed";
    private static final String INPUT_SOURCE_DISALLOWED = "inputSourceDisallowed";
  }

  private static final AuthorizerMapper AUTH_MAPPER = new AuthorizerMapper(null)
  {
    @Override
    public Authorizer getAuthorizer(String name)
    {
      return new Authorizer()
      {
        @Override
        public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
        {
          final String username = authenticationResult.getIdentity();
          switch (resource.getType()) {
            case ResourceType.EXTERNAL:
              return new Access(
                  action == Action.READ && Users.INPUT_SOURCE_ALLOWED.equals(username)
              );
            default:
              return new Access(true);
          }
        }

      };
    }
  };

  @Before
  public void setUp()
  {
    req = EasyMock.createStrictMock(HttpServletRequest.class);
    authConfig = EasyMock.createMock(AuthConfig.class);
    samplerSpec = EasyMock.createMock(SamplerSpec.class);

    samplerResource = new SamplerResource(AUTH_MAPPER, authConfig);
  }

  @Test
  public void test_post_properResourcesAuthorized()
  {
    expectAuthorizationTokenCheck(Users.INPUT_SOURCE_DISALLOWED);
    Authorizer mockAuthorizer = EasyMock.createMock(Authorizer.class);
    AuthorizerMapper mockAuthMapper = EasyMock.createMock(AuthorizerMapper.class);
    EasyMock.expect(mockAuthMapper.getAuthorizer("druid")).andReturn(mockAuthorizer);
    EasyMock.expect(mockAuthorizer.authorize(
        EasyMock.anyObject(AuthenticationResult.class),
        EasyMock.eq(Resource.STATE_RESOURCE),
        EasyMock.eq(Action.WRITE))).andReturn(Access.OK);
    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(false);
    EasyMock.expect(samplerSpec.sample()).andReturn(null);
    EasyMock.replay(
        req,
        authConfig,
        mockAuthorizer,
        mockAuthMapper,
        samplerSpec
    );
    samplerResource = new SamplerResource(mockAuthMapper, authConfig);
    samplerResource.post(samplerSpec, req);
  }

  @Test
  public void test_post_inputSourceSecurityEnabledAndinputSourceDisAllowed_throwsAuthError()
  {
    expectAuthorizationTokenCheck(Users.INPUT_SOURCE_DISALLOWED);
    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(true);
    EasyMock.expect(samplerSpec.getInputSourceResources()).andReturn(
        Collections.singleton(new ResourceAction(new Resource("test", ResourceType.EXTERNAL), Action.READ)));
    EasyMock.replay(
        req,
        authConfig,
        samplerSpec
    );

    Assert.assertThrows(ForbiddenException.class, () -> samplerResource.post(samplerSpec, req));
  }

  @Test
  public void test_post_inputSourceSecurityEnabledAndinputSourceAllowed_samples()
  {
    expectAuthorizationTokenCheck(Users.INPUT_SOURCE_ALLOWED);
    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(true);
    EasyMock.expect(samplerSpec.getInputSourceResources()).andReturn(
        Collections.singleton(new ResourceAction(new Resource("test", ResourceType.EXTERNAL), Action.READ)));
    EasyMock.expect(samplerSpec.sample()).andReturn(null);
    EasyMock.replay(
        req,
        authConfig,
        samplerSpec
    );

    samplerResource.post(samplerSpec, req);
  }

  @Test
  public void test_post_inputSourceSecurityDisabledAndinputSourceDisAllowed_samples()
  {
    expectAuthorizationTokenCheck(Users.INPUT_SOURCE_DISALLOWED);
    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(false);
    EasyMock.expect(samplerSpec.sample()).andReturn(null);
    EasyMock.replay(
        req,
        authConfig,
        samplerSpec
    );

    samplerResource.post(samplerSpec, req);
  }

  @Test
  public void test_post_inputSourceSecurityEnabledAndinputSourcNotSupported_throwsUOE()
  {
    expectAuthorizationTokenCheck(Users.INPUT_SOURCE_ALLOWED);
    EasyMock.expect(authConfig.isEnableInputSourceSecurity()).andReturn(true);
    EasyMock.expect(samplerSpec.getInputSourceResources()).andThrow(
        new UOE("input source type 'test' does not support input source security feature"));
    EasyMock.expect(samplerSpec.sample()).andReturn(null);
    EasyMock.replay(
        req,
        authConfig,
        samplerSpec
    );

    Assert.assertThrows(UOE.class, () -> samplerResource.post(samplerSpec, req));
  }

  private void expectAuthorizationTokenCheck(String username)
  {
    AuthenticationResult authenticationResult = new AuthenticationResult(username, "druid", null, null);
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
            .andReturn(authenticationResult)
            .atLeastOnce();

    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, false);
    EasyMock.expectLastCall().anyTimes();

    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
  }
}
