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

package org.apache.druid.testing.embedded.auth;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AllowAllAuthenticator;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.server.security.Resource;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedResource;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class AllowAllWithPolicyAuthResource implements EmbeddedResource
{
  private static final String AUTH_TYPE = "actually-allow-all";

  @Override
  public void start() throws Exception
  {
    // Do nothing
  }

  @Override
  public void stop() throws Exception
  {
    // Do nothing
  }

  @Override
  public void beforeStart(EmbeddedDruidCluster cluster)
  {
    cluster.addExtension(AllowAllWithPolicyAuthModule.class)
           .addCommonProperty("druid.auth.authenticatorChain", "[\"test\"]")
           .addCommonProperty("druid.auth.authenticator.test.type", "actually-allow-all")
           .addCommonProperty("druid.auth.authorizers", "[\"test\"]")
           .addCommonProperty("druid.escalator.type", "actually-allow-all")
           .addCommonProperty("druid.auth.authorizer.test.type", "actually-allow-all")
           .addCommonProperty("druid.policy.enforcer.type", "restrictAllTables")
           .addCommonProperty(
               "druid.policy.enforcer.allowedPolicies",
               "[\"org.apache.druid.query.policy.NoRestrictionPolicy\"]"
           );
  }

  @JsonTypeName(AUTH_TYPE)
  static class AllowAllWithPolicyAuthenticator extends AllowAllAuthenticator
  {
    private static final AuthenticationResult RESULT = new AuthenticationResult("anon", "test", "test", null);

    @Override
    public Filter getFilter()
    {
      return new Filter()
      {
        @Override
        public void init(FilterConfig filterConfig) throws ServletException
        {

        }

        @Override
        public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException
        {
          servletRequest.setAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT, RESULT);
          filterChain.doFilter(servletRequest, servletResponse);
        }

        @Override
        public void destroy()
        {

        }
      };
    }

    @Override
    public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
    {
      return RESULT;
    }
  }

  @JsonTypeName(AUTH_TYPE)
  static class AllowAllWithPolicyAuthorizer implements Authorizer
  {
    @Override
    public Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action)
    {
      if (AuthorizationUtils.RESTRICTION_APPLICABLE_RESOURCE_TYPES.contains(resource.getType()) && Action.READ.equals(action)) {
        return Access.allowWithRestriction(NoRestrictionPolicy.instance());
      }
      return Access.OK;
    }
  }

  @JsonTypeName(AUTH_TYPE)
  static class AllowAllWithPolicyEscalator extends NoopEscalator
  {
    @Override
    public AuthenticationResult createEscalatedAuthenticationResult()
    {
      return AllowAllWithPolicyAuthenticator.RESULT;
    }
  }

  @JsonTypeName(AUTH_TYPE)
  public static class AllowAllWithPolicyAuthModule implements DruidModule
  {
    @Override
    public void configure(Binder binder)
    {
      final MapBinder<String, Authenticator> authenticatorMapBinder = PolyBind.optionBinder(
          binder,
          Key.get(Authenticator.class)
      );
      authenticatorMapBinder.addBinding(AUTH_TYPE)
                            .to(AllowAllWithPolicyAuthenticator.class)
                            .in(LazySingleton.class);
      final MapBinder<String, Authorizer> authorizerMapBinder = PolyBind.optionBinder(
          binder,
          Key.get(Authorizer.class)
      );
      authorizerMapBinder.addBinding(AUTH_TYPE)
                         .to(AllowAllWithPolicyAuthorizer.class)
                         .in(LazySingleton.class);
    }

    @Override
    public List<? extends Module> getJacksonModules()
    {
      return List.of(
          new SimpleModule().registerSubtypes(
              AllowAllWithPolicyAuthenticator.class,
              AllowAllWithPolicyAuthorizer.class,
              AllowAllWithPolicyEscalator.class
          )
      );
    }
  }
}
