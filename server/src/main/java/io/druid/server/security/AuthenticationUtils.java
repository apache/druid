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

package io.druid.server.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;

import java.util.List;

public class AuthenticationUtils
{
  public static void addAuthenticationFilterChain(
      ServletContextHandler root,
      List<Authenticator> authenticators
  )
  {
    for (Authenticator authenticator : authenticators) {
      FilterHolder holder = new FilterHolder(
          new AuthenticationWrappingFilter(authenticator.getFilter())
      );
      if (authenticator.getInitParameters() != null) {
        holder.setInitParameters(authenticator.getInitParameters());
      }
      root.addFilter(
          holder,
          "/*",
          null
      );
    }
  }

  public static void addNoopAuthorizationFilters(ServletContextHandler root, List<String> unsecuredPaths)
  {
    for (String unsecuredPath : unsecuredPaths) {
      root.addFilter(new FilterHolder(new UnsecuredResourceFilter()), unsecuredPath, null);
    }
  }

  public static void addSecuritySanityCheckFilter(
      ServletContextHandler root,
      ObjectMapper jsonMapper
  )
  {
    root.addFilter(
        new FilterHolder(
            new SecuritySanityCheckFilter(jsonMapper)
        ),
        "/*",
        null
    );
  }

  public static void addPreResponseAuthorizationCheckFilter(
      ServletContextHandler root,
      List<Authenticator> authenticators,
      ObjectMapper jsonMapper
  )
  {
    root.addFilter(
        new FilterHolder(
            new PreResponseAuthorizationCheckFilter(authenticators, jsonMapper)
        ),
        "/*",
        null
    );
  }
}
