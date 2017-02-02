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

package io.druid.security.kerberos;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.guice.annotations.Self;
import io.druid.server.DruidNode;
import io.druid.server.initialization.jetty.ServletFilterHolder;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class SpnegoFilterHolder implements ServletFilterHolder
{
  private final SpnegoFilterConfig config;
  private final DruidNode node;

  @Inject
  public SpnegoFilterHolder(SpnegoFilterConfig config, @Self DruidNode node)
  {
    this.config = config;
    this.node = node;
  }

  @Override
  public Filter getFilter()
  {
    return new AuthenticationFilter()
    {
      @Override
      public void init(FilterConfig filterConfig) throws ServletException
      {
        ClassLoader prevLoader = Thread.currentThread().getContextClassLoader();
        try {
          // AuthenticationHandler is created during Authenticationfilter.init using reflection with thread context class loader.
          // In case of druid since the class is actually loaded as an extension and filter init is done in main thread.
          // We need to set the classloader explicitly to extension class loader.
          Thread.currentThread().setContextClassLoader(AuthenticationFilter.class.getClassLoader());
          super.init(filterConfig);
        }
        finally {
          Thread.currentThread().setContextClassLoader(prevLoader);
        }
      }

      @Override
      public void doFilter(
        ServletRequest request, ServletResponse response, FilterChain filterChain
      ) throws IOException, ServletException
      {
        String path = ((HttpServletRequest) request).getRequestURI();
        if (isExcluded(path)) {
          filterChain.doFilter(request, response);
        }
        super.doFilter(request, response, filterChain);
      }
    };
  }

  private boolean isExcluded(String path)
  {
    for (String excluded : config.getExcludedPaths()) {
      if (path.startsWith(excluded)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Class<? extends Filter> getFilterClass()
  {
    return null;
  }

  @Override
  public Map<String, String> getInitParameters()
  {
    Map<String, String> params = new HashMap<String, String>();
    try {
      params.put(
        "kerberos.principal",
        SecurityUtil.getServerPrincipal(config.getPrincipal(), node.getHost())
      );
      params.put("kerberos.keytab", config.getKeytab());
      params.put(AuthenticationFilter.AUTH_TYPE, "kerberos");
      params.put("kerberos.name.rules", config.getAuthToLocal());
      if (config.getCookieSignatureSecret() != null) {
        params.put("signature.secret", config.getCookieSignatureSecret());
      }
    }
    catch (IOException e) {
      Throwables.propagate(e);
    }
    return params;
  }

  @Override
  public String getPath()
  {
    return "/*";
  }

  @Override
  public EnumSet<DispatcherType> getDispatcherType()
  {
    return null;
  }
}
