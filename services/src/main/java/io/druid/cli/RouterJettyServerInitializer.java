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

package io.druid.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.servlet.GuiceFilter;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Json;
import io.druid.guice.http.DruidHttpClientConfig;
import io.druid.server.AsyncManagementForwardingServlet;
import io.druid.server.AsyncQueryForwardingServlet;
import io.druid.server.initialization.jetty.JettyServerInitUtils;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import io.druid.server.router.ManagementProxyConfig;
import io.druid.server.router.Router;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthenticationUtils;
import io.druid.server.security.Authenticator;
import io.druid.server.security.AuthenticatorMapper;
import io.druid.sql.avatica.DruidAvaticaHandler;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.Servlet;
import java.util.List;

public class RouterJettyServerInitializer implements JettyServerInitializer
{
  private static List<String> UNSECURED_PATHS = Lists.newArrayList(
      "/status/health",
      // JDBC authentication uses the JDBC connection context instead of HTTP headers, skip the normal auth checks.
      // The router will keep the connection context in the forwarded message, and the broker is responsible for
      // performing the auth checks.
      DruidAvaticaHandler.AVATICA_PATH
  );

  private final DruidHttpClientConfig routerHttpClientConfig;
  private final DruidHttpClientConfig globalHttpClientConfig;
  private final ManagementProxyConfig managementProxyConfig;
  private final AsyncQueryForwardingServlet asyncQueryForwardingServlet;
  private final AsyncManagementForwardingServlet asyncManagementForwardingServlet;
  private final AuthConfig authConfig;

  @Inject
  public RouterJettyServerInitializer(
      @Router DruidHttpClientConfig routerHttpClientConfig,
      @Global DruidHttpClientConfig globalHttpClientConfig,
      ManagementProxyConfig managementProxyConfig,
      AsyncQueryForwardingServlet asyncQueryForwardingServlet,
      AsyncManagementForwardingServlet asyncManagementForwardingServlet,
      AuthConfig authConfig
  )
  {
    this.routerHttpClientConfig = routerHttpClientConfig;
    this.globalHttpClientConfig = globalHttpClientConfig;
    this.managementProxyConfig = managementProxyConfig;
    this.asyncQueryForwardingServlet = asyncQueryForwardingServlet;
    this.asyncManagementForwardingServlet = asyncManagementForwardingServlet;
    this.authConfig = authConfig;
  }

  @Override
  public void initialize(Server server, Injector injector)
  {
    final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);

    root.addServlet(new ServletHolder(new DefaultServlet()), "/*");

    root.addServlet(buildServletHolder(asyncQueryForwardingServlet, routerHttpClientConfig), "/druid/v2/*");

    if (managementProxyConfig.isEnabled()) {
      ServletHolder managementForwardingServletHolder = buildServletHolder(
          asyncManagementForwardingServlet, globalHttpClientConfig
      );
      root.addServlet(managementForwardingServletHolder, "/druid/coordinator/*");
      root.addServlet(managementForwardingServletHolder, "/druid/indexer/*");
      root.addServlet(managementForwardingServletHolder, "/proxy/*");
    }

    final ObjectMapper jsonMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    final AuthenticatorMapper authenticatorMapper = injector.getInstance(AuthenticatorMapper.class);

    AuthenticationUtils.addSecuritySanityCheckFilter(root, jsonMapper);

    // perform no-op authorization for these resources
    AuthenticationUtils.addNoopAuthorizationFilters(root, UNSECURED_PATHS);
    AuthenticationUtils.addNoopAuthorizationFilters(root, authConfig.getUnsecuredPaths());

    final List<Authenticator> authenticators = authenticatorMapper.getAuthenticatorChain();
    AuthenticationUtils.addAuthenticationFilterChain(root, authenticators);

    AuthenticationUtils.addAllowOptionsFilter(root, authConfig.isAllowUnauthenticatedHttpOptions());

    JettyServerInitUtils.addExtensionFilters(root, injector);

    // Check that requests were authorized before sending responses
    AuthenticationUtils.addPreResponseAuthorizationCheckFilter(
        root,
        authenticators,
        jsonMapper
    );

    // Can't use '/*' here because of Guice conflicts with AsyncQueryForwardingServlet path
    root.addFilter(GuiceFilter.class, "/status/*", null);
    root.addFilter(GuiceFilter.class, "/druid/router/*", null);
    root.addFilter(GuiceFilter.class, "/druid-ext/*", null);

    final HandlerList handlerList = new HandlerList();
    handlerList.setHandlers(
        new Handler[]{
            JettyServerInitUtils.getJettyRequestLogHandler(),
            JettyServerInitUtils.wrapWithDefaultGzipHandler(root)
        }
    );
    server.setHandler(handlerList);
  }

  private ServletHolder buildServletHolder(Servlet servlet, DruidHttpClientConfig httpClientConfig)
  {
    ServletHolder sh = new ServletHolder(servlet);

    //NOTE: explicit maxThreads to workaround https://tickets.puppetlabs.com/browse/TK-152
    sh.setInitParameter("maxThreads", Integer.toString(httpClientConfig.getNumMaxThreads()));

    //Needs to be set in servlet config or else overridden to default value in AbstractProxyServlet.createHttpClient()
    sh.setInitParameter("maxConnections", Integer.toString(httpClientConfig.getNumConnections()));
    sh.setInitParameter("idleTimeout", Long.toString(httpClientConfig.getReadTimeout().getMillis()));
    sh.setInitParameter("timeout", Long.toString(httpClientConfig.getReadTimeout().getMillis()));

    return sh;
  }
}
