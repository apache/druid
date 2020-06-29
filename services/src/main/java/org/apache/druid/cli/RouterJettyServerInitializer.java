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

package org.apache.druid.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.servlet.GuiceFilter;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.server.AsyncManagementForwardingServlet;
import org.apache.druid.server.AsyncQueryForwardingServlet;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.initialization.jetty.JettyServerInitUtils;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.router.ManagementProxyConfig;
import org.apache.druid.server.router.Router;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationUtils;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.sql.avatica.DruidAvaticaHandler;
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
  private static final List<String> UNSECURED_PATHS = ImmutableList.of(
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
  private final ServerConfig serverConfig;

  @Inject
  public RouterJettyServerInitializer(
      @Router DruidHttpClientConfig routerHttpClientConfig,
      @Global DruidHttpClientConfig globalHttpClientConfig,
      ManagementProxyConfig managementProxyConfig,
      AsyncQueryForwardingServlet asyncQueryForwardingServlet,
      AsyncManagementForwardingServlet asyncManagementForwardingServlet,
      AuthConfig authConfig,
      ServerConfig serverConfig
  )
  {
    this.routerHttpClientConfig = routerHttpClientConfig;
    this.globalHttpClientConfig = globalHttpClientConfig;
    this.managementProxyConfig = managementProxyConfig;
    this.asyncQueryForwardingServlet = asyncQueryForwardingServlet;
    this.asyncManagementForwardingServlet = asyncManagementForwardingServlet;
    this.authConfig = authConfig;
    this.serverConfig = serverConfig;
  }

  @Override
  public void initialize(Server server, Injector injector)
  {
    final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
    root.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");

    root.addServlet(new ServletHolder(new DefaultServlet()), "/*");

    ServletHolder queryServletHolder = buildServletHolder(asyncQueryForwardingServlet, routerHttpClientConfig);
    root.addServlet(queryServletHolder, "/druid/v2/*");
    root.addServlet(queryServletHolder, "/druid/v1/lookups/*");

    if (managementProxyConfig.isEnabled()) {
      ServletHolder managementForwardingServletHolder = buildServletHolder(
          asyncManagementForwardingServlet,
          globalHttpClientConfig
      );
      root.addServlet(managementForwardingServletHolder, "/druid/coordinator/*");
      root.addServlet(managementForwardingServletHolder, "/druid/indexer/*");
      root.addServlet(managementForwardingServletHolder, "/proxy/*");
    }


    final ObjectMapper jsonMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    final AuthenticatorMapper authenticatorMapper = injector.getInstance(AuthenticatorMapper.class);

    AuthenticationUtils.addSecuritySanityCheckFilter(root, jsonMapper);

    // perform no-op authorization/authentication for these resources
    AuthenticationUtils.addNoopAuthenticationAndAuthorizationFilters(root, UNSECURED_PATHS);
    WebConsoleJettyServerInitializer.intializeServerForWebConsoleRoot(root);
    AuthenticationUtils.addNoopAuthenticationAndAuthorizationFilters(root, authConfig.getUnsecuredPaths());

    final List<Authenticator> authenticators = authenticatorMapper.getAuthenticatorChain();
    AuthenticationUtils.addAuthenticationFilterChain(root, authenticators);

    AuthenticationUtils.addAllowOptionsFilter(root, authConfig.isAllowUnauthenticatedHttpOptions());
    JettyServerInitUtils.addAllowHttpMethodsFilter(root, serverConfig.getAllowedHttpMethods());

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
            WebConsoleJettyServerInitializer.createWebConsoleRewriteHandler(),
            JettyServerInitUtils.getJettyRequestLogHandler(),
            JettyServerInitUtils.wrapWithDefaultGzipHandler(
                root,
                serverConfig.getInflateBufferSize(),
                serverConfig.getCompressionLevel()
            )
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
