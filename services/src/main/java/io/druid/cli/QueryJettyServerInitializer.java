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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.servlet.GuiceFilter;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.initialization.jetty.JettyServerInitUtils;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import io.druid.server.initialization.jetty.LimitRequestsFilter;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthenticationUtils;
import io.druid.server.security.Authenticator;
import io.druid.server.security.AuthenticatorMapper;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.List;
import java.util.Set;

/**
 */
public class QueryJettyServerInitializer implements JettyServerInitializer
{
  private static final Logger log = new Logger(QueryJettyServerInitializer.class);
  private static List<String> UNSECURED_PATHS = Lists.newArrayList(
      "/status/health"
  );

  private final List<Handler> extensionHandlers;

  private final ServerConfig serverConfig;

  private final AuthConfig authConfig;

  @Inject
  public QueryJettyServerInitializer(Set<Handler> extensionHandlers, ServerConfig serverConfig, AuthConfig authConfig)
  {
    this.extensionHandlers = ImmutableList.copyOf(extensionHandlers);
    this.serverConfig = serverConfig;
    this.authConfig = authConfig;
  }

  @Override
  public void initialize(Server server, Injector injector)
  {
    final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
    root.addServlet(new ServletHolder(new DefaultServlet()), "/*");

    // Add LimitRequestsFilter as first in the chain if enabled.
    if (serverConfig.isEnableRequestLimit()) {
      //To reject xth request, limit should be set to x-1 because (x+1)st request wouldn't reach filter
      // but rather wait on jetty queue.
      Preconditions.checkArgument(
          serverConfig.getNumThreads() > 1,
          "numThreads must be > 1 to enable Request Limit Filter."
      );
      log.info("Enabling Request Limit Filter with limit [%d].", serverConfig.getNumThreads() - 1);
      root.addFilter(new FilterHolder(new LimitRequestsFilter(serverConfig.getNumThreads() - 1)),
                     "/*", null
      );
    }

    final ObjectMapper jsonMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    final AuthenticatorMapper authenticatorMapper = injector.getInstance(AuthenticatorMapper.class);

    List<Authenticator> authenticators = null;
    AuthenticationUtils.addSecuritySanityCheckFilter(root, jsonMapper);

    // perform no-op authorization for these resources
    AuthenticationUtils.addNoopAuthorizationFilters(root, UNSECURED_PATHS);
    AuthenticationUtils.addNoopAuthorizationFilters(root, authConfig.getUnsecuredPaths());

    authenticators = authenticatorMapper.getAuthenticatorChain();
    AuthenticationUtils.addAuthenticationFilterChain(root, authenticators);

    AuthenticationUtils.addAllowOptionsFilter(root, authConfig.isAllowUnauthenticatedHttpOptions());

    JettyServerInitUtils.addExtensionFilters(root, injector);

    // Check that requests were authorized before sending responses
    AuthenticationUtils.addPreResponseAuthorizationCheckFilter(
        root,
        authenticators,
        jsonMapper
    );

    root.addFilter(GuiceFilter.class, "/*", null);

    final HandlerList handlerList = new HandlerList();
    // Do not change the order of the handlers that have already been added
    for (Handler handler : server.getHandlers()) {
      handlerList.addHandler(handler);
    }

    handlerList.addHandler(JettyServerInitUtils.getJettyRequestLogHandler());

    // Add all extension handlers
    for (Handler handler : extensionHandlers) {
      handlerList.addHandler(handler);
    }

    // Add Gzip handler at the very end
    handlerList.addHandler(JettyServerInitUtils.wrapWithDefaultGzipHandler(root));

    final StatisticsHandler statisticsHandler = new StatisticsHandler();
    statisticsHandler.setHandler(handlerList);

    server.setHandler(statisticsHandler);
  }
}
