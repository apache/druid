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
import io.druid.guice.annotations.Json;
import io.druid.guice.http.DruidHttpClientConfig;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.AsyncQueryForwardingServlet;
import io.druid.server.initialization.jetty.JettyServerInitUtils;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import io.druid.server.router.Router;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthenticationUtils;
import io.druid.server.security.Authenticator;
import io.druid.server.security.AuthenticatorMapper;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.List;

/**
 */
public class RouterJettyServerInitializer implements JettyServerInitializer
{
  private static Logger log = new Logger(RouterJettyServerInitializer.class);

  private static List<String> UNSECURED_PATHS = Lists.newArrayList(
      "/status/health"
  );

  private final AsyncQueryForwardingServlet asyncQueryForwardingServlet;
  private final DruidHttpClientConfig httpClientConfig;

  @Inject
  public RouterJettyServerInitializer(
      @Router DruidHttpClientConfig httpClientConfig,
      AsyncQueryForwardingServlet asyncQueryForwardingServlet
  )
  {
    this.httpClientConfig = httpClientConfig;
    this.asyncQueryForwardingServlet = asyncQueryForwardingServlet;
  }

  @Override
  public void initialize(Server server, Injector injector)
  {
    final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);

    root.addServlet(new ServletHolder(new DefaultServlet()), "/*");

    asyncQueryForwardingServlet.setTimeout(httpClientConfig.getReadTimeout().getMillis());
    ServletHolder sh = new ServletHolder(asyncQueryForwardingServlet);
    //NOTE: explicit maxThreads to workaround https://tickets.puppetlabs.com/browse/TK-152
    sh.setInitParameter("maxThreads", Integer.toString(httpClientConfig.getNumMaxThreads()));

    //Needs to be set in servlet config or else overridden to default value in AbstractProxyServlet.createHttpClient()
    sh.setInitParameter("maxConnections", Integer.toString(httpClientConfig.getNumConnections()));
    sh.setInitParameter("idleTimeout", Long.toString(httpClientConfig.getReadTimeout().getMillis()));
    sh.setInitParameter("timeout", Long.toString(httpClientConfig.getReadTimeout().getMillis()));

    root.addServlet(sh, "/druid/v2/*");

    final AuthConfig authConfig = injector.getInstance(AuthConfig.class);
    final ObjectMapper jsonMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    final AuthenticatorMapper authenticatorMapper = injector.getInstance(AuthenticatorMapper.class);

    List<Authenticator> authenticators = null;
    AuthenticationUtils.addSecuritySanityCheckFilter(root, jsonMapper);

    // perform no-op authorization for these resources
    AuthenticationUtils.addNoopAuthorizationFilters(root, UNSECURED_PATHS);

    authenticators = authenticatorMapper.getAuthenticatorChain();
    AuthenticationUtils.addAuthenticationFilterChain(root, authenticators);

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

    final HandlerList handlerList = new HandlerList();
    handlerList.setHandlers(
        new Handler[]{
            JettyServerInitUtils.getJettyRequestLogHandler(),
            JettyServerInitUtils.wrapWithDefaultGzipHandler(root)
        }
    );
    server.setHandler(handlerList);
  }
}
