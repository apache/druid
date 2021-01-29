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
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.google.inject.servlet.GuiceFilter;
import io.airlift.airline.Command;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.http.SelfDiscoveryResource;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.initialization.jetty.JettyServerInitUtils;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.security.AuthenticationUtils;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.List;

@Command(
    name = CliCustomNodeRole.SERVICE_NAME,
    description = "Some custom druid node role defined in an extension"
)
public class CliCustomNodeRole extends ServerRunnable
{
  private static final Logger LOG = new Logger(CliCustomNodeRole.class);

  public static final String SERVICE_NAME = "custom-node-role";
  public static final int PORT = 9301;
  public static final int TLS_PORT = 9501;

  public CliCustomNodeRole()
  {
    super(LOG);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        binder -> {
          LOG.info("starting up");
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to(CliCustomNodeRole.SERVICE_NAME);
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(CliCustomNodeRole.PORT);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(CliCustomNodeRole.TLS_PORT);

          binder.bind(CoordinatorClient.class).in(LazySingleton.class);

          binder.bind(JettyServerInitializer.class).to(CustomJettyServiceInitializer.class).in(LazySingleton.class);
          LifecycleModule.register(binder, Server.class);

          bindNodeRoleAndAnnouncer(
              binder,
              DiscoverySideEffectsProvider.builder(new NodeRole(CliCustomNodeRole.SERVICE_NAME)).build()
          );
          Jerseys.addResource(binder, SelfDiscoveryResource.class);
          LifecycleModule.registerKey(binder, Key.get(SelfDiscoveryResource.class));

        }
    );
  }

  // ugly mimic of other jetty initializers
  private static class CustomJettyServiceInitializer implements JettyServerInitializer
  {
    private static List<String> UNSECURED_PATHS = ImmutableList.of(
        "/status/health"
    );

    private final ServerConfig serverConfig;

    @Inject
    public CustomJettyServiceInitializer(ServerConfig serverConfig)
    {
      this.serverConfig = serverConfig;
    }

    @Override
    public void initialize(Server server, Injector injector)
    {
      final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
      root.addServlet(new ServletHolder(new DefaultServlet()), "/*");

      final ObjectMapper jsonMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
      final AuthenticatorMapper authenticatorMapper = injector.getInstance(AuthenticatorMapper.class);

      AuthenticationUtils.addSecuritySanityCheckFilter(root, jsonMapper);

      // perform no-op authorization for these resources
      AuthenticationUtils.addNoopAuthenticationAndAuthorizationFilters(root, UNSECURED_PATHS);

      List<Authenticator> authenticators = authenticatorMapper.getAuthenticatorChain();
      AuthenticationUtils.addAuthenticationFilterChain(root, authenticators);

      JettyServerInitUtils.addAllowHttpMethodsFilter(root, serverConfig.getAllowedHttpMethods());

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

      // Add Gzip handler at the very end
      handlerList.addHandler(
          JettyServerInitUtils.wrapWithDefaultGzipHandler(
              root,
              serverConfig.getInflateBufferSize(),
              serverConfig.getCompressionLevel()
          )
      );

      final StatisticsHandler statisticsHandler = new StatisticsHandler();
      statisticsHandler.setHandler(handlerList);

      server.setHandler(statisticsHandler);
    }
  }
}
