/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.server.AsyncQueryForwardingServlet;
import io.druid.server.initialization.BaseJettyServerInitializer;
import io.druid.server.log.RequestLogger;
import io.druid.server.router.QueryHostFinder;
import io.druid.server.router.Router;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 */
public class RouterJettyServerInitializer extends BaseJettyServerInitializer
{
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final QueryHostFinder hostFinder;
  private final HttpClient httpClient;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;

  @Inject
  public RouterJettyServerInitializer(
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryHostFinder hostFinder,
      @Router HttpClient httpClient,
      ServiceEmitter emitter,
      RequestLogger requestLogger
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.hostFinder = hostFinder;
    this.httpClient = httpClient;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
  }

  @Override
  public void initialize(Server server, Injector injector)
  {
    final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);

    root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
    root.addServlet(
        new ServletHolder(
            new AsyncQueryForwardingServlet(
                jsonMapper,
                smileMapper,
                hostFinder,
                httpClient,
                emitter,
                requestLogger
            )
        ), "/druid/v2/*"
    );
    root.addFilter(defaultAsyncGzipFilterHolder(), "/*", null);
    // Can't use '/*' here because of Guice conflicts with AsyncQueryForwardingServlet path
    root.addFilter(GuiceFilter.class, "/status/*", null);

    final HandlerList handlerList = new HandlerList();
    handlerList.setHandlers(new Handler[]{root});
    server.setHandler(handlerList);
  }
}
