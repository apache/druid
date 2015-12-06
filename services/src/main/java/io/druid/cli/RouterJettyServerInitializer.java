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
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.guice.http.DruidHttpClientConfig;
import io.druid.server.AsyncQueryForwardingServlet;
import io.druid.server.initialization.jetty.JettyServerInitUtils;
import io.druid.server.initialization.jetty.JettyServerInitializer;
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
public class RouterJettyServerInitializer implements JettyServerInitializer
{
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final QueryHostFinder hostFinder;
  private final Provider<HttpClient> httpClientProvider;
  private final DruidHttpClientConfig httpClientConfig;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;

  @Inject
  public RouterJettyServerInitializer(
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryHostFinder hostFinder,
      @Router Provider<HttpClient> httpClientProvider,
      DruidHttpClientConfig httpClientConfig,
      ServiceEmitter emitter,
      RequestLogger requestLogger
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.hostFinder = hostFinder;
    this.httpClientProvider = httpClientProvider;
    this.httpClientConfig = httpClientConfig;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
  }

  @Override
  public void initialize(Server server, Injector injector)
  {
    final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);

    root.addServlet(new ServletHolder(new DefaultServlet()), "/*");

    final AsyncQueryForwardingServlet asyncQueryForwardingServlet = new AsyncQueryForwardingServlet(
        jsonMapper,
        smileMapper,
        hostFinder,
        httpClientProvider,
        httpClientConfig,
        emitter,
        requestLogger
    );
    asyncQueryForwardingServlet.setTimeout(httpClientConfig.getReadTimeout().getMillis());
    ServletHolder sh = new ServletHolder(asyncQueryForwardingServlet);
    //NOTE: explicit maxThreads to workaround https://tickets.puppetlabs.com/browse/TK-152
    sh.setInitParameter("maxThreads", Integer.toString(httpClientConfig.getNumMaxThreads()));

    root.addServlet(sh, "/druid/v2/*");
    JettyServerInitUtils.addExtensionFilters(root, injector);
    root.addFilter(JettyServerInitUtils.defaultAsyncGzipFilterHolder(), "/*", null);
    // Can't use '/*' here because of Guice conflicts with AsyncQueryForwardingServlet path
    root.addFilter(GuiceFilter.class, "/status/*", null);

    final HandlerList handlerList = new HandlerList();
    handlerList.setHandlers(new Handler[]{JettyServerInitUtils.getJettyRequestLogHandler(), root});
    server.setHandler(handlerList);
  }
}
