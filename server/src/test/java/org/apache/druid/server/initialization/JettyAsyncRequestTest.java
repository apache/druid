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

package org.apache.druid.server.initialization;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.servlet.GuiceFilter;
import org.apache.commons.io.IOUtils;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.http.AsyncRequest;
import org.apache.druid.server.http.AsyncRequest.AsyncResponse;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.eclipse.jetty.ee8.servlet.DefaultServlet;
import org.eclipse.jetty.ee8.servlet.FilterHolder;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.ee8.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

/**
 * Runs {@link AsyncRequest} behind Jersey on a real Jetty server.
 */
public class JettyAsyncRequestTest extends BaseJettyTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  private static volatile CountDownLatch writeStarted;
  private static volatile CountDownLatch dispatchReturned;

  @Override
  protected Injector setupInjector()
  {
    writeStarted = new CountDownLatch(1);
    dispatchReturned = new CountDownLatch(1);
    return Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder,
                    Key.get(DruidNode.class, Self.class),
                    new DruidNode("test", "localhost", true, null, null, true, false)
                );
                binder.bind(JettyServerInitializer.class).to(ServerInit.class).in(LazySingleton.class);
                Jerseys.addResource(binder, AsyncResource.class);
                binder.bind(AuthorizerMapper.class).toInstance(AuthTestUtils.TEST_AUTHORIZER_MAPPER);
                LifecycleModule.register(binder, Server.class);
              }
            }
        )
    );
  }

  @Test
  @Timeout(60)
  public void test_futureCompletesWhileResourceMethodReturns_respondsWithStatusAndBodyOfFuture() throws Exception
  {
    final HttpURLConnection connection = open("/async/future");

    Assertions.assertEquals(200, connection.getResponseCode());
    Assertions.assertEquals(
        "{\"value\":\"done\"}",
        IOUtils.toString(connection.getInputStream(), StandardCharsets.UTF_8)
    );
  }

  @Test
  @Timeout(60)
  public void test_completedWithStatusAndNoBody_respondsWithThatStatus() throws Exception
  {
    Assertions.assertEquals(503, open("/async/unavailable").getResponseCode());
  }

  private HttpURLConnection open(String path) throws IOException
  {
    return (HttpURLConnection) new URL("http://localhost:" + port + path).openConnection();
  }

  @Path("/async")
  public static class AsyncResource
  {
    /**
     * Returns while another thread, which has set the status of the response, is writing its body. The body is
     * written only once the request thread is back in the container.
     */
    @GET
    @Path("/future")
    @Produces(MediaType.APPLICATION_JSON)
    public void future(@Context HttpServletRequest request) throws InterruptedException
    {
      final SettableFuture<SlowResult> future = SettableFuture.create();
      AsyncRequest.create(request, future, 60_000)
                  .contentType(JSON_MAPPER, MediaType.APPLICATION_JSON)
                  .onSuccess(result -> AsyncResponse.of(200, result))
                  .onFailure(t -> AsyncResponse.error(500, t.getMessage()))
                  .start();
      new Thread(() -> future.set(new SlowResult())).start();
      writeStarted.await();
    }

    @GET
    @Path("/unavailable")
    @Produces(MediaType.APPLICATION_JSON)
    public void unavailable(@Context HttpServletRequest request)
    {
      AsyncRequest.completed(request, JSON_MAPPER, AsyncResponse.status(503));
    }
  }

  public static class SlowResult
  {
    @JsonProperty
    public String getValue() throws InterruptedException
    {
      writeStarted.countDown();
      dispatchReturned.await();
      return "done";
    }
  }

  public static class DispatchReturnedFilter implements Filter
  {
    @Override
    public void init(FilterConfig filterConfig)
    {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException
    {
      try {
        chain.doFilter(request, response);
      }
      finally {
        dispatchReturned.countDown();
      }
    }

    @Override
    public void destroy()
    {
    }
  }

  public static class ServerInit implements JettyServerInitializer
  {
    @Override
    public void initialize(Server server, Injector injector)
    {
      final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
      root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
      root.addFilter(new FilterHolder(new DispatchReturnedFilter()), "/*", null);
      root.addFilter(GuiceFilter.class, "/*", null);
      server.setHandler(root);
    }
  }
}
