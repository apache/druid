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

package io.druid.server.initialization;

import com.google.common.base.Throwables;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.servlet.GuiceFilter;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import io.druid.guice.annotations.Self;
import io.druid.guice.http.LifecycleUtils;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.server.DruidNode;
import io.druid.server.initialization.jetty.JettyServerInitUtils;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;

import javax.net.ssl.SSLContext;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public abstract class BaseJettyTest
{
  protected Lifecycle lifecycle;
  protected HttpClient client;
  protected Server server;
  protected int port = -1;

  public static void setProperties()
  {
    System.setProperty("druid.server.http.numThreads", "20");
    System.setProperty("druid.server.http.maxIdleTime", "PT1S");
    System.setProperty("druid.global.http.readTimeout", "PT1S");
  }

  @Before
  public void setup() throws Exception
  {
    setProperties();
    Injector injector = setupInjector();
    final DruidNode node = injector.getInstance(Key.get(DruidNode.class, Self.class));
    port = node.getPort();
    lifecycle = injector.getInstance(Lifecycle.class);
    lifecycle.start();
    ClientHolder holder = injector.getInstance(ClientHolder.class);
    server = injector.getInstance(Server.class);
    client = holder.getClient();
  }

  protected abstract Injector setupInjector();

  @After
  public void teardown()
  {
    lifecycle.stop();
  }

  public static class ClientHolder
  {
    HttpClient client;

    ClientHolder()
    {
      this(1);
    }

    ClientHolder(int maxClientConnections)
    {
      final Lifecycle druidLifecycle = new Lifecycle();

      try {
        this.client = HttpClientInit.createClient(
            new HttpClientConfig(maxClientConnections, SSLContext.getDefault(), Duration.ZERO),
            LifecycleUtils.asMmxLifecycle(druidLifecycle)
        );
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    public HttpClient getClient()
    {
      return client;
    }
  }

  public static class JettyServerInit implements JettyServerInitializer
  {

    @Override
    public void initialize(Server server, Injector injector)
    {
      final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
      root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
      JettyServerInitUtils.addExtensionFilters(root, injector);
      root.addFilter(JettyServerInitUtils.defaultGzipFilterHolder(), "/*", null);
      root.addFilter(GuiceFilter.class, "/*", null);

      final HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(new Handler[]{root});
      server.setHandler(handlerList);
    }

  }

  @Path("/slow")
  public static class SlowResource
  {

    public static Random random = new Random();

    @GET
    @Path("/hello")
    @Produces(MediaType.APPLICATION_JSON)
    public Response hello()
    {
      try {
        TimeUnit.MILLISECONDS.sleep(500 + random.nextInt(1600));
      }
      catch (InterruptedException e) {
        //
      }
      return Response.ok("hello").build();
    }
  }

  @Path("/default")
  public static class DefaultResource
  {
    @DELETE
    @Path("{resource}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response delete()
    {
      return Response.ok("hello").build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response get()
    {
      return Response.ok("hello").build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response post()
    {
      return Response.ok("hello").build();
    }
  }

  @Path("/exception")
  public static class ExceptionResource
  {
    @GET
    @Path("/exception")
    @Produces(MediaType.APPLICATION_JSON)
    public Response exception(
        @Context HttpServletResponse resp
    ) throws IOException
    {
      final ServletOutputStream outputStream = resp.getOutputStream();
      outputStream.println("hello");
      outputStream.flush();
      try {
        TimeUnit.MILLISECONDS.sleep(200);
      }
      catch (InterruptedException e) {
        //
      }
      throw new IOException();
    }
  }

  public static class DummyAuthFilter implements Filter
  {

    public static final String AUTH_HDR = "secretUser";
    public static final String SECRET_USER = "bob";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException
    {
    }

    @Override
    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) throws IOException,
                                                                                             ServletException
    {
      HttpServletRequest request = (HttpServletRequest) req;
      if (request.getHeader(AUTH_HDR) == null || request.getHeader(AUTH_HDR).equals(SECRET_USER)) {
        chain.doFilter(req, resp);
      } else {
        HttpServletResponse response = (HttpServletResponse) resp;
        response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Failed even fake authentication.");
      }
    }

    @Override
    public void destroy()
    {
    }
  }
}
