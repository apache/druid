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

package io.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.servlet.GuiceFilter;

import io.druid.common.utils.SocketUtil;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.annotations.Self;
import io.druid.guice.annotations.Smile;
import io.druid.guice.http.DruidHttpClientConfig;
import io.druid.initialization.Initialization;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.query.DefaultGenericQueryMetricsFactory;
import io.druid.query.MapQueryToolChestWarehouse;
import io.druid.query.Query;
import io.druid.query.QueryToolChest;
import io.druid.server.initialization.BaseJettyTest;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.initialization.jetty.JettyServerInitUtils;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import io.druid.server.log.RequestLogger;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.server.router.QueryHostFinder;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

public class AsyncQueryForwardingServletTest extends BaseJettyTest
{
  private static int port1;
  private static int port2;

  @Override
  @Before
  public void setup() throws Exception
  {
    setProperties();
    Injector injector = setupInjector();
    final DruidNode node = injector.getInstance(Key.get(DruidNode.class, Self.class));
    port = node.getPlaintextPort();
    port1 = SocketUtil.findOpenPortFrom(port + 1);
    port2 = SocketUtil.findOpenPortFrom(port1 + 1);

    lifecycle = injector.getInstance(Lifecycle.class);
    lifecycle.start();
    ClientHolder holder = injector.getInstance(ClientHolder.class);
    client = holder.getClient();
  }

  @Override
  protected Injector setupInjector()
  {
    return Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder, Key.get(DruidNode.class, Self.class), new DruidNode("test", "localhost", null, null, new ServerConfig())
                );
                binder.bind(JettyServerInitializer.class).to(ProxyJettyServerInit.class).in(LazySingleton.class);
                Jerseys.addResource(binder, SlowResource.class);
                Jerseys.addResource(binder, ExceptionResource.class);
                Jerseys.addResource(binder, DefaultResource.class);
                LifecycleModule.register(binder, Server.class);
              }
            }
        )
    );
  }

  @Test
  public void testProxyGzipCompression() throws Exception
  {
    final URL url = new URL("http://localhost:" + port + "/proxy/default");

    final HttpURLConnection get = (HttpURLConnection) url.openConnection();
    get.setRequestProperty("Accept-Encoding", "gzip");
    Assert.assertEquals("gzip", get.getContentEncoding());

    final HttpURLConnection post = (HttpURLConnection) url.openConnection();
    post.setRequestProperty("Accept-Encoding", "gzip");
    post.setRequestMethod("POST");
    Assert.assertEquals("gzip", post.getContentEncoding());

    final HttpURLConnection getNoGzip = (HttpURLConnection) url.openConnection();
    Assert.assertNotEquals("gzip", getNoGzip.getContentEncoding());

    final HttpURLConnection postNoGzip = (HttpURLConnection) url.openConnection();
    postNoGzip.setRequestMethod("POST");
    Assert.assertNotEquals("gzip", postNoGzip.getContentEncoding());
  }

  @Test(timeout = 60_000)
  public void testDeleteBroadcast() throws Exception
  {
    CountDownLatch latch = new CountDownLatch(2);
    makeTestDeleteServer(port1, latch).start();
    makeTestDeleteServer(port2, latch).start();

    final URL url = new URL("http://localhost:" + port + "/druid/v2/abc123");
    final HttpURLConnection post = (HttpURLConnection) url.openConnection();
    post.setRequestMethod("DELETE");
    int code = post.getResponseCode();
    Assert.assertEquals(200, code);

    latch.await();
  }

  private static Server makeTestDeleteServer(int port, final CountDownLatch latch)
  {
    Server server = new Server(port);
    ServletHandler handler = new ServletHandler();
    handler.addServletWithMapping(new ServletHolder(new HttpServlet()
    {
      @Override
      protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException
      {
        latch.countDown();
        resp.setStatus(200);
      }
    }), "/default/*");

    server.setHandler(handler);
    return server;
  }

  public static class ProxyJettyServerInit implements JettyServerInitializer
  {

    private final DruidNode node;

    @Inject
    public ProxyJettyServerInit(@Self DruidNode node)
    {
      this.node = node;
    }

    @Override
    public void initialize(Server server, Injector injector)
    {
      final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
      root.addServlet(new ServletHolder(new DefaultServlet()), "/*");

      final QueryHostFinder hostFinder = new QueryHostFinder(null)
      {
        @Override
        public String getHost(Query query)
        {
          return "localhost:" + node.getPlaintextPort();
        }

        @Override
        public String getDefaultHost()
        {
          return "localhost:" + node.getPlaintextPort();
        }

        @Override
        public Collection<String> getAllHosts()
        {
          return ImmutableList.of(
              "localhost:" + node.getPlaintextPort(),
              "localhost:" + port1,
              "localhost:" + port2
          );
        }
      };

      ObjectMapper jsonMapper = injector.getInstance(ObjectMapper.class);
      ServletHolder holder = new ServletHolder(
          new AsyncQueryForwardingServlet(
              new MapQueryToolChestWarehouse(ImmutableMap.<Class<? extends Query>, QueryToolChest>of()),
              jsonMapper,
              injector.getInstance(Key.get(ObjectMapper.class, Smile.class)),
              hostFinder,
              injector.getProvider(HttpClient.class),
              injector.getInstance(DruidHttpClientConfig.class),
              new NoopServiceEmitter(),
              new RequestLogger()
              {
                @Override
                public void log(RequestLogLine requestLogLine) throws IOException
                {
                  // noop
                }
              },
              new DefaultGenericQueryMetricsFactory(jsonMapper)
          )
          {
            @Override
            protected URI rewriteURI(HttpServletRequest request, String host)
            {
              String uri = super.rewriteURI(request, host).toString();
              if (uri.contains("/druid/v2")) {
                return URI.create(uri.replace("/druid/v2", "/default"));
              }
              return URI.create(uri.replace("/proxy", ""));
            }
          });
      //NOTE: explicit maxThreads to workaround https://tickets.puppetlabs.com/browse/TK-152
      holder.setInitParameter("maxThreads", "256");
      root.addServlet(holder, "/proxy/*");
      root.addServlet(holder, "/druid/v2/*");
      JettyServerInitUtils.addExtensionFilters(root, injector);
      root.addFilter(GuiceFilter.class, "/slow/*", null);
      root.addFilter(GuiceFilter.class, "/default/*", null);
      root.addFilter(GuiceFilter.class, "/exception/*", null);

      final HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(new Handler[]{JettyServerInitUtils.wrapWithDefaultGzipHandler(root)});
      server.setHandler(handlerList);
    }
  }

  @Test
  public void testRewriteURI() throws Exception
  {

    // test params
    Assert.assertEquals(
        new URI("http://localhost:1234/some/path?param=1"),
        AsyncQueryForwardingServlet.makeURI("localhost:1234", "/some/path", "param=1")
    );

    // HttpServletRequest.getQueryString returns encoded form
    // use ascii representation in case URI is using non-ascii characters
    Assert.assertEquals(
        "http://[2a00:1450:4007:805::1007]:1234/some/path?param=1&param2=%E2%82%AC",
        AsyncQueryForwardingServlet.makeURI(
            HostAndPort.fromParts("2a00:1450:4007:805::1007", 1234).toString(),
            "/some/path",
            "param=1&param2=%E2%82%AC"
        ).toASCIIString()
    );

    // test null query
    Assert.assertEquals(
        new URI("http://localhost/"),
        AsyncQueryForwardingServlet.makeURI("localhost", "/", null)
    );
  }
}
