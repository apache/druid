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

package org.apache.druid.server;

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
import org.apache.druid.common.utils.SocketUtil;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.Query;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.initialization.BaseJettyTest;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.initialization.jetty.JettyServerInitUtils;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.apache.druid.server.log.NoopRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.router.QueryHostFinder;
import org.apache.druid.server.router.RendezvousHashAvaticaConnectionBalancer;
import org.apache.druid.server.security.AllowAllAuthorizer;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.easymock.EasyMock;
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

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Deflater;

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
                    binder,
                    Key.get(DruidNode.class, Self.class),
                    new DruidNode("test", "localhost", false, null, null, true, false)
                );
                binder.bind(JettyServerInitializer.class).to(ProxyJettyServerInit.class).in(LazySingleton.class);
                binder.bind(AuthorizerMapper.class).toInstance(
                    new AuthorizerMapper(null)
                    {

                      @Override
                      public Authorizer getAuthorizer(String name)
                      {
                        return new AllowAllAuthorizer();
                      }
                    }
                );
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

  @Test(timeout = 60_000L)
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

  @Test
  public void testQueryProxy() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                        .dataSource("foo")
                                        .intervals("2000/P1D")
                                        .granularity(Granularities.ALL)
                                        .context(ImmutableMap.of("queryId", "dummy"))
                                        .build();

    final QueryHostFinder hostFinder = EasyMock.createMock(QueryHostFinder.class);
    EasyMock.expect(hostFinder.pickServer(query)).andReturn(new TestServer("http", "1.2.3.4", 9999)).once();
    EasyMock.replay(hostFinder);

    final HttpServletRequest requestMock = EasyMock.createMock(HttpServletRequest.class);
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(jsonMapper.writeValueAsBytes(query));
    final ServletInputStream servletInputStream = new ServletInputStream()
    {
      private boolean finished;

      @Override
      public boolean isFinished()
      {
        return finished;
      }

      @Override
      public boolean isReady()
      {
        return true;
      }

      @Override
      public void setReadListener(final ReadListener readListener)
      {
        // do nothing
      }

      @Override
      public int read()
      {
        final int b = inputStream.read();
        if (b < 0) {
          finished = true;
        }
        return b;
      }
    };
    EasyMock.expect(requestMock.getContentType()).andReturn("application/json").times(2);
    requestMock.setAttribute("org.apache.druid.proxy.objectMapper", jsonMapper);
    EasyMock.expectLastCall();
    EasyMock.expect(requestMock.getRequestURI()).andReturn("/druid/v2/");
    EasyMock.expect(requestMock.getMethod()).andReturn("POST");
    EasyMock.expect(requestMock.getInputStream()).andReturn(servletInputStream);
    requestMock.setAttribute("org.apache.druid.proxy.query", query);
    requestMock.setAttribute("org.apache.druid.proxy.to.host", "1.2.3.4:9999");
    requestMock.setAttribute("org.apache.druid.proxy.to.host.scheme", "http");
    EasyMock.expectLastCall();
    EasyMock.replay(requestMock);

    final AtomicLong didService = new AtomicLong();
    final AsyncQueryForwardingServlet servlet = new AsyncQueryForwardingServlet(
        new MapQueryToolChestWarehouse(ImmutableMap.of()),
        jsonMapper,
        TestHelper.makeSmileMapper(),
        hostFinder,
        null,
        null,
        new NoopServiceEmitter(),
        new NoopRequestLogger(),
        new DefaultGenericQueryMetricsFactory(),
        new AuthenticatorMapper(ImmutableMap.of())
    )
    {
      @Override
      protected void doService(
          final HttpServletRequest request,
          final HttpServletResponse response
      )
      {
        didService.incrementAndGet();
      }
    };

    servlet.service(requestMock, null);

    // This test is mostly about verifying that the servlet calls the right methods the right number of times.
    EasyMock.verify(hostFinder, requestMock);
    Assert.assertEquals(1, didService.get());
  }

  private static Server makeTestDeleteServer(int port, final CountDownLatch latch)
  {
    Server server = new Server(port);
    ServletHandler handler = new ServletHandler();
    handler.addServletWithMapping(new ServletHolder(new HttpServlet()
    {
      @Override
      protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
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

      final QueryHostFinder hostFinder = new QueryHostFinder(null, new RendezvousHashAvaticaConnectionBalancer())
      {
        @Override
        public org.apache.druid.client.selector.Server pickServer(Query query)
        {
          return new TestServer("http", "localhost", node.getPlaintextPort());
        }

        @Override
        public org.apache.druid.client.selector.Server pickDefaultServer()
        {
          return new TestServer("http", "localhost", node.getPlaintextPort());
        }

        @Override
        public Collection<org.apache.druid.client.selector.Server> getAllServers()
        {
          return ImmutableList.of(
              new TestServer("http", "localhost", node.getPlaintextPort()),
              new TestServer("http", "localhost", port1),
              new TestServer("http", "localhost", port2)
          );
        }
      };

      ObjectMapper jsonMapper = injector.getInstance(ObjectMapper.class);
      ServletHolder holder = new ServletHolder(
          new AsyncQueryForwardingServlet(
              new MapQueryToolChestWarehouse(ImmutableMap.of()),
              jsonMapper,
              injector.getInstance(Key.get(ObjectMapper.class, Smile.class)),
              hostFinder,
              injector.getProvider(HttpClient.class),
              injector.getInstance(DruidHttpClientConfig.class),
              new NoopServiceEmitter(),
              new NoopRequestLogger(),
              new DefaultGenericQueryMetricsFactory(),
              new AuthenticatorMapper(ImmutableMap.of())
          )
          {
            @Override
            protected String rewriteURI(HttpServletRequest request, String scheme, String host)
            {
              String uri = super.rewriteURI(request, scheme, host);
              if (uri.contains("/druid/v2")) {
                return URI.create(StringUtils.replace(uri, "/druid/v2", "/default")).toString();
              }
              return URI.create(StringUtils.replace(uri, "/proxy", "")).toString();
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
      handlerList.setHandlers(
          new Handler[]{JettyServerInitUtils.wrapWithDefaultGzipHandler(
              root,
              ServerConfig.DEFAULT_GZIP_INFLATE_BUFFER_SIZE,
              Deflater.DEFAULT_COMPRESSION)}
      );
      server.setHandler(handlerList);
    }
  }

  @Test
  public void testRewriteURI()
  {

    // test params
    Assert.assertEquals(
        "http://localhost:1234/some/path?param=1",
        AsyncQueryForwardingServlet.makeURI("http", "localhost:1234", "/some/path", "param=1")
    );

    // HttpServletRequest.getQueryString returns encoded form
    // use ascii representation in case URI is using non-ascii characters
    Assert.assertEquals(
        "http://[2a00:1450:4007:805::1007]:1234/some/path?param=1&param2=%E2%82%AC",
        AsyncQueryForwardingServlet.makeURI(
            "http",
            HostAndPort.fromParts("2a00:1450:4007:805::1007", 1234).toString(),
            "/some/path",
            "param=1&param2=%E2%82%AC"
        )
    );

    // test null query
    Assert.assertEquals(
        "http://localhost/",
        AsyncQueryForwardingServlet.makeURI("http", "localhost", "/", null)
    );

    // Test reWrite Encoded interval with timezone info
    // decoded parameters 1900-01-01T00:00:00.000+01.00 -> 1900-01-01T00:00:00.000+01:00
    Assert.assertEquals(
        "http://localhost:1234/some/path?intervals=1900-01-01T00%3A00%3A00.000%2B01%3A00%2F3000-01-01T00%3A00%3A00.000%2B01%3A00",
        AsyncQueryForwardingServlet.makeURI(
            "http",
            "localhost:1234",
            "/some/path",
            "intervals=1900-01-01T00%3A00%3A00.000%2B01%3A00%2F3000-01-01T00%3A00%3A00.000%2B01%3A00"
        )
    );
  }

  private static class TestServer implements org.apache.druid.client.selector.Server
  {

    private final String scheme;
    private final String address;
    private final int port;

    public TestServer(String scheme, String address, int port)
    {
      this.scheme = scheme;
      this.address = address;
      this.port = port;
    }

    @Override
    public String getScheme()
    {
      return scheme;
    }

    @Override
    public String getHost()
    {
      return address + ":" + port;
    }

    @Override
    public String getAddress()
    {
      return address;
    }

    @Override
    public int getPort()
    {
      return port;
    }
  }
}
