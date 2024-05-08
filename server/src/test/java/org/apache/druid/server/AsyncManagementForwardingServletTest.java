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
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.commons.io.IOUtils;
import org.apache.druid.common.utils.SocketUtil;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.initialization.BaseJettyTest;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.initialization.jetty.JettyServerInitUtils;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.zip.Deflater;

public class AsyncManagementForwardingServletTest extends BaseJettyTest
{
  private static final ExpectedRequest COORDINATOR_EXPECTED_REQUEST = new ExpectedRequest();
  private static final ExpectedRequest OVERLORD_EXPECTED_REQUEST = new ExpectedRequest();

  private static int coordinatorPort;
  private static int overlordPort;
  private static boolean isValidLeader;

  private Server coordinator;
  private Server overlord;

  private static class ExpectedRequest
  {
    private boolean called = false;
    private String path;
    private String query;
    private String method;
    private Map<String, String> headers;
    private String body;

    private void reset()
    {
      called = false;
      path = null;
      query = null;
      method = null;
      headers = null;
      body = null;
    }
  }

  @Override
  @Before
  public void setup() throws Exception
  {
    super.setup();

    coordinatorPort = SocketUtil.findOpenPortFrom(port + 1);
    overlordPort = SocketUtil.findOpenPortFrom(coordinatorPort + 1);

    coordinator = makeTestServer(coordinatorPort, COORDINATOR_EXPECTED_REQUEST);
    overlord = makeTestServer(overlordPort, OVERLORD_EXPECTED_REQUEST);

    coordinator.start();
    overlord.start();
    isValidLeader = true;
  }

  @After
  public void tearDown() throws Exception
  {
    coordinator.stop();
    overlord.stop();

    COORDINATOR_EXPECTED_REQUEST.reset();
    OVERLORD_EXPECTED_REQUEST.reset();
    isValidLeader = true;
  }

  @Override
  protected Injector setupInjector()
  {
    return Initialization.makeInjectorWithModules(GuiceInjectors.makeStartupInjector(), ImmutableList.of((binder) -> {
      JsonConfigProvider.bindInstance(
          binder,
          Key.get(DruidNode.class, Self.class),
          new DruidNode("test", "localhost", false, null, null, true, false)
      );
      binder.bind(JettyServerInitializer.class).to(ProxyJettyServerInit.class).in(LazySingleton.class);
      LifecycleModule.register(binder, Server.class);
    }));
  }

  @Test
  public void testCoordinatorDatasources() throws Exception
  {
    COORDINATOR_EXPECTED_REQUEST.path = "/druid/coordinator/v1/datasources";
    COORDINATOR_EXPECTED_REQUEST.method = "GET";
    COORDINATOR_EXPECTED_REQUEST.headers = ImmutableMap.of("Authorization", "Basic bXl1c2VyOm15cGFzc3dvcmQ=");

    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d%s", port, COORDINATOR_EXPECTED_REQUEST.path))
            .openConnection());
    connection.setRequestMethod(COORDINATOR_EXPECTED_REQUEST.method);

    COORDINATOR_EXPECTED_REQUEST.headers.forEach(connection::setRequestProperty);

    Assert.assertEquals(200, connection.getResponseCode());
    Assert.assertTrue("coordinator called", COORDINATOR_EXPECTED_REQUEST.called);
    Assert.assertFalse("overlord called", OVERLORD_EXPECTED_REQUEST.called);
  }

  @Test
  public void testCoordinatorLoadStatus() throws Exception
  {
    COORDINATOR_EXPECTED_REQUEST.path = "/druid/coordinator/v1/loadstatus";
    COORDINATOR_EXPECTED_REQUEST.query = "full";
    COORDINATOR_EXPECTED_REQUEST.method = "GET";
    COORDINATOR_EXPECTED_REQUEST.headers = ImmutableMap.of("Authorization", "Basic bXl1c2VyOm15cGFzc3dvcmQ=");

    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format(
            "http://localhost:%d%s?%s", port, COORDINATOR_EXPECTED_REQUEST.path, COORDINATOR_EXPECTED_REQUEST.query
        )).openConnection());
    connection.setRequestMethod(COORDINATOR_EXPECTED_REQUEST.method);

    COORDINATOR_EXPECTED_REQUEST.headers.forEach(connection::setRequestProperty);

    Assert.assertEquals(200, connection.getResponseCode());
    Assert.assertTrue("coordinator called", COORDINATOR_EXPECTED_REQUEST.called);
    Assert.assertFalse("overlord called", OVERLORD_EXPECTED_REQUEST.called);
  }

  @Test
  public void testCoordinatorEnable() throws Exception
  {
    COORDINATOR_EXPECTED_REQUEST.path = "/druid/coordinator/v1/datasources/myDatasource";
    COORDINATOR_EXPECTED_REQUEST.method = "POST";

    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d%s", port, COORDINATOR_EXPECTED_REQUEST.path))
            .openConnection());
    connection.setRequestMethod(COORDINATOR_EXPECTED_REQUEST.method);

    Assert.assertEquals(200, connection.getResponseCode());
    Assert.assertTrue("coordinator called", COORDINATOR_EXPECTED_REQUEST.called);
    Assert.assertFalse("overlord called", OVERLORD_EXPECTED_REQUEST.called);
  }

  @Test
  public void testCoordinatorDisable() throws Exception
  {
    COORDINATOR_EXPECTED_REQUEST.path = "/druid/coordinator/v1/datasources/myDatasource/intervals/2016-06-27_2016-06-28";
    COORDINATOR_EXPECTED_REQUEST.method = "DELETE";

    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d%s", port, COORDINATOR_EXPECTED_REQUEST.path))
            .openConnection());
    connection.setRequestMethod(COORDINATOR_EXPECTED_REQUEST.method);

    Assert.assertEquals(200, connection.getResponseCode());
    Assert.assertTrue("coordinator called", COORDINATOR_EXPECTED_REQUEST.called);
    Assert.assertFalse("overlord called", OVERLORD_EXPECTED_REQUEST.called);
  }

  @Test
  public void testCoordinatorProxyStatus() throws Exception
  {
    COORDINATOR_EXPECTED_REQUEST.path = "/status";
    COORDINATOR_EXPECTED_REQUEST.method = "GET";
    COORDINATOR_EXPECTED_REQUEST.headers = ImmutableMap.of("Authorization", "Basic bXl1c2VyOm15cGFzc3dvcmQ=");

    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d/proxy/coordinator%s", port, COORDINATOR_EXPECTED_REQUEST.path))
            .openConnection());
    connection.setRequestMethod(COORDINATOR_EXPECTED_REQUEST.method);

    COORDINATOR_EXPECTED_REQUEST.headers.forEach(connection::setRequestProperty);

    Assert.assertEquals(200, connection.getResponseCode());
    Assert.assertTrue("coordinator called", COORDINATOR_EXPECTED_REQUEST.called);
    Assert.assertFalse("overlord called", OVERLORD_EXPECTED_REQUEST.called);
  }

  @Test
  public void testCoordinatorProxySegments() throws Exception
  {
    COORDINATOR_EXPECTED_REQUEST.path = "/druid/coordinator/v1/metadata/datasources/myDatasource/segments";
    COORDINATOR_EXPECTED_REQUEST.method = "POST";
    COORDINATOR_EXPECTED_REQUEST.headers = ImmutableMap.of("Authorization", "Basic bXl1c2VyOm15cGFzc3dvcmQ=");
    COORDINATOR_EXPECTED_REQUEST.body = "[\"2012-01-01T00:00:00.000/2012-01-03T00:00:00.000\", \"2012-01-05T00:00:00.000/2012-01-07T00:00:00.000\"]";

    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d/proxy/coordinator%s", port, COORDINATOR_EXPECTED_REQUEST.path))
            .openConnection());
    connection.setRequestMethod(COORDINATOR_EXPECTED_REQUEST.method);

    COORDINATOR_EXPECTED_REQUEST.headers.forEach(connection::setRequestProperty);

    connection.setDoOutput(true);
    OutputStream os = connection.getOutputStream();
    os.write(COORDINATOR_EXPECTED_REQUEST.body.getBytes(StandardCharsets.UTF_8));
    os.close();

    Assert.assertEquals(200, connection.getResponseCode());
    Assert.assertTrue("coordinator called", COORDINATOR_EXPECTED_REQUEST.called);
    Assert.assertFalse("overlord called", OVERLORD_EXPECTED_REQUEST.called);
  }

  @Test
  public void testOverlordPostTask() throws Exception
  {
    OVERLORD_EXPECTED_REQUEST.path = "/druid/indexer/v1/task";
    OVERLORD_EXPECTED_REQUEST.method = "POST";
    OVERLORD_EXPECTED_REQUEST.headers = ImmutableMap.of(
        "Authorization", "Basic bXl1c2VyOm15cGFzc3dvcmQ=",
        "Content-Type", "application/json"
    );
    OVERLORD_EXPECTED_REQUEST.body = "{\"type\": \"index\", \"spec\": \"stuffGoesHere\"}";

    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d%s", port, OVERLORD_EXPECTED_REQUEST.path))
            .openConnection());
    connection.setRequestMethod(OVERLORD_EXPECTED_REQUEST.method);

    OVERLORD_EXPECTED_REQUEST.headers.forEach(connection::setRequestProperty);

    connection.setDoOutput(true);
    OutputStream os = connection.getOutputStream();
    os.write(OVERLORD_EXPECTED_REQUEST.body.getBytes(StandardCharsets.UTF_8));
    os.close();

    Assert.assertEquals(200, connection.getResponseCode());
    Assert.assertFalse("coordinator called", COORDINATOR_EXPECTED_REQUEST.called);
    Assert.assertTrue("overlord called", OVERLORD_EXPECTED_REQUEST.called);
  }

  @Test
  public void testOverlordTaskStatus() throws Exception
  {
    OVERLORD_EXPECTED_REQUEST.path = "/druid/indexer/v1/task/myTaskId/status";
    OVERLORD_EXPECTED_REQUEST.method = "GET";
    OVERLORD_EXPECTED_REQUEST.headers = ImmutableMap.of("Authorization", "Basic bXl1c2VyOm15cGFzc3dvcmQ=");

    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d%s", port, OVERLORD_EXPECTED_REQUEST.path))
            .openConnection());
    connection.setRequestMethod(OVERLORD_EXPECTED_REQUEST.method);

    OVERLORD_EXPECTED_REQUEST.headers.forEach(connection::setRequestProperty);

    Assert.assertEquals(200, connection.getResponseCode());
    Assert.assertFalse("coordinator called", COORDINATOR_EXPECTED_REQUEST.called);
    Assert.assertTrue("overlord called", OVERLORD_EXPECTED_REQUEST.called);
  }

  @Test
  public void testOverlordProxyLeader() throws Exception
  {
    OVERLORD_EXPECTED_REQUEST.path = "/druid/indexer/v1/leader";
    OVERLORD_EXPECTED_REQUEST.method = "GET";
    OVERLORD_EXPECTED_REQUEST.headers = ImmutableMap.of("Authorization", "Basic bXl1c2VyOm15cGFzc3dvcmQ=");

    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d/proxy/overlord%s", port, OVERLORD_EXPECTED_REQUEST.path))
            .openConnection());
    connection.setRequestMethod(OVERLORD_EXPECTED_REQUEST.method);

    OVERLORD_EXPECTED_REQUEST.headers.forEach(connection::setRequestProperty);

    Assert.assertEquals(200, connection.getResponseCode());
    Assert.assertFalse("coordinator called", COORDINATOR_EXPECTED_REQUEST.called);
    Assert.assertTrue("overlord called", OVERLORD_EXPECTED_REQUEST.called);
  }

  @Test
  public void testProxyEnebledCheck() throws Exception
  {
    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d/proxy/enabled", port)).openConnection());
    connection.setRequestMethod("GET");

    Assert.assertEquals(200, connection.getResponseCode());
    byte[] bytes = new byte[connection.getContentLength()];
    Assert.assertEquals(connection.getInputStream().read(bytes), connection.getContentLength());
    Assert.assertEquals(ImmutableMap.of("enabled", true), new ObjectMapper().readValue(bytes, Map.class));
    Assert.assertFalse("coordinator called", COORDINATOR_EXPECTED_REQUEST.called);
    Assert.assertFalse("overlord called", OVERLORD_EXPECTED_REQUEST.called);
  }

  @Test
  public void testBadProxyDestination() throws Exception
  {
    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d/proxy/other/status", port)).openConnection());
    connection.setRequestMethod("GET");

    Assert.assertEquals(400, connection.getResponseCode());
    Assert.assertFalse("coordinator called", COORDINATOR_EXPECTED_REQUEST.called);
    Assert.assertFalse("overlord called", OVERLORD_EXPECTED_REQUEST.called);
  }

  @Test
  public void testCoordinatorLeaderUnknown() throws Exception
  {
    isValidLeader = false;
    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d/druid/coordinator", port)).openConnection());
    connection.setRequestMethod("GET");

    Assert.assertEquals(503, connection.getResponseCode());
    Assert.assertFalse("coordinator called", COORDINATOR_EXPECTED_REQUEST.called);
    Assert.assertFalse("overlord called", OVERLORD_EXPECTED_REQUEST.called);
  }

  @Test
  public void testOverlordLeaderUnknown() throws Exception
  {
    isValidLeader = false;
    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d/druid/indexer", port)).openConnection());
    connection.setRequestMethod("GET");

    Assert.assertEquals(503, connection.getResponseCode());
    Assert.assertFalse("coordinator called", COORDINATOR_EXPECTED_REQUEST.called);
    Assert.assertFalse("overlord called", OVERLORD_EXPECTED_REQUEST.called);
    isValidLeader = true;
  }

  @Test
  public void testUnsupportedProxyDestination() throws Exception
  {
    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d/proxy/other/status2", port)).openConnection());
    connection.setRequestMethod("GET");

    Assert.assertEquals(400, connection.getResponseCode());
    Assert.assertFalse("coordinator called", COORDINATOR_EXPECTED_REQUEST.called);
    Assert.assertFalse("overlord called", OVERLORD_EXPECTED_REQUEST.called);
  }

  @Test
  public void testLocalRequest() throws Exception
  {
    HttpURLConnection connection = ((HttpURLConnection)
        new URL(StringUtils.format("http://localhost:%d/status", port)).openConnection());
    connection.setRequestMethod("GET");

    Assert.assertEquals(404, connection.getResponseCode());
    Assert.assertFalse("coordinator called", COORDINATOR_EXPECTED_REQUEST.called);
    Assert.assertFalse("overlord called", OVERLORD_EXPECTED_REQUEST.called);
  }

  private static Server makeTestServer(int port, ExpectedRequest expectedRequest)
  {
    Server server = new Server(port);
    ServletHandler handler = new ServletHandler();
    handler.addServletWithMapping(new ServletHolder(new HttpServlet()
    {
      @Override
      protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException
      {
        handle(req, resp);
      }

      @Override
      protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException
      {
        handle(req, resp);
      }

      @Override
      protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws IOException
      {
        handle(req, resp);
      }

      @Override
      protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws IOException
      {
        handle(req, resp);
      }

      private void handle(HttpServletRequest req, HttpServletResponse resp) throws IOException
      {
        boolean passed = expectedRequest.path.equals(req.getRequestURI());
        passed &= expectedRequest.query == null || expectedRequest.query.equals(req.getQueryString());
        passed &= expectedRequest.method.equals(req.getMethod());

        if (expectedRequest.headers != null) {
          for (Map.Entry<String, String> header : expectedRequest.headers.entrySet()) {
            passed &= header.getValue().equals(req.getHeader(header.getKey()));
          }
        }

        passed &= expectedRequest.body == null || expectedRequest.body.equals(IOUtils.toString(req.getReader()));

        expectedRequest.called = true;
        resp.setStatus(passed ? 200 : 400);
      }
    }), "/*");

    server.setHandler(handler);
    return server;
  }

  public static class ProxyJettyServerInit implements JettyServerInitializer
  {
    @Override
    public void initialize(Server server, Injector injector)
    {
      final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
      root.addServlet(new ServletHolder(new DefaultServlet()), "/*");

      final DruidLeaderSelector coordinatorLeaderSelector = new TestDruidLeaderSelector()
      {
        @Override
        public String getCurrentLeader()
        {
          if (isValidLeader) {
            return StringUtils.format("http://localhost:%d", coordinatorPort);
          } else {
            return null;
          }
        }
      };

      final DruidLeaderSelector overlordLeaderSelector = new TestDruidLeaderSelector()
      {
        @Override
        public String getCurrentLeader()
        {
          if (isValidLeader) {
            return StringUtils.format("http://localhost:%d", overlordPort);
          } else {
            return null;
          }
        }
      };

      ServletHolder holder = new ServletHolder(
          new AsyncManagementForwardingServlet(
              injector.getInstance(ObjectMapper.class),
              injector.getProvider(HttpClient.class),
              injector.getInstance(DruidHttpClientConfig.class),
              coordinatorLeaderSelector,
              overlordLeaderSelector
          )
      );

      //NOTE: explicit maxThreads to workaround https://tickets.puppetlabs.com/browse/TK-152
      holder.setInitParameter("maxThreads", "256");

      root.addServlet(holder, "/druid/coordinator/*");
      root.addServlet(holder, "/druid/indexer/*");
      root.addServlet(holder, "/proxy/*");

      JettyServerInitUtils.addExtensionFilters(root, injector);

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

  private static class TestDruidLeaderSelector implements DruidLeaderSelector
  {
    @Nullable
    @Override
    public String getCurrentLeader()
    {
      return null;
    }

    @Override
    public boolean isLeader()
    {
      return false;
    }

    @Override
    public int localTerm()
    {
      return 0;
    }

    @Override
    public void registerListener(Listener listener)
    {
    }

    @Override
    public void unregisterListener()
    {
    }
  }
}
