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

package org.apache.druid.discovery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.servlet.GuiceFilter;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.initialization.BaseJettyTest;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.easymock.EasyMock;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 */
public class DruidLeaderClientTest extends BaseJettyTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private DiscoveryDruidNode discoveryDruidNode;
  private HttpClient httpClient;

  @Override
  protected Injector setupInjector()
  {
    final DruidNode node = new DruidNode("test", "localhost", false, null, null, true, false);
    discoveryDruidNode = new DiscoveryDruidNode(node, NodeRole.PEON, ImmutableMap.of());

    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder,
                    Key.get(DruidNode.class, Self.class),
                    node
                );
                binder.bind(Integer.class).annotatedWith(Names.named("port")).toInstance(node.getPlaintextPort());
                binder.bind(JettyServerInitializer.class).to(TestJettyServerInitializer.class).in(LazySingleton.class);
                Jerseys.addResource(binder, SimpleResource.class);
                LifecycleModule.register(binder, Server.class);
              }
            }
        )
    );
    httpClient = injector.getInstance(ClientHolder.class).getClient();
    return injector;
  }

  @Test
  public void testSimple() throws Exception
  {
    DruidNodeDiscovery druidNodeDiscovery = EasyMock.createMock(DruidNodeDiscovery.class);
    EasyMock.expect(druidNodeDiscovery.getAllNodes()).andReturn(
        ImmutableList.of(discoveryDruidNode)
    );

    DruidNodeDiscoveryProvider druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForNodeRole(NodeRole.PEON)).andReturn(druidNodeDiscovery);

    EasyMock.replay(druidNodeDiscovery, druidNodeDiscoveryProvider);

    DruidLeaderClient druidLeaderClient = new DruidLeaderClient(
        httpClient,
        druidNodeDiscoveryProvider,
        NodeRole.PEON,
        "/simple/leader"
    );
    druidLeaderClient.start();

    Request request = druidLeaderClient.makeRequest(HttpMethod.POST, "/simple/direct");
    request.setContent("hello".getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals("hello", druidLeaderClient.go(request).getContent());
  }

  @Test
  public void testNoLeaderFound() throws Exception
  {
    DruidNodeDiscovery druidNodeDiscovery = EasyMock.createMock(DruidNodeDiscovery.class);
    EasyMock.expect(druidNodeDiscovery.getAllNodes()).andReturn(ImmutableList.of());

    DruidNodeDiscoveryProvider druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForNodeRole(NodeRole.PEON)).andReturn(druidNodeDiscovery);

    EasyMock.replay(druidNodeDiscovery, druidNodeDiscoveryProvider);

    DruidLeaderClient druidLeaderClient = new DruidLeaderClient(
        httpClient,
        druidNodeDiscoveryProvider,
        NodeRole.PEON,
        "/simple/leader"
    );
    druidLeaderClient.start();

    expectedException.expect(IOException.class);
    expectedException.expectMessage("No known server");
    druidLeaderClient.makeRequest(HttpMethod.POST, "/simple/direct");
  }

  @Test
  public void testRedirection() throws Exception
  {
    DruidNodeDiscovery druidNodeDiscovery = EasyMock.createMock(DruidNodeDiscovery.class);
    EasyMock.expect(druidNodeDiscovery.getAllNodes()).andReturn(
        ImmutableList.of(discoveryDruidNode)
    );

    DruidNodeDiscoveryProvider druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForNodeRole(NodeRole.PEON)).andReturn(druidNodeDiscovery);

    EasyMock.replay(druidNodeDiscovery, druidNodeDiscoveryProvider);

    DruidLeaderClient druidLeaderClient = new DruidLeaderClient(
        httpClient,
        druidNodeDiscoveryProvider,
        NodeRole.PEON,
        "/simple/leader"
    );
    druidLeaderClient.start();

    Request request = druidLeaderClient.makeRequest(HttpMethod.POST, "/simple/redirect");
    request.setContent("hello".getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals("hello", druidLeaderClient.go(request).getContent());
  }

  @Test
  public void testServerFailureAndRedirect() throws Exception
  {
    DruidNodeDiscovery druidNodeDiscovery = EasyMock.createMock(DruidNodeDiscovery.class);
    DiscoveryDruidNode dummyNode = new DiscoveryDruidNode(
        new DruidNode("test", "dummyhost", false, 64231, null, true, false),
        NodeRole.PEON,
        ImmutableMap.of()
    );
    EasyMock.expect(druidNodeDiscovery.getAllNodes()).andReturn(ImmutableList.of(dummyNode));
    EasyMock.expect(druidNodeDiscovery.getAllNodes()).andReturn(ImmutableList.of(discoveryDruidNode));

    DruidNodeDiscoveryProvider druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForNodeRole(NodeRole.PEON)).andReturn(druidNodeDiscovery).anyTimes();

    EasyMock.replay(druidNodeDiscovery, druidNodeDiscoveryProvider);

    DruidLeaderClient druidLeaderClient = new DruidLeaderClient(
        httpClient,
        druidNodeDiscoveryProvider,
        NodeRole.PEON,
        "/simple/leader"
    );
    druidLeaderClient.start();

    Request request = druidLeaderClient.makeRequest(HttpMethod.POST, "/simple/redirect");
    request.setContent("hello".getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals("hello", druidLeaderClient.go(request).getContent());
  }

  @Test
  public void testFindCurrentLeader()
  {
    DruidNodeDiscovery druidNodeDiscovery = EasyMock.createMock(DruidNodeDiscovery.class);
    EasyMock.expect(druidNodeDiscovery.getAllNodes()).andReturn(
        ImmutableList.of(discoveryDruidNode)
    );

    DruidNodeDiscoveryProvider druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForNodeRole(NodeRole.PEON)).andReturn(druidNodeDiscovery);

    EasyMock.replay(druidNodeDiscovery, druidNodeDiscoveryProvider);

    DruidLeaderClient druidLeaderClient = new DruidLeaderClient(
        httpClient,
        druidNodeDiscoveryProvider,
        NodeRole.PEON,
        "/simple/leader"
    );
    druidLeaderClient.start();

    Assert.assertEquals("http://localhost:1234/", druidLeaderClient.findCurrentLeader());
  }

  private static class TestJettyServerInitializer implements JettyServerInitializer
  {
    @Override
    public void initialize(Server server, Injector injector)
    {
      final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
      root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
      root.addFilter(GuiceFilter.class, "/*", null);

      final HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(new Handler[]{root});
      server.setHandler(handlerList);
    }
  }

  @Path("/simple")
  public static class SimpleResource
  {
    private final int port;

    @Inject
    public SimpleResource(@Named("port") int port)
    {
      this.port = port;
    }

    @POST
    @Path("/direct")
    @Produces(MediaType.APPLICATION_JSON)
    public Response direct(String input)
    {
      if ("hello".equals(input)) {
        return Response.ok("hello").build();
      } else {
        return Response.serverError().build();
      }
    }

    @POST
    @Path("/redirect")
    @Produces(MediaType.APPLICATION_JSON)
    public Response redirecting() throws Exception
    {
      return Response.temporaryRedirect(new URI(StringUtils.format("http://localhost:%s/simple/direct", port))).build();
    }

    @GET
    @Path("/leader")
    @Produces(MediaType.APPLICATION_JSON)
    public Response leader()
    {
      return Response.ok("http://localhost:1234/").build();
    }
  }
}
