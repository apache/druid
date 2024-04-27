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
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.initialization.BaseJettyTest;
import org.apache.druid.server.initialization.jetty.JettyServerInitializer;
import org.easymock.EasyMock;
import org.eclipse.jetty.server.Server;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;

public class BrokerClientTest extends BaseJettyTest
{
  private DiscoveryDruidNode discoveryDruidNode;
  private HttpClient httpClient;

  @Override
  protected Injector setupInjector()
  {
    final DruidNode node = new DruidNode("test", "localhost", false, null, null, true, false);
    discoveryDruidNode = new DiscoveryDruidNode(node, NodeRole.BROKER, ImmutableMap.of());

    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.of(
            binder -> {
              JsonConfigProvider.bindInstance(
                  binder,
                  Key.get(DruidNode.class, Self.class),
                  node
              );
              binder.bind(Integer.class).annotatedWith(Names.named("port")).toInstance(node.getPlaintextPort());
              binder.bind(JettyServerInitializer.class).to(DruidLeaderClientTest.TestJettyServerInitializer.class).in(LazySingleton.class);
              Jerseys.addResource(binder, SimpleResource.class);
              LifecycleModule.register(binder, Server.class);
            }
        )
    );
    httpClient = injector.getInstance(BaseJettyTest.ClientHolder.class).getClient();
    return injector;
  }

  @Test
  public void testSimple() throws Exception
  {
    DruidNodeDiscovery druidNodeDiscovery = EasyMock.createMock(DruidNodeDiscovery.class);
    EasyMock.expect(druidNodeDiscovery.getAllNodes()).andReturn(ImmutableList.of(discoveryDruidNode)).anyTimes();

    DruidNodeDiscoveryProvider druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForNodeRole(NodeRole.BROKER)).andReturn(druidNodeDiscovery);

    EasyMock.replay(druidNodeDiscovery, druidNodeDiscoveryProvider);

    BrokerClient brokerClient = new BrokerClient(
        httpClient,
        druidNodeDiscoveryProvider
    );

    Request request = brokerClient.makeRequest(HttpMethod.POST, "/simple/direct");
    request.setContent("hello".getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals("hello", brokerClient.sendQuery(request));
  }

  @Test
  public void testError() throws Exception
  {
    DruidNodeDiscovery druidNodeDiscovery = EasyMock.createMock(DruidNodeDiscovery.class);
    EasyMock.expect(druidNodeDiscovery.getAllNodes()).andReturn(ImmutableList.of(discoveryDruidNode)).anyTimes();

    DruidNodeDiscoveryProvider druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForNodeRole(NodeRole.BROKER)).andReturn(druidNodeDiscovery);

    EasyMock.replay(druidNodeDiscovery, druidNodeDiscoveryProvider);

    BrokerClient brokerClient = new BrokerClient(
        httpClient,
        druidNodeDiscoveryProvider
    );

    Request request = brokerClient.makeRequest(HttpMethod.POST, "/simple/flakey");
    request.setContent("hello".getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals("hello", brokerClient.sendQuery(request));
  }

  @Path("/simple")
  public static class SimpleResource
  {
    private static int attempt = 0;

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
    @Path("/flakey")
    @Produces(MediaType.APPLICATION_JSON)
    public Response redirecting()
    {
      if (attempt > 2) {
        return Response.ok("hello").build();
      } else {
        attempt += 1;
        return Response.status(504).build();
      }
    }
  }
}
