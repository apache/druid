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

package org.apache.druid.extensions.watermarking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.container.grizzly2.GrizzlyServerFactory;
import com.sun.jersey.api.core.ClassNamesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.core.spi.component.ioc.IoCComponentProviderFactory;
import com.sun.jersey.guice.spi.container.GuiceComponentProviderFactory;
import com.sun.jersey.spi.inject.SingletonTypeInjectableProvider;
import org.apache.druid.extensions.watermarking.http.WatermarkKeeperResource;
import org.apache.druid.extensions.watermarking.storage.WatermarkSink;
import org.apache.druid.extensions.watermarking.storage.WatermarkSource;
import org.apache.druid.extensions.watermarking.watermarks.BatchCompletenessLowWatermark;
import org.apache.druid.extensions.watermarking.watermarks.StableDataHighWatermark;
import org.apache.druid.extensions.watermarking.watermarks.WatermarkCursors;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.server.security.AuthorizerMapper;
import org.easymock.EasyMock;
import org.glassfish.grizzly.http.server.HttpServer;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class WatermarkKeeperServerIntegrationTest extends TimelineMetadataCollectorTestBase
{
  private static final URI BASE_URI = getBaseURI();
  private static final String testDatasource = "testDatasource";
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final TypeFactory FACTORY = TypeFactory.defaultInstance();
  private HttpServer server;

  private static URI getBaseURI()
  {
    return UriBuilder.fromUri("http://localhost/").port(9998).build();
  }

  @Before
  public void startServer() throws Exception
  {
    setupStore();

    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(binder -> {
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to(
              WatermarkKeeper.SERVICE_NAME
          );
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8080);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8081);
          binder.bind(Key.get(WatermarkSource.class)).toInstance(timelineStore);
          binder.bind(Key.get(WatermarkSink.class)).toInstance(timelineStore);
          binder.bind(Key.get(ServiceEmitter.class)).toInstance(new NoopServiceEmitter());
          binder.bind(Key.get(AuthConfig.class)).toInstance(new AuthConfig());
          binder.bind(AuthorizerMapper.class).toInstance(AuthTestUtils.TEST_AUTHORIZER_MAPPER);
          binder.bind(AuthenticatorMapper.class).toInstance(AuthTestUtils.TEST_AUTHENTICATOR_MAPPER);
          binder.bind(Key.get(HttpClient.class, org.apache.druid.guice.annotations.Client.class))
                .toInstance(EasyMock.createMock(HttpClient.class));
          binder.install(new WatermarkCursors());
        })
    );

    ResourceConfig rc = new ClassNamesResourceConfig(
        WatermarkKeeperResource.class.getName()
        + ';'
        + MockHttpServletRequest.class.getName()
    );
    IoCComponentProviderFactory ioc = new GuiceComponentProviderFactory(rc, injector);
    server = GrizzlyServerFactory.createHttpServer(BASE_URI, rc, ioc);
  }

  @After
  public void stopServer()
  {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void testGetDatasourceEmpty() throws IOException
  {
    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(getBaseURI());

    ClientResponse resp = service.path("druid/watermarking/v1/keeper/datasources")
                                 .path(testDatasource)
                                 .accept(MediaType.APPLICATION_JSON)
                                 .get(ClientResponse.class);
    Assert.assertEquals(204, resp.getStatus());
  }

  @Test
  public void testGetDatasourceNotEmpty() throws IOException
  {
    DateTime t1 = DateTimes.nowUtc();
    DateTime t2 = t1.minusHours(5);
    timelineStore.update(testDatasource, StableDataHighWatermark.TYPE, t1);
    timelineStore.update(testDatasource, BatchCompletenessLowWatermark.TYPE, t2);
    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(getBaseURI());

    ClientResponse resp = service.path("druid/watermarking/v1/keeper/datasources")
                                 .path(testDatasource)
                                 .accept(MediaType.APPLICATION_JSON)
                                 .get(ClientResponse.class);

    Assert.assertEquals(200, resp.getStatus());

    String text = resp.getEntity(String.class);

    Map<String, DateTime> actual = MAPPER.readValue(
        text,
        FACTORY.constructMapType(Map.class, String.class, DateTime.class)
    );
    Map<String, DateTime> expected = ImmutableMap.of(
        StableDataHighWatermark.TYPE,
        t1,
        BatchCompletenessLowWatermark.TYPE,
        t2
    );


    for (String key : expected.keySet()) {
      Assert.assertEquals(expected.get(key).getMillis(), actual.get(key).getMillis());
    }
  }

  @Test
  public void testGetDatasourceKeyEmpty() throws IOException
  {
    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(getBaseURI());

    ClientResponse resp = service.path("druid/watermarking/v1/keeper/datasources")
                                 .path(testDatasource)
                                 .path(StableDataHighWatermark.TYPE)
                                 .accept(MediaType.APPLICATION_JSON)
                                 .get(ClientResponse.class);
    Assert.assertEquals(204, resp.getStatus());
  }

  @Test
  public void testGetDatasourceKeyNotEmpty() throws IOException
  {
    DateTime t1 = DateTimes.nowUtc();
    DateTime t2 = t1.minusHours(5);
    timelineStore.update(testDatasource, StableDataHighWatermark.TYPE, t2);
    timelineStore.update(testDatasource, StableDataHighWatermark.TYPE, t1);
    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(getBaseURI());

    ClientResponse resp = service.path("druid/watermarking/v1/keeper/datasources")
                                 .path(testDatasource)
                                 .path(StableDataHighWatermark.TYPE)
                                 .accept(MediaType.APPLICATION_JSON)
                                 .get(ClientResponse.class);

    Assert.assertEquals(200, resp.getStatus());

    String text = resp.getEntity(String.class);

    Map<String, DateTime> actual = MAPPER.readValue(
        text,
        FACTORY.constructMapType(Map.class, String.class, DateTime.class)
    );
    Map<String, DateTime> expected = ImmutableMap.of(
        StableDataHighWatermark.TYPE,
        t1
    );

    for (String key : expected.keySet()) {
      Assert.assertEquals(expected.get(key).getMillis(), actual.get(key).getMillis());
    }
  }

  @Test
  public void testGetDatasourceHistoryEmpty() throws IOException
  {
    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(getBaseURI());

    ClientResponse resp = service.path("druid/watermarking/v1/keeper/datasources")
                                 .path(testDatasource)
                                 .path(StableDataHighWatermark.TYPE)
                                 .path("history")
                                 .accept(MediaType.APPLICATION_JSON)
                                 .get(ClientResponse.class);
    Assert.assertEquals(204, resp.getStatus());
  }

  @Test
  public void testGetDatasourceHistoryNotEmpty() throws IOException
  {
    DateTime t1 = DateTimes.nowUtc();
    DateTime t2 = t1.minusHours(5);
    timelineStore.update(testDatasource, StableDataHighWatermark.TYPE, t2);
    timelineStore.update(testDatasource, StableDataHighWatermark.TYPE, t1);
    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(getBaseURI());

    ClientResponse resp = service.path("druid/watermarking/v1/keeper/datasources")
                                 .path(testDatasource)
                                 .path(StableDataHighWatermark.TYPE)
                                 .path("history")
                                 .accept(MediaType.APPLICATION_JSON)
                                 .get(ClientResponse.class);

    Assert.assertEquals(200, resp.getStatus());

    String text = resp.getEntity(String.class);

    // todo: deserialize as List<Map<String,DateTime>> instead?
    List<Map<String, String>> actual = MAPPER.readValue(text, FACTORY.constructCollectionType(List.class, Map.class));

    Assert.assertEquals(t2.toString(ISODateTimeFormat.dateTime()), actual.get(0).get("timestamp"));
    Assert.assertEquals(t1.toString(ISODateTimeFormat.dateTime()), actual.get(1).get("timestamp"));
  }

  @Provider
  public static class MockHttpServletRequest extends
      SingletonTypeInjectableProvider<Context, HttpServletRequest>
  {
    public MockHttpServletRequest()
    {
      super(
          HttpServletRequest.class,
          createMockRequest()
      );
    }

    static HttpServletRequest createMockRequest()
    {
      HttpServletRequest mockRequest = EasyMock.createNiceMock(HttpServletRequest.class);
      AuthenticationResult authenticationResult = new AuthenticationResult(
          "druid",
          "druid",
          null,
          Collections.emptyMap()
      );

      EasyMock.expect(mockRequest.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).anyTimes();
      EasyMock.expect(mockRequest.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
              .andReturn(authenticationResult)
              .anyTimes();
      EasyMock.replay(mockRequest);
      return mockRequest;
    }
  }
}
