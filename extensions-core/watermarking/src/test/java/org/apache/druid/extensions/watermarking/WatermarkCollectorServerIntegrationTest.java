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
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.extensions.timeline.metadata.TimelineMetadataCollectorServerView;
import org.apache.druid.extensions.watermarking.gaps.BatchGapDetector;
import org.apache.druid.extensions.watermarking.gaps.GapDetectors;
import org.apache.druid.extensions.watermarking.gaps.SegmentGapDetector;
import org.apache.druid.extensions.watermarking.http.WatermarkCollectorResource;
import org.apache.druid.extensions.watermarking.storage.WatermarkSink;
import org.apache.druid.extensions.watermarking.storage.WatermarkSource;
import org.apache.druid.extensions.watermarking.watermarks.DataCompletenessLowWatermark;
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
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.server.security.AuthorizerMapper;
import org.easymock.EasyMock;
import org.glassfish.grizzly.http.server.HttpServer;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class WatermarkCollectorServerIntegrationTest extends TimelineMetadataCollectorTestBase
{
  private static final URI BASE_URI = getBaseURI();
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final TypeFactory TYPE_FACTORY = TypeFactory.defaultInstance();
  private HttpServer server;

  private static URI getBaseURI()
  {
    return UriBuilder.fromUri("http://localhost/").port(9998).build();
  }

  @Before
  public void startServer() throws Exception
  {

    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(initBatchSegmentsWithGap.size());
    segmentRemovedLatch = new CountDownLatch(0);

    addSegments(
        initBatchSegmentsWithGap
    );

    setupViews();

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(binder -> {
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to(
              WatermarkCollector.SERVICE_NAME
          );
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8080);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8081);
          binder.bind(Key.get(WatermarkSource.class)).toInstance(timelineStore);
          binder.bind(Key.get(WatermarkSink.class)).toInstance(timelineStore);
          binder.bind(TimelineMetadataCollectorServerView.class).toInstance(timelineMetadataCollectorServerView);
          binder.bind(TimelineServerView.class).to(TimelineMetadataCollectorServerView.class);

          binder.bind(Key.get(ServiceEmitter.class)).toInstance(new NoopServiceEmitter());
          binder.bind(Key.get(AuthConfig.class)).toInstance(new AuthConfig());
          binder.bind(AuthorizerMapper.class).toInstance(AuthTestUtils.TEST_AUTHORIZER_MAPPER);
          binder.bind(AuthenticatorMapper.class).toInstance(AuthTestUtils.TEST_AUTHENTICATOR_MAPPER);
          binder.bind(Key.get(HttpClient.class, org.apache.druid.guice.annotations.Client.class))
                .toInstance(EasyMock.createMock(HttpClient.class));
          binder.install(new WatermarkCursors());
          binder.install(new GapDetectors());
        })
    );

    ResourceConfig rc = new ClassNamesResourceConfig(
        WatermarkCollectorResource.class.getName()
        + ';'
        + WatermarkKeeperServerIntegrationTest.MockHttpServletRequest.class.getName()
    );
    IoCComponentProviderFactory ioc = new GuiceComponentProviderFactory(rc, injector);
    server = GrizzlyServerFactory.createHttpServer(BASE_URI, rc, ioc);


    super.setUp();

  }


  @After
  public void stopServer()
  {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void testLoadStatus() throws Exception
  {
    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(getBaseURI());

    ClientResponse resp = service.path("druid/watermarking/v1/collector/loadstatus")
                                 .accept(MediaType.APPLICATION_JSON)
                                 .get(ClientResponse.class);

    String text = resp.getEntity(String.class);
    final String expected = "{"
                            + "\"inventoryInitialized\":true"
                            + "}";

    Assert.assertEquals(200, resp.getStatus());
    Assert.assertEquals(expected, text);
  }

  @Test
  public void testWatermarks() throws Exception
  {
    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(getBaseURI());

    ClientResponse resp = service.path("druid/watermarking/v1/collector/watermarks")
                                 .accept(MediaType.APPLICATION_JSON)
                                 .get(ClientResponse.class);

    String text = resp.getEntity(String.class);
    final String expected = "{"
                            + "\"inventoryInitialized\":true,"
                            + "\"watermarks\":{"
                            + "\"" + testDatasource + "\":{"
                            + "\"batch_low\":\"2011-04-06T00:00:00.000Z\","
                            + "\"mintime\":\"2011-04-01T00:00:00.000Z\","
                            + "\"batch_high\":\"2011-04-09T00:00:00.000Z\","
                            + "\"stable_low\":\"2011-04-06T00:00:00.000Z\","
                            + "\"stable_high\":\"2011-04-09T00:00:00.000Z\","
                            + "\"maxtime\":\"2011-04-09T00:00:00.000Z\"}}}";

    Assert.assertEquals(200, resp.getStatus());
    Assert.assertEquals(expected, text);
  }

  @Test
  public void testStatus() throws Exception
  {
    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(getBaseURI());

    ClientResponse resp = service.path("druid/watermarking/v1/collector/status")
                                 .accept(MediaType.APPLICATION_JSON)
                                 .get(ClientResponse.class);

    String text = resp.getEntity(String.class);
    final String expected = "{"
                            + "\"inventoryInitialized\":true,"
                            + "\"datasources\":{"
                            + "\"" + testDatasource + "\""
                            + ":{\"watermarks\":{\""
                            + "batch_low\":\"2011-04-06T00:00:00.000Z\","
                            + "\"mintime\":\"2011-04-01T00:00:00.000Z\","
                            + "\"batch_high\":\"2011-04-09T00:00:00.000Z\","
                            + "\"stable_low\":\"2011-04-06T00:00:00.000Z\","
                            + "\"stable_high\":\"2011-04-09T00:00:00.000Z\","
                            + "\"maxtime\":\"2011-04-09T00:00:00.000Z\""
                            + "},\"gaps\":{"
                            + "\"data\":[\"2011-04-06T00:00:00.000Z/2011-04-07T00:00:00.000Z\"],"
                            + "\"batch\":[\"2011-04-06T00:00:00.000Z/2011-04-07T00:00:00.000Z\"]"
                            + "}}}}";

    Assert.assertEquals(200, resp.getStatus());
    Assert.assertEquals(expected, text);
  }

  @Test
  public void testDatasourceStatus() throws Exception
  {
    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(getBaseURI());

    ClientResponse resp = service.path("druid/watermarking/v1/collector/datasources/" + testDatasource + "/status")
                                 .accept(MediaType.APPLICATION_JSON)
                                 .get(ClientResponse.class);

    String text = resp.getEntity(String.class);
    final String expected = "{"
                            + "\"inventoryInitialized\":true,"
                            + "\"" + testDatasource + "\""
                            + ":{\"watermarks\":{\""
                            + "batch_low\":\"2011-04-06T00:00:00.000Z\","
                            + "\"mintime\":\"2011-04-01T00:00:00.000Z\","
                            + "\"batch_high\":\"2011-04-09T00:00:00.000Z\","
                            + "\"stable_low\":\"2011-04-06T00:00:00.000Z\","
                            + "\"stable_high\":\"2011-04-09T00:00:00.000Z\","
                            + "\"maxtime\":\"2011-04-09T00:00:00.000Z\""
                            + "},\"gaps\":{"
                            + "\"data\":[\"2011-04-06T00:00:00.000Z/2011-04-07T00:00:00.000Z\"],"
                            + "\"batch\":[\"2011-04-06T00:00:00.000Z/2011-04-07T00:00:00.000Z\"]"
                            + "}}}";

    Assert.assertEquals(200, resp.getStatus());
    Assert.assertEquals(expected, text);
  }

  @Test
  public void testKeeperResourcesAvailable() throws IOException
  {
    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(getBaseURI());

    ClientResponse resp = service.path("druid/watermarking/v1/collector/datasources")
                                 .path(testDatasource)
                                 .accept(MediaType.APPLICATION_JSON)
                                 .get(ClientResponse.class);
    Assert.assertEquals(200, resp.getStatus());
  }

  @Test
  public void testUpdateDatasourceKeyNoTimestamp()
  {
    setupMockView();

    DateTime t1 = DateTimes.nowUtc();
    DateTime t2 = t1.minusHours(5);
    timelineStore.update(testDatasource, DataCompletenessLowWatermark.TYPE, t2);

    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(getBaseURI());

    ClientResponse resp = service.path("druid/watermarking/v1/collector/datasources")
                                 .path(testDatasource)
                                 .path(DataCompletenessLowWatermark.TYPE)
                                 .accept(MediaType.APPLICATION_JSON)
                                 .post(ClientResponse.class);

    Assert.assertEquals(400, resp.getStatus());
  }

  @Test
  public void testUpdateDatasourceKeyBadTimestamp()
  {
    setupMockView();

    DateTime t1 = DateTimes.nowUtc();
    DateTime t2 = t1.minusHours(5);
    timelineStore.update(testDatasource, DataCompletenessLowWatermark.TYPE, t2);

    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(getBaseURI());

    ClientResponse resp = service.path("druid/watermarking/v1/collector/datasources")
                                 .path(testDatasource)
                                 .path(DataCompletenessLowWatermark.TYPE)
                                 .queryParam("timestamp", "NOT A REAL DATE")
                                 .accept(MediaType.APPLICATION_JSON)
                                 .post(ClientResponse.class);

    Assert.assertEquals(400, resp.getStatus());
  }

  @Test
  public void testUpdateDatasourceKeyAnotherBadTimestamp()
  {
    setupMockView();

    DateTime t1 = DateTimes.nowUtc();
    DateTime t2 = t1.minusHours(5);
    timelineStore.update(testDatasource, DataCompletenessLowWatermark.TYPE, t2);

    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(getBaseURI());

    ClientResponse resp = service.path("druid/watermarking/v1/collector/datasources")
                                 .path(testDatasource)
                                 .path(DataCompletenessLowWatermark.TYPE)
                                 .queryParam("timestamp", t1.toString(DateTimeFormat.forPattern("ddMMMyy")))
                                 .accept(MediaType.APPLICATION_JSON)
                                 .post(ClientResponse.class);

    Assert.assertEquals(400, resp.getStatus());
  }

  @Test
  public void testUpdateDatasourceKey() throws Exception
  {
    setupMockView();

    DateTime t1 = DateTimes.nowUtc();
    DateTime t2 = t1.minusHours(5);
    timelineStore.update(testDatasource, DataCompletenessLowWatermark.TYPE, t2);
    cache.set(testDatasource, DataCompletenessLowWatermark.TYPE, t2);

    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(getBaseURI());

    ClientResponse resp = service.path("druid/watermarking/v1/collector/datasources")
                                 .path(testDatasource)
                                 .path(DataCompletenessLowWatermark.TYPE)
                                 .queryParam("timestamp", t1.toString(ISODateTimeFormat.dateTimeNoMillis()))
                                 .accept(MediaType.APPLICATION_JSON)
                                 .post(ClientResponse.class);

    Assert.assertEquals(200, resp.getStatus());
    Thread.sleep(3000);
    Assert.assertEquals(
        t1.minuteOfDay().roundCeilingCopy().getMillis(),
        timelineStore.getValue(testDatasource, DataCompletenessLowWatermark.TYPE).getMillis()
    );
  }

  @Test
  public void testUpdateDatasourceKeyFailure() throws Exception
  {
    setupMockView();

    DateTime t1 = DateTimes.nowUtc();
    DateTime t2 = t1.minusHours(5);
    timelineStore.update(testDatasource, StableDataHighWatermark.TYPE, t2);
    cache.set(testDatasource, StableDataHighWatermark.TYPE, t2);

    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(getBaseURI());

    ClientResponse resp = service.path("druid/watermarking/v1/collector/datasources")
                                 .path(testDatasource)
                                 .path(StableDataHighWatermark.TYPE)
                                 .queryParam("timestamp", t1.toString(ISODateTimeFormat.dateTimeNoMillis()))
                                 .accept(MediaType.APPLICATION_JSON)
                                 .post(ClientResponse.class);


    Assert.assertEquals(400, resp.getStatus());
    Assert.assertEquals(
        t2.getMillis(),
        timelineStore.getValue(testDatasource, StableDataHighWatermark.TYPE).getMillis()
    );
  }

  @Test
  public void testDetectGaps() throws Exception
  {
    Client client = Client.create(new DefaultClientConfig());
    WebResource service = client.resource(getBaseURI());

    ClientResponse resp = service.path("druid/watermarking/v1/collector/datasources")
                                 .path(testDatasource)
                                 .path("gaps")
                                 .accept(MediaType.APPLICATION_JSON)
                                 .get(ClientResponse.class);


    Assert.assertEquals(200, resp.getStatus());
    String text = resp.getEntity(String.class);

    Map<String, List<String>> actual = MAPPER.readValue(
        text,
        TYPE_FACTORY.constructMapType(Map.class, String.class, List.class)
    );
    String[] gap = {"2011-04-06T00:00:00.000Z/2011-04-07T00:00:00.000Z"};
    Assert.assertTrue(actual.keySet().contains(SegmentGapDetector.GAP_TYPE));
    Assert.assertTrue(actual.keySet().contains(BatchGapDetector.GAP_TYPE));
    Assert.assertArrayEquals(gap, actual.get(SegmentGapDetector.GAP_TYPE).toArray());
    Assert.assertArrayEquals(gap, actual.get(BatchGapDetector.GAP_TYPE).toArray());
  }
}
