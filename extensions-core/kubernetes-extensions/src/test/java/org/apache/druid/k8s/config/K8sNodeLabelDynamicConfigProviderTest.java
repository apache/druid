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

package org.apache.druid.k8s.config;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.kubernetes.client.openapi.ApiClient;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.k8s.discovery.K8sDiscoveryModule;
import org.apache.druid.metadata.DynamicConfigProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class K8sNodeLabelDynamicConfigProviderTest
{
  private static final String NODE_NAME = "ip-10-0-0-1.us-west-2.compute.internal";
  private static final String ZONE_ID_LABEL = "topology.k8s.aws/zone-id";
  private static final String ZONE_NAME_LABEL = "topology.kubernetes.io/zone";
  private static final String CLIENT_RACK = "client.rack";
  private static final String TOKEN = "test-token";
  private static final String DEFAULT_VARIABLE = K8sNodeLabelDynamicConfigProvider.DEFAULT_NODE_NAME_VARIABLE;

  private static final Map<String, String> RACK_FROM_ZONE_ID = ImmutableMap.of(CLIENT_RACK, ZONE_ID_LABEL);

  private static final String LABELED_NODE =
      "{\"metadata\":{\"name\":\"node\",\"labels\":{\"" + ZONE_ID_LABEL + "\":\"usw2-az1\","
      + "\"kubernetes.io/os\":\"linux\",\"node-role.kubernetes.io/worker\":\"\"}}}";
  private static final String UNLABELED_NODE = "{\"metadata\":{\"labels\":{\"kubernetes.io/os\":\"linux\"}}}";

  private final AtomicInteger requestCount = new AtomicInteger();
  private final List<ApiClient> apiClients = new ArrayList<>();
  private final CountDownLatch teardown = new CountDownLatch(1);

  private HttpServer server;
  private URI apiServerUri;
  private volatile String requestPath;
  private volatile String authorizationHeader;

  @BeforeEach
  public void setUp() throws IOException
  {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    apiServerUri = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
  }

  @AfterEach
  public void tearDown()
  {
    // Lets a stalling handler finish so that stop() does not join a sleeping thread.
    teardown.countDown();
    server.stop(0);
    apiClients.forEach(c -> c.getHttpClient().connectionPool().evictAll());
  }

  @Test
  public void testFullNodeResponse()
  {
    // The whole node object, which is what the API server sends when nothing trims the representation.
    serve(200, "{\"kind\":\"Node\",\"apiVersion\":\"v1\",\"metadata\":{\"name\":\"node\",\"resourceVersion\":\"123\",\"creationTimestamp\":\"2024-01-01T00:00:00Z\",\"labels\":{\"topology.k8s.aws/zone-id\":\"usw2-az1\",\"kubernetes.io/os\":\"linux\"}},\"spec\":{\"podCIDR\":\"10.0.0.0/24\",\"providerID\":\"aws:///us-west-2a/i-0abc\"},\"status\":{\"capacity\":{\"cpu\":\"8\",\"memory\":\"32Gi\"},\"images\":[{\"names\":[\"apache/druid:latest\"],\"sizeBytes\":123456789}],\"conditions\":[{\"type\":\"Ready\",\"status\":\"True\",\"lastHeartbeatTime\":\"2024-01-01T00:00:00Z\"}]}}");

    Assertions.assertEquals(ImmutableMap.of(CLIENT_RACK, "usw2-az1"), provider(RACK_FROM_ZONE_ID).getConfig());
  }

  @Test
  public void testNodeIsReadWithBearerToken()
  {
    serve(200, LABELED_NODE);

    provider(RACK_FROM_ZONE_ID).getConfig();

    Assertions.assertEquals("/api/v1/nodes/" + NODE_NAME, requestPath);
    Assertions.assertEquals("Bearer " + TOKEN, authorizationHeader);
  }

  @Test
  public void testLabelAbsent()
  {
    serve(200, UNLABELED_NODE);

    Assertions.assertEquals(Collections.emptyMap(), provider(RACK_FROM_ZONE_ID).getConfig());
  }

  @Test
  public void testOnlyTheLabelsTheNodeHasAreReturned()
  {
    serve(200, LABELED_NODE);

    Assertions.assertEquals(
        ImmutableMap.of(CLIENT_RACK, "usw2-az1"),
        provider(ImmutableMap.of(CLIENT_RACK, ZONE_ID_LABEL, "some.property", ZONE_NAME_LABEL)).getConfig()
    );
  }

  @Test
  public void testEmptyLabelValueIsSkipped()
  {
    serve(200, LABELED_NODE);

    Assertions.assertEquals(
        Collections.emptyMap(),
        provider(ImmutableMap.of("some.property", "node-role.kubernetes.io/worker")).getConfig()
    );
  }

  @Test
  public void testForbidden()
  {
    serve(403, "{\"kind\":\"Status\",\"code\":403}");

    Assertions.assertEquals(Collections.emptyMap(), provider(RACK_FROM_ZONE_ID).getConfig());
  }

  @Test
  public void testUnparseableResponse()
  {
    serve(200, "not json");

    Assertions.assertEquals(Collections.emptyMap(), provider(RACK_FROM_ZONE_ID).getConfig());
  }

  @Test
  public void testResponseWithoutMetadataIsTreatedAsAFailure()
  {
    serve(200, "{\"kind\":\"Node\",\"apiVersion\":\"v1\"}");
    final NodeLabelReader reader = new CachingNodeLabelReader(reader(Duration.ofSeconds(5)));

    Assertions.assertEquals(Collections.emptyMap(), provider(RACK_FROM_ZONE_ID, reader).getConfig());
    Assertions.assertEquals(Collections.emptyMap(), provider(RACK_FROM_ZONE_ID, reader).getConfig());

    // A 200 that does not describe a node is a failure, not an answer, so it must not be cached.
    Assertions.assertEquals(2, requestCount.get());
  }

  /**
   * A server that answers and then stalls mid-body. The client's own read timeout would eventually catch this, but
   * only after its default, so the reader bounds the whole call instead. The elapsed assertion is what holds that
   * bound in place: without it the test still passes, just ten seconds later.
   */
  @Test
  @Timeout(value = 30_000, unit = TimeUnit.MILLISECONDS)
  public void testStalledResponseBodyTimesOut()
  {
    server.createContext("/", exchange -> {
      requestCount.incrementAndGet();
      exchange.sendResponseHeaders(200, LABELED_NODE.length());
      exchange.getResponseBody().write("{".getBytes(StandardCharsets.UTF_8));
      exchange.getResponseBody().flush();
      awaitTeardown();
    });
    server.start();

    // Built before the clock starts so that class loading is not counted against the bound.
    final K8sNodeLabelDynamicConfigProvider provider = provider(RACK_FROM_ZONE_ID, reader(Duration.ofMillis(200)));

    final long startMillis = System.currentTimeMillis();
    Assertions.assertEquals(Collections.emptyMap(), provider.getConfig());
    final long elapsedMillis = System.currentTimeMillis() - startMillis;

    Assertions.assertEquals(1, requestCount.get());
    Assertions.assertTrue(elapsedMillis < 3000, "gave up only after " + elapsedMillis + " ms");
  }

  @Test
  public void testNodeWithNoLabelsAtAll()
  {
    serve(200, "{\"metadata\":{\"name\":\"node\"}}");
    final NodeLabelReader reader = new CachingNodeLabelReader(reader(Duration.ofSeconds(5)));

    Assertions.assertEquals(Collections.emptyMap(), provider(RACK_FROM_ZONE_ID, reader).getConfig());
    Assertions.assertEquals(Collections.emptyMap(), provider(RACK_FROM_ZONE_ID, reader).getConfig());

    // A node carrying no labels is an answer, so it is cached like any other.
    Assertions.assertEquals(1, requestCount.get());
  }

  @Test
  public void testEmptyResponseBody()
  {
    serve(200, "");

    Assertions.assertEquals(Collections.emptyMap(), provider(RACK_FROM_ZONE_ID).getConfig());
  }

  @Test
  public void testRedirectIsNotFollowed()
  {
    server.createContext("/", exchange -> {
      requestCount.incrementAndGet();
      exchange.getResponseHeaders().set("Location", "http://example.invalid/api/v1/nodes/" + NODE_NAME);
      exchange.sendResponseHeaders(302, -1);
      exchange.close();
    });
    server.start();

    Assertions.assertEquals(Collections.emptyMap(), provider(RACK_FROM_ZONE_ID).getConfig());

    // Following it would carry the bearer token to whatever host the redirect names.
    Assertions.assertEquals(1, requestCount.get());
  }

  @Test
  public void testNodeNameVariableUnsetOrBlank()
  {
    serve(200, LABELED_NODE);
    final NodeLabelReader reader = reader(Duration.ofSeconds(5));

    Assertions.assertEquals(Collections.emptyMap(), provider(RACK_FROM_ZONE_ID, DEFAULT_VARIABLE, null, reader).getConfig());
    Assertions.assertEquals(Collections.emptyMap(), provider(RACK_FROM_ZONE_ID, DEFAULT_VARIABLE, "  ", reader).getConfig());
    Assertions.assertEquals(0, requestCount.get());
  }

  @Test
  public void testCustomNodeNameVariable()
  {
    serve(200, LABELED_NODE);

    Assertions.assertEquals(
        ImmutableMap.of(CLIENT_RACK, "usw2-az1"),
        provider(RACK_FROM_ZONE_ID, "NODE_NAME", NODE_NAME, reader(Duration.ofSeconds(5))).getConfig()
    );
  }

  @Test
  public void testSuccessfulLookupIsCached()
  {
    serve(200, LABELED_NODE);
    final NodeLabelReader reader = new CachingNodeLabelReader(reader(Duration.ofSeconds(5)));

    final Map<String, String> expected = ImmutableMap.of(CLIENT_RACK, "usw2-az1");
    Assertions.assertEquals(expected, provider(RACK_FROM_ZONE_ID, reader).getConfig());
    Assertions.assertEquals(expected, provider(RACK_FROM_ZONE_ID, reader).getConfig());

    Assertions.assertEquals(1, requestCount.get());
  }

  @Test
  public void testNodeWithoutTheLabelIsStillCached()
  {
    serve(200, UNLABELED_NODE);
    final NodeLabelReader reader = new CachingNodeLabelReader(reader(Duration.ofSeconds(5)));

    Assertions.assertEquals(Collections.emptyMap(), provider(RACK_FROM_ZONE_ID, reader).getConfig());
    Assertions.assertEquals(Collections.emptyMap(), provider(RACK_FROM_ZONE_ID, reader).getConfig());

    // The node answered; it simply has no such label. That is an answer, so it is not asked again.
    Assertions.assertEquals(1, requestCount.get());
  }

  @Test
  public void testFailedLookupIsNotCached()
  {
    serve(403, "{\"kind\":\"Status\",\"code\":403}");
    final NodeLabelReader reader = new CachingNodeLabelReader(reader(Duration.ofSeconds(5)));

    provider(RACK_FROM_ZONE_ID, reader).getConfig();
    provider(RACK_FROM_ZONE_ID, reader).getConfig();

    Assertions.assertEquals(2, requestCount.get());
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    for (Module module : new K8sDiscoveryModule().getJacksonModules()) {
      mapper.registerModule(module);
    }

    final DynamicConfigProvider<?> defaulted = mapper.readValue(
        "{\"type\":\"k8sNodeLabel\",\"labels\":{\"client.rack\":\"topology.k8s.aws/zone-id\"}}",
        DynamicConfigProvider.class
    );
    Assertions.assertEquals(new K8sNodeLabelDynamicConfigProvider(RACK_FROM_ZONE_ID, null), defaulted);
    Assertions.assertEquals(
        DEFAULT_VARIABLE,
        ((K8sNodeLabelDynamicConfigProvider) defaulted).getNodeNameVariable()
    );
    Assertions.assertEquals(
        defaulted,
        mapper.readValue(mapper.writeValueAsString(defaulted), DynamicConfigProvider.class)
    );

    final DynamicConfigProvider<?> explicit = mapper.readValue(
        "{\"type\":\"k8sNodeLabel\",\"labels\":{\"client.rack\":\"topology.kubernetes.io/zone\"},"
        + "\"nodeNameVariable\":\"NODE_NAME\"}",
        DynamicConfigProvider.class
    );
    Assertions.assertEquals(
        new K8sNodeLabelDynamicConfigProvider(ImmutableMap.of(CLIENT_RACK, ZONE_NAME_LABEL), "NODE_NAME"),
        explicit
    );
    Assertions.assertEquals(
        explicit,
        mapper.readValue(mapper.writeValueAsString(explicit), DynamicConfigProvider.class)
    );
  }

  @Test
  public void testEqualsAndHashCode()
  {
    final K8sNodeLabelDynamicConfigProvider provider =
        new K8sNodeLabelDynamicConfigProvider(RACK_FROM_ZONE_ID, null);
    final K8sNodeLabelDynamicConfigProvider same =
        new K8sNodeLabelDynamicConfigProvider(RACK_FROM_ZONE_ID, DEFAULT_VARIABLE);
    final K8sNodeLabelDynamicConfigProvider otherLabel =
        new K8sNodeLabelDynamicConfigProvider(ImmutableMap.of(CLIENT_RACK, ZONE_NAME_LABEL), null);
    final K8sNodeLabelDynamicConfigProvider otherVariable =
        new K8sNodeLabelDynamicConfigProvider(RACK_FROM_ZONE_ID, "NODE_NAME");

    Assertions.assertEquals(provider, same);
    Assertions.assertEquals(provider.hashCode(), same.hashCode());
    Assertions.assertNotEquals(provider, otherLabel);
    Assertions.assertNotEquals(provider, otherVariable);
    Assertions.assertNotEquals(provider, null);
  }

  @Test
  public void testLabelsAreRequired()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new K8sNodeLabelDynamicConfigProvider(null, null)
    );
  }

  private void serve(int status, String body)
  {
    server.createContext("/", exchange -> {
      requestCount.incrementAndGet();
      requestPath = exchange.getRequestURI().getPath();
      authorizationHeader = exchange.getRequestHeaders().getFirst("Authorization");
      respond(exchange, status, body);
    });
    server.start();
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException
  {
    final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  /**
   * Holds a handler open until the test is over, so a stalling server does not also stall the suite.
   */
  private void awaitTeardown()
  {
    try {
      teardown.await(30, TimeUnit.SECONDS);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private K8sNodeLabelDynamicConfigProvider provider(Map<String, String> labels)
  {
    return provider(labels, reader(Duration.ofSeconds(5)));
  }

  private K8sNodeLabelDynamicConfigProvider provider(Map<String, String> labels, NodeLabelReader reader)
  {
    return provider(labels, DEFAULT_VARIABLE, NODE_NAME, reader);
  }

  /**
   * The fake environment answers only for {@code variable}, with {@code nodeName}, or null for an unset variable.
   */
  private K8sNodeLabelDynamicConfigProvider provider(
      Map<String, String> labels,
      String variable,
      @Nullable String nodeName,
      NodeLabelReader reader
  )
  {
    return new K8sNodeLabelDynamicConfigProvider(labels, variable, reader, var -> variable.equals(var) ? nodeName : null);
  }

  private NodeLabelReader reader(Duration timeout)
  {
    // Production gets its token from an OkHttp interceptor installed by ClientBuilder, not from a default header,
    // so the test uses an interceptor too. A default header would survive the client swap either way.
    final ApiClient apiClient = new ApiClient().setBasePath(apiServerUri.toString());
    apiClient.setHttpClient(
        apiClient.getHttpClient()
                 .newBuilder()
                 .addInterceptor(chain -> chain.proceed(
                     chain.request().newBuilder().header("Authorization", "Bearer " + TOKEN).build()
                 ))
                 .build()
    );
    apiClients.add(apiClient);
    return new ApiNodeLabelReader(apiClient, timeout);
  }
}
