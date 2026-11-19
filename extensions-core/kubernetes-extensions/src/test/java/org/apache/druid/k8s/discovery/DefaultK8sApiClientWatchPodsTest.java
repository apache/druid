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

package org.apache.druid.k8s.discovery;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Watch;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.SocketEffect;
import okhttp3.Headers;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.DruidNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Exercises {@link DefaultK8sApiClient#watchPods} end to end against a real (but local, canned)
 * HTTP watch stream: BOOKMARK events must surface their resourceVersion to the caller instead of
 * a stale or null response, and a mid-stream connection reset must surface as
 * {@link ChannelResetException} rather than an unrelated exception type.
 */
public class DefaultK8sApiClientWatchPodsTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  static {
    // DiscoveryDruidNode's @JsonCreator takes a @JacksonInject ObjectMapper, so the mapper
    // used to deserialize it must know how to inject itself.
    JSON_MAPPER.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, JSON_MAPPER));
  }

  private final DiscoveryDruidNode testNode = new DiscoveryDruidNode(
      new DruidNode("druid/router", "test-host", true, 80, null, true, false), NodeRole.ROUTER, null);

  private MockWebServer server;
  private DefaultK8sApiClient client;

  @BeforeEach
  public void setUp() throws IOException
  {
    server = new MockWebServer();
    server.start();

    ApiClient apiClient = new ApiClient().setBasePath(server.url("/").toString());
    client = new DefaultK8sApiClient(apiClient, JSON_MAPPER);
  }

  @AfterEach
  public void tearDown() throws IOException
  {
    server.close();
  }

  @Test
  public void testBookmarkAsFirstEventSurfacesResourceVersion() throws Exception
  {
    enqueueWatchStream(bookmarkLine("100"));

    try (WatchResult result = client.watchPods("testns", null, "99", NodeRole.ROUTER)) {
      Assertions.assertTrue(result.hasNext());
      Watch.Response<DiscoveryDruidNodeAndResourceVersion> response = result.next();

      Assertions.assertNotNull(response, "BOOKMARK event must not surface as a null response");
      Assertions.assertEquals(WatchResult.BOOKMARK, response.type);
      Assertions.assertNotNull(response.object);
      Assertions.assertEquals("100", response.object.getResourceVersion());
    }
  }

  @Test
  public void testBookmarkAfterRealEventSurfacesItsOwnResourceVersion() throws Exception
  {
    enqueueWatchStream(addedLine("200", testNode) + bookmarkLine("201"));

    try (WatchResult result = client.watchPods("testns", null, "99", NodeRole.ROUTER)) {
      Assertions.assertTrue(result.hasNext());
      Watch.Response<DiscoveryDruidNodeAndResourceVersion> added = result.next();
      Assertions.assertEquals(WatchResult.ADDED, added.type);
      Assertions.assertEquals("200", added.object.getResourceVersion());

      Assertions.assertTrue(result.hasNext());
      Watch.Response<DiscoveryDruidNodeAndResourceVersion> bookmark = result.next();

      Assertions.assertNotNull(bookmark, "BOOKMARK event must not surface as a null response");
      Assertions.assertEquals(WatchResult.BOOKMARK, bookmark.type);
      Assertions.assertNotNull(bookmark.object);
      Assertions.assertEquals(
          "201",
          bookmark.object.getResourceVersion(),
          "BOOKMARK must carry its own resourceVersion, not the preceding event's");
    }
  }

  @Test
  public void testConnectionResetMidStreamSurfacesAsChannelResetException() throws Exception
  {
    // Truncate the body mid-object and sever the connection while it's being read, simulating
    // a server-side stream reset. okhttp surfaces this as a ProtocolException, which
    // DefaultK8sApiClient must remap to ChannelResetException rather than an unrelated
    // RuntimeException, so callers can distinguish it from other IO failures.
    server.enqueue(new MockResponse.Builder()
        .code(200)
        .headers(new Headers.Builder()
            .add("Content-Type", "application/json")
            .build())
        .body(addedLine("200", testNode).substring(0, 20))
        .onResponseBody(new SocketEffect.CloseSocket(true, true, true))
        .build());

    try (WatchResult result = client.watchPods("testns", null, "99", NodeRole.ROUTER)) {
      Assertions.assertThrows(ChannelResetException.class, result::hasNext);
    }
  }

  private void enqueueWatchStream(String body)
  {
    server.enqueue(new MockResponse(
        200,
        new Headers.Builder().add("Content-Type", "application/json").build(),
        body));
  }

  private static String bookmarkLine(String resourceVersion)
  {
    return "{\"type\":\"BOOKMARK\",\"object\":{\"kind\":\"Pod\",\"apiVersion\":\"v1\",\"metadata\":{\"resourceVersion\":\""
        + resourceVersion + "\"}}}\n";
  }

  private static String addedLine(String resourceVersion, DiscoveryDruidNode node) throws Exception
  {
    String infoAnnotation = K8sDruidNodeAnnouncer.getInfoAnnotation(node.getNodeRole());
    String nodeJson = StringUtils.replace(JSON_MAPPER.writeValueAsString(node), "\"", "\\\"");
    return "{\"type\":\"ADDED\",\"object\":{\"kind\":\"Pod\",\"apiVersion\":\"v1\",\"metadata\":{"
        + "\"name\":\"test-pod\",\"resourceVersion\":\"" + resourceVersion + "\","
        + "\"annotations\":{\"" + infoAnnotation + "\":\"" + nodeJson + "\"}},"
        + "\"status\":{\"containerStatuses\":[{\"name\":\"main\",\"ready\":true}]}}}\n";
  }
}
