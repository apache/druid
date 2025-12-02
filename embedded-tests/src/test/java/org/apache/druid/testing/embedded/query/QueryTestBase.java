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

package org.apache.druid.testing.embedded.query;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.guice.SleepModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import javax.ws.rs.core.MediaType;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public abstract class QueryTestBase extends EmbeddedClusterTestBase
{
  protected static final String SQL_QUERY_ROUTE = "%s/druid/v2/sql";
  public static List<Boolean> SHOULD_USE_BROKER_TO_QUERY = List.of(true, false);

  protected final EmbeddedBroker broker = new EmbeddedBroker();
  protected final EmbeddedRouter router = new EmbeddedRouter();
  protected final EmbeddedOverlord overlord = new EmbeddedOverlord();
  protected final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  protected final EmbeddedIndexer indexer = new EmbeddedIndexer();
  protected final EmbeddedHistorical historical = new EmbeddedHistorical();

  protected HttpClient httpClientRef;
  protected String brokerEndpoint;
  protected String routerEndpoint;

  /**
   * Hook for the additional setup that needs to be done before all tests.
   */
  protected void beforeAll()
  {
    // No-op dy default
  }

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    overlord.addProperty("druid.manager.segments.pollDuration", "PT0.1s");
    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "always");
    indexer.setServerMemory(500_000_000)
           .addProperty("druid.worker.capacity", "4")
           .addProperty("druid.processing.numThreads", "2")
           .addProperty("druid.segment.handoff.pollDuration", "PT0.1s");

    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(overlord)
                               .addServer(coordinator)
                               .addServer(broker)
                               .addServer(router)
                               .addServer(indexer)
                               .addServer(historical)
                               .addExtension(ServerManagerForQueryErrorTestModule.class)
                               .addExtension(SleepModule.class);
  }

  @BeforeAll
  void setUp()
  {
    httpClientRef = router.bindings().globalHttpClient();
    brokerEndpoint = StringUtils.format(SQL_QUERY_ROUTE, getServerUrl(broker));
    routerEndpoint = StringUtils.format(SQL_QUERY_ROUTE, getServerUrl(router));
    try {
      beforeAll();
    }
    catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  /**
   * Execute an async SQL query against the given endpoint via the HTTP client.
   */
  protected ListenableFuture<StatusResponseHolder> executeQueryAsync(String endpoint, String query)
  {
    URL url;
    try {
      url = new URL(endpoint);
    }
    catch (MalformedURLException e) {
      throw new AssertionError("Malformed URL");
    }

    Request request = new Request(HttpMethod.POST, url);
    request.addHeader("Content-Type", MediaType.APPLICATION_JSON);
    request.setContent(query.getBytes(StandardCharsets.UTF_8));
    return httpClientRef.go(request, StatusResponseHandler.getInstance());
  }

  /**
   * Execute a SQL query against the given endpoint via the HTTP client.
   */
  protected void executeQueryWithContentType(
      String endpoint,
      String contentType,
      String query,
      Consumer<Request> onRequest,
      BiConsumer<Integer, String> onResponse
  )
  {
    URL url;
    try {
      url = new URL(endpoint);
    }
    catch (MalformedURLException e) {
      throw new AssertionError("Malformed URL");
    }

    Request request = new Request(HttpMethod.POST, url);
    if (contentType != null) {
      request.addHeader("Content-Type", contentType);
    }

    if (query != null) {
      request.setContent(query.getBytes(StandardCharsets.UTF_8));
    }

    if (onRequest != null) {
      onRequest.accept(request);
    }

    StatusResponseHolder response;
    try {
      response = httpClientRef.go(request, StatusResponseHandler.getInstance())
                              .get();
    }
    catch (InterruptedException | ExecutionException e) {
      throw new AssertionError("Failed to execute a request", e);
    }

    Assertions.assertNotNull(response);

    onResponse.accept(
        response.getStatus().getCode(),
        response.getContent().trim()
    );
  }

  /**
   * Execute a SQL query against the given endpoint via the HTTP client.
   */
  protected void cancelQuery(String endpoint, String queryId, Consumer<StatusResponseHolder> onResponse)
  {
    URL url;
    try {
      url = new URL(StringUtils.format("%s/%s", endpoint, queryId));
    }
    catch (MalformedURLException e) {
      throw new AssertionError("Malformed URL");
    }

    Request request = new Request(HttpMethod.DELETE, url);
    StatusResponseHolder response;
    try {
      response = httpClientRef.go(request, StatusResponseHandler.getInstance())
                              .get();
    }
    catch (InterruptedException | ExecutionException e) {
      throw new AssertionError("Failed to execute a request", e);
    }

    Assertions.assertNotNull(response);
    onResponse.accept(response);
  }
}
