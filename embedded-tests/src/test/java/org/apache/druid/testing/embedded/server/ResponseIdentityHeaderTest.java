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

package org.apache.druid.testing.embedded.server;

import org.apache.druid.server.initialization.jetty.ResponseIdentityHeaderHandler;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedDruidServer;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

public class ResponseIdentityHeaderTest extends EmbeddedClusterTestBase
{
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedRouter router = new EmbeddedRouter();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final HttpClient client = HttpClient.newHttpClient();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .addCommonProperty("druid.server.http.enableResponseIdentityHeaders", "true")
                               .addServer(coordinator)
                               .addServer(overlord)
                               .addServer(broker)
                               .addServer(historical)
                               .addServer(router);
  }

  @AfterAll
  public void closeClient()
  {
    client.close();
  }

  @Test
  @Timeout(30)
  public void testCoordinatorResponseIdentity_directAndViaRouter() throws Exception
  {
    assertResponseIdentity(
        sendGet(getServerUrl(coordinator) + "/druid/coordinator/v1/isLeader"),
        coordinator
    );
    assertResponseIdentity(
        sendGet(getServerUrl(router) + "/druid/coordinator/v1/isLeader"),
        coordinator
    );
  }

  @Test
  @Timeout(30)
  public void testOverlordResponseIdentity_directAndViaRouter() throws Exception
  {
    assertResponseIdentity(
        sendGet(getServerUrl(overlord) + "/druid/indexer/v1/isLeader"),
        overlord
    );
    assertResponseIdentity(
        sendGet(getServerUrl(router) + "/druid/indexer/v1/isLeader"),
        overlord
    );
  }

  @Test
  @Timeout(30)
  public void testBrokerNativeQueryResponseIdentity_directAndViaRouter() throws Exception
  {
    assertResponseIdentity(sendNativeQuery(broker), broker);
    assertResponseIdentity(sendNativeQuery(router), broker);
  }

  @Test
  @Timeout(30)
  public void testBrokerSqlResponseIdentity_directAndViaRouter() throws Exception
  {
    assertResponseIdentity(sendSqlQuery(broker), broker);
    assertResponseIdentity(sendSqlQuery(router), broker);
  }

  @Test
  @Timeout(30)
  public void testHistoricalNativeQueryResponseIdentity() throws Exception
  {
    assertResponseIdentity(sendNativeQuery(historical), historical);
  }

  @Test
  @Timeout(30)
  public void testRouterGeneratedAndEarlyErrorResponsesUseRouterIdentity() throws Exception
  {
    assertResponseIdentity(sendGet(getServerUrl(router) + "/status/health"), router);

    final HttpRequest request = HttpRequest.newBuilder(URI.create(getServerUrl(router) + "/status/health"))
                                           .timeout(Duration.ofSeconds(10))
                                           .method("PATCH", HttpRequest.BodyPublishers.noBody())
                                           .build();
    assertResponseIdentity(client.send(request, HttpResponse.BodyHandlers.ofString()), router, 405);
  }

  private HttpResponse<String> sendGet(final String url) throws Exception
  {
    final HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                                           .timeout(Duration.ofSeconds(10))
                                           .GET()
                                           .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> sendNativeQuery(final EmbeddedDruidServer<?> server) throws Exception
  {
    final HttpRequest request = HttpRequest.newBuilder(URI.create(getServerUrl(server) + "/druid/v2"))
                                           .header("Content-Type", "application/json")
                                           .timeout(Duration.ofSeconds(10))
                                           .POST(
                                               HttpRequest.BodyPublishers.ofString(
                                                   """
                                                   {
                                                     "queryType": "timeseries",
                                                     "dataSource": "missing_datasource",
                                                     "granularity": "all",
                                                     "intervals": ["2000/3000"],
                                                     "aggregations": [{"type": "count", "name": "rows"}]
                                                   }
                                                   """
                                               )
                                           )
                                           .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private HttpResponse<String> sendSqlQuery(final EmbeddedDruidServer<?> server) throws Exception
  {
    final HttpRequest request = HttpRequest.newBuilder(URI.create(getServerUrl(server) + "/druid/v2/sql"))
                                           .header("Content-Type", "application/json")
                                           .timeout(Duration.ofSeconds(10))
                                           .POST(
                                               HttpRequest.BodyPublishers.ofString(
                                                   """
                                                   {
                                                     "query": "SELECT 1"
                                                   }
                                                   """
                                               )
                                           )
                                           .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private static void assertResponseIdentity(
      final HttpResponse<String> response,
      final EmbeddedDruidServer<?> expectedServer
  )
  {
    assertResponseIdentity(response, expectedServer, 200);
  }

  private static void assertResponseIdentity(
      final HttpResponse<String> response,
      final EmbeddedDruidServer<?> expectedServer,
      final int expectedStatus
  )
  {
    Assertions.assertEquals(expectedStatus, response.statusCode(), response.body());
    Assertions.assertEquals(
        List.of(expectedServer.bindings().selfNode().getHostAndPortToUse()),
        response.headers().allValues(ResponseIdentityHeaderHandler.RESPONSE_SERVER_HEADER)
    );
    Assertions.assertEquals(
        List.of(expectedServer.bindings().selfNode().getServiceName()),
        response.headers().allValues(ResponseIdentityHeaderHandler.RESPONSE_SERVICE_HEADER)
    );
  }
}
