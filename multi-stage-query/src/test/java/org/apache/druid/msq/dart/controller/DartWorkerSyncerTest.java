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

package org.apache.druid.msq.dart.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.msq.dart.guice.DartControllerConfig;
import org.apache.druid.msq.dart.worker.WorkerId;
import org.apache.druid.msq.dart.worker.http.DartWorkerResource;
import org.apache.druid.msq.dart.worker.http.SyncWorkersRequest;
import org.apache.druid.msq.dart.worker.http.SyncWorkersResponse;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerHolder;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.ServiceLocations;
import org.apache.druid.rpc.ServiceLocator;
import org.apache.druid.server.DruidNode;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DartWorkerSyncerTest
{
  private static final String SELF_HOST = "broker:8082";
  private static final String QUERY_ID = "q1";
  private static final WorkerId WORKER_A = new WorkerId("http", "historical-a:8083", QUERY_ID);
  private static final WorkerId WORKER_B = new WorkerId("https", "historical-b:8283", QUERY_ID);
  private static final DiscoveryDruidNode NODE_A = new DiscoveryDruidNode(
      new DruidNode("historical", "historical-a", false, 8083, -1, true, false),
      NodeRole.HISTORICAL,
      Map.of()
  );
  private static final DiscoveryDruidNode NODE_B = new DiscoveryDruidNode(
      new DruidNode("historical", "historical-b", false, -1, 8283, false, true),
      NodeRole.HISTORICAL,
      Map.of()
  );

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  /**
   * Server host:port -> mock client for that server.
   */
  private final Map<String, ServiceClient> clients = new HashMap<>();

  /**
   * Server host:port -> locator given to {@link ServiceClientFactory#makeClient} for that server.
   */
  private final Map<String, ServiceLocator> locators = new HashMap<>();

  /**
   * Servers that are in service discovery.
   */
  private final List<DiscoveryDruidNode> discoveredNodes = new ArrayList<>(List.of(NODE_A, NODE_B));

  /**
   * If set, the next sync request to any server returns this future.
   */
  private SettableFuture<BytesFullResponseHolder> nextResponse;

  private DartControllerRegistry registry;
  private DartWorkerSyncer syncer;

  @BeforeEach
  public void setUp()
  {
    final ServiceClientFactory clientFactory = Mockito.mock(ServiceClientFactory.class);
    Mockito.when(clientFactory.makeClient(ArgumentMatchers.anyString(), ArgumentMatchers.any(), ArgumentMatchers.any()))
           .thenAnswer(invocation -> {
             final String host = invocation.getArgument(0);
             final ServiceClient client = Mockito.mock(ServiceClient.class);
             Mockito.when(client.asyncRequest(ArgumentMatchers.any(), ArgumentMatchers.any()))
                    .thenAnswer(requestInvocation -> respond());
             clients.put(host, client);
             locators.put(host, invocation.getArgument(1));
             return client;
           });

    final DruidNodeDiscovery discovery = Mockito.mock(DruidNodeDiscovery.class);
    Mockito.when(discovery.getAllNodes()).thenAnswer(invocation -> List.copyOf(discoveredNodes));

    final DartControllerConfig config = new DartControllerConfig();
    registry = new DartControllerRegistry(config);
    syncer = new DartWorkerSyncer(
        registry,
        () -> discovery,
        clientFactory,
        jsonMapper,
        SELF_HOST,
        config,
        ScheduledExecutors::fixed
    );
  }

  @AfterEach
  public void tearDown()
  {
    syncer.stop();
  }

  @Test
  public void test_constructor_zeroSyncPeriod()
  {
    final DartControllerConfig config = new DartControllerConfig()
    {
      @Override
      public Period getSyncPeriod()
      {
        return Period.ZERO;
      }
    };

    Assertions.assertThrows(
        DruidException.class,
        () -> new DartWorkerSyncer(
            registry,
            () -> Mockito.mock(DruidNodeDiscovery.class),
            Mockito.mock(ServiceClientFactory.class),
            jsonMapper,
            SELF_HOST,
            config,
            ScheduledExecutors::fixed
        )
    );
  }

  @Test
  public void test_sync_sendsQueriesToEachWorker() throws Exception
  {
    registry.register(makeControllerHolder(QUERY_ID, List.of(WORKER_A.toString(), WORKER_B.toString())));
    syncer.sync();

    Assertions.assertEquals(Set.of(WORKER_A.getHostAndPort(), WORKER_B.getHostAndPort()), clients.keySet());
    verifySyncs(WORKER_A, Set.of(QUERY_ID), 1);
    verifySyncs(WORKER_B, Set.of(QUERY_ID), 1);

    Assertions.assertEquals(
        ServiceLocations.forLocation(new ServiceLocation("historical-a", 8083, -1, DartWorkerResource.PATH)),
        locators.get(WORKER_A.getHostAndPort()).locate().get()
    );
    Assertions.assertEquals(
        ServiceLocations.forLocation(new ServiceLocation("historical-b", -1, 8283, DartWorkerResource.PATH)),
        locators.get(WORKER_B.getHostAndPort()).locate().get()
    );
  }

  @Test
  public void test_sync_continuesAfterQueryEnds()
  {
    final ControllerHolder holder = makeControllerHolder(QUERY_ID, List.of(WORKER_A.toString()));
    registry.register(holder);
    syncer.sync();
    verifySyncs(WORKER_A, Set.of(QUERY_ID), 1);

    // Query ends. Syncs continue, so the server cancels the worker if its stop was lost.
    registry.deregister(holder, null);
    syncer.sync();
    syncer.sync();
    verifySyncs(WORKER_A, Set.of(), 2);
  }

  @Test
  public void test_sync_queryBetweenSyncs()
  {
    syncer.sync();
    verifySyncs(WORKER_A, Set.of(), 1);

    // A query starts and ends between two syncs, so no sync ever lists it. The next sync still goes to the server, so
    // the server cancels the worker if its stop was lost.
    final ControllerHolder holder = makeControllerHolder(QUERY_ID, List.of(WORKER_A.toString()));
    registry.register(holder);
    registry.deregister(holder, null);
    syncer.sync();
    verifySyncs(WORKER_A, Set.of(), 2);
    verifySyncs(WORKER_A, Set.of(QUERY_ID), 0);
  }

  @Test
  public void test_sync_serversWithoutQueries()
  {
    // Each discovered server gets a sync every time, even though none of our queries are running.
    syncer.sync();
    syncer.sync();
    verifySyncs(WORKER_A, Set.of(), 2);
    verifySyncs(WORKER_B, Set.of(), 2);
  }

  @Test
  public void test_sync_stopsWhenServerLeavesDiscovery() throws Exception
  {
    final ControllerHolder holder = makeControllerHolder(QUERY_ID, List.of(WORKER_A.toString()));
    registry.register(holder);
    syncer.sync();
    verifySyncs(WORKER_A, Set.of(QUERY_ID), 1);

    // Server goes away while still running a query.
    discoveredNodes.remove(NODE_A);
    syncer.sync();
    verifySyncs(WORKER_A, Set.of(QUERY_ID), 1);
    Assertions.assertEquals(ServiceLocations.closed(), locators.get(WORKER_A.getHostAndPort()).locate().get());
  }

  @Test
  public void test_sync_serverRejoinsDiscovery()
  {
    syncer.sync();
    verifySyncs(WORKER_B, Set.of(), 1);

    // Server B leaves discovery and comes back, so syncs resume through a new client.
    final ServiceClient clientB = clients.get(WORKER_B.getHostAndPort());
    discoveredNodes.remove(NODE_B);
    syncer.sync();
    discoveredNodes.add(NODE_B);
    syncer.sync();
    syncer.sync();
    Assertions.assertNotSame(clientB, clients.get(WORKER_B.getHostAndPort()));
    verifySyncs(WORKER_B, Set.of(), 2);
  }

  @Test
  public void test_sync_skipsServerWithSyncInFlight()
  {
    // Only server A is in discovery, so the held response goes to it.
    discoveredNodes.remove(NODE_B);
    registry.register(makeControllerHolder(QUERY_ID, List.of(WORKER_A.toString())));

    // Make the first sync hang.
    final SettableFuture<BytesFullResponseHolder> heldResponse = SettableFuture.create();
    nextResponse = heldResponse;
    syncer.sync();
    verifySyncs(WORKER_A, Set.of(QUERY_ID), 1);

    // No new sync while the previous one is in flight.
    syncer.sync();
    verifySyncs(WORKER_A, Set.of(QUERY_ID), 1);

    // Once it resolves, syncs resume.
    heldResponse.set(makeResponse(Set.of(QUERY_ID)));
    syncer.sync();
    verifySyncs(WORKER_A, Set.of(QUERY_ID), 2);
  }

  private ListenableFuture<BytesFullResponseHolder> respond()
  {
    if (nextResponse != null) {
      final SettableFuture<BytesFullResponseHolder> retVal = nextResponse;
      nextResponse = null;
      return retVal;
    } else {
      return Futures.immediateFuture(makeResponse(Set.of()));
    }
  }

  private BytesFullResponseHolder makeResponse(final Set<String> queryIds)
  {
    try {
      return new BytesFullResponseHolder(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK))
          .addChunk(jsonMapper.writeValueAsBytes(new SyncWorkersResponse(queryIds)));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void verifySyncs(final WorkerId server, final Set<String> queryIds, final int times)
  {
    final RequestBuilder expectedRequest =
        new RequestBuilder(HttpMethod.POST, "/sync")
            .jsonContent(jsonMapper, new SyncWorkersRequest(queryIds))
            .timeout(Duration.millis(DartWorkerSyncer.SYNC_TIMEOUT_MILLIS))
            .header(DartWorkerResource.HEADER_CONTROLLER_HOST, SELF_HOST);

    Mockito.verify(clients.get(server.getHostAndPort()), Mockito.times(times))
           .asyncRequest(ArgumentMatchers.eq(expectedRequest), ArgumentMatchers.any());
  }

  private static ControllerHolder makeControllerHolder(final String queryId, final List<String> workerIds)
  {
    final Controller controller = Mockito.mock(Controller.class);
    Mockito.when(controller.queryId()).thenReturn(queryId);
    Mockito.when(controller.getWorkerIds()).thenReturn(workerIds);
    return new ControllerHolder(controller, "sql-" + queryId, "SELECT 1", null, DateTimes.of("2000"));
  }
}
