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

package org.apache.druid.server.metrics;

import com.google.inject.Provider;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.eclipse.jetty.client.AbstractConnectionPool;
import org.eclipse.jetty.client.ConnectionPool;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpDestination;
import org.eclipse.jetty.client.Origin;
import org.eclipse.jetty.client.api.Destination;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RouterHttpClientMonitorTest
{
  private HttpClient httpClient;
  private RouterHttpClientMonitor monitor;

  @Before
  public void setUp()
  {
    httpClient = Mockito.mock(HttpClient.class);
    Provider<HttpClient> httpClientProvider = () -> httpClient;
    monitor = new RouterHttpClientMonitor(httpClientProvider);
  }

  @Test
  public void testDoMonitorReturnsTrue()
  {
    Mockito.when(httpClient.getDestinations()).thenReturn(Collections.emptyList());
    Assert.assertTrue(monitor.doMonitor(new StubServiceEmitter("router", "localhost")));
  }

  @Test
  public void testNoDestinationsEmitsNoEvents()
  {
    Mockito.when(httpClient.getDestinations()).thenReturn(Collections.emptyList());
    final StubServiceEmitter emitter = new StubServiceEmitter("router", "localhost");
    monitor.doMonitor(emitter);

    Assert.assertTrue(emitter.getEvents().isEmpty());
  }

  @Test
  public void testMultipleDestinationsEmitsPerDestinationMetrics()
  {
    HttpDestination dest1 = mockDest("http://host1:8082", 3, 2);
    HttpDestination dest2 = mockDest("http://host2:8082", 5, 2);
    Mockito.when(httpClient.getDestinations()).thenReturn(Arrays.asList(dest1, dest2));
    final StubServiceEmitter emitter = new StubServiceEmitter("router", "localhost");
    monitor.doMonitor(emitter);

    // 2 metrics per destination: numRequestsQueued + numActiveConnections
    Assert.assertEquals(4, emitter.getEvents().size());

    List<Map<String, Object>> events = emitter.getEvents().stream()
                                              .map(e -> e.toMap())
                                              .collect(java.util.stream.Collectors.toList());

    Assert.assertTrue(events.stream().anyMatch(e ->
        "router/http/numRequestsQueued".equals(e.get("metric")) &&
        "http://host1:8082".equals(e.get("destination")) &&
        Integer.valueOf(3).equals(e.get("value"))
    ));
    Assert.assertTrue(events.stream().anyMatch(e ->
        "router/http/numActiveConnections".equals(e.get("metric")) &&
        "http://host1:8082".equals(e.get("destination")) &&
        Integer.valueOf(2).equals(e.get("value"))
    ));
    Assert.assertTrue(events.stream().anyMatch(e ->
        "router/http/numRequestsQueued".equals(e.get("metric")) &&
        "http://host2:8082".equals(e.get("destination")) &&
        Integer.valueOf(5).equals(e.get("value"))
    ));
    Assert.assertTrue(events.stream().anyMatch(e ->
        "router/http/numActiveConnections".equals(e.get("metric")) &&
        "http://host2:8082".equals(e.get("destination")) &&
        Integer.valueOf(2).equals(e.get("value"))
    ));
  }

  @Test
  public void testNonHttpDestinationIsSkipped()
  {
    Destination plainDest = Mockito.mock(Destination.class);
    HttpDestination httpDest = mockDest("http://host:8082", 4, 1);
    Mockito.when(httpClient.getDestinations()).thenReturn(Arrays.asList(plainDest, httpDest));
    final StubServiceEmitter emitter = new StubServiceEmitter("router", "localhost");
    monitor.doMonitor(emitter);

    // Only the HttpDestination emits: numRequestsQueued + numActiveConnections
    Assert.assertEquals(2, emitter.getEvents().size());
    List<Map<String, Object>> events = emitter.getEvents().stream()
                                              .map(e -> e.toMap())
                                              .collect(java.util.stream.Collectors.toList());
    Assert.assertTrue(events.stream().anyMatch(e ->
        "router/http/numRequestsQueued".equals(e.get("metric")) &&
        Integer.valueOf(4).equals(e.get("value"))
    ));
    Assert.assertTrue(events.stream().anyMatch(e ->
        "router/http/numActiveConnections".equals(e.get("metric")) &&
        Integer.valueOf(1).equals(e.get("value"))
    ));
  }

  @Test
  public void testNonAbstractConnectionPoolSkipsActiveConnections()
  {
    HttpDestination dest = mockDest("http://host:8082", 2, Mockito.mock(ConnectionPool.class));
    Mockito.when(httpClient.getDestinations()).thenReturn(Collections.singletonList(dest));

    final StubServiceEmitter emitter = new StubServiceEmitter("router", "localhost");
    monitor.doMonitor(emitter);

    // Only numRequestsQueued is emitted; numActiveConnections is skipped
    Assert.assertEquals(1, emitter.getEvents().size());
    Assert.assertEquals("router/http/numRequestsQueued", emitter.getEvents().get(0).toMap().get("metric"));
  }

  private static HttpDestination mockDest(String originStr, int queuedCount, int activeCount)
  {
    AbstractConnectionPool pool = Mockito.mock(AbstractConnectionPool.class);
    Mockito.when(pool.getActiveConnectionCount()).thenReturn(activeCount);
    return mockDest(originStr, queuedCount, pool);
  }

  private static HttpDestination mockDest(String originStr, int queuedCount, ConnectionPool pool)
  {
    HttpDestination dest = Mockito.mock(HttpDestination.class);
    Mockito.when(dest.getQueuedRequestCount()).thenReturn(queuedCount);
    Origin origin = Mockito.mock(Origin.class);
    Mockito.when(origin.asString()).thenReturn(originStr);
    Mockito.when(dest.getOrigin()).thenReturn(origin);
    Mockito.when(dest.getConnectionPool()).thenReturn(pool);
    return dest;
  }
}
