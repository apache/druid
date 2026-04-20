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
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpDestination;
import org.eclipse.jetty.client.api.Destination;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
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
  public void testNoDestinationsEmitsZero()
  {
    Mockito.when(httpClient.getDestinations()).thenReturn(Collections.emptyList());
    final StubServiceEmitter emitter = new StubServiceEmitter("router", "localhost");
    monitor.doMonitor(emitter);

    Assert.assertEquals(1, emitter.getEvents().size());
    Map<String, Object> event = emitter.getEvents().get(0).toMap();
    Assert.assertEquals("router/http/numRequestsQueued", event.get("metric"));
    Assert.assertEquals(0, event.get("value"));
  }

  @Test
  public void testMultipleDestinationsQueueCountSummed()
  {
    HttpDestination dest1 = mockDest(3);
    HttpDestination dest2 = mockDest(5);
    Mockito.when(httpClient.getDestinations()).thenReturn(Arrays.asList(dest1, dest2));
    final StubServiceEmitter emitter = new StubServiceEmitter("router", "localhost");
    monitor.doMonitor(emitter);

    Assert.assertEquals(1, emitter.getEvents().size());
    Assert.assertEquals(8, emitter.getEvents().get(0).toMap().get("value"));
  }

  @Test
  public void testNonHttpDestinationIsSkipped()
  {
    Destination plainDest = Mockito.mock(Destination.class);
    HttpDestination httpDest = mockDest(4);
    Mockito.when(httpClient.getDestinations()).thenReturn(Arrays.asList(plainDest, httpDest));
    final StubServiceEmitter emitter = new StubServiceEmitter("router", "localhost");
    monitor.doMonitor(emitter);

    Assert.assertEquals(1, emitter.getEvents().size());
    Assert.assertEquals(4, emitter.getEvents().get(0).toMap().get("value"));
  }

  private static HttpDestination mockDest(int queuedCount)
  {
    HttpDestination dest = Mockito.mock(HttpDestination.class);
    Mockito.when(dest.getQueuedRequestCount()).thenReturn(queuedCount);
    return dest;
  }
}
