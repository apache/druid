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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.http.client.pool.ResourceContainer;
import org.apache.druid.java.util.http.client.pool.ResourceFactory;
import org.apache.druid.java.util.http.client.pool.ResourcePool;
import org.apache.druid.java.util.http.client.pool.ResourcePoolConfig;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HttpClientPoolMonitorTest
{
  private static final Map<String, Object> BILLY = Map.of("httpClient", "global", "server", "billy");
  private static final Map<String, Object> SALLY = Map.of("httpClient", "global", "server", "sally");

  private HttpClientPoolRegistry registry;
  private StubServiceEmitter emitter;
  private ResourcePool<String, String> pool;

  @BeforeEach
  public void setUp()
  {
    registry = new HttpClientPoolRegistry();
    emitter = new StubServiceEmitter("service", "host");
    pool = new ResourcePool<>(
        new TestResourceFactory(),
        new ResourcePoolConfig(2, TimeUnit.MINUTES.toMillis(5)),
        false
    );
  }

  /**
   * Every remote end gets its own row, tagged with the client that talks to it, so that nothing has to be summed up
   * before it is emitted.
   */
  @Test
  public void testEveryRemoteEndIsReportedOnItsOwn()
  {
    registry.register("global", pool);
    pool.take("billy").returnResource();
    pool.take("sally").returnResource();
    pool.take("sally").returnResource();

    new HttpClientPoolMonitor(registry).doMonitor(emitter);

    Assertions.assertEquals(List.of(1L), emitter.getMetricValues("httpClient/pool/taken", BILLY));
    Assertions.assertEquals(List.of(2L), emitter.getMetricValues("httpClient/pool/taken", SALLY));
    Assertions.assertEquals(List.of(1L), emitter.getMetricValues("httpClient/pool/opened", BILLY));
    Assertions.assertEquals(List.of(1), emitter.getMetricValues("httpClient/pool/idle", SALLY));
  }

  /**
   * The counted events are drained as they are emitted, so a quiet remote end reports zeroes rather than the totals
   * of the previous emission, while what it holds keeps being reported.
   */
  @Test
  public void testEmissionsReportWhatHappenedSinceThePreviousOne()
  {
    registry.register("global", pool);
    final HttpClientPoolMonitor monitor = new HttpClientPoolMonitor(registry);

    pool.take("billy").returnResource();
    monitor.doMonitor(emitter);
    emitter.flush();

    monitor.doMonitor(emitter);
    Assertions.assertEquals(List.of(0L), emitter.getMetricValues("httpClient/pool/opened", BILLY));
    Assertions.assertEquals(List.of(0L), emitter.getMetricValues("httpClient/pool/taken", BILLY));
    Assertions.assertEquals(List.of(1), emitter.getMetricValues("httpClient/pool/idle", BILLY));
  }

  /**
   * The connections in the hands of callers are reported while they are held, which is what the load over one remote
   * end looks like, and go back to zero once they are given back.
   */
  @Test
  public void testUsedConnectionsAreReportedWhileTheyAreHeld()
  {
    registry.register("global", pool);
    final HttpClientPoolMonitor monitor = new HttpClientPoolMonitor(registry);

    final ResourceContainer<String> lent = pool.take("billy");
    monitor.doMonitor(emitter);
    Assertions.assertEquals(List.of(1), emitter.getMetricValues("httpClient/pool/used", BILLY));
    Assertions.assertEquals(List.of(0), emitter.getMetricValues("httpClient/pool/idle", BILLY));

    lent.returnResource();
    emitter.flush();
    monitor.doMonitor(emitter);
    Assertions.assertEquals(List.of(1L), emitter.getMetricValues("httpClient/pool/returned", BILLY));
    Assertions.assertEquals(List.of(0), emitter.getMetricValues("httpClient/pool/used", BILLY));
    Assertions.assertEquals(List.of(1), emitter.getMetricValues("httpClient/pool/idle", BILLY));
  }

  /**
   * The scheme of the pool key is left out, so that the remote end reads the same as under the {@code server}
   * dimension of the query metrics.
   */
  @Test
  public void testTheRemoteEndIsReportedWithoutItsScheme()
  {
    registry.register("global", pool);
    pool.take("https://1.2.3.4:8283").returnResource();

    new HttpClientPoolMonitor(registry).doMonitor(emitter);

    Assertions.assertEquals(
        List.of(1L),
        emitter.getMetricValues(
            "httpClient/pool/taken",
            Map.of("httpClient", "global", "server", "1.2.3.4:8283")
        )
    );
  }

  @Test
  public void testAClientNameIsRegisteredOnlyOnce()
  {
    registry.register("global", pool);
    Assertions.assertThrows(ISE.class, () -> registry.register("global", pool));
  }

  @Test
  public void testNothingIsEmittedWithoutARegisteredPool()
  {
    new HttpClientPoolMonitor(registry).doMonitor(emitter);
    Assertions.assertEquals(List.of(), emitter.getMetricEvents("httpClient/pool/opened"));
  }

  /**
   * A registered client that has not talked to anybody yet has no key to report.
   */
  @Test
  public void testNothingIsEmittedForAnUntouchedPool()
  {
    registry.register("global", pool);
    new HttpClientPoolMonitor(registry).doMonitor(emitter);
    Assertions.assertEquals(List.of(), emitter.getMetricEvents("httpClient/pool/opened"));
  }

  private static class TestResourceFactory implements ResourceFactory<String, String>
  {
    private final AtomicInteger sequence = new AtomicInteger();

    @Override
    public String generate(String key)
    {
      return key + "#" + sequence.getAndIncrement();
    }

    @Override
    public boolean isGood(String resource)
    {
      return true;
    }

    @Override
    public void close(String resource)
    {
    }
  }
}
