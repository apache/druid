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
  private static final Map<String, Object> GLOBAL_CLIENT = Map.of("httpClient", "global");

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
        new ResourcePoolConfig(1, TimeUnit.MINUTES.toMillis(5)),
        false
    );
  }

  @Test
  public void testPoolsAreReportedUnderTheirClientName()
  {
    registry.register("global", pool.getCounters());
    pool.take("billy").returnResource();

    new HttpClientPoolMonitor(registry).doMonitor(emitter);

    Assertions.assertEquals(List.of(1L), emitter.getMetricValues("httpClient/pool/opened", GLOBAL_CLIENT));
    Assertions.assertEquals(List.of(0L), emitter.getMetricValues("httpClient/pool/closed", GLOBAL_CLIENT));
    Assertions.assertEquals(List.of(0L), emitter.getMetricValues("httpClient/pool/errored", GLOBAL_CLIENT));
    Assertions.assertEquals(List.of(0L), emitter.getMetricValues("httpClient/pool/timedOut", GLOBAL_CLIENT));
  }

  /**
   * Each emission reports what happened since the previous one, so a pool that opened nothing new reports zero
   * rather than the total it reported before.
   */
  @Test
  public void testEmissionsReportWhatHappenedSinceThePreviousOne()
  {
    registry.register("global", pool.getCounters());
    final HttpClientPoolMonitor monitor = new HttpClientPoolMonitor(registry);

    pool.take("billy").returnResource();
    monitor.doMonitor(emitter);
    emitter.flush();

    pool.take("billy").returnResource();
    monitor.doMonitor(emitter);
    Assertions.assertEquals(List.of(0L), emitter.getMetricValues("httpClient/pool/opened", GLOBAL_CLIENT));

    pool.close();
    emitter.flush();
    monitor.doMonitor(emitter);
    Assertions.assertEquals(List.of(1L), emitter.getMetricValues("httpClient/pool/closed", GLOBAL_CLIENT));
  }

  /**
   * The open connections are a level rather than a per emission count: every emission reports what stands open, and
   * closing the pool takes it back to zero.
   */
  @Test
  public void testOpenConnectionsAreReportedAtEveryEmission()
  {
    registry.register("global", pool.getCounters());
    final HttpClientPoolMonitor monitor = new HttpClientPoolMonitor(registry);

    pool.take("billy").returnResource();
    monitor.doMonitor(emitter);
    monitor.doMonitor(emitter);
    Assertions.assertEquals(List.of(1L, 1L), emitter.getMetricValues("httpClient/pool/open", GLOBAL_CLIENT));

    pool.close();
    emitter.flush();
    monitor.doMonitor(emitter);
    Assertions.assertEquals(List.of(0L), emitter.getMetricValues("httpClient/pool/open", GLOBAL_CLIENT));
  }

  @Test
  public void testAClientNameIsRegisteredOnlyOnce()
  {
    registry.register("global", pool.getCounters());
    Assertions.assertThrows(ISE.class, () -> registry.register("global", pool.getCounters()));
  }

  @Test
  public void testNothingIsEmittedWithoutARegisteredPool()
  {
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
