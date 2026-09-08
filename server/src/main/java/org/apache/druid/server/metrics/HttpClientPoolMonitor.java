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

import com.google.inject.Inject;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.http.client.pool.ResourcePool;
import org.apache.druid.java.util.metrics.AbstractMonitor;

import java.util.HashMap;
import java.util.Map;

/**
 * Emits what the connection pool of each HTTP client of this process did since the previous emission, under the
 * {@code httpClient} dimension of the client that owns the pool. The {@code currentlyOpen} and {@code currentlyUsed}
 * metrics are the exception: they are levels, not differences.
 */
public class HttpClientPoolMonitor extends AbstractMonitor
{
  private static final String CLIENT_DIMENSION = "httpClient";

  private final HttpClientPoolRegistry registry;
  private final Map<String, Snapshot> previous = new HashMap<>();

  @Inject
  public HttpClientPoolMonitor(HttpClientPoolRegistry registry)
  {
    this.registry = registry;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    for (Map.Entry<String, ResourcePool.Counters> pool : registry.getPools().entrySet()) {
      final String clientName = pool.getKey();
      final Snapshot current = Snapshot.of(pool.getValue());
      final Snapshot delta = current.since(previous.put(clientName, current));

      final ServiceMetricEvent.Builder builder =
          ServiceMetricEvent.builder().setDimension(CLIENT_DIMENSION, clientName);
      emitter.emit(builder.setMetric("httpClient/pool/opened", delta.opened()));
      emitter.emit(builder.setMetric("httpClient/pool/closed", delta.closed()));
      emitter.emit(builder.setMetric("httpClient/pool/errored", delta.errored()));
      emitter.emit(builder.setMetric("httpClient/pool/timedOut", delta.timedOut()));
      emitter.emit(builder.setMetric("httpClient/pool/taken", delta.taken()));
      emitter.emit(builder.setMetric("httpClient/pool/returned", delta.returned()));
      emitter.emit(builder.setMetric("httpClient/pool/currentlyOpen", current.currentlyOpen()));
      emitter.emit(builder.setMetric("httpClient/pool/currentlyUsed", current.currentlyUsed()));
    }
    return true;
  }

  private record Snapshot(long opened, long closed, long errored, long timedOut, long taken, long returned)
  {
    private static Snapshot of(ResourcePool.Counters counters)
    {
      return new Snapshot(
          counters.getOpened(),
          counters.getClosed(),
          counters.getErrored(),
          counters.getTimedOut(),
          counters.getTaken(),
          counters.getReturned()
      );
    }

    private long currentlyOpen()
    {
      return opened - closed;
    }

    private long currentlyUsed()
    {
      return taken - returned;
    }

    private Snapshot since(Snapshot earlier)
    {
      if (earlier == null) {
        return this;
      }
      return new Snapshot(
          opened - earlier.opened,
          closed - earlier.closed,
          errored - earlier.errored,
          timedOut - earlier.timedOut,
          taken - earlier.taken,
          returned - earlier.returned
      );
    }
  }
}
