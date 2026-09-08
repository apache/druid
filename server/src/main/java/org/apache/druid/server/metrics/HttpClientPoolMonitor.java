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

import java.util.Map;

/**
 * Emits one row per remote end that an HTTP client of this process pools connections for: what happened since the
 * previous emission, plus what that remote end holds right now. Aggregating the rows of a client is left to whoever
 * receives them.
 *
 * This monitor drains the counters of the pools it reads, so it must be the only reader of them.
 */
public class HttpClientPoolMonitor extends AbstractMonitor
{
  private static final String CLIENT_DIMENSION = "httpClient";
  private static final String SERVER_DIMENSION = "server";

  private final HttpClientPoolRegistry registry;

  @Inject
  public HttpClientPoolMonitor(HttpClientPoolRegistry registry)
  {
    this.registry = registry;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    for (Map.Entry<String, ResourcePool<?, ?>> client : registry.getPools().entrySet()) {
      for (Map.Entry<?, ResourcePool.Stats> pool : client.getValue().drainStats().entrySet()) {
        final ResourcePool.Stats stats = pool.getValue();
        final ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder()
                                                                    .setDimension(CLIENT_DIMENSION, client.getKey())
                                                                    .setDimension(
                                                                        SERVER_DIMENSION,
                                                                        String.valueOf(pool.getKey())
                                                                    );
        emitter.emit(builder.setMetric("httpClient/pool/opened", stats.opened()));
        emitter.emit(builder.setMetric("httpClient/pool/closed", stats.closed()));
        emitter.emit(builder.setMetric("httpClient/pool/errored", stats.errored()));
        emitter.emit(builder.setMetric("httpClient/pool/timedOut", stats.timedOut()));
        emitter.emit(builder.setMetric("httpClient/pool/taken", stats.taken()));
        emitter.emit(builder.setMetric("httpClient/pool/returned", stats.returned()));
        emitter.emit(builder.setMetric("httpClient/pool/used", stats.used()));
        emitter.emit(builder.setMetric("httpClient/pool/idle", stats.idle()));
      }
    }
    return true;
  }
}
