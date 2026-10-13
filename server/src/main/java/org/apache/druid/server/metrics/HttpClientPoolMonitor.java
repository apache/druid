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
  public static final String CLIENT_DIMENSION = "httpClient";
  public static final String SERVER_DIMENSION = "server";

  public static final String OPENED_METRIC = "httpClient/pool/opened";
  public static final String CLOSED_METRIC = "httpClient/pool/closed";
  public static final String ERRORED_METRIC = "httpClient/pool/errored";
  public static final String TIMED_OUT_METRIC = "httpClient/pool/timedOut";
  public static final String TAKEN_METRIC = "httpClient/pool/taken";
  public static final String RETURNED_METRIC = "httpClient/pool/returned";
  public static final String USED_METRIC = "httpClient/pool/used";
  public static final String IDLE_METRIC = "httpClient/pool/idle";

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
            .setDimension(SERVER_DIMENSION, serverOf(pool.getKey()));
        emitter.emit(builder.setMetric(OPENED_METRIC, stats.opened()));
        emitter.emit(builder.setMetric(CLOSED_METRIC, stats.closed()));
        emitter.emit(builder.setMetric(ERRORED_METRIC, stats.errored()));
        emitter.emit(builder.setMetric(TIMED_OUT_METRIC, stats.timedOut()));
        emitter.emit(builder.setMetric(TAKEN_METRIC, stats.taken()));
        emitter.emit(builder.setMetric(RETURNED_METRIC, stats.returned()));
        emitter.emit(builder.setMetric(USED_METRIC, stats.used()));
        emitter.emit(builder.setMetric(IDLE_METRIC, stats.idle()));
      }
    }
    return true;
  }

  /**
   * The pool key without its scheme, so that {@code host:port} reads the same here as it does under the
   * {@code server} dimension of the query metrics.
   */
  private static String serverOf(Object poolKey)
  {
    final String key = String.valueOf(poolKey);
    final int schemeEnd = key.indexOf("://");
    return schemeEnd < 0 ? key : key.substring(schemeEnd + 3);
  }
}
