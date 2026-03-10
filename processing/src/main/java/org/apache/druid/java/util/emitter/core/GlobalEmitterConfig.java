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

package org.apache.druid.java.util.emitter.core;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

/**
 * Shared metric-filtering configuration for emitter implementations.
 */
public class GlobalEmitterConfig
{
  /**
   * When true, only metrics listed in the allowed metrics configuration are emitted.
   * If {@link #metricSpecPath} is null/empty, the bundled default allowlist
   * (`defaultMetrics.json` on the classpath) is used. If a path is provided,
   * it is loaded from that file instead.
   * Defaults to false (emit all metrics).
   */
  @JsonProperty
  private boolean shouldFilterMetrics;

  /**
   * Optional path to a JSON file containing a JSON object keyed by allowed metric names,
   * for example `{"query/time": [], "jvm/gc/cpu": []}`.
   * Only used when {@link #shouldFilterMetrics} is true.
   * If null or empty, the bundled default resource (`defaultMetrics.json`) is loaded
   * from the classpath.
   */
  @JsonProperty
  private String metricSpecPath;

  public boolean isShouldFilterMetrics()
  {
    return shouldFilterMetrics;
  }

  public Optional<String> getMetricSpecPath()
  {
    return Optional.ofNullable(metricSpecPath);
  }

  public void setShouldFilterMetrics(boolean shouldFilterMetrics)
  {
    this.shouldFilterMetrics = shouldFilterMetrics;
  }

  public void setMetricSpecPath(String metricSpecPath)
  {
    this.metricSpecPath = metricSpecPath;
  }
}
