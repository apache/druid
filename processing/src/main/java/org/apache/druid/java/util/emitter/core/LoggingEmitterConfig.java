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

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

/**
 */
public class LoggingEmitterConfig
{
  @NotNull
  @JsonProperty
  private String loggerClass = LoggingEmitter.class.getName();

  @NotNull
  @JsonProperty
  private String logLevel = "info";

  /**
   * When true, only metrics listed in the allowed metrics configuration are emitted.
   * If {@link #allowedMetricsPath} is null/empty, the bundled default allowlist
   * (defaultMetrics.json on the classpath) is used. If a path is provided,
   * it is loaded from that file instead.
   * Defaults to false (emit all metrics, backward-compatible behavior).
   */
  @JsonProperty("shouldFilterMetrics")
  private boolean shouldFilterMetrics = false;

  /**
   * Optional path to a JSON file containing an array of allowed metric names.
   * Only used when {@link #shouldFilterMetrics} is true.
   * If null or empty, the bundled default resource (defaultMetrics.json) is loaded
   * from the classpath, mirroring how the Prometheus emitter loads its defaultMetrics.json.
   */
  @JsonProperty
  @Nullable
  private String allowedMetricsPath = null;

  public String getLoggerClass()
  {
    return loggerClass;
  }

  public String getLogLevel()
  {
    return logLevel;
  }

  public boolean shouldFilterMetrics()
  {
    return shouldFilterMetrics;
  }

  @Nullable
  public String getAllowedMetricsPath()
  {
    return allowedMetricsPath;
  }

  @Override
  public String toString()
  {
    return "LoggingEmitterConfig{" +
           "loggerClass='" + loggerClass + '\'' +
           ", logLevel='" + logLevel + '\'' +
           ", shouldFilterMetrics=" + shouldFilterMetrics +
           ", allowedMetricsPath='" + allowedMetricsPath + '\'' +
           '}';
  }
}
