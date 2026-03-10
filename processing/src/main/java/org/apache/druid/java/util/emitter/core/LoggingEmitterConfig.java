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

import javax.validation.constraints.NotNull;

/**
 */
public class LoggingEmitterConfig extends GlobalEmitterConfig
{
  public static final String DEFAULT_METRIC_SPEC_PATH = "defaultMetrics.json";

  @NotNull
  @JsonProperty
  private String loggerClass = LoggingEmitter.class.getName();

  @NotNull
  @JsonProperty
  private String logLevel = "info";

  public String getLoggerClass()
  {
    return loggerClass;
  }

  public String getLogLevel()
  {
    return logLevel;
  }

  @Override
  public String toString()
  {
    return "LoggingEmitterConfig{" +
           "loggerClass='" + loggerClass + '\'' +
           ", logLevel='" + logLevel + '\'' +
           ", shouldFilterMetrics=" + isShouldFilterMetrics() +
           ", metricSpecPath='" + getMetricSpecPath() + '\'' +
           '}';
  }
}
