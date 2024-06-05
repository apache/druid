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

package org.apache.druid.server.coordinator.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.joda.time.Duration;

public class HttpLoadQueuePeonConfig
{
  private static final Duration DEFAULT_LOAD_TIMEOUT = Duration.standardMinutes(15);

  @JsonProperty
  private final Duration hostTimeout;

  @JsonProperty
  private final Duration repeatDelay;

  @JsonProperty
  private final int batchSize;

  @JsonCreator
  public HttpLoadQueuePeonConfig(
      @JsonProperty("hostTimeout") Duration hostTimeout,
      @JsonProperty("repeatDelay") Duration repeatDelay,
      @JsonProperty("batchSize") Integer batchSize
  )
  {
    this.hostTimeout = Configs.valueOrDefault(hostTimeout, Duration.standardMinutes(5));
    this.repeatDelay = Configs.valueOrDefault(repeatDelay, Duration.standardMinutes(1));
    this.batchSize = Configs.valueOrDefault(batchSize, 1);
  }

  public int getBatchSize()
  {
    return batchSize;
  }

  public Duration getHostTimeout()
  {
    return hostTimeout;
  }

  public Duration getRepeatDelay()
  {
    return repeatDelay;
  }

  public Duration getLoadTimeout()
  {
    return DEFAULT_LOAD_TIMEOUT;
  }
}
