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

package org.apache.druid.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.common.config.Configs;
import org.joda.time.Duration;
import org.joda.time.Period;

/**
 */
public class HttpServerInventoryViewConfig
{
  private final Duration requestTimeout;
  private final Duration unstableAlertTimeout;
  private final int numThreads;

  @JsonCreator
  public HttpServerInventoryViewConfig(
      @JsonProperty("serverTimeout") Period requestTimeout,
      @JsonProperty("serverUnstabilityTimeout") Period unstableAlertTimeout,
      @JsonProperty("numThreads") Integer numThreads
  )
  {
    this.requestTimeout = Configs.valueOrDefault(requestTimeout, Period.minutes(4))
                                 .toStandardDuration();
    this.unstableAlertTimeout = Configs.valueOrDefault(unstableAlertTimeout, Period.minutes(1))
                                       .toStandardDuration();
    this.numThreads = Configs.valueOrDefault(numThreads, 5);

    Preconditions.checkArgument(this.requestTimeout.getMillis() > 0, "server timeout must be > 0 ms");
    Preconditions.checkArgument(this.numThreads > 1, "numThreads must be > 1");
  }

  public Duration getRequestTimeout()
  {
    return requestTimeout;
  }

  /**
   * Delay after which an alert is raised for an unstable server.
   */
  public Duration getUnstableAlertTimeout()
  {
    return unstableAlertTimeout;
  }

  public int getNumThreads()
  {
    return numThreads;
  }
}
