/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.security.basic;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BasicAuthCommonCacheConfig
{
  private static final long DEFAULT_POLLING_PERIOD = 60000;
  private static final long DEFAULT_MAX_RANDOM_DELAY = DEFAULT_POLLING_PERIOD / 10;

  @JsonProperty
  private final long pollingPeriod;

  @JsonProperty
  private final long maxRandomDelay;

  @JsonCreator
  public BasicAuthCommonCacheConfig(
      @JsonProperty("pollingPeriod") Long pollingPeriod,
      @JsonProperty("maxRandomDelay") Long maxRandomDelay
  )
  {
    this.pollingPeriod = pollingPeriod == null ? DEFAULT_POLLING_PERIOD : pollingPeriod;
    this.maxRandomDelay = maxRandomDelay == null ? DEFAULT_MAX_RANDOM_DELAY : maxRandomDelay;
  }

  @JsonProperty
  public long getPollingPeriod()
  {
    return pollingPeriod;
  }

  @JsonProperty
  public long getMaxRandomDelay()
  {
    return maxRandomDelay;
  }
}
