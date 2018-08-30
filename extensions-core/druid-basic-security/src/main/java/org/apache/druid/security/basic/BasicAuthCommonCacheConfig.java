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

package org.apache.druid.security.basic;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BasicAuthCommonCacheConfig
{
  private static final long DEFAULT_POLLING_PERIOD = 60000;
  private static final long DEFAULT_MAX_RANDOM_DELAY = DEFAULT_POLLING_PERIOD / 10;
  private static final int DEFAULT_MAX_SYNC_RETRIES = 10;

  @JsonProperty
  private final long pollingPeriod;

  @JsonProperty
  private final long maxRandomDelay;

  @JsonProperty
  private final String cacheDirectory;

  @JsonProperty
  private final int maxSyncRetries;

  @JsonCreator
  public BasicAuthCommonCacheConfig(
      @JsonProperty("pollingPeriod") Long pollingPeriod,
      @JsonProperty("maxRandomDelay") Long maxRandomDelay,
      @JsonProperty("cacheDirectory") String cacheDirectory,
      @JsonProperty("maxSyncRetries") Integer maxSyncRetries
  )
  {
    this.pollingPeriod = pollingPeriod == null ? DEFAULT_POLLING_PERIOD : pollingPeriod;
    this.maxRandomDelay = maxRandomDelay == null ? DEFAULT_MAX_RANDOM_DELAY : maxRandomDelay;
    this.cacheDirectory = cacheDirectory;
    this.maxSyncRetries = maxSyncRetries == null ? DEFAULT_MAX_SYNC_RETRIES : maxSyncRetries;
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

  @JsonProperty
  public String getCacheDirectory()
  {
    return cacheDirectory;
  }

  @JsonProperty
  public int getMaxSyncRetries()
  {
    return maxSyncRetries;
  }
}
