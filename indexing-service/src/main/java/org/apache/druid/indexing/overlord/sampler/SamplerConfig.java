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

package org.apache.druid.indexing.overlord.sampler;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SamplerConfig
{
  private static final int DEFAULT_NUM_ROWS = 200;
  private static final boolean DEFAULT_SKIP_CACHE = false;
  private static final int DEFAULT_TIMEOUT_MS = 10000;

  private final int numRows;
  private final String cacheKey;
  private final boolean skipCache;
  private final int timeoutMs;

  @JsonCreator
  public SamplerConfig(
      @JsonProperty("numRows") Integer numRows,
      @JsonProperty("cacheKey") String cacheKey,
      @JsonProperty("skipCache") Boolean skipCache,
      @JsonProperty("timeoutMs") Integer timeoutMs
  )
  {
    this.numRows = numRows != null ? numRows : DEFAULT_NUM_ROWS;
    this.cacheKey = cacheKey;
    this.skipCache = skipCache != null ? skipCache : DEFAULT_SKIP_CACHE;
    this.timeoutMs = timeoutMs != null ? timeoutMs : DEFAULT_TIMEOUT_MS;
  }

  public int getNumRows()
  {
    return numRows;
  }

  public String getCacheKey()
  {
    return cacheKey;
  }

  public boolean isSkipCache()
  {
    return skipCache;
  }

  public int getTimeoutMs()
  {
    return timeoutMs;
  }

  public static SamplerConfig empty()
  {
    return new SamplerConfig(null, null, null, null);
  }
}
