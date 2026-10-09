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

package org.apache.druid.server.lookup.namespace;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class NamespaceExtractionConfig
{
  private static final long DEFAULT_RETIRED_CACHE_ENTRY_TIMEOUT_MILLIS = 15 * 60 * 1000L;

  /**
   * The default value of two is chosen because the overhead of having an extra idle thread of the minimum priority is
   * very low, but having more than one thread may save when one namespace extraction is stuck or taking too long time,
   * so all the others won't queue up and timeout.
   */
  @JsonProperty
  private int numExtractionThreads = 2;

  @JsonProperty
  private int numBufferedEntries = 100_000;

  @JsonProperty
  private int maxRetiredCacheEntries = 1;

  @JsonProperty
  private long retiredCacheEntryTimeoutMillis = DEFAULT_RETIRED_CACHE_ENTRY_TIMEOUT_MILLIS;

  public int getNumExtractionThreads()
  {
    return numExtractionThreads;
  }

  public void setNumExtractionThreads(int numExtractionThreads)
  {
    this.numExtractionThreads = numExtractionThreads;
  }

  public int getNumBufferedEntries()
  {
    return numBufferedEntries;
  }

  public void setNumBufferedEntries(int numBufferedEntries)
  {
    this.numBufferedEntries = numBufferedEntries;
  }

  public int getMaxRetiredCacheEntries()
  {
    return maxRetiredCacheEntries;
  }

  public void setMaxRetiredCacheEntries(int maxRetiredCacheEntries)
  {
    Preconditions.checkArgument(maxRetiredCacheEntries >= 1, "maxRetiredCacheEntries must be at least 1");
    this.maxRetiredCacheEntries = maxRetiredCacheEntries;
  }

  public long getRetiredCacheEntryTimeoutMillis()
  {
    return retiredCacheEntryTimeoutMillis;
  }

  public void setRetiredCacheEntryTimeoutMillis(long retiredCacheEntryTimeoutMillis)
  {
    Preconditions.checkArgument(
        retiredCacheEntryTimeoutMillis >= 0,
        "retiredCacheEntryTimeoutMillis must be non-negative"
    );
    this.retiredCacheEntryTimeoutMillis = retiredCacheEntryTimeoutMillis;
  }
}
