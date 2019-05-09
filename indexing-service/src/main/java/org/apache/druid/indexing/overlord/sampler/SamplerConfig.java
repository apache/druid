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
import com.google.common.base.Preconditions;

public class SamplerConfig
{
  private static final int DEFAULT_NUM_ROWS = 200;
  private static final int MAX_NUM_ROWS = 5000;
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

    Preconditions.checkArgument(this.numRows <= MAX_NUM_ROWS, "numRows must be <= %s", MAX_NUM_ROWS);
  }

  /**
   * The maximum number of rows to return in a response. The actual number of returned rows may be less if:
   *   - The sampled source contains less data.
   *   - We are reading from the cache ({@link SamplerConfig#cacheKey} is set and {@link SamplerConfig#isSkipCache()}
   *     is false) and the cache contains less data.
   *   - {@link SamplerConfig#timeoutMs} elapses before this value is reached.
   *   - {@link org.apache.druid.segment.indexing.granularity.GranularitySpec#isRollup()} is true and input rows get
   *     rolled-up into fewer indexed rows.
   *
   * @return maximum number of sampled rows to return
   */
  public int getNumRows()
  {
    return numRows;
  }

  /**
   * The sampler uses a best-effort system to attempt to cache the raw data so that future requests to the sampler
   * can be answered without reading again from the source. In addition to responsiveness benefits, this also provides a
   * better user experience for sources such as streams, where repeated calls to the sampler (which would happen as the
   * user tweaks data schema configurations) would otherwise return a different set of sampled data every time. For the
   * caching system to work, 1) the sampler must have access to the raw data (e.g. for {@link FirehoseSampler},
   * {@link org.apache.druid.data.input.InputRowPlusRaw#getRaw()} must be non-null) and 2) the parser must be an
   * implementation of {@link org.apache.druid.data.input.ByteBufferInputRowParser} since the data is cached as a byte
   * array. If these conditions are not satisfied, the cache returns a miss and the sampler would read from source.
   * <p>
   * {@link SamplerResponse} returns a {@link SamplerResponse#cacheKey} which should be supplied here in
   * {@link SamplerConfig} for future requests to prefer the cache if available. This field is ignored if
   * {@link SamplerConfig#skipCache} is true.
   *
   * @return key to use for locating previously cached raw data
   */
  public String getCacheKey()
  {
    return cacheKey;
  }

  /**
   * Whether to read/write to the cache. See cache description in {@link SamplerConfig#getCacheKey()}.
   *
   * @return true if cache reads and writes should be skipped
   */
  public boolean isSkipCache()
  {
    return skipCache;
  }

  /**
   * Time to wait in milliseconds before closing the sampler and returning the data which has already been read.
   * Particularly useful for handling streaming input sources where the rate of data is unknown, to prevent the sampler
   * from taking an excessively long time trying to reach {@link SamplerConfig#numRows}.
   *
   * @return timeout in milliseconds
   */
  public int getTimeoutMs()
  {
    return timeoutMs;
  }

  public static SamplerConfig empty()
  {
    return new SamplerConfig(null, null, null, null);
  }
}
