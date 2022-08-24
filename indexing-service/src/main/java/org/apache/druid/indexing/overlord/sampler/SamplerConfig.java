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
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.segment.indexing.DataSchema;

import javax.annotation.Nullable;

public class SamplerConfig
{
  private static final int DEFAULT_NUM_ROWS = 200;
  private static final int MAX_NUM_ROWS = 5000;
  private static final int DEFAULT_TIMEOUT_MS = 10000;



  private final int numRows;
  private final int timeoutMs;

  private final long maxBytesInMemory;

  private final long maxClientResponseBytes;

  @JsonCreator
  public SamplerConfig(
      @JsonProperty("numRows") @Nullable Integer numRows,
      @JsonProperty("timeoutMs") @Nullable Integer timeoutMs,
      @JsonProperty("maxBytesInMemory") @Nullable HumanReadableBytes maxBytesInMemory,
      @JsonProperty("maxClientResponseBytes") @Nullable HumanReadableBytes maxClientResponseBytes
  )
  {
    this.numRows = numRows != null ? numRows : DEFAULT_NUM_ROWS;
    this.timeoutMs = timeoutMs != null ? timeoutMs : DEFAULT_TIMEOUT_MS;
    this.maxBytesInMemory = maxBytesInMemory != null ? maxBytesInMemory.getBytes() : Long.MAX_VALUE;
    this.maxClientResponseBytes = maxClientResponseBytes != null ? maxClientResponseBytes.getBytes() : 0;

    Preconditions.checkArgument(this.numRows <= MAX_NUM_ROWS, "numRows must be <= %s", MAX_NUM_ROWS);
  }

  /**
   * The maximum number of rows to return in a response. The actual number of returned rows may be less if:
   *   - The sampled source contains less data.
   *   - {@link SamplerConfig#timeoutMs} elapses before this value is reached
   *   - {@link org.apache.druid.segment.indexing.granularity.GranularitySpec#isRollup()} is true and input rows get
   *     rolled-up into fewer indexed rows.
   *   - The incremental index performing the sampling reaches {@link SamplerConfig#getMaxBytesInMemory()} before this
   *     value is reached
   *   - The estimated size of the {@link org.apache.druid.client.indexing.SamplerResponse} crosses
   *     {@link SamplerConfig#getMaxClientResponseBytes()}
   *
   * @return maximum number of sampled rows to return
   */
  public int getNumRows()
  {
    return numRows;
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

  /**
   * Maximum number of bytes in memory that the {@link org.apache.druid.segment.incremental.IncrementalIndex} used by
   * {@link InputSourceSampler#sample(InputSource, InputFormat, DataSchema, SamplerConfig)} will be allowed to
   * accumulate before aborting sampling. Particularly useful for limiting footprint of sample operations as well as
   * overall response size from sample requests. However, it is not directly correlated to response size since it
   * also contains the "raw" input data, so actual responses will likely be at least twice the size of this value,
   * depending on factors such as number of transforms, aggregations in the case of rollup, whether all columns
   * of the input are present in the dimension spec, and so on. If it is preferred to control client response size,
   * use {@link SamplerConfig#getMaxClientResponseBytes()} instead.
   */
  public long getMaxBytesInMemory()
  {
    return maxBytesInMemory;
  }

  /**
   * Maximum number of bytes to accumulate for a {@link org.apache.druid.client.indexing.SamplerResponse} before
   * shutting off sampling. To directly control the size of the
   * {@link org.apache.druid.segment.incremental.IncrementalIndex} used for sampling, use
   * {@link SamplerConfig#getMaxBytesInMemory()} instead.
   */
  public long getMaxClientResponseBytes()
  {
    return maxClientResponseBytes;
  }

  public static SamplerConfig empty()
  {
    return new SamplerConfig(null, null, null, null);
  }
}
