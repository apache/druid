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
  private static final int DEFAULT_TIMEOUT_MS = 10000;

  private final int numRows;
  private final int timeoutMs;

  @JsonCreator
  public SamplerConfig(
      @JsonProperty("numRows") Integer numRows,
      @JsonProperty("timeoutMs") Integer timeoutMs
  )
  {
    this.numRows = numRows != null ? numRows : DEFAULT_NUM_ROWS;
    this.timeoutMs = timeoutMs != null ? timeoutMs : DEFAULT_TIMEOUT_MS;

    Preconditions.checkArgument(this.numRows <= MAX_NUM_ROWS, "numRows must be <= %s", MAX_NUM_ROWS);
  }

  /**
   * The maximum number of rows to return in a response. The actual number of returned rows may be less if:
   *   - The sampled source contains less data.
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
    return new SamplerConfig(null, null);
  }
}
