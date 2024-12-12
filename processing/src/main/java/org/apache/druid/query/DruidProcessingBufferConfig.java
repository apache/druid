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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.apache.druid.java.util.common.HumanReadableBytes;

import javax.annotation.Nullable;

public class DruidProcessingBufferConfig
{
  public static final HumanReadableBytes DEFAULT_PROCESSING_BUFFER_SIZE_BYTES = HumanReadableBytes.valueOf(-1);
  public static final int MAX_DEFAULT_PROCESSING_BUFFER_SIZE_BYTES = 1024 * 1024 * 1024;
  public static final int DEFAULT_INITIAL_BUFFERS_FOR_INTERMEDIATE_POOL = 0;

  @JsonProperty
  private final HumanReadableBytes sizeBytes;

  @JsonProperty
  private final int poolCacheMaxCount;

  @JsonProperty
  private final int poolCacheInitialCount;

  @JsonCreator
  public DruidProcessingBufferConfig(
      @JsonProperty("sizeBytes") @Nullable HumanReadableBytes sizeBytes,
      @JsonProperty("poolCacheMaxCount") @Nullable Integer poolCacheMaxCount,
      @JsonProperty("poolCacheInitialCount") @Nullable Integer poolCacheInitialCount
  )
  {
    this.sizeBytes = Configs.valueOrDefault(sizeBytes, DEFAULT_PROCESSING_BUFFER_SIZE_BYTES);
    this.poolCacheInitialCount = Configs.valueOrDefault(
        poolCacheInitialCount,
        DEFAULT_INITIAL_BUFFERS_FOR_INTERMEDIATE_POOL
    );
    this.poolCacheMaxCount = Configs.valueOrDefault(poolCacheMaxCount, Integer.MAX_VALUE);
  }

  public DruidProcessingBufferConfig()
  {
    this(null, null, null);
  }

  public HumanReadableBytes getBufferSize()
  {
    return sizeBytes;
  }

  public int getPoolCacheMaxCount()
  {
    return poolCacheMaxCount;
  }

  public int getPoolCacheInitialCount()
  {
    return poolCacheInitialCount;
  }
}
