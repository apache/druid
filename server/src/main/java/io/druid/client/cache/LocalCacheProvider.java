/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.client.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;

/**
 */
public class LocalCacheProvider implements CacheProvider
{
  public LocalCacheProvider(){
    this(null, null, null);
  }
  @JsonCreator
  public LocalCacheProvider(
      @Min(0) @JsonProperty("sizeInBytes") Long sizeInBytes,
      @Min(0) @JsonProperty("initialSize") Integer initialSize,
      @Min(0) @JsonProperty("logEvictionCount") Integer logEvictionCount
  ){
    this.sizeInBytes = sizeInBytes == null ? 0 : sizeInBytes;
    this.initialSize = initialSize == null ? 500_000 : initialSize;
    this.logEvictionCount = logEvictionCount == null ? 0 : logEvictionCount;
  }
  @JsonProperty
  @Min(0)
  private final long sizeInBytes;

  @JsonProperty
  @Min(0)
  private final int initialSize;

  @JsonProperty
  @Min(0)
  private final int logEvictionCount;

  @Override
  public Cache get()
  {
    return new MapCache(new ByteCountingLRUMap(initialSize, logEvictionCount, sizeInBytes));
  }
}
