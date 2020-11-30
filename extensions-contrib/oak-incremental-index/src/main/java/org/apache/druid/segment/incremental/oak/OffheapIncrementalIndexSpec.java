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

package org.apache.druid.segment.incremental.oak;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.segment.incremental.AppendableIndexBuilder;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.incremental.OffheapIncrementalIndex;
import org.apache.druid.utils.JvmUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * This extension also bundles the option to use the off-heap index for ingestion, that is not supported by default.
 */
public class OffheapIncrementalIndexSpec implements AppendableIndexSpec, Supplier<ByteBuffer>
{
  public static final String TYPE = "offheap";
  static final int DEFAULT_BUFFER_SIZE = 1 << 23;
  static final int DEFAULT_CACHE_SIZE = 1 << 30;

  final int bufferSize;
  final int cacheSize;

  final StupidPool<ByteBuffer> bufferPool;

  @JsonCreator
  public OffheapIncrementalIndexSpec(
      final @JsonProperty("bufferSize") @Nullable Integer bufferSize,
      final @JsonProperty("cacheSize") @Nullable Integer cacheSize
  )
  {
    this.bufferSize = bufferSize != null && bufferSize > 0 ? bufferSize : DEFAULT_BUFFER_SIZE;
    this.cacheSize = cacheSize != null && cacheSize > this.bufferSize ? cacheSize : DEFAULT_CACHE_SIZE;
    this.bufferPool = new StupidPool<>(
        "Off-heap incremental-index buffer pool",
        this,
        0,
        this.cacheSize / this.bufferSize
    );
  }

  @JsonProperty
  public int getBufferSize()
  {
    return bufferSize;
  }

  @JsonProperty
  public int getCacheSize()
  {
    return cacheSize;
  }

  @Override
  public ByteBuffer get()
  {
    return ByteBuffer.allocateDirect(bufferSize);
  }

  @Override
  public AppendableIndexBuilder builder()
  {
    return new OffheapIncrementalIndex.Builder().setBufferPool(bufferPool);
  }

  @Override
  public long getDefaultMaxBytesInMemory()
  {
    // In the realtime node, the entire JVM's direct memory is utilized for ingestion and persist operations.
    // But maxBytesInMemory only refers to the active index size and not to the index being flushed to disk and the
    // persist buffer.
    // To account for that, we set default to 1/2 of the max jvm's direct memory.
    return JvmUtils.getRuntimeInfo().getDirectMemorySizeBytes() / 2;
  }
}
