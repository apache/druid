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
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.utils.JvmUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Oak incremental index spec (describes the in-memory indexing method for data ingestion).
 */
public class OakIncrementalIndexSpec implements AppendableIndexSpec
{
  public static final String TYPE = "oak";

  final long oakMaxMemoryCapacity;
  final int oakBlockSize;
  final int oakChunkMaxItems;

  @JsonCreator
  public OakIncrementalIndexSpec(
      final @JsonProperty("oakMaxMemoryCapacity") @Nullable Long oakMaxMemoryCapacity,
      final @JsonProperty("oakBlockSize") @Nullable Integer oakBlockSize,
      final @JsonProperty("oakChunkMaxItems") @Nullable Integer oakChunkMaxItems
  )
  {
    this.oakMaxMemoryCapacity = oakMaxMemoryCapacity != null && oakMaxMemoryCapacity > 0 ? oakMaxMemoryCapacity :
        OakIncrementalIndex.Builder.DEFAULT_OAK_MAX_MEMORY_CAPACITY;
    this.oakBlockSize = oakBlockSize != null && oakBlockSize > 0 ? oakBlockSize :
        OakIncrementalIndex.Builder.DEFAULT_OAK_BLOCK_SIZE;
    this.oakChunkMaxItems = oakChunkMaxItems != null && oakChunkMaxItems > 0 ? oakChunkMaxItems :
        OakIncrementalIndex.Builder.DEFAULT_OAK_CHUNK_MAX_ITEMS;
  }

  @JsonProperty
  public long getOakMaxMemoryCapacity()
  {
    return oakMaxMemoryCapacity;
  }

  @JsonProperty
  public int getOakBlockSize()
  {
    return oakBlockSize;
  }

  @JsonProperty
  public int getOakChunkMaxItems()
  {
    return oakChunkMaxItems;
  }

  @Nonnull
  @Override
  public OakIncrementalIndex.Builder builder()
  {
    return new OakIncrementalIndex.Builder()
        .setOakMaxMemoryCapacity(oakMaxMemoryCapacity)
        .setOakBlockSize(oakBlockSize)
        .setOakChunkMaxItems(oakChunkMaxItems);
  }

  @Override
  public long getDefaultMaxBytesInMemory()
  {
    // Since Oak allocates its keys/values directly, it is not subject to the JVM's on/off-heap limitations.
    // Despite this, we have to respect runtime resource limits if they aren't specified by the ingestion specs.
    // To ensure that the Oak index doesn't use more resources than available, we use the same default
    // `maxBytesInMemory` as the on-heap index, i.e., 1/6 of the maximal heap size.
    // It assumes that the middle-manager is configured correctly according to the machine's resources.
    return JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes() / 6;
  }
}
