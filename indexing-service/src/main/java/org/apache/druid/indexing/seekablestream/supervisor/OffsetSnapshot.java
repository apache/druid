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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable snapshot containing a consistent pair of offset maps: the highest ingested offsets
 * reported by tasks and the latest end offsets fetched from the underlying stream.
 *
 * <p>
 * The supervisor fetches task-reported ingested offsets first, then fetches end offsets from the stream.
 * Because these two values are captured at slightly different instants, the reported lag
 * (latestOffsetsFromStream - highestIngestedOffsets) may be slightly larger than the actual lag at any
 * precise moment.
 *
 * <p>
 * By publishing both maps together as a single atomic snapshot (using {@link java.util.concurrent.atomic.AtomicReference}),
 * readers (such as lag metrics and supervisor status) always observe a coherent and consistent view.
 * This produces stable and monotonic lag trends, avoiding artifacts like temporary negative lags.
 *
 * <p>
 * This class is generic and can be reused by all seekable-stream supervisors (Kafka, Kinesis, etc.).
 */
public final class OffsetSnapshot<PartitionIdType, SequenceOffsetType>
{
  private final ImmutableMap<PartitionIdType, SequenceOffsetType> highestIngestedOffsets;
  private final ImmutableMap<PartitionIdType, SequenceOffsetType> latestOffsetsFromStream;

  private OffsetSnapshot(
      @Nullable Map<PartitionIdType, SequenceOffsetType> highestIngestedOffsets,
      @Nullable Map<PartitionIdType, SequenceOffsetType> latestOffsetsFromStream
  )
  {
    this.highestIngestedOffsets = toImmutableOffsetMap(highestIngestedOffsets);
    this.latestOffsetsFromStream = toImmutableOffsetMap(latestOffsetsFromStream);
  }

  public static <PartitionIdType, SequenceOffsetType> OffsetSnapshot<PartitionIdType, SequenceOffsetType> of(
      @Nullable Map<PartitionIdType, SequenceOffsetType> currentOffsets,
      @Nullable Map<PartitionIdType, SequenceOffsetType> endOffsets
  )
  {
    return new OffsetSnapshot<>(currentOffsets, endOffsets);
  }

  private ImmutableMap<PartitionIdType, SequenceOffsetType> toImmutableOffsetMap(
      @Nullable Map<PartitionIdType, SequenceOffsetType> input
  )
  {
    if (input == null) {
      return ImmutableMap.of();
    }

    return ImmutableMap.copyOf(Maps.filterValues(input, Objects::nonNull));
  }

  public ImmutableMap<PartitionIdType, SequenceOffsetType> getHighestIngestedOffsets()
  {
    return highestIngestedOffsets;
  }

  public ImmutableMap<PartitionIdType, SequenceOffsetType> getLatestOffsetsFromStream()
  {
    return latestOffsetsFromStream;
  }
}
