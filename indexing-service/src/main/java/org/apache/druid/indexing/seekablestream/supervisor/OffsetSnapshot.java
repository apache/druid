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
 * Previously, the supervisor kept these maps in separate `volatile` fields and updated them
 * in two steps. Because updates to two separate volatiles are not atomic as a pair,
 * refresh cycles could interleave: task-reported current offsets might be updated while the
 * end-offsets remained stale (or vice versa). Under load or when some tasks are slow, this
 * interleaving produced inconsistent snapshots and occasionally resulted in negative partition
 * lag (endOffset - currentOffset < 0) in metrics.
 *
 * This class is used together with an AtomicReference so the supervisor can publish both maps as
 * a single atomic unit. Readers (lag metrics, supervisor status, etc.) always see a coherent snapshot
 * taken at one logical point in time, eliminating negative-lag artifacts caused by inconsistent state.
 *
 * The class is generic so it can be reused by all seekable-stream supervisors (Kafka, Kinesis, etc.).
 *
 * PR: https://github.com/apache/druid/pull/18750
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
