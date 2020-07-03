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

package org.apache.druid.timeline.partition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.RangeSet;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.timeline.DataSegment;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This shardSpec is used only for the segments created by overwriting tasks with segment lock enabled.
 * When the segment lock is used, there is a concept of atomic update group which is a set of segments atomically
 * becoming queryable together in Brokers. It is a similar concept to the core partition set (explained
 * {@link NumberedShardSpec}), but different in a sense that there is only one core partition set per time chunk
 * while there could be multiple atomic update groups in one time chunk.
 *
 * The atomic update group has the root partition range and the minor version to determine the visibility between
 * atomic update groups; the group of the highest minor version in the same root partition range becomes queryable
 * when they have the same major version ({@link DataSegment#getVersion()}).
 *
 * Note that this shardSpec is used only when you overwrite existing segments with segment lock enabled.
 * If the task doesn't overwrite segments, it will use NumberedShardSpec instead even when segment lock is used.
 * Similar to NumberedShardSpec, the size of the atomic update group is determined when the task publishes segments
 * at the end of ingestion. As a result, {@link #atomicUpdateGroupSize} is set to
 * {@link PartitionIds#UNKNOWN_ATOMIC_UPDATE_GROUP_SIZE} first, and updated when publishing segments
 * in {@code SegmentPublisherHelper#annotateShardSpec}.
 *
 * @see AtomicUpdateGroup
 */
public class NumberedOverwriteShardSpec implements OverwriteShardSpec
{
  public static final String TYPE = "numbered_overwrite";
  private final int partitionId;

  private final short startRootPartitionId;
  private final short endRootPartitionId; // exclusive
  private final short minorVersion;
  private final short atomicUpdateGroupSize; // number of segments in atomicUpdateGroup

  @JsonCreator
  public NumberedOverwriteShardSpec(
      @JsonProperty("partitionId") int partitionId,
      @JsonProperty("startRootPartitionId") int startRootPartitionId,
      @JsonProperty("endRootPartitionId") int endRootPartitionId,
      @JsonProperty("minorVersion") short minorVersion,
      @JsonProperty("atomicUpdateGroupSize") short atomicUpdateGroupSize
  )
  {
    Preconditions.checkArgument(
        partitionId >= PartitionIds.NON_ROOT_GEN_START_PARTITION_ID
        && partitionId < PartitionIds.NON_ROOT_GEN_END_PARTITION_ID,
        "partitionNum[%s] >= %s && partitionNum[%s] < %s",
        partitionId,
        PartitionIds.NON_ROOT_GEN_START_PARTITION_ID,
        partitionId,
        PartitionIds.NON_ROOT_GEN_END_PARTITION_ID
    );
    Preconditions.checkArgument(
        startRootPartitionId >= PartitionIds.ROOT_GEN_START_PARTITION_ID
        && startRootPartitionId < PartitionIds.ROOT_GEN_END_PARTITION_ID,
        "startRootPartitionId[%s] >= %s && startRootPartitionId[%s] < %s",
        startRootPartitionId,
        PartitionIds.ROOT_GEN_START_PARTITION_ID,
        startRootPartitionId,
        PartitionIds.ROOT_GEN_END_PARTITION_ID
    );
    Preconditions.checkArgument(
        endRootPartitionId >= PartitionIds.ROOT_GEN_START_PARTITION_ID
        && endRootPartitionId < PartitionIds.ROOT_GEN_END_PARTITION_ID,
        "endRootPartitionId[%s] >= %s && endRootPartitionId[%s] < %s",
        endRootPartitionId,
        PartitionIds.ROOT_GEN_START_PARTITION_ID,
        endRootPartitionId,
        PartitionIds.ROOT_GEN_END_PARTITION_ID
    );
    Preconditions.checkArgument(minorVersion > 0, "minorVersion[%s] > 0", minorVersion);
    Preconditions.checkArgument(
        atomicUpdateGroupSize > 0 || atomicUpdateGroupSize == PartitionIds.UNKNOWN_ATOMIC_UPDATE_GROUP_SIZE,
        "atomicUpdateGroupSize[%s] > 0 or == %s",
        atomicUpdateGroupSize,
        PartitionIds.UNKNOWN_ATOMIC_UPDATE_GROUP_SIZE
    );

    this.partitionId = partitionId;
    this.startRootPartitionId = (short) startRootPartitionId;
    this.endRootPartitionId = (short) endRootPartitionId;
    this.minorVersion = minorVersion;
    this.atomicUpdateGroupSize = atomicUpdateGroupSize;
  }

  public NumberedOverwriteShardSpec(
      int partitionId,
      int startRootPartitionId,
      int endRootPartitionId,
      short minorVersion
  )
  {
    this(
        partitionId,
        startRootPartitionId,
        endRootPartitionId,
        minorVersion,
        PartitionIds.UNKNOWN_ATOMIC_UPDATE_GROUP_SIZE
    );
  }

  @Override
  public OverwriteShardSpec withAtomicUpdateGroupSize(short atomicUpdateGroupSize)
  {
    return new NumberedOverwriteShardSpec(
        this.partitionId,
        this.startRootPartitionId,
        this.endRootPartitionId,
        this.minorVersion,
        atomicUpdateGroupSize
    );
  }

  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    return new NumberedOverwritingPartitionChunk<>(partitionId, obj);
  }

  @Override
  public boolean isInChunk(long timestamp, InputRow inputRow)
  {
    return true;
  }

  @JsonProperty("partitionId")
  @Override
  public int getPartitionNum()
  {
    return partitionId;
  }

  @JsonProperty
  @Override
  public int getStartRootPartitionId()
  {
    return Short.toUnsignedInt(startRootPartitionId);
  }

  @JsonProperty
  @Override
  public int getEndRootPartitionId()
  {
    return Short.toUnsignedInt(endRootPartitionId);
  }

  @JsonProperty
  @Override
  public short getMinorVersion()
  {
    return minorVersion;
  }

  @JsonProperty
  @Override
  public short getAtomicUpdateGroupSize()
  {
    return atomicUpdateGroupSize;
  }

  @Override
  public ShardSpecLookup getLookup(List<? extends ShardSpec> shardSpecs)
  {
    return (long timestamp, InputRow row) -> shardSpecs.get(0);
  }

  @Override
  public List<String> getDomainDimensions()
  {
    return Collections.emptyList();
  }

  @Override
  public boolean possibleInDomain(Map<String, RangeSet<String>> domain)
  {
    return true;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NumberedOverwriteShardSpec that = (NumberedOverwriteShardSpec) o;
    return partitionId == that.partitionId &&
           startRootPartitionId == that.startRootPartitionId &&
           endRootPartitionId == that.endRootPartitionId &&
           minorVersion == that.minorVersion &&
           atomicUpdateGroupSize == that.atomicUpdateGroupSize;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(partitionId, startRootPartitionId, endRootPartitionId, minorVersion, atomicUpdateGroupSize);
  }

  @Override
  public String toString()
  {
    return "NumberedOverwriteShardSpec{" +
           "partitionId=" + partitionId +
           ", startRootPartitionId=" + startRootPartitionId +
           ", endRootPartitionId=" + endRootPartitionId +
           ", minorVersion=" + minorVersion +
           ", atomicUpdateGroupSize=" + atomicUpdateGroupSize +
           '}';
  }
}
