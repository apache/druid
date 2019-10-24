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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.RangeSet;

import java.util.List;
import java.util.Map;

/**
 * A Marker interface that exists to combine ShardSpec objects together for Jackson. Note that this is not an
 * extension API. Extensions are not expected to create new kinds of ShardSpecs.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(name = ShardSpec.Type.NONE, value = NoneShardSpec.class),
    @JsonSubTypes.Type(name = ShardSpec.Type.SINGLE, value = SingleDimensionShardSpec.class),
    @JsonSubTypes.Type(name = ShardSpec.Type.RANGE, value = DimensionRangeShardSpec.class),
    @JsonSubTypes.Type(name = ShardSpec.Type.LINEAR, value = LinearShardSpec.class),
    @JsonSubTypes.Type(name = ShardSpec.Type.NUMBERED, value = NumberedShardSpec.class),
    @JsonSubTypes.Type(name = ShardSpec.Type.HASHED, value = HashBasedNumberedShardSpec.class),
    @JsonSubTypes.Type(name = ShardSpec.Type.NUMBERED_OVERWRITE, value = NumberedOverwriteShardSpec.class),
    // BuildingShardSpecs are the shardSpec with missing numCorePartitions, and thus must not be published.
    // See BuildingShardSpec for more details.
    @JsonSubTypes.Type(name = ShardSpec.Type.BUILDING_NUMBERED, value = BuildingNumberedShardSpec.class),
    @JsonSubTypes.Type(name = ShardSpec.Type.BUILDING_HASHED, value = BuildingHashBasedNumberedShardSpec.class),
    @JsonSubTypes.Type(name = ShardSpec.Type.BUILDING_SINGLE_DIM, value = BuildingSingleDimensionShardSpec.class),
    @JsonSubTypes.Type(name = ShardSpec.Type.BUILDING_RANGE, value = BuildingDimensionRangeShardSpec.class),
    // BucketShardSpecs are the shardSpec with missing partitionId and numCorePartitions.
    // These shardSpecs must not be used in segment push.
    // See BucketShardSpec for more details.
    @JsonSubTypes.Type(name = ShardSpec.Type.BUCKET_HASH, value = HashBucketShardSpec.class),
    @JsonSubTypes.Type(name = ShardSpec.Type.BUCKET_SINGLE_DIM, value = SingleDimensionRangeBucketShardSpec.class),
    @JsonSubTypes.Type(name = ShardSpec.Type.BUCKET_RANGE, value = DimensionRangeBucketShardSpec.class)
})
public interface ShardSpec
{
  @JsonIgnore
  <T> PartitionChunk<T> createChunk(T obj);

  /**
   * Returns the partition ID of this segment.
   */
  int getPartitionNum();

  int getNumCorePartitions();

  /**
   * Returns the start root partition ID of the atomic update group which this segment belongs to.
   *
   * @see AtomicUpdateGroup
   */
  default int getStartRootPartitionId()
  {
    return getPartitionNum();
  }

  /**
   * Returns the end root partition ID of the atomic update group which this segment belongs to.
   *
   * @see AtomicUpdateGroup
   */
  default int getEndRootPartitionId()
  {
    return getPartitionNum() + 1;
  }

  /**
   * Returns the minor version associated to the atomic update group which this segment belongs to.
   *
   * @see AtomicUpdateGroup
   */
  default short getMinorVersion()
  {
    return 0;
  }

  /**
   * Returns the atomic update group size which this segment belongs to.
   *
   * @see AtomicUpdateGroup
   */
  default short getAtomicUpdateGroupSize()
  {
    return 1;
  }

  @JsonIgnore
  ShardSpecLookup getLookup(List<? extends ShardSpec> shardSpecs);

  /**
   * Get dimensions who have possible range for the rows this shard contains.
   *
   * @return list of dimensions who has its possible range. Dimensions with unknown possible range are not listed
   */
  @JsonIgnore
  List<String> getDomainDimensions();

  /**
   * if given domain ranges are not possible in this shard, return false; otherwise return true;
   * @return possibility of in domain
   */
  @JsonIgnore
  boolean possibleInDomain(Map<String, RangeSet<String>> domain);

  /**
   * Added in for NamedNumberedShardSpec
   * @return unique identifier for ShardSpec
   */
  default Object getIdentifier()
  {
    return this.getPartitionNum();
  }

  /**
   * Get the type name of this ShardSpec.
   */
  @JsonIgnore
  default String getType()
  {
    return Type.UNKNOWN;
  }

  /**
   * Returns true if this shardSpec and the given {@link PartialShardSpec} share the same partition space.
   * All shardSpecs except {@link OverwriteShardSpec} use the root-generation partition space and thus share the same
   * space.
   *
   * @see PartitionIds
   */
  default boolean sharePartitionSpace(PartialShardSpec partialShardSpec)
  {
    return !partialShardSpec.useNonRootGenerationPartitionSpace();
  }

  /**
   * ShardSpec type names.
   */
  interface Type
  {
    String UNKNOWN = "unknown";
    String NONE = "none";

    String SINGLE = "single";
    String RANGE = "range";
    String LINEAR = "linear";
    String NUMBERED = "numbered";
    String HASHED = "hashed";

    String NUMBERED_OVERWRITE = "numbered_overwrite";

    String BUILDING_NUMBERED = "building_numbered";
    String BUILDING_HASHED = "building_hashed";
    String BUILDING_SINGLE_DIM = "building_single_dim";
    String BUILDING_RANGE = "building_range";

    String BUCKET_HASH = "bucket_hash";
    String BUCKET_SINGLE_DIM = "bucket_single_dim";
    String BUCKET_RANGE = "bucket_range";
  }
}
