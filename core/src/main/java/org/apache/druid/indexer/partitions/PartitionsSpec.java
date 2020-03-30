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

package org.apache.druid.indexer.partitions;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;

/**
 * PartitionsSpec describes the secondary partitioning method for data ingestion.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = HashedPartitionsSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = SingleDimensionPartitionsSpec.NAME, value = SingleDimensionPartitionsSpec.class),
    @JsonSubTypes.Type(name = SingleDimensionPartitionsSpec.OLD_NAME, value = SingleDimensionPartitionsSpec.class),  // for backward compatibility
    @JsonSubTypes.Type(name = HashedPartitionsSpec.NAME, value = HashedPartitionsSpec.class),
    @JsonSubTypes.Type(name = DynamicPartitionsSpec.NAME, value = DynamicPartitionsSpec.class)
})
public interface PartitionsSpec
{
  int DEFAULT_MAX_ROWS_PER_SEGMENT = 5_000_000;
  String MAX_ROWS_PER_SEGMENT = "maxRowsPerSegment";
  int HISTORICAL_NULL = -1;

  @JsonIgnore
  SecondaryPartitionType getType();

  /**
   * Returns the max number of rows per segment.
   * Implementations can have different default values which it could be even null.
   * Callers should use the right value depending on the context if this returns null.
   */
  @Nullable
  Integer getMaxRowsPerSegment();

  /**
   * Returns true if this partitionsSpec needs to determine the number of partitions to start data ingestion.
   * It should usually return true if perfect rollup is enforced but number of partitions is not specified.
   */
  boolean needsDeterminePartitions(boolean useForHadoopTask);

  /**
   * @return True if this partitionSpec's type is compatible with forceGuaranteedRollup=true.
   */
  @JsonIgnore
  default boolean isForceGuaranteedRollupCompatibleType()
  {
    return !(this instanceof DynamicPartitionsSpec);
  }

  /**
   * @return True if this partitionSpec's property values are compatible with forceGuaranteedRollup=true.
   */
  @JsonIgnore
  default boolean isForceGuaranteedRollupCompatible()
  {
    return getForceGuaranteedRollupIncompatiblityReason().isEmpty();
  }

  /**
   * @return Message describing why this partitionSpec is incompatible with forceGuaranteedRollup=true. Empty string if
   * the partitionSpec is compatible.
   */
  @JsonIgnore
  String getForceGuaranteedRollupIncompatiblityReason();

  /**
   * '-1' regarded as null for some historical reason.
   */
  static boolean isEffectivelyNull(@Nullable Integer val)
  {
    return val == null || val == HISTORICAL_NULL;
  }

  /**
   * '-1' regarded as null for some historical reason.
   */
  static boolean isEffectivelyNull(@Nullable Long val)
  {
    return val == null || val == HISTORICAL_NULL;
  }

  @Nullable
  static Integer resolveHistoricalNullIfNeeded(@Nullable Integer val)
  {
    return isEffectivelyNull(val) ? null : val;
  }
}
