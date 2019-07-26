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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import javax.annotation.Nullable;

/**
 * PartitionsSpec describes the secondary partitioning method for data ingestion.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "single_dim_partitions", value = SingleDimensionPartitionsSpec.class),
    @JsonSubTypes.Type(name = "hashed_partitions", value = HashedPartitionsSpec.class),
    @JsonSubTypes.Type(name = "dynamic_partitions", value = DynamicPartitionsSpec.class)
})
public interface PartitionsSpec
{
  int DEFAULT_MAX_ROWS_PER_SEGMENT = 5_000_000;

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
  boolean needsDeterminePartitions();

  /**
   * '-1' regarded as null for some historical reason.
   */
  static boolean isEffectivelyNull(@Nullable Integer val)
  {
    return val == null || val == -1;
  }

  /**
   * '-1' regarded as null for some historical reason.
   */
  static boolean isEffectivelyNull(@Nullable Long val)
  {
    return val == null || val == -1;
  }
}
