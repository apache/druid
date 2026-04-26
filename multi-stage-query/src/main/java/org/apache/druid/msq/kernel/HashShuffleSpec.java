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

package org.apache.druid.msq.kernel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.java.util.common.IAE;

import java.util.Objects;

public class HashShuffleSpec implements ShuffleSpec
{
  public static final String TYPE = "hash";

  private final ClusterBy clusterBy;
  private final int numPartitions;
  private final boolean adjustable;

  @JsonCreator
  public HashShuffleSpec(
      @JsonProperty("clusterBy") final ClusterBy clusterBy,
      @JsonProperty("partitions") final int numPartitions,
      @JsonProperty("adjustable") final boolean adjustable
  )
  {
    this.clusterBy = clusterBy;
    this.numPartitions = numPartitions;
    this.adjustable = adjustable;

    if (adjustable && numPartitions != 1) {
      throw new IAE("Partition count must be 1 when adjustable is true, but was [%d]", numPartitions);
    }

    if (clusterBy.getBucketByCount() > 0) {
      // Only GlobalSortTargetSizeShuffleSpec supports bucket-by.
      throw new IAE("Cannot bucket with %s partitioning (clusterBy = %s)", TYPE, clusterBy);
    }
  }

  @Override
  public ShuffleKind kind()
  {
    return clusterBy.sortable() && !clusterBy.isEmpty() ? ShuffleKind.HASH_LOCAL_SORT : ShuffleKind.HASH;
  }

  @Override
  @JsonProperty
  public ClusterBy clusterBy()
  {
    return clusterBy;
  }

  @Override
  @JsonProperty("partitions")
  public int partitionCount()
  {
    return numPartitions;
  }

  @Override
  @JsonProperty("adjustable")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isAdjustable()
  {
    return adjustable;
  }

  @Override
  public ShuffleSpec withPartitionCount(final int partitionCount)
  {
    return new HashShuffleSpec(
        clusterBy,
        partitionCount,
        false
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HashShuffleSpec that = (HashShuffleSpec) o;
    return numPartitions == that.numPartitions
           && adjustable == that.adjustable
           && Objects.equals(clusterBy, that.clusterBy);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(clusterBy, numPartitions, adjustable);
  }

  @Override
  public String toString()
  {
    return "HashShuffleSpec{" +
           "clusterBy=" + clusterBy +
           ", numPartitions=" + numPartitions +
           ", adjustable=" + adjustable +
           '}';
  }
}
