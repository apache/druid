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
import com.google.common.base.Preconditions;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Shuffle spec that generates up to a certain number of output partitions. Commonly used for shuffles between stages.
 */
public class GlobalSortMaxCountShuffleSpec implements GlobalSortShuffleSpec
{
  public static final String TYPE = "maxCount";

  private final ClusterBy clusterBy;
  private final int maxPartitions;
  private final boolean aggregate;

  @JsonCreator
  public GlobalSortMaxCountShuffleSpec(
      @JsonProperty("clusterBy") final ClusterBy clusterBy,
      @JsonProperty("partitions") final int maxPartitions,
      @JsonProperty("aggregate") final boolean aggregate
  )
  {
    this.clusterBy = Preconditions.checkNotNull(clusterBy, "clusterBy");
    this.maxPartitions = maxPartitions;
    this.aggregate = aggregate;

    if (maxPartitions < 1) {
      throw new IAE("Partition count must be at least 1");
    }

    if (!clusterBy.sortable()) {
      throw new IAE("ClusterBy key must be sortable");
    }

    if (clusterBy.getBucketByCount() > 0) {
      // Only GlobalSortTargetSizeShuffleSpec supports bucket-by.
      throw new IAE("Cannot bucket with %s partitioning", TYPE);
    }
  }

  @Override
  public ShuffleKind kind()
  {
    return ShuffleKind.GLOBAL_SORT;
  }

  @Override
  @JsonProperty("aggregate")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean doesAggregate()
  {
    return aggregate;
  }

  @Override
  public boolean mustGatherResultKeyStatistics()
  {
    return maxPartitions > 1 || clusterBy.getBucketByCount() > 0;
  }

  @Override
  public Either<Long, ClusterByPartitions> generatePartitionsForGlobalSort(
      @Nullable final ClusterByStatisticsCollector collector,
      final int maxNumPartitions
  )
  {
    if (!mustGatherResultKeyStatistics()) {
      return Either.value(ClusterByPartitions.oneUniversalPartition());
    } else if (maxPartitions > maxNumPartitions) {
      return Either.error((long) maxPartitions);
    } else {
      final ClusterByPartitions generatedPartitions = collector.generatePartitionsWithMaxCount(maxPartitions);
      if (generatedPartitions.size() <= maxNumPartitions) {
        return Either.value(generatedPartitions);
      } else {
        return Either.error((long) generatedPartitions.size());
      }
    }
  }

  @Override
  @JsonProperty
  public ClusterBy clusterBy()
  {
    return clusterBy;
  }

  @Override
  public int partitionCount()
  {
    if (maxPartitions == 1) {
      return 1;
    } else {
      throw new ISE("Number of partitions not known for [%s] with maxPartitions[%d].", kind(), maxPartitions);
    }
  }

  @JsonProperty("partitions")
  public int getMaxPartitions()
  {
    return maxPartitions;
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
    GlobalSortMaxCountShuffleSpec that = (GlobalSortMaxCountShuffleSpec) o;
    return maxPartitions == that.maxPartitions
           && aggregate == that.aggregate
           && Objects.equals(clusterBy, that.clusterBy);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(clusterBy, maxPartitions, aggregate);
  }

  @Override
  public String toString()
  {
    return "MaxCountShuffleSpec{" +
           "clusterBy=" + clusterBy +
           ", partitions=" + maxPartitions +
           ", aggregate=" + aggregate +
           '}';
  }
}
