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
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Shuffle spec that generates up to a certain number of output partitions. Commonly used for shuffles between stages.
 */
public class MaxCountShuffleSpec implements ShuffleSpec
{
  private final ClusterBy clusterBy;
  private final int partitions;
  private final boolean aggregate;

  @JsonCreator
  public MaxCountShuffleSpec(
      @JsonProperty("clusterBy") final ClusterBy clusterBy,
      @JsonProperty("partitions") final int partitions,
      @JsonProperty("aggregate") final boolean aggregate
  )
  {
    this.clusterBy = Preconditions.checkNotNull(clusterBy, "clusterBy");
    this.partitions = partitions;
    this.aggregate = aggregate;

    if (partitions < 1) {
      throw new IAE("Partition count must be at least 1");
    }
  }

  @Override
  @JsonProperty("aggregate")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean doesAggregateByClusterKey()
  {
    return aggregate;
  }

  @Override
  public boolean needsStatistics()
  {
    return partitions > 1 || clusterBy.getBucketByCount() > 0;
  }

  @Override
  public Either<Long, ClusterByPartitions> generatePartitions(
      @Nullable final ClusterByStatisticsCollector collector,
      final int maxNumPartitions
  )
  {
    if (!needsStatistics()) {
      return Either.value(ClusterByPartitions.oneUniversalPartition());
    } else if (partitions > maxNumPartitions) {
      return Either.error((long) partitions);
    } else {
      final ClusterByPartitions generatedPartitions = collector.generatePartitionsWithMaxCount(partitions);
      if (generatedPartitions.size() <= maxNumPartitions) {
        return Either.value(generatedPartitions);
      } else {
        return Either.error((long) generatedPartitions.size());
      }
    }
  }

  @Override
  @JsonProperty
  public ClusterBy getClusterBy()
  {
    return clusterBy;
  }

  @JsonProperty
  int getPartitions()
  {
    return partitions;
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
    MaxCountShuffleSpec that = (MaxCountShuffleSpec) o;
    return partitions == that.partitions
           && aggregate == that.aggregate
           && Objects.equals(clusterBy, that.clusterBy);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(clusterBy, partitions, aggregate);
  }

  @Override
  public String toString()
  {
    return "MaxCountShuffleSpec{" +
           "clusterBy=" + clusterBy +
           ", partitions=" + partitions +
           ", aggregate=" + aggregate +
           '}';
  }
}
