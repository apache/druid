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
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Shuffle spec that generates a variable number of partitions, attempting to keep the number of rows in each partition
 * to a particular {@link #targetSize}. Commonly used when generating segments, which we want to have a certain number
 * of rows per segment.
 */
public class TargetSizeShuffleSpec implements ShuffleSpec
{
  private final ClusterBy clusterBy;
  private final long targetSize;
  private final boolean aggregate;

  @JsonCreator
  public TargetSizeShuffleSpec(
      @JsonProperty("clusterBy") final ClusterBy clusterBy,
      @JsonProperty("targetSize") final long targetSize,
      @JsonProperty("aggregate") final boolean aggregate
  )
  {
    this.clusterBy = Preconditions.checkNotNull(clusterBy, "clusterBy");
    this.targetSize = targetSize;
    this.aggregate = aggregate;
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
    return true;
  }

  @Override
  public Either<Long, ClusterByPartitions> generatePartitions(
      @Nullable final ClusterByStatisticsCollector collector,
      final int maxNumPartitions
  )
  {
    final long expectedPartitions = collector.estimatedTotalWeight() / targetSize;

    if (expectedPartitions > maxNumPartitions) {
      return Either.error(expectedPartitions);
    } else {
      final ClusterByPartitions generatedPartitions = collector.generatePartitionsWithTargetWeight(targetSize);
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
  long getTargetSize()
  {
    return targetSize;
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
    TargetSizeShuffleSpec that = (TargetSizeShuffleSpec) o;
    return targetSize == that.targetSize && aggregate == that.aggregate && Objects.equals(clusterBy, that.clusterBy);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(clusterBy, targetSize, aggregate);
  }

  @Override
  public String toString()
  {
    return "TargetSizeShuffleSpec{" +
           "clusterBy=" + clusterBy +
           ", targetSize=" + targetSize +
           ", aggregate=" + aggregate +
           '}';
  }
}
