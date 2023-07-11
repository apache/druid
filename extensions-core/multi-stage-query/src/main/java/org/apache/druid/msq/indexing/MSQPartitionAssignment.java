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

package org.apache.druid.msq.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;

import java.util.Map;
import java.util.Objects;

public class MSQPartitionAssignment
{
  private final ClusterByPartitions partitions;
  private final Map<Integer, SegmentIdWithShardSpec> allocations;

  @JsonCreator
  public MSQPartitionAssignment(
      @JsonProperty("partitions") ClusterByPartitions partitions,
      @JsonProperty("allocations") Map<Integer, SegmentIdWithShardSpec> allocations
  )
  {
    this.partitions = Preconditions.checkNotNull(partitions, "partitions");
    this.allocations = Preconditions.checkNotNull(allocations, "allocations");

    // Sanity checks.
    for (final int partitionNumber : allocations.keySet()) {
      if (partitionNumber < 0 || partitionNumber >= partitions.size()) {
        throw new IAE("Partition [%s] out of bounds", partitionNumber);
      }
    }
  }

  @JsonProperty
  public ClusterByPartitions getPartitions()
  {
    return partitions;
  }

  @JsonProperty
  public Map<Integer, SegmentIdWithShardSpec> getAllocations()
  {
    return allocations;
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
    MSQPartitionAssignment that = (MSQPartitionAssignment) o;
    return Objects.equals(partitions, that.partitions) && Objects.equals(
        allocations,
        that.allocations
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(partitions, allocations);
  }

  @Override
  public String toString()
  {
    return "MSQPartitionAssignment{" +
           "partitions=" + partitions +
           ", allocations=" + allocations +
           '}';
  }
}
