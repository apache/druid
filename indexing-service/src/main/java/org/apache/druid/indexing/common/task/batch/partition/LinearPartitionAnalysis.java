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

package org.apache.druid.indexing.common.task.batch.partition;

import com.google.common.base.Preconditions;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.IAE;
import org.joda.time.Interval;

import java.util.HashSet;
import java.util.Set;

/**
 * Partition analysis for the linear partitioning. This analysis is not complete because, in the linear partitioning,
 * segments are partitioned by their size which means they will be allocated dynamically during the indexing.
 */
public class LinearPartitionAnalysis implements PartitionAnalysis<Integer, DynamicPartitionsSpec>
{
  private final Set<Interval> intervals = new HashSet<>();
  private final DynamicPartitionsSpec partitionsSpec;

  public LinearPartitionAnalysis(DynamicPartitionsSpec partitionsSpec)
  {
    this.partitionsSpec = partitionsSpec;
  }

  @Override
  public DynamicPartitionsSpec getPartitionsSpec()
  {
    return partitionsSpec;
  }

  @Override
  public void updateBucket(Interval interval, Integer bucketAnalysis)
  {
    Preconditions.checkArgument(bucketAnalysis == 1, "There should be only one bucket with linear partitioining");
    intervals.add(interval);
  }

  @Override
  public Integer getBucketAnalysis(Interval interval)
  {
    if (intervals.contains(interval)) {
      return 1;
    } else {
      throw new IAE("Missing bucket analysis for interval[%s]", interval);
    }
  }

  @Override
  public Set<Interval> getAllIntervalsToIndex()
  {
    return intervals;
  }

  @Override
  public int getNumTimePartitions()
  {
    return intervals.size();
  }
}
