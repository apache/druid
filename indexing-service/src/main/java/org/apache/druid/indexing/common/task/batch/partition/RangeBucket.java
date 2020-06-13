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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.timeline.partition.BuildingSingleDimensionShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;

public class RangeBucket implements PartitionBucket
{
  private final Interval interval;
  private final int bucketId;
  private final String dimension;
  @Nullable
  private final String start;
  @Nullable
  private final String end;

  public RangeBucket(
      Interval interval,
      int bucketId,
      String dimension,
      @Nullable String start,
      @Nullable String end
  )
  {
    this.interval = interval;
    this.bucketId = bucketId;
    this.dimension = dimension;
    this.start = start;
    this.end = end;
  }

  @Override
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public int getBucketId()
  {
    return bucketId;
  }

  @Override
  public ShardSpec toShardSpec(int partitionId)
  {
    return new BuildingSingleDimensionShardSpec(dimension, start, end, partitionId);
  }

  public boolean isInBucket(long timestamp, InputRow row)
  {
    final List<String> values = row.getDimension(dimension);

    if (values == null || values.size() != 1) {
      return checkValue(null);
    } else {
      return checkValue(values.get(0));
    }
  }

  private boolean checkValue(String value)
  {
    if (value == null) {
      return start == null;
    }

    if (start == null) {
      return end == null || value.compareTo(end) < 0;
    }

    return value.compareTo(start) >= 0 &&
           (end == null || value.compareTo(end) < 0);
  }

  @Override
  public String toString()
  {
    return "RangeBucket{" +
           "interval=" + interval +
           ", bucketId=" + bucketId +
           ", dimension='" + dimension + '\'' +
           ", start='" + start + '\'' +
           ", end='" + end + '\'' +
           '}';
  }
}
