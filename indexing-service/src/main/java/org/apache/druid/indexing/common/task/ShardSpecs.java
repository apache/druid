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

package org.apache.druid.indexing.common.task;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.timeline.partition.BucketNumberedShardSpec;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 * This class represents a map of (Interval, ShardSpec) and is used for easy shardSpec generation.
 */
public class ShardSpecs
{
  private final Map<Interval, List<BucketNumberedShardSpec<?>>> map;
  private final Granularity queryGranularity;

  ShardSpecs(final Map<Interval, List<BucketNumberedShardSpec<?>>> map, Granularity queryGranularity)
  {
    this.map = map;
    this.queryGranularity = queryGranularity;
  }

  /**
   * Return a shardSpec for the given interval and input row.
   *
   * @param interval interval for shardSpec
   * @param row      input row
   *
   * @return a shardSpec
   */
  BucketNumberedShardSpec<?> getShardSpec(Interval interval, InputRow row)
  {
    final List<BucketNumberedShardSpec<?>> shardSpecs = map.get(interval);
    if (shardSpecs == null || shardSpecs.isEmpty()) {
      throw new ISE("Failed to get shardSpec for interval[%s]", interval);
    }
    final long truncatedTimestamp = queryGranularity.bucketStart(row.getTimestamp()).getMillis();
    return (BucketNumberedShardSpec<?>) shardSpecs.get(0).getLookup(shardSpecs).getShardSpec(truncatedTimestamp, row);
  }
}
