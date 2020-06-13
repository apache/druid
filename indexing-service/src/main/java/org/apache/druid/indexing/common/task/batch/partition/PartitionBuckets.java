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

import com.google.common.base.Optional;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.joda.time.Interval;

import java.util.Map;

public class PartitionBuckets
{
  private final GranularitySpec granularitySpec;
  private final Granularity queryGranularity;
  private final Map<Interval, PartitionBucketLookup> intervalToBuckets;

  public PartitionBuckets(GranularitySpec granularitySpec, Map<Interval, PartitionBucketLookup> intervalToBuckets)
  {
    this.granularitySpec = granularitySpec;
    this.queryGranularity = granularitySpec.getQueryGranularity();
    this.intervalToBuckets = intervalToBuckets;
  }

  public PartitionBucket lookupBucket(InputRow row)
  {
    final Optional<Interval> optInterval = granularitySpec.bucketInterval(row.getTimestamp());
    if (!optInterval.isPresent()) {
      throw new ISE("Cannot compute a bucketed interval for row[%s]", row);
    }
    final Interval interval = optInterval.get();
    final PartitionBucketLookup bucketLookup = intervalToBuckets.get(interval);
    if (bucketLookup == null) {
      throw new ISE("Cannot find partitionBucketLookup for row[%s], bucketed interval[%s]", row, interval);
    }
    final long bucketedTimestamp = queryGranularity.bucketStart(row.getTimestamp()).getMillis();
    return bucketLookup.find(bucketedTimestamp, row);
  }
}
