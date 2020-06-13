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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class HashBucketLookup implements PartitionBucketLookup<HashBucket>
{
  private final ObjectMapper jsonMapper;
  private final List<String> partitionDimensions;
  private final int numBuckets;
  private final Int2ObjectMap<HashBucket> bucketIdToBucket;

  public HashBucketLookup(
      ObjectMapper jsonMapper,
      List<String> partitionDimensions,
      Collection<HashBucket> buckets
  )
  {
    this.jsonMapper = jsonMapper;
    this.partitionDimensions = partitionDimensions;
    this.numBuckets = buckets.size();
    bucketIdToBucket = new Int2ObjectOpenHashMap<>(buckets.size());
    buckets.forEach(bucket -> {
      final HashBucket old = bucketIdToBucket.put(bucket.getBucketId(), bucket);
      if (old != null) {
        throw new ISE("Duplicate bucketId in bucket[%s] and bucket[%s]", bucket, old);
      }
    });
  }

  @Override
  public HashBucket find(long timestamp, InputRow row)
  {
    final int hash =
        Math.abs(HashBasedNumberedShardSpec.hash(jsonMapper, partitionDimensions, timestamp, row) % numBuckets);
    return Preconditions.checkNotNull(
        bucketIdToBucket.get(hash),
        "Cannot find bucket for timestamp[%s] and row[%s]",
        timestamp,
        row
    );
  }

  @Override
  public Iterator<HashBucket> iterator()
  {
    return bucketIdToBucket.values().iterator();
  }
}
