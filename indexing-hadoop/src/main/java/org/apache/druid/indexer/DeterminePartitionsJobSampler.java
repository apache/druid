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

package org.apache.druid.indexer;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.concurrent.ThreadLocalRandom;

public class DeterminePartitionsJobSampler
{
  private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32();

  private final int samplingFactor;

  private final int sampledTargetPartitionSize;

  private final int sampledMaxRowsPerSegment;

  public DeterminePartitionsJobSampler(int samplingFactor, int targetPartitionSize, int maxRowsPerSegment)
  {
    this.samplingFactor = Math.max(samplingFactor, 1);
    this.sampledTargetPartitionSize = targetPartitionSize / this.samplingFactor;
    this.sampledMaxRowsPerSegment = maxRowsPerSegment / this.samplingFactor;
  }

  /**
   * If input rows is duplicate, we can use hash and mod to do sample. As we hash on whole group key,
   * there will not likely data skew if the hash function is balanced enough.
   */
  boolean shouldEmitRow(byte[] groupKeyBytes)
  {
    return samplingFactor == 1 || HASH_FUNCTION.hashBytes(groupKeyBytes).asInt() % samplingFactor == 0;
  }

  /**
   * If input rows is not duplicate, we can sample at random.
   */
  boolean shouldEmitRow()
  {
    return samplingFactor == 1 || ThreadLocalRandom.current().nextInt(samplingFactor) == 0;
  }

  public int getSampledTargetPartitionSize()
  {
    return sampledTargetPartitionSize;
  }

  public int getSampledMaxRowsPerSegment()
  {
    return sampledMaxRowsPerSegment;
  }
}
