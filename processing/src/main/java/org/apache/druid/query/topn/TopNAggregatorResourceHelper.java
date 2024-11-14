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

package org.apache.druid.query.topn;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.ResourceLimitExceededException;

import java.util.concurrent.atomic.AtomicLong;

public class TopNAggregatorResourceHelper
{
  private final long maxAggregatorHeapSizeBytes;
  private final long newAggregatorEstimatedMemorySize;
  private final AtomicLong used;

  TopNAggregatorResourceHelper(final long newAggregatorEstimatedMemorySize, final long maxAggregatorHeapSizeBytes)
  {
    this.newAggregatorEstimatedMemorySize = newAggregatorEstimatedMemorySize;
    this.maxAggregatorHeapSizeBytes = maxAggregatorHeapSizeBytes;
    this.used = new AtomicLong(0);
  }

  // A ctor for updating estimated memory size + max memory size while preserving count
  TopNAggregatorResourceHelper(
      final long newAggregatorEstimatedMemorySize,
      final long maxAggregatorHeapSizeBytes,
      final TopNAggregatorResourceHelper other
  )
  {
    this.newAggregatorEstimatedMemorySize = newAggregatorEstimatedMemorySize;
    this.maxAggregatorHeapSizeBytes = maxAggregatorHeapSizeBytes;
    this.used = other.used;
  }

  public void addAggregatorMemory()
  {
    final long newTotal = used.addAndGet(newAggregatorEstimatedMemorySize);
    if (newTotal > maxAggregatorHeapSizeBytes) {
      throw new ResourceLimitExceededException(StringUtils.format(
          "Query ran out of memory. Maximum allowed bytes=[%d], Hit bytes=[%d]",
          maxAggregatorHeapSizeBytes,
          newTotal
      ));
    }
  }
}
