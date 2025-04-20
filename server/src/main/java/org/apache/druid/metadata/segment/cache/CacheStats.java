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

package org.apache.druid.metadata.segment.cache;

/**
 * Summary of current contents of the cache.
 */
public class CacheStats
{
  private final int numUsedSegments;
  private final int numUnusedSegments;
  private final int numIntervals;
  private final int numPendingSegments;

  public CacheStats(int numIntervals, int numUsedSegments, int numUnusedSegments, int numPendingSegments)
  {
    this.numUsedSegments = numUsedSegments;
    this.numUnusedSegments = numUnusedSegments;
    this.numIntervals = numIntervals;
    this.numPendingSegments = numPendingSegments;
  }

  public int getNumUsedSegments()
  {
    return numUsedSegments;
  }

  public int getNumUnusedSegments()
  {
    return numUnusedSegments;
  }

  public int getNumIntervals()
  {
    return numIntervals;
  }

  public int getNumPendingSegments()
  {
    return numPendingSegments;
  }
}
