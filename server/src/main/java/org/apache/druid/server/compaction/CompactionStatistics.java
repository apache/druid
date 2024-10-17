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

package org.apache.druid.server.compaction;

/**
 * Used to track statistics for segments in different states of compaction.
 */
public class CompactionStatistics
{
  private long totalBytes;
  private long numSegments;
  private long numIntervals;

  public static CompactionStatistics create(long bytes, long numSegments, long numIntervals)
  {
    final CompactionStatistics stats = new CompactionStatistics();
    stats.totalBytes = bytes;
    stats.numIntervals = numIntervals;
    stats.numSegments = numSegments;
    return stats;
  }

  public long getTotalBytes()
  {
    return totalBytes;
  }

  public long getNumSegments()
  {
    return numSegments;
  }

  public long getNumIntervals()
  {
    return numIntervals;
  }

  public void increment(CompactionStatistics other)
  {
    totalBytes += other.getTotalBytes();
    numIntervals += other.getNumIntervals();
    numSegments += other.getNumSegments();
  }
}
