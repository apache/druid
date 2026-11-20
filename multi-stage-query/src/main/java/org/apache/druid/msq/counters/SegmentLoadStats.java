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

package org.apache.druid.msq.counters;

import org.apache.druid.query.QueryMetrics;

/**
 * Totals and maximums for segments loaded on demand, as recorded by {@link ChannelCounters#addLoad}. Reported as the
 * {@code query/load} metrics by {@link #report}.
 */
public class SegmentLoadStats
{
  public static final SegmentLoadStats EMPTY = new SegmentLoadStats(0, 0, 0, 0, 0, 0);

  private final long count;
  private final long bytes;
  private final long totalLoadTimeNs;
  private final long maxLoadTimeNs;
  private final long totalWaitTimeNs;
  private final long maxWaitTimeNs;

  public SegmentLoadStats(
      final long count,
      final long bytes,
      final long totalLoadTimeNs,
      final long maxLoadTimeNs,
      final long totalWaitTimeNs,
      final long maxWaitTimeNs
  )
  {
    this.count = count;
    this.bytes = bytes;
    this.totalLoadTimeNs = totalLoadTimeNs;
    this.maxLoadTimeNs = maxLoadTimeNs;
    this.totalWaitTimeNs = totalWaitTimeNs;
    this.maxWaitTimeNs = maxWaitTimeNs;
  }

  /**
   * Returns stats combining these and the provided stats.
   */
  public SegmentLoadStats plus(final SegmentLoadStats other)
  {
    return new SegmentLoadStats(
        count + other.count,
        bytes + other.bytes,
        totalLoadTimeNs + other.totalLoadTimeNs,
        Math.max(maxLoadTimeNs, other.maxLoadTimeNs),
        totalWaitTimeNs + other.totalWaitTimeNs,
        Math.max(maxWaitTimeNs, other.maxWaitTimeNs)
    );
  }

  /**
   * Reports these stats to the provided query metrics. Does nothing if no segments were loaded. Averages are per loaded
   * segment.
   */
  public void report(final QueryMetrics<?> queryMetrics)
  {
    if (count > 0) {
      queryMetrics.reportSegmentOnDemandLoadCount(count);
      queryMetrics.reportSegmentOnDemandLoadBytes(bytes);
      queryMetrics.reportSegmentOnDemandLoadTimeAvg(totalLoadTimeNs / count);
      queryMetrics.reportSegmentOnDemandLoadTimeMax(maxLoadTimeNs);
      queryMetrics.reportSegmentOnDemandLoadWaitTimeAvg(totalWaitTimeNs / count);
      queryMetrics.reportSegmentOnDemandLoadWaitTimeMax(maxWaitTimeNs);
    }
  }
}
