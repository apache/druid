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

package org.apache.druid.query.groupby;

import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.Pair;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects stats for group by queries like used merged buffer count, spilled bytes and group by resource acquisition time.
 */
@LazySingleton
public class GroupByStatsProvider
{
  private long mergeBufferAcquisitionTimeNs = 0;
  private long mergeBufferAcquisitionCount = 0;

  private long spilledBytesSince = 0;
  private long spilledQueries = 0;

  private final Map<String, AtomicLong> spilledBytesPerQuery;

  public GroupByStatsProvider()
  {
    this.spilledBytesPerQuery = new ConcurrentHashMap<>();
  }

  public synchronized void mergeBufferAcquisitionTimeNs(long delayNs)
  {
    mergeBufferAcquisitionTimeNs += delayNs;
    mergeBufferAcquisitionCount++;
  }

  public synchronized Pair<Long, Long> getAndResetMergeBufferAcquisitionStats()
  {
    Pair<Long, Long> pair = Pair.of(mergeBufferAcquisitionCount, mergeBufferAcquisitionTimeNs);

    // reset
    mergeBufferAcquisitionTimeNs = 0;
    mergeBufferAcquisitionCount = 0;

    return pair;
  }

  public void reportSpilledBytes(String queryId, long bytes)
  {
    if (queryId == null) {
      return;
    }
    if (bytes > 0) {
      spilledBytesPerQuery.computeIfAbsent(queryId, value -> new AtomicLong(0)).addAndGet(bytes);
    }
  }

  public synchronized void closeQuery(String queryId)
  {
    if (queryId == null) {
      return;
    }
    AtomicLong spilledBytes = spilledBytesPerQuery.remove(queryId);
    if (spilledBytes != null) {
      spilledQueries++;
      spilledBytesSince += spilledBytes.get();
    }
  }

  public synchronized Pair<Long, Long> getAndResetSpilledBytes()
  {
    Pair<Long, Long> pair = Pair.of(spilledQueries, spilledBytesSince);

    // reset
    spilledQueries = 0;
    spilledBytesSince = 0;

    return pair;
  }
}
