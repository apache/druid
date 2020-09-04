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

package org.apache.druid.indexing.worker.shuffle;

import com.google.common.annotations.VisibleForTesting;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class ShuffleMetrics
{
  private final AtomicReference<ConcurrentHashMap<String, PerDatasourceShuffleMetrics>> datasourceMetrics =
      new AtomicReference<>();

  public ShuffleMetrics()
  {
    datasourceMetrics.set(new ConcurrentHashMap<>());
  }

  public void shuffleRequested(String supervisorTaskId, long fileLength)
  {
    datasourceMetrics
        .get()
        .computeIfAbsent(supervisorTaskId, k -> new PerDatasourceShuffleMetrics()).accumulate(fileLength);
  }

  public Map<String, PerDatasourceShuffleMetrics> snapshot()
  {
    return Collections.unmodifiableMap(datasourceMetrics.getAndSet(new ConcurrentHashMap<>()));
  }

  /**
   * This method is visible only for testing. Use {@link #snapshot()} instead to get the current snapshot.
   */
  @VisibleForTesting
  Map<String, PerDatasourceShuffleMetrics> getDatasourceMetrics()
  {
    return datasourceMetrics.get();
  }

  public static class PerDatasourceShuffleMetrics
  {
    private long shuffleBytes;
    private int shuffleRequests;

    public void accumulate(long shuffleBytes)
    {
      this.shuffleBytes += shuffleBytes;
      this.shuffleRequests++;
    }

    public long getShuffleBytes()
    {
      return shuffleBytes;
    }

    public int getShuffleRequests()
    {
      return shuffleRequests;
    }
  }
}
