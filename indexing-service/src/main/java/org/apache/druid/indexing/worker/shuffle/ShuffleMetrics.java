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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.com.google.common.annotations.VisibleForTesting;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Shuffle metrcis for middleManagers and indexers. This class is thread-safe because shuffle can be performed by
 * multiple HTTP threads while a monitoring thread periodically emits the snapshot of metrics.
 *
 * @see ShuffleResource
 * @see org.apache.druid.java.util.metrics.MonitorScheduler
 */
public class ShuffleMetrics
{
  /**
   * This lock is used to synchronize accesses to the reference to {@link #datasourceMetrics} and the
   * {@link PerDatasourceShuffleMetrics} values of the map. This means,
   *
   * - Any updates on PerDatasourceShuffleMetrics in the map (and thus its key as well) should be synchronized
   * under this lock.
   * - Any updates on the reference to datasourceMetrics should be synchronized under this lock.
   */
  private final Object lock = new Object();

  /**
   * A map of (datasource name) -> {@link PerDatasourceShuffleMetrics}. This map is replaced with an empty map
   * whenever a snapshot is taken since the map can keep growing over time otherwise. For concurrent access pattern,
   * see {@link #shuffleRequested} and {@link #snapshotAndReset()}.
   */
  @GuardedBy("lock")
  private Map<String, PerDatasourceShuffleMetrics> datasourceMetrics = new HashMap<>();

  /**
   * This method is called whenever a new shuffle is requested. Multiple tasks can request shuffle at the same time,
   * while the monitoring thread takes a snapshot of the metrics. There is a happens-before relationship between
   * shuffleRequested and {@link #snapshotAndReset()}.
   */
  public void shuffleRequested(String supervisorTaskId, long fileLength)
  {
    synchronized (lock) {
      datasourceMetrics.computeIfAbsent(supervisorTaskId, k -> new PerDatasourceShuffleMetrics())
                       .accumulate(fileLength);
    }
  }

  /**
   * This method is called whenever the monitoring thread takes a snapshot of the current metrics.
   * {@link #datasourceMetrics} will be reset to an empty map after this call. This is to return the snapshot
   * metrics collected during the monitornig period. There is a happens-before relationship between snapshotAndReset()
   * and {@link #shuffleRequested}.
   */
  public Map<String, PerDatasourceShuffleMetrics> snapshotAndReset()
  {
    synchronized (lock) {
      final Map<String, PerDatasourceShuffleMetrics> snapshot = Collections.unmodifiableMap(datasourceMetrics);
      datasourceMetrics = new HashMap<>();
      return snapshot;
    }
  }

  /**
   * This method is visible only for testing. Use {@link #snapshotAndReset()} instead to get the current snapshot.
   */
  @VisibleForTesting
  Map<String, PerDatasourceShuffleMetrics> getDatasourceMetrics()
  {
    synchronized (lock) {
      return datasourceMetrics;
    }
  }

  /**
   * This class represents shuffle metrics of one datasource. This class is not thread-safe and should never be accessed
   * by multiple threads at the same time.
   */
  public static class PerDatasourceShuffleMetrics
  {
    private long shuffleBytes;
    private int shuffleRequests;

    @VisibleForTesting
    void accumulate(long shuffleBytes)
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
