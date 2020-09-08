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
   * A reference to a map of (datasource name) -> {@link PerDatasourceShuffleMetrics}. See {@link #shuffleRequested}
   * and {@link #snapshotAndReset()} for details of the concurrent access pattern.
   */
  private final AtomicReference<ConcurrentHashMap<String, PerDatasourceShuffleMetrics>> datasourceMetrics =
      new AtomicReference<>();

  public ShuffleMetrics()
  {
    datasourceMetrics.set(new ConcurrentHashMap<>());
  }

  /**
   * This method is called whenever a new shuffle is requested. Multiple tasks can request shuffle at the same time,
   * while the monitoring thread takes a snapshot of the metrics. When {@link #snapshotAndReset()} is called
   * before shuffleRequested(), the result map of snapshotAndReset() will not include the update made by
   * shuffleRequested(). When this method is called before snapshotAndReset(), the result map of snapshotAndReset()
   * can either include the update of the last shuffleRequested() or not. If the update is not in the result map,
   * it will be included in the next call to snapshotAndReset().
   */
  public void shuffleRequested(String supervisorTaskId, long fileLength)
  {
    datasourceMetrics
        .get()
        .computeIfAbsent(supervisorTaskId, k -> new PerDatasourceShuffleMetrics()).accumulate(fileLength);
  }

  /**
   * This method is called whenever the monitoring thread takes a snapshot of the current metrics. The map inside
   * AtomicReference will be reset to an empty map after this call. This is to return the snapshot metrics collected
   * during the monitornig period.
   *
   * This method can be called while {@link #shuffleRequested} is called. When snapshotAndReset() is called
   * before shuffleRequested(), the result map of snapshotAndReset() will not include the update made by
   * shuffleRequested(). When shuffleRequested() is called before snapshotAndReset(), the result map of
   * snapshotAndReset() can either include the update of the last shuffleRequested() or not. If the update is not
   * in the result map, it will be included in the next call to snapshotAndReset().
   */
  public Map<String, PerDatasourceShuffleMetrics> snapshotAndReset()
  {
    return Collections.unmodifiableMap(datasourceMetrics.getAndSet(new ConcurrentHashMap<>()));
  }

  /**
   * This method is visible only for testing. Use {@link #snapshotAndReset()} instead to get the current snapshot.
   */
  @VisibleForTesting
  Map<String, PerDatasourceShuffleMetrics> getDatasourceMetrics()
  {
    return datasourceMetrics.get();
  }

  /**
   * This class represents shuffle metrics of one datasource. This class is not thread-safe and should never accessed
   * by multiple threads at the same time.
   */
  public static class PerDatasourceShuffleMetrics
  {
    private long shuffleBytes;
    private int shuffleRequests;

    private void accumulate(long shuffleBytes)
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
