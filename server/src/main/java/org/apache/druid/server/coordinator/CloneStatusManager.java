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

package org.apache.druid.server.coordinator;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.server.coordinator.loading.SegmentAction;
import org.apache.druid.timeline.DataSegment;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class CloneStatusManager
{
  private final AtomicReference<Map<String, CloneStatusMetrics>> cloneStatusSnapshot;

  public CloneStatusManager()
  {
    this.cloneStatusSnapshot = new AtomicReference<>(ImmutableMap.of());
  }

  public Map<String, CloneStatusMetrics> getStatusForAllServers()
  {
    return cloneStatusSnapshot.get();
  }

  public CloneStatusMetrics getStatusForServer(String targetServer)
  {
    return cloneStatusSnapshot.get().get(targetServer);
  }

  public void updateStats(Map<String, ServerHolder> historicalMap, Map<String, String> cloneServers)
  {
    final Map<String, CloneStatusMetrics> newStatusMap = new HashMap<>();

    for (Map.Entry<String, String> entry : cloneServers.entrySet()) {
      final String targetServerName = entry.getKey();
      final ServerHolder targetServer = historicalMap.get(entry.getKey());
      final String sourceServerName = entry.getValue();

      long segmentLoad = 0L;
      long bytesLeft = 0L;
      long segmentDrop = 0L;

      CloneStatusMetrics newStatus;
      if (targetServer == null) {
        newStatus = CloneStatusMetrics.unknown(sourceServerName);
      } else {
        CloneStatusMetrics.Status status;

        if (!historicalMap.containsKey(sourceServerName)) {
          status = CloneStatusMetrics.Status.SOURCE_SERVER_MISSING;
        } else {
          status = CloneStatusMetrics.Status.LOADING;
        }

        for (Map.Entry<DataSegment, SegmentAction> queuedSegment : targetServer.getQueuedSegments().entrySet()) {
          if (queuedSegment.getValue().isLoad()) {
            segmentLoad += 1;
            bytesLeft += queuedSegment.getKey().getSize();
          } else {
            segmentDrop += 1;
          }
        }
        newStatus = new CloneStatusMetrics(sourceServerName, status, segmentLoad, segmentDrop, bytesLeft);
      }
      newStatusMap.put(targetServerName, newStatus);
    }

    cloneStatusSnapshot.set(newStatusMap);
  }
}

