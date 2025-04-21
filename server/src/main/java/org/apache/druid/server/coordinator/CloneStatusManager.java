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

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class CloneStatusManager
{
  private final AtomicReference<Map<String, ServerCloneStatus>> cloneStatusSnapshot;

  public CloneStatusManager()
  {
    this.cloneStatusSnapshot = new AtomicReference<>(Map.of());
  }

  public Map<String, ServerCloneStatus> getStatusForAllServers()
  {
    return cloneStatusSnapshot.get();
  }

  @Nullable
  public ServerCloneStatus getStatusForServer(String targetServer)
  {
    return cloneStatusSnapshot.get().get(targetServer);
  }

  public void updateStatus(Map<String, ServerHolder> historicalMap, Map<String, String> cloneServers)
  {
    final Map<String, ServerCloneStatus> newStatusMap = new HashMap<>();

    for (Map.Entry<String, String> entry : cloneServers.entrySet()) {
      final String targetServerName = entry.getKey();
      final ServerHolder targetServer = historicalMap.get(entry.getKey());
      final String sourceServerName = entry.getValue();

      long segmentLoad = 0L;
      long bytesLeft = 0L;
      long segmentDrop = 0L;

      ServerCloneStatus newStatus;
      if (targetServer == null) {
        newStatus = ServerCloneStatus.unknown(sourceServerName);
      } else {

        ServerCloneStatus.State state;
        if (!historicalMap.containsKey(sourceServerName)) {
          state = ServerCloneStatus.State.SOURCE_SERVER_MISSING;
        } else {
          state = ServerCloneStatus.State.LOADING;
        }

        for (Map.Entry<DataSegment, SegmentAction> queuedSegment : targetServer.getQueuedSegments().entrySet()) {
          if (queuedSegment.getValue().isLoad()) {
            segmentLoad += 1;
            bytesLeft += queuedSegment.getKey().getSize();
          } else {
            segmentDrop += 1;
          }
        }
        newStatus = new ServerCloneStatus(sourceServerName, state, segmentLoad, segmentDrop, bytesLeft);
      }
      newStatusMap.put(targetServerName, newStatus);
    }

    cloneStatusSnapshot.set(ImmutableMap.copyOf(newStatusMap));
  }
}

