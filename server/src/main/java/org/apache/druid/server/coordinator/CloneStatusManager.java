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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.loading.SegmentAction;
import org.apache.druid.timeline.DataSegment;

import java.util.HashMap;
import java.util.Map;

public class CloneStatusManager
{
  private static final Logger log = new Logger(CloneStatusManager.class);
  @GuardedBy("this")
  private final Map<String, CloneDetails> cloneStatusMap;

  public CloneStatusManager()
  {
    cloneStatusMap = new HashMap<>();
  }

  public Map<String, CloneDetails> getCloneStatusMap()
  {
    synchronized (this) {
      return cloneStatusMap;
    }
  }

  public void updateStats(Map<String, ServerHolder> historicalMap, Map<String, String> cloneServers)
  {
    synchronized (this) {
      cloneStatusMap.clear();

      for (Map.Entry<String, String> entry : cloneServers.entrySet()) {
        String targetServerName = entry.getKey();
        ServerHolder targetServer = historicalMap.get(entry.getKey());
        String sourceServerName = entry.getValue();

        int segmentsLeft = 0;
        long bytesLeft = 0;

        if (targetServer == null) {
          cloneStatusMap.put(targetServerName, new CloneDetails(sourceServerName, 0, 0));
          continue;
        }

        for (Map.Entry<DataSegment, SegmentAction> queuedSegment : targetServer.getQueuedSegments().entrySet()) {
          if (queuedSegment.getValue().isLoad()) {
            segmentsLeft++;
            bytesLeft += queuedSegment.getKey().getSize();
          }
        }

        cloneStatusMap.put(targetServerName, new CloneDetails(sourceServerName, segmentsLeft, bytesLeft));
      }
    }
  }
}

