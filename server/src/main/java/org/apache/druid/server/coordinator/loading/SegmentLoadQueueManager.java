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

package org.apache.druid.server.coordinator.loading;

import com.google.inject.Inject;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;

/**
 * Manager for addition/removal of segments to server load queues and the
 * corresponding success/failure callbacks.
 */
public class SegmentLoadQueueManager
{
  private static final Logger log = new Logger(SegmentLoadQueueManager.class);

  private final LoadQueueTaskMaster taskMaster;
  private final ServerInventoryView serverInventoryView;

  @Inject
  public SegmentLoadQueueManager(
      ServerInventoryView serverInventoryView,
      LoadQueueTaskMaster taskMaster
  )
  {
    this.serverInventoryView = serverInventoryView;
    this.taskMaster = taskMaster;
  }

  /**
   * Queues load of the segment on the given server.
   */
  public boolean loadSegment(DataSegment segment, ServerHolder server, SegmentAction action)
  {
    try {
      if (!server.startOperation(action, segment)) {
        return false;
      }

      server.getPeon().loadSegment(segment, action, null);
      return true;
    }
    catch (Exception e) {
      server.cancelOperation(action, segment);
      final String serverName = server.getServer().getName();
      log.error(e, "Error while loading segment[%s] on server[%s]", segment.getId(), serverName);
      return false;
    }
  }

  public boolean dropSegment(DataSegment segment, ServerHolder server)
  {
    try {
      if (server.startOperation(SegmentAction.DROP, segment)) {
        server.getPeon().dropSegment(segment, null);
        return true;
      } else {
        return false;
      }
    }
    catch (Exception e) {
      server.cancelOperation(SegmentAction.DROP, segment);
      final String serverName = server.getServer().getName();
      log.error(e, "Error while dropping segment[%s] from server[%s]", segment.getId(), serverName);
      return false;
    }
  }

  public boolean moveSegment(
      DataSegment segment,
      ServerHolder serverA,
      ServerHolder serverB
  )
  {
    final LoadQueuePeon peonA = serverA.getPeon();
    final LoadPeonCallback moveFinishCallback = success -> peonA.unmarkSegmentToDrop(segment);

    if (!serverA.startOperation(SegmentAction.MOVE_FROM, segment)) {
      return false;
    }
    if (!serverB.startOperation(SegmentAction.MOVE_TO, segment)) {
      serverA.cancelOperation(SegmentAction.MOVE_FROM, segment);
      return false;
    }

    // mark segment to drop before it is actually loaded on server
    // to be able to account for this information in BalancerStrategy immediately
    peonA.markSegmentToDrop(segment);

    final LoadQueuePeon peonB = serverB.getPeon();
    final String serverNameB = serverB.getServer().getName();
    try {
      peonB.loadSegment(
          segment,
          SegmentAction.MOVE_TO,
          success -> {
            // Drop segment only if:
            // (1) segment load was successful on serverB
            // AND (2) segment is not already queued for drop on serverA
            // AND (3a) loading is http-based
            //     OR (3b) inventory shows segment loaded on serverB

            // Do not check the inventory with http loading as the HTTP
            // response is enough to determine load success or failure
            if (success
                && !peonA.getSegmentsToDrop().contains(segment)
                && (taskMaster.isHttpLoading()
                    || serverInventoryView.isSegmentLoadedByServer(serverNameB, segment))) {
              peonA.unmarkSegmentToDrop(segment);
              peonA.dropSegment(segment, moveFinishCallback);
            } else {
              moveFinishCallback.execute(success);
            }
          }
      );
    }
    catch (Exception e) {
      serverA.cancelOperation(SegmentAction.MOVE_FROM, segment);
      serverB.cancelOperation(SegmentAction.MOVE_TO, segment);
      moveFinishCallback.execute(false);
      log.error(e, "Error while moving segment[%s] to server[%s]", segment.getId(), serverNameB);
      return false;
    }

    return true;
  }

}
