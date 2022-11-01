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

import org.apache.druid.timeline.DataSegment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

/**
 * Maintains a map containing the state of a segment on all servers of a tier.
 */
public class SegmentTierStatus
{
  private final DataSegment segment;

  private final List<ServerHolder> eligibleLoadServers = new ArrayList<>();
  private final List<ServerHolder> eligibleDropServers = new ArrayList<>();

  private final Map<SegmentAction, List<ServerHolder>> serversWithQueuedActions = new HashMap<>();

  public SegmentTierStatus(DataSegment segment, NavigableSet<ServerHolder> historicals)
  {
    this.segment = segment;
    historicals.forEach(this::handleServer);
  }

  public List<ServerHolder> getServersEligibleToLoad()
  {
    return eligibleLoadServers;
  }

  public List<ServerHolder> getServersEligibleToDrop()
  {
    return eligibleDropServers;
  }

  public List<ServerHolder> getServersPerforming(SegmentAction action)
  {
    return serversWithQueuedActions.getOrDefault(action, Collections.emptyList());
  }

  private void handleServer(ServerHolder server)
  {
    final SegmentAction action = server.getActionOnSegment(segment);
    if (server.isServingSegment(segment)) {
      eligibleDropServers.add(server);
    } else if (server.canLoadSegment(segment)) {
      eligibleLoadServers.add(server);
    } else if (action != null) {
      serversWithQueuedActions.computeIfAbsent(action, a -> new ArrayList<>()).add(server);
    }
  }

}
