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

package org.apache.druid.server.coordinator.balancer;

import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.timeline.DataSegment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A simple {@link BalancerStrategy} that
 * <ul>
 *   <li>assigns segments randomly amongst eligible servers</li>
 *   <li>performs no balancing</li>
 * </ul>
 */
public class RandomBalancerStrategy implements BalancerStrategy
{
  @Override
  public Iterator<ServerHolder> findServersToLoadSegment(
      DataSegment segmentToLoad,
      List<ServerHolder> serverHolders
  )
  {
    // Filter out servers which cannot load this segment
    final List<ServerHolder> usableServerHolders =
        serverHolders.stream()
                     .filter(server -> server.canLoadSegment(segmentToLoad))
                     .collect(Collectors.toList());
    Collections.shuffle(usableServerHolders);
    return usableServerHolders.iterator();
  }

  @Override
  public ServerHolder findDestinationServerToMoveSegment(
      DataSegment segmentToMove,
      ServerHolder sourceServer,
      List<ServerHolder> serverHolders
  )
  {
    // This strategy does not do any balancing
    return null;
  }

  @Override
  public Iterator<ServerHolder> findServersToDropSegment(DataSegment segmentToDrop, List<ServerHolder> serverHolders)
  {
    List<ServerHolder> serverList = new ArrayList<>(serverHolders);
    Collections.shuffle(serverList);
    return serverList.iterator();
  }

  @Override
  public CoordinatorRunStats getStats()
  {
    return CoordinatorRunStats.empty();
  }
}
