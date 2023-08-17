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

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;

/**
 * Segment balancing strategy, used in every coordinator run by
 * {@link org.apache.druid.server.coordinator.loading.StrategicSegmentAssigner}
 * to choose optimal servers to load, move or drop a segment.
 */
public interface BalancerStrategy
{

  /**
   * Finds the best server from the list of {@code destinationServers} to load
   * the {@code segmentToMove}, if it is moved from the {@code sourceServer}.
   * <p>
   * In order to avoid unnecessary moves when the segment is already optimally placed,
   * include the {@code sourceServer} in the list of {@code destinationServers}.
   *
   * @return The server to move to, or null if the segment is already optimally placed.
   */
  @Nullable
  ServerHolder findDestinationServerToMoveSegment(
      DataSegment segmentToMove,
      ServerHolder sourceServer,
      List<ServerHolder> destinationServers
  );

  /**
   * Finds the best servers to load the given segment. This method can be used
   * both for placing the first copy of a segment in a tier or a replica of an
   * already available segment.
   *
   * @return Iterator over the best servers (in order of preference) to load
   * the segment.
   */
  Iterator<ServerHolder> findServersToLoadSegment(
      DataSegment segmentToLoad,
      List<ServerHolder> serverHolders
  );

  /**
   * Finds the best servers to drop the given segment.
   *
   * @return Iterator over the servers (in order of preference) to drop the segment
   */
  Iterator<ServerHolder> findServersToDropSegment(DataSegment segmentToDrop, List<ServerHolder> serverHolders);

  /**
   * Returns the stats collected by the strategy.
   */
  CoordinatorRunStats getStats();
}
