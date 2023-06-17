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
import org.apache.druid.server.coordinator.duty.BalanceSegments;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;

/**
 * This interface describes the coordinator balancing strategy, which is responsible for making decisions on where
 * to place {@link DataSegment}s on historical servers (described by {@link ServerHolder}). The balancing strategy
 * is used by {@link org.apache.druid.server.coordinator.rules.LoadRule} to assign and drop segments, and by
 * {@link BalanceSegments} to migrate segments between historicals.
 */
public interface BalancerStrategy
{

  /**
   * Finds the best server to move a segment to according to the balancing strategy.
   *
   * @param proposalSegment    segment to move
   * @param sourceServer       Server the segment is currently placed on.
   * @param destinationServers servers to consider as move destinations
   * @return The server to move to, or null if no move should be made or no server is suitable
   */
  @Nullable
  ServerHolder findDestinationServerToMoveSegment(
      DataSegment proposalSegment,
      ServerHolder sourceServer,
      List<ServerHolder> destinationServers
  );

  /**
   * Finds the best servers on which to place the {@code proposalSegment}.
   * This method can be used both for placing the first copy of a segment
   * in the tier or a replica of the segment.
   *
   * @param proposalSegment segment to place on servers
   * @param serverHolders   servers to consider as segment homes
   * @return Iterator over the best servers (in order) on which the segment
   * can be placed.
   */
  Iterator<ServerHolder> findServersToLoadSegment(
      DataSegment proposalSegment,
      List<ServerHolder> serverHolders
  );

  /**
   * Returns an iterator for a set of servers to drop from, ordered by preference of which server to drop from first
   * for a given drop strategy. One or more segments may be dropped, depending on how much the segment is
   * over-replicated.
   * @param toDropSegment segment to drop from one or more servers
   * @param serverHolders set of historicals to consider dropping from
   * @return Iterator for set of historicals, ordered by drop preference
   */
  Iterator<ServerHolder> pickServersToDropSegment(DataSegment toDropSegment, NavigableSet<ServerHolder> serverHolders);

  /**
   * Add balancing strategy stats during the 'balanceTier' operation of
   * {@link BalanceSegments} to be included
   * @param tier historical tier being balanced
   * @param stats stats object to add balancing strategy stats to
   * @param serverHolderList servers in tier being balanced
   */
  void emitStats(String tier, CoordinatorRunStats stats, List<ServerHolder> serverHolderList);
}
