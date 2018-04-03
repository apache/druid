/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator;

import io.druid.timeline.DataSegment;

import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;

/**
 * This interface describes the coordinator balancing strategy, which is responsible for making decisions on where
 * to place {@link DataSegment}s on historical servers (described by {@link ServerHolder}). The balancing strategy
 * is used by {@link io.druid.server.coordinator.rules.LoadRule} to assign and drop segments, and by
 * {@link io.druid.server.coordinator.helper.DruidCoordinatorBalancer} to migrate segments between historicals.
 */
public interface BalancerStrategy
{
  /**
   * Find the best server to move a {@link DataSegment} to according the the balancing strategy.
   * @param proposalSegment segment to move
   * @param serverHolders servers to consider as move destinations
   * @return The server to move to, or null if no move should be made or no server is suitable
   */
  ServerHolder findNewSegmentHomeBalancer(DataSegment proposalSegment, List<ServerHolder> serverHolders);

  /**
   * Find the best server on which to place a {@link DataSegment} replica according to the balancing strategy
   * @param proposalSegment segment to replicate
   * @param serverHolders servers to consider as replica holders
   * @return The server to replicate to, or null if no suitable server is found
   */
  ServerHolder findNewSegmentHomeReplicator(DataSegment proposalSegment, List<ServerHolder> serverHolders);

  /**
   * Pick the best segment to move from one of the supplied set of servers according to the balancing strategy.
   * @param serverHolders set of historicals to consider for moving segments
   * @return {@link BalancerSegmentHolder} containing segment to move and server it current resides on
   */
  BalancerSegmentHolder pickSegmentToMove(List<ServerHolder> serverHolders);

  /**
   * Returns an iterator for a set of servers to drop from, ordered by preference of which server to drop from first
   * for a given drop strategy. One or more segments may be dropped, depending on how much the segment is
   * over-replicated.
   * @param toDropSegment segment to drop from one or more servers
   * @param serverHolders set of historicals to consider dropping from
   * @return Iterator for set of historicals, ordered by drop preference
   */
  default Iterator<ServerHolder> pickServersToDrop(DataSegment toDropSegment, NavigableSet<ServerHolder> serverHolders)
  {
    // By default, use the reverse order to get the holders with least available size first.
    return serverHolders.descendingIterator();
  }

  /**
   * Add balancing strategy stats during the 'balanceTier' operation of
   * {@link io.druid.server.coordinator.helper.DruidCoordinatorBalancer} to be included
   * @param tier historical tier being balanced
   * @param stats stats object to add balancing strategy stats to
   * @param serverHolderList servers in tier being balanced
   */
  void emitStats(String tier, CoordinatorStats stats, List<ServerHolder> serverHolderList);
}
