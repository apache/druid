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

import org.apache.druid.server.coordinator.duty.BalanceSegments;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;

/**
 * This interface describes the coordinator balancing strategy, which is responsible for making decisions on where
 * to place {@link DataSegment}s on historical servers (described by {@link ServerHolder}). The balancing strategy
 * is used by {@link org.apache.druid.server.coordinator.rules.LoadRule} to assign and drop segments, and by
 * {@link BalanceSegments} to migrate segments between historicals.
 */
public interface BalancerStrategy
{
  /**
   * Find the best server to move a {@link DataSegment} to according the balancing strategy.
   * @param proposalSegment segment to move
   * @param serverHolders servers to consider as move destinations
   * @return The server to move to, or null if no move should be made or no server is suitable
   */
  @Nullable
  ServerHolder findNewSegmentHomeBalancer(DataSegment proposalSegment, List<ServerHolder> serverHolders);

  /**
   * Finds the best servers on which to place a replica of the {@code proposalSegment}
   * according to the balancing strategy.
   *
   * @param proposalSegment segment to replicate
   * @param serverHolders   servers to consider as replica holders
   * @return Iterator over the best servers (in order) on which the replica(s)
   * can be placed.
   */
  Iterator<ServerHolder> findNewSegmentHomeReplicator(
      DataSegment proposalSegment,
      List<ServerHolder> serverHolders
  );

  /**
   * Pick the best segments to move from one of the supplied set of servers according to the balancing strategy.
   *
   * @param serverHolders set of historicals to consider for moving segments
   * @param broadcastDatasources Datasources that contain segments which were loaded via broadcast rules.
   *                             Balancing strategies should avoid rebalancing segments for such datasources, since
   *                             they should be loaded on all servers anyway.
   *                             NOTE: this should really be handled on a per-segment basis, to properly support
   *                                   the interval or period-based broadcast rules. For simplicity of the initial
   *                                   implementation, only forever broadcast rules are supported.
   * @param reservoirSize the reservoir size maintained by the Reservoir Sampling algorithm.
   * @return Iterator for set of {@link BalancerSegmentHolder} containing segment to move and server they currently
   * reside on, or empty if there are no segments to pick from (i. e. all provided serverHolders are empty).
   */
  default Iterator<BalancerSegmentHolder> pickSegmentsToMove(
      List<ServerHolder> serverHolders,
      Set<String> broadcastDatasources,
      int reservoirSize
  )
  {
    return new Iterator<BalancerSegmentHolder>()
    {
      private Iterator<BalancerSegmentHolder> it = sample();
      private Iterator<BalancerSegmentHolder> sample()
      {
        return ReservoirSegmentSampler.getRandomBalancerSegmentHolders(
            serverHolders,
            broadcastDatasources,
            reservoirSize
        ).iterator();
      }

      @Override
      public boolean hasNext()
      {
        if (it.hasNext()) {
          return true;
        }
        it = sample();
        return it.hasNext();
      }

      @Override
      public BalancerSegmentHolder next()
      {
        return it.next();
      }
    };
  }

  /**
   * Pick the best segments to move from one of the supplied set of servers according to the balancing strategy. This
   * is the deprecated way of picking a segment to move. pickSegmentsToMove(List<ServerHoler>, Set<String>, int) uses
   * a more performant bathced sampling method that will become the default picking mode in the future.
   *
   * @param serverHolders set of historicals to consider for moving segments
   * @param broadcastDatasources Datasources that contain segments which were loaded via broadcast rules.
   *                             Balancing strategies should avoid rebalancing segments for such datasources, since
   *                             they should be loaded on all servers anyway.
   *                             NOTE: this should really be handled on a per-segment basis, to properly support
   *                                   the interval or period-based broadcast rules. For simplicity of the initial
   *                                   implementation, only forever broadcast rules are supported.
   * @param percentOfSegmentsToConsider The percentage of the total number of segments that we will consider when
   *                                    choosing which segment to move. {@link CoordinatorDynamicConfig} defines a
   *                                    config percentOfSegmentsToConsiderPerMove that will be used as an argument
   *                                    for implementations of this method.
   * @return Iterator for set of {@link BalancerSegmentHolder} containing segment to move and server they currently
   * reside on, or empty if there are no segments to pick from (i. e. all provided serverHolders are empty).
   */
  @Deprecated
  default Iterator<BalancerSegmentHolder> pickSegmentsToMove(
      List<ServerHolder> serverHolders,
      Set<String> broadcastDatasources,
      double percentOfSegmentsToConsider
  )
  {
    return new Iterator<BalancerSegmentHolder>()
    {
      private BalancerSegmentHolder next = sample();

      private BalancerSegmentHolder sample()
      {
        return ReservoirSegmentSampler.getRandomBalancerSegmentHolder(
            serverHolders,
            broadcastDatasources,
            percentOfSegmentsToConsider
        );
      }

      @Override
      public boolean hasNext()
      {
        return next != null;
      }

      @Override
      public BalancerSegmentHolder next()
      {
        BalancerSegmentHolder ret = next;
        next = sample();
        return ret;
      }
    };
  }

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
   * {@link BalanceSegments} to be included
   * @param tier historical tier being balanced
   * @param stats stats object to add balancing strategy stats to
   * @param serverHolderList servers in tier being balanced
   */
  void emitStats(String tier, CoordinatorStats stats, List<ServerHolder> serverHolderList);
}
