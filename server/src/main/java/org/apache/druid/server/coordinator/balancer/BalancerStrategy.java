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

import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.duty.BalanceSegments;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
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
  Iterator<ServerHolder> findServerToLoadSegment(
      DataSegment proposalSegment,
      List<ServerHolder> serverHolders
  );

  /**
   * Picks segments from the given set of servers based on the balancing strategy.
   * Default behaviour is to pick segments using reservoir sampling.
   *
   * @param serverHolders        Set of historicals to consider for picking segments
   * @param broadcastDatasources Segments belonging to these datasources will not
   *                             be picked for balancing, since they should be
   *                             loaded on all servers anyway.
   * @param maxSegmentsToPick    Maximum number of segments to pick
   * @param pickLoadingSegments  If true, picks only segments currently being
   *                             loaded on a server. If false, picks segments
   *                             already loaded on a server.
   * @return Iterator over {@link BalancerSegmentHolder}s, each of which contains
   * a segment picked for moving and the server currently serving/loading it.
   */
  default Iterator<BalancerSegmentHolder> pickSegmentsToMove(
      List<ServerHolder> serverHolders,
      Set<String> broadcastDatasources,
      int maxSegmentsToPick,
      boolean pickLoadingSegments
  )
  {
    return ReservoirSegmentSampler.getRandomBalancerSegmentHolders(
        serverHolders,
        broadcastDatasources,
        maxSegmentsToPick,
        pickLoadingSegments
    ).iterator();
  }

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
   * @param percentOfSegmentsToConsider The percentage of the total number of segments that we will consider when
   *                                    choosing which segment to move. {@link CoordinatorDynamicConfig} defines a
   *                                    config percentOfSegmentsToConsiderPerMove that will be used as an argument
   *                                    for implementations of this method.
   * @return Iterator for set of {@link BalancerSegmentHolder} containing segment to move and server they currently
   * reside on, or empty if there are no segments to pick from (i. e. all provided serverHolders are empty).
   *
   * @deprecated Use {@link #pickSegmentsToMove(List, Set, int, boolean)} instead as it is
   * a much more performant sampling method which does not allow duplicates. This
   * method will be removed in future releases.
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
    // By default, return holders with least available size first.
    return serverHolders.iterator();
  }

  /**
   * Add balancing strategy stats during the 'balanceTier' operation of
   * {@link BalanceSegments} to be included
   * @param tier historical tier being balanced
   * @param stats stats object to add balancing strategy stats to
   * @param serverHolderList servers in tier being balanced
   */
  void emitStats(String tier, CoordinatorRunStats stats, List<ServerHolder> serverHolderList);
}
