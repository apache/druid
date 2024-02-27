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

import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.SegmentAction;
import org.apache.druid.timeline.DataSegment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

public final class ReservoirSegmentSampler
{

  private static final EmittingLogger log = new EmittingLogger(ReservoirSegmentSampler.class);

  /**
   * Picks segments from the given set of servers that can be moved to other
   * servers for balancing.
   *
   * @param serverHolders        Set of historicals to consider for picking segments
   * @param maxSegmentsToPick    Maximum number of segments to pick
   * @param segmentProvider      Function to extract all movable segments from a
   *                             {@link ServerHolder}.
   * @param broadcastDatasources Segments belonging to these datasources will not
   *                             be picked for balancing, since they should be
   *                             loaded on all servers anyway.
   * @return Iterator over {@link BalancerSegmentHolder}s, each of which contains
   * a segment picked for moving and the server from which it was picked.
   */
  public static List<BalancerSegmentHolder> pickMovableSegmentsFrom(
      List<ServerHolder> serverHolders,
      int maxSegmentsToPick,
      Function<ServerHolder, Collection<DataSegment>> segmentProvider,
      Set<String> broadcastDatasources
  )
  {
    if (maxSegmentsToPick == 0 || serverHolders.isEmpty()) {
      return Collections.emptyList();
    }

    final List<BalancerSegmentHolder> pickedSegments = new ArrayList<>(maxSegmentsToPick);
    int numSoFar = 0;

    for (ServerHolder server : serverHolders) {
      if (!server.getServer().getType().isSegmentReplicationTarget()) {
        // if the server only handles broadcast segments (which don't need to be rebalanced), we have nothing to do
        continue;
      }

      final Collection<DataSegment> movableSegments = segmentProvider.apply(server);
      for (DataSegment segment : movableSegments) {
        if (broadcastDatasources.contains(segment.getDataSource())) {
          // we don't need to rebalance segments that were assigned via broadcast rules
          continue;
        } else if (server.getActionOnSegment(segment) == SegmentAction.MOVE_FROM) {
          // Do not pick a segment which is already being moved
          continue;
        }

        if (numSoFar < maxSegmentsToPick) {
          pickedSegments.add(new BalancerSegmentHolder(server, segment));
          numSoFar++;
          continue;
        }
        int randNum = ThreadLocalRandom.current().nextInt(numSoFar + 1);
        if (randNum < maxSegmentsToPick) {
          pickedSegments.set(randNum, new BalancerSegmentHolder(server, segment));
        }
        numSoFar++;
      }
    }
    return pickedSegments;
  }

  /**
   * Iterates over segments that live on the candidate servers passed in {@link ServerHolder} and (possibly) picks a
   * segment to return to caller in a {@link BalancerSegmentHolder} object.
   *
   * @param serverHolders List of {@link ServerHolder} objects containing segments who are candidates to be chosen.
   * @param broadcastDatasources Set of DataSource names that identify broadcast datasources. We don't want to consider
   *                             segments from these datasources.
   * @param percentOfSegmentsToConsider The % of total cluster segments to consider before short-circuiting and
   *                                   returning immediately.
   * @return
   */
  @Deprecated
  static BalancerSegmentHolder getRandomBalancerSegmentHolder(
      final List<ServerHolder> serverHolders,
      Set<String> broadcastDatasources,
      double percentOfSegmentsToConsider
  )
  {
    ServerHolder fromServerHolder = null;
    DataSegment proposalSegment = null;
    int calculatedSegmentLimit = Integer.MAX_VALUE;
    int numSoFar = 0;

    // Reset a bad value of percentOfSegmentsToConsider to 100. We don't allow consideration less than or equal to
    // 0% of segments or greater than 100% of segments.
    if (percentOfSegmentsToConsider <= 0 || percentOfSegmentsToConsider > 100) {
      log.warn("Resetting percentOfSegmentsToConsider to 100 because only values from 1 to 100 are allowed."
                + " You Provided [%f]", percentOfSegmentsToConsider);
      percentOfSegmentsToConsider = 100;
    }

    // Calculate the integer limit for the number of segments to be considered for moving if % is less than 100
    if (percentOfSegmentsToConsider < 100) {
      int totalSegments = 0;
      for (ServerHolder server : serverHolders) {
        totalSegments += server.getServer().getNumSegments();
      }
      // If totalSegments are zero, we will assume it is a mistake and move on to iteration without updating
      // calculatedSegmentLimit
      if (totalSegments != 0) {
        calculatedSegmentLimit = (int) Math.ceil((double) totalSegments * (percentOfSegmentsToConsider / 100.0));
      } else {
        log.warn("Unable to calculate limit on segments to consider because ServerHolder collection indicates"
                 + " zero segments existing in the cluster.");
      }
    }

    for (ServerHolder server : serverHolders) {
      if (!server.getServer().getType().isSegmentReplicationTarget()) {
        // if the server only handles broadcast segments (which don't need to be rebalanced), we have nothing to do
        continue;
      }

      for (DataSegment segment : server.getServer().iterateAllSegments()) {
        if (broadcastDatasources.contains(segment.getDataSource())) {
          // we don't need to rebalance segments that were assigned via broadcast rules
          continue;
        }

        int randNum = ThreadLocalRandom.current().nextInt(numSoFar + 1);
        // w.p. 1 / (numSoFar+1), swap out the server and segment
        if (randNum == numSoFar) {
          fromServerHolder = server;
          proposalSegment = segment;
        }
        numSoFar++;

        // We have iterated over the alloted number of segments and will return the currently proposed segment or null
        // We will only break out early if we are iterating less than 100% of the total cluster segments
        if (percentOfSegmentsToConsider < 100 && numSoFar >= calculatedSegmentLimit) {
          log.debug("Breaking out of iteration over potential segments to move because we hit the limit [%f percent] of"
                    + " segments to consider to move. Segments Iterated: [%d]", percentOfSegmentsToConsider, numSoFar);
          break;
        }
      }
      // We have iterated over the alloted number of segments and will return the currently proposed segment or null
      // We will only break out early if we are iterating less than 100% of the total cluster segments
      if (percentOfSegmentsToConsider < 100 && numSoFar >= calculatedSegmentLimit) {
        break;
      }
    }
    if (fromServerHolder != null) {
      return new BalancerSegmentHolder(fromServerHolder, proposalSegment);
    } else {
      return null;
    }
  }

  private ReservoirSegmentSampler()
  {
  }
}
