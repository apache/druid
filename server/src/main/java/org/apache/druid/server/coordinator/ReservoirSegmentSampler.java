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

import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.timeline.DataSegment;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

final class ReservoirSegmentSampler
{

  private static final EmittingLogger log = new EmittingLogger(ReservoirSegmentSampler.class);

  /**
   * Iterates over segments that live on the candidate servers passed in {@link ServerHolder} and (possibly) picks a
   * segment to return to caller in a {@link BalancerSegmentHolder} object.
   *
   * @param serverHolders List of {@link ServerHolder} objects containing segments who are candidates to be chosen.
   * @param broadcastDatasources Set of DataSource names that identify broadcast datasources. We don't want to consider
   *                             segments from these datasources.
   * @param numberOfSegmentsToConsider A limit on the number of segments to consider before short-circuiting and
   *                                   returning immediately.
   * @return
   */
  static BalancerSegmentHolder getRandomBalancerSegmentHolder(
      final List<ServerHolder> serverHolders,
      Set<String> broadcastDatasources,
      int numberOfSegmentsToConsider
  )
  {
    ServerHolder fromServerHolder = null;
    DataSegment proposalSegment = null;
    int numSoFar = 0;

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
        if (numSoFar >= numberOfSegmentsToConsider) {
          log.debug(
              "Breaking out of iteration over potential segments to move because the limit for " +
              "numberOfSegmentsToCondider [%d] has been hit", numberOfSegmentsToConsider
          );
          break;
        }
      }
      // We have iterated over the alloted number of segments and will return the currently proposed segment or null
      if (numSoFar >= numberOfSegmentsToConsider) {
        break;
      }
    }
    if (fromServerHolder != null) {
      return new BalancerSegmentHolder(fromServerHolder.getServer(), proposalSegment);
    } else {
      return null;
    }
  }

  private ReservoirSegmentSampler()
  {
  }
}
