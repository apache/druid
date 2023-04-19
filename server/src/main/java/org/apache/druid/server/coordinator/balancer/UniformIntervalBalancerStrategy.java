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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A balancing strategy that tries to have a uniform number of segments in each
 * datasource-interval across all servers in a tier.
 * <p>
 * The uniform interval strategy may move a segment S of datasource D, interval I
 * from server A to B only if:
 * <ol>
 *   <li>A has atleast 2 more segments in (D,I) than B</li>
 *   <li>OR, A has exactly 1 more segment in (D,I) than B and
 *   {@code size(A) - size(B) >= 2 * size(S)}</li>
 * </ol>
 * These rules are to ensure that every move takes the cluster closer to a
 * balanced state and to avoid unnecessary moves, which just result in a
 * reversal of roles of A and B.
 * <p>
 * <b>Principle:</b>
 * {@link CostBalancerStrategy} works on the principle that segments that are
 * interval adjacent should be placed on separate servers. This works well when
 * the number of segments in an interval is smaller than the number of available
 * servers. But if it is higher (which is very likely the case in practice), some
 * segments for an interval must be pigeon-holed into a server which already has
 * a segment for that same interval. This essentially boils down to each server
 * having a near-equal number of segments for that interval.
 * <p>
 * This strategy is thus an over-simplification of the cost strategy.
 * <p>
 * <b>Advantages:</b>
 * <ul>
 *   <li>Cost computation is much simpler compared to cost strategy, thus
 *   significantly faster.</li>
 *   <li>It does not assign higher weight to coarser granularity segments, thus
 *   cluster is always balanced even if there are a few coarse grain segments (such as ALL).</li>
 *   <li>It also handles cases where a datasource has only a single segment per
 *   interval. Cost strategy fails to attain a balanced cluster for such situations.</li>
 *   <li>Unlike {@link CachingCostBalancerStrategy}, it does not require a cache to
 *   be initialized. Every {@link ServerHolder} created at the start of a coordinator
 *   run is initialized with the segment count in each datasource-interval.</li>
 * </ul>
 * <p>
 * <b>Assumptions:</b>
 * <ul>
 *   <li>All segments in a given datasource-interval are nearly of the same size.</li>
 *   <li>All servers in a tier have the same disk capacity. (This can be remedied
 *   in later versions by computing percentages in Rule 2 rather than absolute sizes.)</li>
 * </ul>
 */
public class UniformIntervalBalancerStrategy implements BalancerStrategy
{

  /**
   * Orders servers first by number of segments in datasource interval (lowest first)
   * and then by used percentage (lowest first).
   */
  private static final Comparator<ServerSegmentCount> CHEAPEST_SERVERS_FIRST
      = Comparator.<ServerSegmentCount>comparingInt(entry -> entry.segmentCount)
      .thenComparingDouble(entry -> entry.server.getPercentUsed())
      .thenComparingInt(entry -> ThreadLocalRandom.current().nextInt());

  @Nullable
  @Override
  public ServerHolder findDestinationServerToMoveSegment(
      DataSegment segment,
      ServerHolder sourceServer,
      List<ServerHolder> destinationServers
  )
  {
    Iterator<ServerHolder> bestServers = chooseBestServers(segment, destinationServers, true);
    if (!bestServers.hasNext()) {
      return null;
    }

    ServerHolder targetServer = bestServers.next();
    final int countOnSource = sourceServer.getNumSegmentsInDatasourceInterval(segment);
    final int countOnTarget = targetServer.getNumSegmentsInDatasourceInterval(segment);

    // Avoid unnecessary moves that do not lead to balance and would only result
    // in a reversal of roles of source and target
    if (countOnSource - countOnTarget >= 2) {
      return targetServer;
    } else if (countOnSource - countOnTarget == 1
               && (sourceServer.getSizeUsed() - targetServer.getSizeUsed() >= 2 * segment.getSize())) {
      return targetServer;
    } else {
      return null;
    }
  }

  @Override
  public Iterator<ServerHolder> findServerToLoadSegment(
      DataSegment proposalSegment,
      List<ServerHolder> serverHolders
  )
  {
    return chooseBestServers(proposalSegment, serverHolders, false);
  }

  @Override
  public void emitStats(String tier, CoordinatorRunStats stats, List<ServerHolder> serverHolderList)
  {
    // Do not emit anything
  }

  private Iterator<ServerHolder> chooseBestServers(
      final DataSegment proposalSegment,
      final Iterable<ServerHolder> serverHolders,
      final boolean includeCurrentServer
  )
  {
    final PriorityQueue<ServerSegmentCount> costPrioritizedServers =
        new PriorityQueue<>(CHEAPEST_SERVERS_FIRST);

    for (ServerHolder server : serverHolders) {
      final int count = server.getNumSegmentsInDatasourceInterval(proposalSegment);
      if (includeCurrentServer || server.canLoadSegment(proposalSegment)) {
        costPrioritizedServers.add(new ServerSegmentCount(server, count));
      }
    }

    // Include current server only if specified
    return costPrioritizedServers.stream()
                                 .filter(entry -> includeCurrentServer
                                                  || entry.server.canLoadSegment(proposalSegment))
                                 .map(entry -> entry.server).iterator();
  }

  private static class ServerSegmentCount
  {
    private final int segmentCount;
    private final ServerHolder server;

    private ServerSegmentCount(ServerHolder server, int segmentCount)
    {
      this.server = server;
      this.segmentCount = segmentCount;
    }
  }
}
