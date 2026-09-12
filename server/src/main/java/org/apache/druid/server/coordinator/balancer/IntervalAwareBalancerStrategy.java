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

import com.google.common.base.Stopwatch;
import org.apache.druid.server.coordinator.SegmentCountsPerInterval;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A lightweight {@link BalancerStrategy} that spreads segments covering the same
 * time interval as evenly as possible across the historicals in a tier.
 * <p>
 * For a segment being placed (or moved), the "cost" of a server is simply the
 * number of segments already projected on that server for the segment's interval
 * (see {@link SegmentCountsPerInterval}). The cheapest
 * (least-loaded-for-this-interval) server is preferred.
 * <p>
 * The count can be scoped in two ways, controlled by {@code perDatasource}:
 * <ul>
 *   <li>{@code perDatasource=true} (default): counts only segments of the
 *   <b>same datasource</b> for the interval. Each datasource is balanced
 *   independently, which is optimal when a query targets a single datasource.</li>
 *   <li>{@code perDatasource=false}: counts segments of <b>all datasources</b>
 *   for the interval. This is optimal when a query unions multiple datasources
 *   covering the same time range.</li>
 * </ul>
 * Compared to {@link CostBalancerStrategy}, this strategy:
 * <ul>
 *   <li>does not model query-time-decay across the whole retention window; it
 *   only equalises the per-interval segment count, which is what matters for
 *   workloads that query a narrow, recent time range;</li>
 *   <li>is O(numServers) per placement using an O(1) per-server count lookup,
 *   instead of the O(segmentsPerServer) pairwise cost computation, so it does
 *   not inflate the coordinator duty-cycle time.</li>
 * </ul>
 * Ties are broken randomly (via a shuffle before a stable sort) so that servers
 * holding an equal number of segments for the interval are not always picked in
 * the same order.
 * <p>
 * This strategy is only consulted for initial placement when round-robin
 * assignment is disabled (i.e. {@code useRoundRobinSegmentAssignment=false},
 * which also requires {@code smartSegmentLoading=false}). It is always used for
 * balancing moves.
 */
public class IntervalAwareBalancerStrategy implements BalancerStrategy
{
  private final boolean perDatasource;

  private final CoordinatorRunStats stats = new CoordinatorRunStats();
  private final AtomicLong computeTimeNanos = new AtomicLong(0);

  public IntervalAwareBalancerStrategy(boolean perDatasource)
  {
    this.perDatasource = perDatasource;
  }

  @Override
  public Iterator<ServerHolder> findServersToLoadSegment(
      DataSegment segmentToLoad,
      List<ServerHolder> serverHolders
  )
  {
    final Stopwatch computeTime = Stopwatch.createStarted();
    try {
      final List<ServerHolder> eligibleServers = new ArrayList<>();
      for (ServerHolder server : serverHolders) {
        if (server.canLoadSegment(segmentToLoad)) {
          eligibleServers.add(server);
        }
      }

      // Shuffle first so that a subsequent stable sort breaks ties randomly.
      Collections.shuffle(eligibleServers);
      eligibleServers.sort(Comparator.comparingInt(server -> countSegmentsInInterval(server, segmentToLoad)));

      return eligibleServers.iterator();
    }
    finally {
      recordComputeTime(computeTime);
    }
  }

  @Override
  public ServerHolder findDestinationServerToMoveSegment(
      DataSegment segmentToMove,
      ServerHolder sourceServer,
      List<ServerHolder> destinationServers
  )
  {
    final Stopwatch computeTime = Stopwatch.createStarted();
    try {
      final int sourceCount = countSegmentsInInterval(sourceServer, segmentToMove);

      ServerHolder bestDestination = null;
      int bestCount = Integer.MAX_VALUE;
      int numTiedAtBest = 0;
      for (ServerHolder server : destinationServers) {
        if (server.equals(sourceServer) || !server.canLoadSegment(segmentToMove)) {
          continue;
        }
        final int count = countSegmentsInInterval(server, segmentToMove);
        if (count < bestCount) {
          bestCount = count;
          bestDestination = server;
          numTiedAtBest = 1;
        } else if (count == bestCount) {
          // Reservoir sampling over the servers tied at the minimum count, so that
          // ties are broken uniformly at random rather than always picking the
          // server that appears earliest in the list. Without this, when many
          // servers share the lowest count for an interval (common on a large,
          // lightly-loaded tier), moves would persistently favour the same
          // early-ordered servers and skew the distribution over repeated runs.
          if (ThreadLocalRandom.current().nextInt(++numTiedAtBest) == 0) {
            bestDestination = server;
          }
        }
      }

      // A decommissioning source must be fully evacuated regardless of interval
      // balance, so the anti-oscillation guard below is skipped for it. The source
      // is never itself a candidate here (the caller excludes a decommissioning
      // source from the destination list), so any chosen destination strictly
      // makes progress towards draining the server.
      if (bestDestination != null && sourceServer.isDecommissioning()) {
        return bestDestination;
      }

      // Otherwise, only move if it strictly reduces the maximum per-interval count,
      // i.e. the destination (after gaining the segment) would hold fewer segments
      // for this interval than the source currently does. This avoids pointless
      // moves and oscillation between two servers that differ by a single segment.
      if (bestDestination != null && bestCount + 1 < sourceCount) {
        return bestDestination;
      }
      return null;
    }
    finally {
      recordComputeTime(computeTime);
    }
  }

  @Override
  public Iterator<ServerHolder> findServersToDropSegment(
      DataSegment segmentToDrop,
      List<ServerHolder> serverHolders
  )
  {
    final List<ServerHolder> servers = new ArrayList<>(serverHolders);
    // Shuffle first so that a subsequent stable sort breaks ties randomly.
    Collections.shuffle(servers);
    // Drop from the most heavily loaded server for this interval first.
    servers.sort(Comparator.comparingInt((ServerHolder server) -> countSegmentsInInterval(server, segmentToDrop)).reversed());

    return servers.iterator();
  }

  @Override
  public CoordinatorRunStats getStats()
  {
    stats.add(
        Stats.Balancer.COMPUTATION_TIME,
        TimeUnit.NANOSECONDS.toMillis(computeTimeNanos.getAndSet(0))
    );
    return stats;
  }

  private void recordComputeTime(Stopwatch computeTime)
  {
    computeTime.stop();
    stats.add(Stats.Balancer.COMPUTATION_COUNT, 1);
    computeTimeNanos.addAndGet(computeTime.elapsed(TimeUnit.NANOSECONDS));
  }

  /**
   * Number of segments projected on the given server for the segment's interval,
   * scoped either to the segment's datasource or to all datasources depending on
   * {@link #perDatasource}. Uses the O(1) per-interval counts maintained by
   * {@link ServerHolder#getProjectedSegmentCounts()}.
   */
  private int countSegmentsInInterval(ServerHolder server, DataSegment segment)
  {
    final SegmentCountsPerInterval counts = server.getProjectedSegmentCounts();
    final Interval interval = segment.getInterval();
    if (perDatasource) {
      return counts.getIntervalToSegmentCount(segment.getDataSource()).getInt(interval);
    } else {
      return counts.getIntervalToTotalSegmentCount().getInt(interval);
    }
  }
}
