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
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.commons.math3.util.FastMath;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.SegmentCountsPerInterval;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.SegmentAction;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class CostBalancerStrategy implements BalancerStrategy
{
  private static final EmittingLogger log = new EmittingLogger(CostBalancerStrategy.class);

  private static final double HALF_LIFE = 24.0; // cost function half-life in hours
  static final double LAMBDA = Math.log(2) / HALF_LIFE;
  static final double INV_LAMBDA_SQUARE = 1 / (LAMBDA * LAMBDA);

  private static final double MILLIS_IN_HOUR = 3_600_000.0;
  private static final double MILLIS_FACTOR = MILLIS_IN_HOUR / LAMBDA;

  /**
   * Comparator that prioritizes servers by cost. Cheaper servers come before
   * costlier servers. Servers with the same cost may appear in a random order.
   */
  private static final Comparator<Pair<Double, ServerHolder>> CHEAPEST_SERVERS_FIRST
      = Comparator.<Pair<Double, ServerHolder>, Double>comparing(pair -> pair.lhs)
      .thenComparing(pair -> pair.rhs);

  private final CoordinatorRunStats stats = new CoordinatorRunStats();
  private final AtomicLong computeTimeNanos = new AtomicLong(0);

  public static double computeJointSegmentsCost(DataSegment segment, Iterable<DataSegment> segmentSet)
  {
    final Interval costComputeInterval = getCostComputeInterval(segment);
    double totalCost = 0;
    for (DataSegment s : segmentSet) {
      if (costComputeInterval.overlaps(s.getInterval())) {
        totalCost += computeJointSegmentsCost(segment, s);
      }
    }
    return totalCost;
  }

  /**
   * This defines the unnormalized cost function between two segments.
   *
   * See https://github.com/apache/druid/pull/2972 for more details about the cost function.
   *
   * intervalCost: segments close together are more likely to be queried together
   *
   * multiplier: if two segments belong to the same data source, they are more likely to be involved
   * in the same queries
   *
   * @param segmentA The first DataSegment.
   * @param segmentB The second DataSegment.
   *
   * @return the joint cost of placing the two DataSegments together on one node.
   */
  public static double computeJointSegmentsCost(final DataSegment segmentA, final DataSegment segmentB)
  {
    final Interval intervalA = segmentA.getInterval();
    final Interval intervalB = segmentB.getInterval();

    // constant cost-multiplier for segments of the same datsource
    final double multiplier = segmentA.getDataSource().equals(segmentB.getDataSource())
                              ? 2.0 : 1.0;
    return intervalCost(intervalA, intervalB) * multiplier;
  }

  public static double intervalCost(Interval intervalA, Interval intervalB)
  {
    final double t0 = intervalA.getStartMillis();
    final double t1 = (intervalA.getEndMillis() - t0) / MILLIS_FACTOR;
    final double start = (intervalB.getStartMillis() - t0) / MILLIS_FACTOR;
    final double end = (intervalB.getEndMillis() - t0) / MILLIS_FACTOR;

    return INV_LAMBDA_SQUARE * intervalCost(t1, start, end);
  }

  /**
   * Computes the joint cost of two intervals X = [x_0 = 0, x_1) and Y = [y_0, y_1)
   *
   * cost(X, Y) = \int_{x_0}^{x_1} \int_{y_0}^{y_1} e^{-\lambda |x-y|}dxdy $$
   *
   * lambda = 1 in this particular implementation
   *
   * Other values of lambda can be calculated by multiplying inputs by lambda
   * and multiplying the result by 1 / lambda ^ 2
   *
   * Interval start and end are all relative to x_0.
   * Therefore this function assumes x_0 = 0, x1 >= 0, and y1 > y0
   *
   * @param x1 end of interval X
   * @param y0 start of interval Y
   * @param y1 end o interval Y
   *
   * @return joint cost of X and Y
   */
  public static double intervalCost(double x1, double y0, double y1)
  {
    if (x1 == 0 || y1 == y0) {
      return 0;
    }

    // cost(X, Y) = cost(Y, X), so we swap X and Y to
    // have x_0 <= y_0 and simplify the calculations below
    if (y0 < 0) {
      // swap X and Y
      double tmp = x1;
      x1 = y1 - y0;
      y1 = tmp - y0;
      y0 = -y0;
    }

    // since x_0 <= y_0, Y must overlap X if y_0 < x_1
    if (y0 < x1) {
      /*
       * We have two possible cases of overlap:
       *
       * X  = [ A )[ B )[ C )   or  [ A )[ B )
       * Y  =      [   )                 [   )[ C )
       *
       * A is empty if y0 = 0
       * C is empty if y1 = x1
       *
       * cost(X, Y) = cost(A, Y) + cost(B, C) + cost(B, B)
       *
       * cost(A, Y) and cost(B, C) can be calculated using the non-overlapping case,
       * which reduces the overlapping case to computing
       *
       * cost(B, B) = \int_0^{\beta} \int_{0}^{\beta} e^{-|x-y|}dxdy
       *            = 2 \cdot (\beta + e^{-\beta} - 1)
       *
       *            where \beta is the length of interval B
       *
       */
      final double beta;  // b1 - y0, length of interval B
      final double gamma; // c1 - y0, length of interval C
      if (y1 <= x1) {
        beta = y1 - y0;
        gamma = x1 - y0;
      } else {
        beta = x1 - y0;
        gamma = y1 - y0;
      }
      //noinspection SuspiciousNameCombination
      return intervalCost(y0, y0, y1) + // cost(A, Y)
             intervalCost(beta, beta, gamma) + // cost(B, C)
             2 * (beta + FastMath.exp(-beta) - 1); // cost(B, B)
    } else {
      /*
       * In the case where there is no overlap:
       *
       * Given that x_0 <= y_0,
       * then x <= y must be true for all x in [x_0, x_1] and y in [y_0, y_1).
       *
       * therefore,
       *
       * cost(X, Y) = \int_0^{x_1} \int_{y_0}^{y_1} e^{-|x-y|} dxdy
       *            = \int_0^{x_1} \int_{y_0}^{y_1} e^{x-y} dxdy
       *            = (e^{-y_1} - e^{-y_0}) - (e^{x_1-y_1} - e^{x_1-y_0})
       *
       * Note, this expression could be further reduced by factoring out (e^{x_1} - 1),
       * but we prefer to keep the smaller values x_1 - y_0 and x_1 - y_1 in the exponent
       * to avoid numerical overflow caused by calculating e^{x_1}
       */
      final double exy0 = FastMath.exp(x1 - y0);
      final double exy1 = FastMath.exp(x1 - y1);
      final double ey0 = FastMath.exp(0f - y0);
      final double ey1 = FastMath.exp(0f - y1);

      return (ey1 - ey0) - (exy1 - exy0);
    }
  }

  private final ListeningExecutorService exec;

  public CostBalancerStrategy(ListeningExecutorService exec)
  {
    this.exec = exec;
  }

  @Override
  public Iterator<ServerHolder> findServersToLoadSegment(
      DataSegment segmentToLoad,
      List<ServerHolder> serverHolders
  )
  {
    return orderServersByPlacementCost(segmentToLoad, serverHolders, SegmentAction.LOAD)
        .stream()
        .filter(server -> server.canLoadSegment(segmentToLoad))
        .iterator();
  }

  @Override
  public ServerHolder findDestinationServerToMoveSegment(
      DataSegment segmentToMove,
      ServerHolder sourceServer,
      List<ServerHolder> serverHolders
  )
  {
    List<ServerHolder> servers =
        orderServersByPlacementCost(segmentToMove, serverHolders, SegmentAction.MOVE_TO);
    if (servers.isEmpty()) {
      return null;
    }

    ServerHolder candidateServer = servers.get(0);
    return candidateServer.equals(sourceServer) ? null : candidateServer;
  }

  @Override
  public Iterator<ServerHolder> findServersToDropSegment(
      DataSegment segmentToDrop,
      List<ServerHolder> serverHolders
  )
  {
    List<ServerHolder> serversByCost =
        orderServersByPlacementCost(segmentToDrop, serverHolders, SegmentAction.DROP);

    // Prioritize drop from highest cost servers
    return Lists.reverse(serversByCost).iterator();
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

  /**
   * Computes the cost of placing a segment on this server.
   */
  protected double computePlacementCost(DataSegment proposalSegment, ServerHolder server)
  {
    final Interval costComputeInterval = getCostComputeInterval(proposalSegment);

    // Compute number of segments in each interval
    final Object2IntOpenHashMap<Interval> intervalToSegmentCount = new Object2IntOpenHashMap<>();

    final SegmentCountsPerInterval projectedSegments = server.getProjectedSegments();
    projectedSegments.getIntervalToTotalSegmentCount().object2IntEntrySet().forEach(entry -> {
      final Interval interval = entry.getKey();
      if (costComputeInterval.overlaps(interval)) {
        intervalToSegmentCount.addTo(interval, entry.getIntValue());
      }
    });

    // Count the segments for the same datasource twice as they have twice the cost
    final String datasource = proposalSegment.getDataSource();
    projectedSegments.getIntervalToSegmentCount(datasource).object2IntEntrySet().forEach(entry -> {
      final Interval interval = entry.getKey();
      if (costComputeInterval.overlaps(interval)) {
        intervalToSegmentCount.addTo(interval, entry.getIntValue());
      }
    });

    // Compute joint cost for each interval
    double cost = 0;
    final Interval segmentInterval = proposalSegment.getInterval();
    cost += intervalToSegmentCount.object2IntEntrySet().stream().mapToDouble(
        entry -> intervalCost(segmentInterval, entry.getKey())
                 * entry.getIntValue()
    ).sum();

    // Minus the self cost of the segment
    if (server.isProjectedSegment(proposalSegment)) {
      cost -= intervalCost(segmentInterval, segmentInterval) * 2.0;
    }

    return cost;
  }

  /**
   * Orders the servers by increasing cost for placing the given segment.
   */
  private List<ServerHolder> orderServersByPlacementCost(
      DataSegment segment,
      List<ServerHolder> serverHolders,
      SegmentAction action
  )
  {
    final Stopwatch computeTime = Stopwatch.createStarted();
    final List<ListenableFuture<Pair<Double, ServerHolder>>> futures = new ArrayList<>();
    for (ServerHolder server : serverHolders) {
      futures.add(
          exec.submit(
              () -> Pair.of(computePlacementCost(segment, server), server)
          )
      );
    }

    String tier = serverHolders.isEmpty() ? null : serverHolders.get(0).getServer().getTier();
    final RowKey metricKey = RowKey.with(Dimension.TIER, tier)
                                   .with(Dimension.DATASOURCE, segment.getDataSource())
                                   .and(Dimension.DESCRIPTION, action.name());

    final PriorityQueue<Pair<Double, ServerHolder>> costPrioritizedServers =
        new PriorityQueue<>(CHEAPEST_SERVERS_FIRST);
    try {
      // 1 minute is the typical time for a full run of all historical management duties
      // and is more than enough time for the cost computation of a single segment
      costPrioritizedServers.addAll(
          Futures.allAsList(futures).get(1, TimeUnit.MINUTES)
      );
    }
    catch (Exception e) {
      stats.add(Stats.Balancer.COMPUTATION_ERRORS, metricKey, 1);
      handleFailure(e, segment, action);
    }

    // Report computation stats
    computeTime.stop();
    stats.add(Stats.Balancer.COMPUTATION_COUNT, 1);
    computeTimeNanos.addAndGet(computeTime.elapsed(TimeUnit.NANOSECONDS));

    return costPrioritizedServers.stream().map(pair -> pair.rhs)
                                 .collect(Collectors.toList());
  }

  private void handleFailure(
      Exception e,
      DataSegment segment,
      SegmentAction action
  )
  {
    final String reason;
    String suggestion = "";
    if (exec.isShutdown()) {
      reason = "Executor shutdown";
    } else if (e instanceof TimeoutException) {
      reason = "Timed out";
      suggestion = " Try setting a higher value for 'balancerComputeThreads'.";
    } else {
      reason = e.getMessage();
    }

    String msgFormat = "Cost strategy computations failed for action[%s] on segment[%s] due to reason[%s].[%s]";
    log.noStackTrace().warn(e, msgFormat, action, segment.getId(), reason, suggestion);
  }

  /**
   * The cost compute interval for a segment is {@code [start-45days, end+45days)}.
   * This is because the joint cost of any two segments that are 45 days apart is
   * negligible.
   */
  private static Interval getCostComputeInterval(DataSegment segment)
  {
    final Interval segmentInterval = segment.getInterval();
    if (Intervals.isEternity(segmentInterval)) {
      return segmentInterval;
    } else {
      final long maxGap = TimeUnit.DAYS.toMillis(45);
      return Intervals.utc(
          segmentInterval.getStartMillis() - maxGap,
          segmentInterval.getEndMillis() + maxGap
      );
    }
  }

}

