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

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.druid.java.util.common.Pair;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.timeline.DataSegment;
import org.apache.commons.math3.util.FastMath;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ThreadLocalRandom;
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
   * This defines the unnormalized cost function between two segments.
   *
   * See https://github.com/druid-io/druid/pull/2972 for more details about the cost function.
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

    final double t0 = intervalA.getStartMillis();
    final double t1 = (intervalA.getEndMillis() - t0) / MILLIS_FACTOR;
    final double start = (intervalB.getStartMillis() - t0) / MILLIS_FACTOR;
    final double end = (intervalB.getEndMillis() - t0) / MILLIS_FACTOR;

    // constant cost-multiplier for segments of the same datsource
    final double multiplier = segmentA.getDataSource().equals(segmentB.getDataSource()) ? 2.0 : 1.0;

    return INV_LAMBDA_SQUARE * intervalCost(t1, start, end) * multiplier;
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
      /**
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
      /**
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
  public ServerHolder findNewSegmentHomeReplicator(
      DataSegment proposalSegment, List<ServerHolder> serverHolders
  )
  {
    ServerHolder holder = chooseBestServer(proposalSegment, serverHolders, false).rhs;
    if (holder != null && !holder.isServingSegment(proposalSegment)) {
      return holder;
    }
    return null;
  }


  @Override
  public ServerHolder findNewSegmentHomeBalancer(
      DataSegment proposalSegment, List<ServerHolder> serverHolders
  )
  {
    return chooseBestServer(proposalSegment, serverHolders, true).rhs;
  }

  static double computeJointSegmentsCost(final DataSegment segment, final Iterable<DataSegment> segmentSet)
  {
    double totalCost = 0;
    for (DataSegment s : segmentSet) {
      totalCost += computeJointSegmentsCost(segment, s);
    }
    return totalCost;
  }


  @Override
  public BalancerSegmentHolder pickSegmentToMove(final List<ServerHolder> serverHolders)
  {
    ReservoirSegmentSampler sampler = new ReservoirSegmentSampler();
    return sampler.getRandomBalancerSegmentHolder(serverHolders);
  }

  @Override
  public Iterator<ServerHolder> pickServersToDrop(DataSegment toDrop, NavigableSet<ServerHolder> serverHolders)
  {
    List<ListenableFuture<Pair<Double, ServerHolder>>> futures = Lists.newArrayList();

    for (final ServerHolder server : serverHolders) {
      futures.add(
          exec.submit(
              () -> Pair.of(computeCost(toDrop, server, true), server)
          )
      );
    }

    final ListenableFuture<List<Pair<Double, ServerHolder>>> resultsFuture = Futures.allAsList(futures);

    try {
      // results is an un-ordered list of a pair consisting of the 'cost' of a segment being on a server and the server
      List<Pair<Double, ServerHolder>> results = resultsFuture.get();
      return results.stream()
                    // Comparator.comapringDouble will order by lowest cost...
                    // reverse it because we want to drop from the highest cost servers first
                    .sorted(Comparator.comparingDouble((Pair<Double, ServerHolder> o) -> o.lhs).reversed())
                    .map(x -> x.rhs).collect(Collectors.toList())
                    .iterator();
    }
    catch (Exception e) {
      log.makeAlert(e, "Cost Balancer Multithread strategy wasn't able to complete cost computation.").emit();
    }
    return Collections.emptyIterator();
  }

  /**
   * Calculates the initial cost of the Druid segment configuration.
   *
   * @param serverHolders A list of ServerHolders for a particular tier.
   *
   * @return The initial cost of the Druid tier.
   */
  public double calculateInitialTotalCost(final List<ServerHolder> serverHolders)
  {
    double cost = 0;
    for (ServerHolder server : serverHolders) {
      Iterable<DataSegment> segments = server.getServer().getSegments().values();
      for (DataSegment s : segments) {
        cost += computeJointSegmentsCost(s, segments);
      }
    }
    return cost;
  }

  /**
   * Calculates the cost normalization.  This is such that the normalized cost is lower bounded
   * by 1 (e.g. when each segment gets its own historical node).
   *
   * @param serverHolders A list of ServerHolders for a particular tier.
   *
   * @return The normalization value (the sum of the diagonal entries in the
   * pairwise cost matrix).  This is the cost of a cluster if each
   * segment were to get its own historical node.
   */
  public double calculateNormalization(final List<ServerHolder> serverHolders)
  {
    double cost = 0;
    for (ServerHolder server : serverHolders) {
      for (DataSegment segment : server.getServer().getSegments().values()) {
        cost += computeJointSegmentsCost(segment, segment);
      }
    }
    return cost;
  }

  @Override
  public void emitStats(
      String tier,
      CoordinatorStats stats, List<ServerHolder> serverHolderList
  )
  {
    final double initialTotalCost = calculateInitialTotalCost(serverHolderList);
    final double normalization = calculateNormalization(serverHolderList);
    final double normalizedInitialCost = initialTotalCost / normalization;

    stats.addToTieredStat("initialCost", tier, (long) initialTotalCost);
    stats.addToTieredStat("normalization", tier, (long) normalization);
    stats.addToTieredStat("normalizedInitialCostTimesOneThousand", tier, (long) (normalizedInitialCost * 1000));

    log.info(
        "[%s]: Initial Total Cost: [%f], Normalization: [%f], Initial Normalized Cost: [%f]",
        tier,
        initialTotalCost,
        normalization,
        normalizedInitialCost
    );
  }

  protected double computeCost(
      final DataSegment proposalSegment, final ServerHolder server, final boolean includeCurrentServer
  )
  {
    final long proposalSegmentSize = proposalSegment.getSize();

    // (optional) Don't include server if it is already serving segment
    if (!includeCurrentServer && server.isServingSegment(proposalSegment)) {
      return Double.POSITIVE_INFINITY;
    }

    // Don't calculate cost if the server doesn't have enough space or is loading the segment
    if (proposalSegmentSize > server.getAvailableSize() || server.isLoadingSegment(proposalSegment)) {
      return Double.POSITIVE_INFINITY;
    }

    // The contribution to the total cost of a given server by proposing to move the segment to that server is...
    double cost = 0d;

    // the sum of the costs of other (exclusive of the proposalSegment) segments on the server
    cost += computeJointSegmentsCost(
        proposalSegment,
        Iterables.filter(
            server.getServer().getSegments().values(),
            Predicates.not(Predicates.equalTo(proposalSegment))
        )
    );

    // plus the costs of segments that will be loaded
    cost += computeJointSegmentsCost(proposalSegment, server.getPeon().getSegmentsToLoad());

    // minus the costs of segments that are marked to be dropped
    cost -= computeJointSegmentsCost(proposalSegment, server.getPeon().getSegmentsMarkedToDrop());

    return cost;
  }

  /**
   * For assignment, we want to move to the lowest cost server that isn't already serving the segment.
   *
   * @param proposalSegment A DataSegment that we are proposing to move.
   * @param serverHolders   An iterable of ServerHolders for a particular tier.
   *
   * @return A ServerHolder with the new home for a segment.
   */

  protected Pair<Double, ServerHolder> chooseBestServer(
      final DataSegment proposalSegment,
      final Iterable<ServerHolder> serverHolders,
      final boolean includeCurrentServer
  )
  {
    Pair<Double, ServerHolder> bestServer = Pair.of(Double.POSITIVE_INFINITY, null);

    List<ListenableFuture<Pair<Double, ServerHolder>>> futures = Lists.newArrayList();

    for (final ServerHolder server : serverHolders) {
      futures.add(
          exec.submit(
              () -> Pair.of(computeCost(proposalSegment, server, includeCurrentServer), server)
          )
      );
    }

    final ListenableFuture<List<Pair<Double, ServerHolder>>> resultsFuture = Futures.allAsList(futures);
    final List<Pair<Double, ServerHolder>> bestServers = new ArrayList<>();
    bestServers.add(bestServer);
    try {
      for (Pair<Double, ServerHolder> server : resultsFuture.get()) {
        if (server.lhs <= bestServers.get(0).lhs) {
          if (server.lhs < bestServers.get(0).lhs) {
            bestServers.clear();
          }
          bestServers.add(server);
        }
      }

      // Randomly choose a server from the best servers
      bestServer = bestServers.get(ThreadLocalRandom.current().nextInt(bestServers.size()));
    }
    catch (Exception e) {
      log.makeAlert(e, "Cost Balancer Multithread strategy wasn't able to complete cost computation.").emit();
    }
    return bestServer;
  }
}

