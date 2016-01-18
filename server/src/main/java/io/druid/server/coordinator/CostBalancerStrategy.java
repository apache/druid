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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.Pair;
import com.metamx.emitter.EmittingLogger;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

public class CostBalancerStrategy implements BalancerStrategy
{
  private static final EmittingLogger log = new EmittingLogger(CostBalancerStrategy.class);
  private static final long DAY_IN_MILLIS = 1000 * 60 * 60 * 24;
  private static final long SEVEN_DAYS_IN_MILLIS = 7 * DAY_IN_MILLIS;
  private static final long THIRTY_DAYS_IN_MILLIS = 30 * DAY_IN_MILLIS;
  private final long referenceTimestamp;
  private final int threadCount;

  public CostBalancerStrategy(DateTime referenceTimestamp, int threadCount)
  {
    this.referenceTimestamp = referenceTimestamp.getMillis();
    this.threadCount = threadCount;
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

  /**
   * This defines the unnormalized cost function between two segments.  There is a base cost given by
   * the minimum size of the two segments and additional penalties.
   * recencyPenalty: it is more likely that recent segments will be queried together
   * dataSourcePenalty: if two segments belong to the same data source, they are more likely to be involved
   * in the same queries
   * gapPenalty: it is more likely that segments close together in time will be queried together
   *
   * @param segment1 The first DataSegment.
   * @param segment2 The second DataSegment.
   *
   * @return The joint cost of placing the two DataSegments together on one node.
   */
  public double computeJointSegmentCosts(final DataSegment segment1, final DataSegment segment2)
  {
    final Interval gap = segment1.getInterval().gap(segment2.getInterval());

    final double baseCost = Math.min(segment1.getSize(), segment2.getSize());
    double recencyPenalty = 1;
    double dataSourcePenalty = 1;
    double gapPenalty = 1;

    if (segment1.getDataSource().equals(segment2.getDataSource())) {
      dataSourcePenalty = 2;
    }

    double segment1diff = referenceTimestamp - segment1.getInterval().getEndMillis();
    double segment2diff = referenceTimestamp - segment2.getInterval().getEndMillis();
    if (segment1diff < SEVEN_DAYS_IN_MILLIS && segment2diff < SEVEN_DAYS_IN_MILLIS) {
      recencyPenalty = (2 - segment1diff / SEVEN_DAYS_IN_MILLIS) * (2 - segment2diff / SEVEN_DAYS_IN_MILLIS);
    }

    /** gap is null if the two segment intervals overlap or if they're adjacent */
    if (gap == null) {
      gapPenalty = 2;
    } else {
      long gapMillis = gap.toDurationMillis();
      if (gapMillis < THIRTY_DAYS_IN_MILLIS) {
        gapPenalty = 2 - gapMillis / THIRTY_DAYS_IN_MILLIS;
      }
    }

    final double cost = baseCost * recencyPenalty * dataSourcePenalty * gapPenalty;

    return cost;
  }

  public BalancerSegmentHolder pickSegmentToMove(final List<ServerHolder> serverHolders)
  {
    ReservoirSegmentSampler sampler = new ReservoirSegmentSampler();
    return sampler.getRandomBalancerSegmentHolder(serverHolders);
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
      DataSegment[] segments = server.getServer().getSegments().values().toArray(new DataSegment[]{});
      for (int i = 0; i < segments.length; ++i) {
        for (int j = i; j < segments.length; ++j) {
          cost += computeJointSegmentCosts(segments[i], segments[j]);
        }
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
   *         pairwise cost matrix).  This is the cost of a cluster if each
   *         segment were to get its own historical node.
   */
  public double calculateNormalization(final List<ServerHolder> serverHolders)
  {
    double cost = 0;
    for (ServerHolder server : serverHolders) {
      for (DataSegment segment : server.getServer().getSegments().values()) {
        cost += computeJointSegmentCosts(segment, segment);
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

    if (includeCurrentServer || !server.isServingSegment(proposalSegment)) {
      /** Don't calculate cost if the server doesn't have enough space or is loading the segment */
      if (proposalSegmentSize > server.getAvailableSize() || server.isLoadingSegment(proposalSegment)) {
        return Double.POSITIVE_INFINITY;
      }

      /** The contribution to the total cost of a given server by proposing to move the segment to that server is... */
      double cost = 0f;
      /**  the sum of the costs of other (exclusive of the proposalSegment) segments on the server */
      for (DataSegment segment : server.getServer().getSegments().values()) {
        if (!proposalSegment.equals(segment)) {
          cost += computeJointSegmentCosts(proposalSegment, segment);
        }
      }
      /**  plus the costs of segments that will be loaded */
      for (DataSegment segment : server.getPeon().getSegmentsToLoad()) {
        cost += computeJointSegmentCosts(proposalSegment, segment);
      }
      return cost;
    }
    return Double.POSITIVE_INFINITY;
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

    ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threadCount));
    List<ListenableFuture<Pair<Double, ServerHolder>>> futures = Lists.newArrayList();

    for (final ServerHolder server : serverHolders) {
      futures.add(
          service.submit(
              new Callable<Pair<Double, ServerHolder>>()
              {
                @Override
                public Pair<Double, ServerHolder> call() throws Exception
                {
                  return Pair.of(computeCost(proposalSegment, server, includeCurrentServer), server);
                }
              }
          )
      );
    }

    final ListenableFuture<List<Pair<Double, ServerHolder>>> resultsFuture = Futures.allAsList(futures);

    try {
      for (Pair<Double, ServerHolder> server : resultsFuture.get()) {
        if (server.lhs < bestServer.lhs) {
          bestServer = server;
        }
      }
    }
    catch (Exception e) {
      log.makeAlert(e, "Cost Balancer Multithread strategy wasn't able to complete cost computation.").emit();
    }
    service.shutdown();
    return bestServer;
  }

}

