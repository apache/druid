/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.master;

import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.common.Pair;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * The BalancerCostAnalyzer will compute the total initial cost of the cluster, with costs defined in
 * computeJointSegmentCosts.  It will then propose to move (pseudo-)randomly chosen segments from their
 * respective initial servers to other servers, chosen greedily to minimize the cost of the cluster.
 */
public class BalancerCostAnalyzer
{
  private static final Logger log = new Logger(BalancerCostAnalyzer.class);
  private static final int DAY_IN_MILLIS = 1000 * 60 * 60 * 24;
  private static final int SEVEN_DAYS_IN_MILLIS = 7 * DAY_IN_MILLIS;
  private static final int THIRTY_DAYS_IN_MILLIS = 30 * DAY_IN_MILLIS;
  private final Random rand;
  private final DateTime referenceTimestamp;

  public BalancerCostAnalyzer(DateTime referenceTimestamp)
  {
    this.referenceTimestamp = referenceTimestamp;
    rand = new Random(0);
  }

  /**
   * Calculates the cost normalization.  This is such that the normalized cost is lower bounded
   * by 1 (e.g. when each segment gets its own compute node).
   *
   * @param serverHolders A list of ServerHolders for a particular tier.
   *
   * @return The normalization value (the sum of the diagonal entries in the
   *         pairwise cost matrix).  This is the cost of a cluster if each
   *         segment were to get its own compute node.
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

    double maxDiff = Math.max(
        referenceTimestamp.getMillis() - segment1.getInterval().getEndMillis(),
        referenceTimestamp.getMillis() - segment2.getInterval().getEndMillis()
    );
    if (maxDiff < SEVEN_DAYS_IN_MILLIS) {
      recencyPenalty = 2 - maxDiff / SEVEN_DAYS_IN_MILLIS;
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

  /**
   * The balancing application requires us to pick a proposal segment uniformly at random from the set of
   * all servers.  We use reservoir sampling to do this.
   *
   * @param serverHolders A list of ServerHolders for a particular tier.
   *
   * @return A BalancerSegmentHolder sampled uniformly at random.
   */
  public BalancerSegmentHolder pickSegmentToMove(final List<ServerHolder> serverHolders)
  {
    ServerHolder fromServerHolder = null;
    DataSegment proposalSegment = null;
    int numSoFar = 0;

    for (ServerHolder server : serverHolders) {
      for (DataSegment segment : server.getServer().getSegments().values()) {
        int randNum = rand.nextInt(numSoFar + 1);
        // w.p. 1 / (numSoFar + 1), swap out the server and segment
        if (randNum == numSoFar) {
          fromServerHolder = server;
          proposalSegment = segment;
          numSoFar++;
        }
      }
    }

    return new BalancerSegmentHolder(fromServerHolder.getServer(), proposalSegment);
  }

  /**
   * For balancing, we want to only make a move if the minimum cost server is not already serving the segment.
   *
   * @param proposalSegment A DataSegment that we are proposing to move.
   * @param serverHolders   An iterable of ServerHolders for a particular tier.
   *
   * @return A ServerHolder with the new home for a segment.
   */
  public ServerHolder findNewSegmentHomeBalance(
      final DataSegment proposalSegment,
      final Iterable<ServerHolder> serverHolders
  )
  {
    MinMaxPriorityQueue<Pair<Double, ServerHolder>> costsAndServers = computeCosts(proposalSegment, serverHolders);
    if (costsAndServers.isEmpty()) {
      return null;
    }

    ServerHolder toServer = costsAndServers.pollFirst().rhs;
    if (!toServer.isServingSegment(proposalSegment)) {
      return toServer;
    }

    return null;
  }

  /**
   * For assignment, we want to move to the lowest cost server that isn't already serving the segment.
   *
   * @param proposalSegment A DataSegment that we are proposing to move.
   * @param serverHolders   An iterable of ServerHolders for a particular tier.
   *
   * @return A ServerHolder with the new home for a segment.
   */
  public ServerHolder findNewSegmentHomeAssign(
      final DataSegment proposalSegment,
      final Iterable<ServerHolder> serverHolders
  )
  {
    MinMaxPriorityQueue<Pair<Double, ServerHolder>> costsAndServers = computeCosts(proposalSegment, serverHolders);
    while (!costsAndServers.isEmpty()) {
      ServerHolder toServer = costsAndServers.pollFirst().rhs;
      if (!toServer.isServingSegment(proposalSegment)) {
        return toServer;
      }
    }

    return null;
  }

  private MinMaxPriorityQueue<Pair<Double, ServerHolder>> computeCosts(
      final DataSegment proposalSegment,
      final Iterable<ServerHolder> serverHolders
  )
  {
    MinMaxPriorityQueue<Pair<Double, ServerHolder>> costsAndServers = MinMaxPriorityQueue.orderedBy(
        new Comparator<Pair<Double, ServerHolder>>()
        {
          @Override
          public int compare(
              Pair<Double, ServerHolder> o,
              Pair<Double, ServerHolder> o1
          )
          {
            return Double.compare(o.lhs, o1.lhs);
          }
        }
    ).create();

    final long proposalSegmentSize = proposalSegment.getSize();

    for (ServerHolder server : serverHolders) {
      /** Don't calculate cost if the server doesn't have enough space or is loading the segment */
      if (proposalSegmentSize > server.getAvailableSize() || server.isLoadingSegment(proposalSegment)) {
        continue;
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

      costsAndServers.add(Pair.of(cost, server));
    }

    return costsAndServers;
  }

}
