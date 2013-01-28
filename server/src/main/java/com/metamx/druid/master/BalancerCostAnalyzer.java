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

import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

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

  /*
   * Calculates the cost normalization.  This is such that the normalized cost is lower bounded
   * by 1 (e.g. when each segment gets its own compute node).
   */
  public double calculateNormalization(List<ServerHolder> serverHolderList)
  {
    double cost = 0;
    for (ServerHolder server : serverHolderList) {
      for (DataSegment segment : server.getServer().getSegments().values()) {
        cost += computeJointSegmentCosts(segment, segment);
      }
    }
    return cost;
  }

  // Calculates the initial cost of the Druid segment configuration.
  public double calculateInitialTotalCost(List<ServerHolder> serverHolderList)
  {
    double cost = 0;
    for (ServerHolder server : serverHolderList) {
      DataSegment[] segments = server.getServer().getSegments().values().toArray(new DataSegment[]{});
      for (int i = 0; i < segments.length; ++i) {
        for (int j = i; j < segments.length; ++j) {
          cost += computeJointSegmentCosts(segments[i], segments[j]);
        }
      }
    }
    return cost;
  }

  /*
   * This defines the unnormalized cost function between two segments.  There is a base cost given by
   * the minimum size of the two segments and additional penalties.
   * recencyPenalty: it is more likely that recent segments will be queried together
   * dataSourcePenalty: if two segments belong to the same data source, they are more likely to be involved
   * in the same queries
   * gapPenalty: it is more likely that segments close together in time will be queried together
   */
  public double computeJointSegmentCosts(DataSegment segment1, DataSegment segment2)
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

    // gap is null if the two segment intervals overlap or if they're adjacent
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

  /*
   * Sample from each server with probability proportional to the number of segments on that server.
   */
  private ServerHolder sampleServer(List<ServerHolder> serverHolderList, int numSegments)
  {
    final int num = rand.nextInt(numSegments);
    int cumulativeSegments = 0;
    int numToStopAt = 0;

    while (cumulativeSegments <= num) {
      cumulativeSegments += serverHolderList.get(numToStopAt).getServer().getSegments().size();
      numToStopAt++;
    }

    return serverHolderList.get(numToStopAt - 1);
  }

  // The balancing application requires us to pick a proposal segment.
  public BalancerSegmentHolder pickSegmentToMove(List<ServerHolder> serverHolders, int numSegments)
  {
    // We want to sample from each server w.p. numSegmentsOnServer / totalSegments
    ServerHolder fromServerHolder = sampleServer(serverHolders, numSegments);

    // and actually pick that segment uniformly at random w.p. 1 / numSegmentsOnServer
    // so that the probability of picking a segment is 1 / totalSegments.
    List<DataSegment> segments = Lists.newArrayList(fromServerHolder.getServer().getSegments().values());

    DataSegment proposalSegment = segments.get(rand.nextInt(segments.size()));
    return new BalancerSegmentHolder(fromServerHolder.getServer(), proposalSegment);
  }

  // The assignment application requires us to supply a proposal segment.
  public ServerHolder findNewSegmentHome(DataSegment proposalSegment, Iterable<ServerHolder> serverHolders)
  {
    final long proposalSegmentSize = proposalSegment.getSize();
    double minCost = Double.MAX_VALUE;
    ServerHolder toServer = null;

    for (ServerHolder server : serverHolders) {
      // Only calculate costs if the server has enough space.
      if (proposalSegmentSize > server.getAvailableSize()) {
        break;
      }

      // The contribution to the total cost of a given server by proposing to move the segment to that server is...
      double cost = 0f;
      // the sum of the costs of other (exclusive of the proposalSegment) segments on the server
      for (DataSegment segment : server.getServer().getSegments().values()) {
        if (!proposalSegment.equals(segment)) {
          cost += computeJointSegmentCosts(proposalSegment, segment);
        }
      }
      // plus the costs of segments that will be loaded
      for (DataSegment segment : server.getPeon().getSegmentsToLoad()) {
        cost += computeJointSegmentCosts(proposalSegment, segment);
      }

      if (cost < minCost) {
        minCost = cost;
        toServer = server;
      }
    }

    return toServer;
  }

}


