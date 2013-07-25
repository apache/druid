package com.metamx.druid.master;

import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.common.Pair;
import com.metamx.druid.client.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Comparator;
import java.util.List;

public class CostBalancerStrategy extends BalancerStrategy
{
  private static final int DAY_IN_MILLIS = 1000 * 60 * 60 * 24;
  private static final int SEVEN_DAYS_IN_MILLIS = 7 * DAY_IN_MILLIS;
  private static final int THIRTY_DAYS_IN_MILLIS = 30 * DAY_IN_MILLIS;

  public CostBalancerStrategy(DateTime referenceTimeStamp)
  {
    super(referenceTimeStamp);
  }

  @Override
  public ServerHolder findNewSegmentHome(
      DataSegment proposalSegment, Iterable<ServerHolder> serverHolders
  )
  {
    return computeCosts(proposalSegment, serverHolders).rhs;
  }


  /**
   * For assignment, we want to move to the lowest cost server that isn't already serving the segment.
   *
   * @param proposalSegment A DataSegment that we are proposing to move.
   * @param serverHolders   An iterable of ServerHolders for a particular tier.
   *
   * @return A ServerHolder with the new home for a segment.
   */

  private Pair<Double, ServerHolder> computeCosts(
      final DataSegment proposalSegment,
      final Iterable<ServerHolder> serverHolders
  )
  {

    Pair<Double,ServerHolder> bestServer = Pair.of(Double.POSITIVE_INFINITY,null);
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

      if (cost<bestServer.lhs && !server.isServingSegment(proposalSegment)){
        bestServer= Pair.of(cost,server);
      }
    }

    return bestServer;
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

  public BalancerSegmentHolder pickSegmentToMove(final List<ServerHolder> serverHolders)
  {
    ReservoirSegmentSampler sampler = new ReservoirSegmentSampler();
    return sampler.getRandomBalancerSegmentHolder(serverHolders);
  }


}