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
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
import com.metamx.common.Pair;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import org.joda.time.Interval;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;


/**
 * The BalancerCostAnalyzer will compute the total initial cost of the cluster, with costs defined in
 * computeJointSegmentCosts.  It will then propose to move randomly chosen segments from their respective
 * initial servers to other servers, chosen greedily to minimize the cost of the cluster.
 */
public class BalancerCostAnalyzer
{
  private static final Logger log = new Logger(BalancerCostAnalyzer.class);
  private static final int MAX_SEGMENTS_TO_MOVE = 5;
  private static final int DAY_IN_MILLIS = 1000 * 60 * 60 * 24;

  private List<ServerHolder> serverHolderList;
  private Random rand;

  private double initialTotalCost;
  private double totalCostChange;

  public BalancerCostAnalyzer()
  {
    rand = new Random(0);
    totalCostChange = 0;
  }

  public void init(List<ServerHolder> serverHolderList)
  {
    this.initialTotalCost = calculateInitialTotalCost(serverHolderList);
    this.serverHolderList = serverHolderList;
  }

  public double getInitialTotalCost()
  {
    return initialTotalCost;
  }

  public double getTotalCostChange()
  {
    return totalCostChange;
  }

  private double calculateInitialTotalCost(List<ServerHolder> serverHolderList)
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

  public double computeJointSegmentCosts(DataSegment segment1, DataSegment segment2)
  {
    double cost = 0;
    Interval gap = segment1.getInterval().gap(segment2.getInterval());

    // gap is null if the two segment intervals overlap or if they're adjacent
    if (gap == null) {
      cost += 1f;
    } else {
      long gapMillis = gap.toDurationMillis();
      if (gapMillis < DAY_IN_MILLIS) {
        cost += 1f;
      }
    }

    if (segment1.getDataSource().equals(segment2.getDataSource())) {
      cost += 1f;
    }

    return cost;
  }

  public class NullServerHolder extends ServerHolder
  {
    public NullServerHolder()
    {
      super(null, null);
    }

    @Override
    public boolean equals(Object o)
    {
      return false;
    }
  }

  public class BalancerCostAnalyzerHelper
  {
    private MinMaxPriorityQueue<Pair<Double, ServerHolder>> costsServerHolderPairs;
    private List<ServerHolder> serverHolderList;
    private DataSegment proposalSegment;
    private ServerHolder fromServerHolder;
    private Set<BalancerSegmentHolder> segmentHoldersToMove;
    private double currCost;

    public MinMaxPriorityQueue<Pair<Double, ServerHolder>> getCostsServerHolderPairs()
    {
      return costsServerHolderPairs;
    }

    public List<ServerHolder> getServerHolderList()
    {
      return serverHolderList;
    }

    public DataSegment getProposalSegment()
    {
      return proposalSegment;
    }

    public ServerHolder getFromServerHolder()
    {
      return fromServerHolder;
    }

    public Set<BalancerSegmentHolder> getSegmentHoldersToMove()
    {
      return segmentHoldersToMove;
    }

    public double getCurrCost()
    {
      return currCost;
    }

    public BalancerCostAnalyzerHelper(
        List<ServerHolder> serverHolderList,
        DataSegment proposalSegment
    )
    {
      this(serverHolderList, proposalSegment, new NullServerHolder(), Sets.<BalancerSegmentHolder>newHashSet());
    }

    public BalancerCostAnalyzerHelper(
        List<ServerHolder> serverHolderList,
        DataSegment proposalSegment,
        ServerHolder fromServerHolder,
        Set<BalancerSegmentHolder> segmentHoldersToMove
    )
    {
      // Just need a regular priority queue for the min. element.
      this.costsServerHolderPairs = MinMaxPriorityQueue.orderedBy(
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
      this.serverHolderList = serverHolderList;
      this.proposalSegment = proposalSegment;
      this.fromServerHolder = fromServerHolder;
      this.segmentHoldersToMove = segmentHoldersToMove;
      this.currCost = 0;
    }

    public void computeAllCosts()
    {
      for (ServerHolder server : serverHolderList) {
        double cost = 0f;
        for (DataSegment segment : server.getServer().getSegments().values()) {
          cost += computeJointSegmentCosts(proposalSegment, segment);
        }

        // self cost
        if (!server.getServer().equals(fromServerHolder.getServer())) {
          cost += computeJointSegmentCosts(proposalSegment, proposalSegment);
        }

        // Take into account costs of segments that will be moved.
        Iterator it = segmentHoldersToMove.iterator();
        while (it.hasNext()) {
          BalancerSegmentHolder segmentToMove = (BalancerSegmentHolder) it.next();
          if (server.getServer().equals(segmentToMove.getToServer())) {
            cost += computeJointSegmentCosts(proposalSegment, segmentToMove.getSegment());
          }
          if (server.getServer().equals(segmentToMove.getFromServer())) {
            cost -= computeJointSegmentCosts(proposalSegment, segmentToMove.getSegment());
          }
        }

        if (server.getServer().equals(fromServerHolder.getServer())) {
          currCost = cost;
        }

        if (proposalSegment.getSize() < server.getAvailableSize()) {
          costsServerHolderPairs.add(Pair.of(cost, server));
        }

      }
    }

  }

  public Set<BalancerSegmentHolder> findSegmentsToMove()
  {
    Set<BalancerSegmentHolder> segmentHoldersToMove = Sets.newHashSet();
    Set<DataSegment> movingSegments = Sets.newHashSet();

    int counter = 0;

    while (segmentHoldersToMove.size() < MAX_SEGMENTS_TO_MOVE && counter < 3 * MAX_SEGMENTS_TO_MOVE) {
      counter++;
      ServerHolder fromServerHolder = serverHolderList.get(rand.nextInt(serverHolderList.size()));
      List<DataSegment> segments = Lists.newArrayList(fromServerHolder.getServer().getSegments().values());
      if (segments.size() == 0) {
        continue;
      }
      DataSegment proposalSegment = segments.get(rand.nextInt(segments.size()));
      if (movingSegments.contains(proposalSegment)) {
        continue;
      }

      BalancerCostAnalyzerHelper helper = new BalancerCostAnalyzerHelper(
          serverHolderList,
          proposalSegment,
          fromServerHolder,
          segmentHoldersToMove
      );
      helper.computeAllCosts();

      Pair<Double, ServerHolder> minPair = helper.getCostsServerHolderPairs().pollFirst();

      if (minPair.rhs != null && !minPair.rhs.equals(fromServerHolder)) {
        movingSegments.add(proposalSegment);
        segmentHoldersToMove.add(
            new BalancerSegmentHolder(
                fromServerHolder.getServer(),
                minPair.rhs.getServer(),
                proposalSegment
            )
        );
        totalCostChange += helper.getCurrCost() - minPair.lhs;
      }
    }

    return segmentHoldersToMove;
  }
}


