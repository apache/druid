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
import com.metamx.druid.client.DruidDataSource;
import com.metamx.druid.client.DruidServer;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * The BalancerCostAnalyzer
 */
public class BalancerCostAnalyzer
{
  private static final Logger log = new Logger(BalancerCostAnalyzer.class);
  private static final int MAX_SEGMENTS_TO_MOVE = 5;
  private static final int DAY_IN_MILLIS = 1000 * 60 * 60 * 24;
  private float initialTotalCost;
  private float totalCostChange;

  private List<ServerHolder> serverHolderList;
  private Random rand;

  public BalancerCostAnalyzer(){
    rand = new Random(0);
    totalCostChange = 0f;
  }

  public void init(List<ServerHolder> serverHolderList){
    this.initialTotalCost = calculateInitialTotalCost(serverHolderList);
    this.serverHolderList = serverHolderList;
  }

  public float getInitialTotalCost() {
    return initialTotalCost;
  }

  public float getTotalCostChange() {
    return totalCostChange;
  }

  private float calculateInitialTotalCost(List<ServerHolder> serverHolderList){
    float cost = 0;
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

  public float computeJointSegmentCosts(DataSegment segment1, DataSegment segment2){
    float cost = 0f;
    Interval gap = segment1.getInterval().gap(segment2.getInterval());

    // gap is null if the two segment intervals overlap or if they're adjacent
    if (gap == null) {
      cost += 1f;
    } else {
      long gapMillis = gap.toDurationMillis();
      if (gapMillis < DAY_IN_MILLIS) cost += 1f;
    }

    if(segment1.getDataSource().equals(segment2.getDataSource())) cost += 1f;

    return cost;
  }

  public Set<BalancerSegmentHolder2> findSegmentsToMove()
  {
    Set<BalancerSegmentHolder2> segmentHoldersToMove = Sets.newHashSet();
    Set<DataSegment> movingSegments = Sets.newHashSet();

    int counter = 0;
    float currCost = 0f;

    while (segmentHoldersToMove.size() < MAX_SEGMENTS_TO_MOVE && counter < 3 * MAX_SEGMENTS_TO_MOVE) {
      counter++;
      ServerHolder fromServerHolder = serverHolderList.get(rand.nextInt(serverHolderList.size()));
      List<DataSegment> segments = Lists.newArrayList(fromServerHolder.getServer().getSegments().values());
      if (segments.size() == 0) continue;
      DataSegment proposalSegment = segments.get(rand.nextInt(segments.size()));
      if (movingSegments.contains(proposalSegment)) continue;

      // Just need a regular priority queue for the min. element.
      MinMaxPriorityQueue<Pair<Float, ServerHolder>> pQueue = MinMaxPriorityQueue.orderedBy(
          new Comparator<Pair<Float, ServerHolder>>()
          {
            @Override
            public int compare(
                Pair<Float, ServerHolder> o,
                Pair<Float, ServerHolder> o1
            )
            {
              return Float.compare(o.lhs, o1.lhs);
            }
          }
      ).create();

      for (ServerHolder server : serverHolderList) {
        float cost = 0f;
        for (DataSegment segment : server.getServer().getSegments().values()) {
          cost += computeJointSegmentCosts(proposalSegment, segment);
        }

        // self cost
        if (!server.getServer().equals(fromServerHolder.getServer())) cost += computeJointSegmentCosts(proposalSegment, proposalSegment);

        // Take into account costs of segments that will be moved.
        Iterator it = segmentHoldersToMove.iterator();
        while (it.hasNext()) {
          BalancerSegmentHolder2 segmentToMove = (BalancerSegmentHolder2) it.next();
          if (server.getServer().equals(segmentToMove.getToServer())) {
            cost += computeJointSegmentCosts(proposalSegment, segmentToMove.getSegment());
          }
          if (server.getServer().equals(segmentToMove.getFromServer())) {
            cost -= computeJointSegmentCosts(proposalSegment, segmentToMove.getSegment());
          }
        }

        if (server.getServer().equals(fromServerHolder.getServer())){
          currCost = cost;
        }

        pQueue.add(Pair.of(cost, server));
      }

      Pair<Float, ServerHolder> minPair = pQueue.peekFirst();
      if (!minPair.rhs.equals(fromServerHolder)) {
        movingSegments.add(proposalSegment);
        segmentHoldersToMove.add(
            new BalancerSegmentHolder2(
                fromServerHolder.getServer(),
                minPair.rhs.getServer(),
                proposalSegment
            )
        );
        totalCostChange += currCost - minPair.lhs;
      }

    }

    return segmentHoldersToMove;
  }
}


