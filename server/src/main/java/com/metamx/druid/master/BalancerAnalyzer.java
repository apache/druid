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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidDataSource;
import com.metamx.druid.client.DruidServer;

/**
 * The BalancerAnalyzer keeps the state of the highest and lowest percent used servers. It will update
 * these states and perform lookaheads to make sure the updated states result in a more balanced cluster.
 */
public class BalancerAnalyzer
{
  private static final Logger log = new Logger(BalancerAnalyzer.class);
  private static final int PERCENT_THRESHOLD = 3;
  private static final int MAX_SEGMENTS_TO_MOVE = 5;

  private volatile Long highestSizeUsed;
  private volatile double highestPercentUsed;
  private volatile Long highestPercentUsedServerMaxSize;
  private volatile Long lowestSizeUsed;
  private volatile double lowestPercentUsed;
  private volatile Long lowestPercentUsedServerMaxSize;

  public BalancerAnalyzer()
  {
    this.highestSizeUsed = 0L;
    this.highestPercentUsed = 0;
    this.highestPercentUsedServerMaxSize = 0L;
    this.lowestSizeUsed = 0L;
    this.lowestPercentUsed = 0;
    this.lowestPercentUsedServerMaxSize = 0L;
  }

  public void init(ServerHolder highestPercentUsedServer, ServerHolder lowestPercentUsedServer)
  {
    highestSizeUsed = highestPercentUsedServer.getSizeUsed();
    highestPercentUsed = highestPercentUsedServer.getPercentUsed();
    highestPercentUsedServerMaxSize = highestPercentUsedServer.getMaxSize();
    lowestSizeUsed = lowestPercentUsedServer.getSizeUsed();
    lowestPercentUsed = lowestPercentUsedServer.getPercentUsed();
    lowestPercentUsedServerMaxSize = lowestPercentUsedServer.getMaxSize();
  }

  public void update(long newHighestSizeUsed, long newLowestSizedUsed)
  {
    highestSizeUsed = newHighestSizeUsed;
    highestPercentUsed = highestSizeUsed.doubleValue() / highestPercentUsedServerMaxSize;
    lowestSizeUsed = newLowestSizedUsed;
    lowestPercentUsed = lowestSizeUsed.doubleValue() / lowestPercentUsedServerMaxSize;
  }

  public double getPercentDiff()
  {
    return Math.abs(
        100 * ((highestPercentUsed - lowestPercentUsed)
               / ((highestPercentUsed + lowestPercentUsed) / 2))
    );
  }

  public double getLookaheadPercentDiff(Long newHighestSizeUsed, Long newLowestSizedUsed)
  {
    double newHighestPercentUsed = 100 * (newHighestSizeUsed.doubleValue() / highestPercentUsedServerMaxSize);
    double newLowestPercentUsed = 100 * (newLowestSizedUsed.doubleValue() / lowestPercentUsedServerMaxSize);

    return Math.abs(
        100 * ((newHighestPercentUsed - newLowestPercentUsed)
               / ((newHighestPercentUsed + newLowestPercentUsed) / 2))
    );
  }

  public Set<BalancerSegmentHolder> findSegmentsToMove(DruidServer server)
  {
    Set<BalancerSegmentHolder> segmentsToMove = Sets.newHashSet();
    double currPercentDiff = getPercentDiff();

    if (currPercentDiff < PERCENT_THRESHOLD) {
      log.info("Cluster usage is balanced.");
      return segmentsToMove;
    }

    List<DruidDataSource> dataSources = Lists.newArrayList(server.getDataSources());
    Collections.shuffle(dataSources);

    for (DruidDataSource dataSource : dataSources) {
      List<DataSegment> segments = Lists.newArrayList(dataSource.getSegments());
      Collections.shuffle(segments);

      for (DataSegment segment : segments) {
        if (segmentsToMove.size() >= MAX_SEGMENTS_TO_MOVE) {
          return segmentsToMove;
        }

        if (getLookaheadPercentDiff(highestSizeUsed - segment.getSize(), lowestSizeUsed + segment.getSize())
            < currPercentDiff) {
          segmentsToMove.add(new BalancerSegmentHolder(server, segment));
          update(highestSizeUsed - segment.getSize(), lowestSizeUsed + segment.getSize());
        }
      }
    }

    return segmentsToMove;
  }
}


