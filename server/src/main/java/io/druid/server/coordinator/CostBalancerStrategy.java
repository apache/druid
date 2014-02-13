/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.server.coordinator;

import com.metamx.common.Pair;
import com.metamx.emitter.EmittingLogger;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;

public class CostBalancerStrategy extends AbstractCostBalancerStrategy
{
  private static final EmittingLogger log = new EmittingLogger(CostBalancerStrategy.class);

  public CostBalancerStrategy(DateTime referenceTimestamp)
  {
    super(referenceTimestamp);
  }

  protected Pair<Double, ServerHolder> chooseBestServer(
      final DataSegment proposalSegment,
      final Iterable<ServerHolder> serverHolders,
      boolean includeCurrentServer
  )
  {

    Pair<Double, ServerHolder> bestServer = Pair.of(Double.POSITIVE_INFINITY, null);
    final long proposalSegmentSize = proposalSegment.getSize();

    for (ServerHolder server : serverHolders) {
      if (includeCurrentServer || !server.isServingSegment(proposalSegment)) {
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

        if (cost < bestServer.lhs) {
          bestServer = Pair.of(cost, server);
        }
      }
    }

    return bestServer;
  }
}