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

    for (ServerHolder server : serverHolders) {
      double cost = computeCost(proposalSegment, server, includeCurrentServer);
      if (cost < bestServer.lhs) {
        bestServer = Pair.of(cost, server);
      }
    }
    return bestServer;
  }
}

