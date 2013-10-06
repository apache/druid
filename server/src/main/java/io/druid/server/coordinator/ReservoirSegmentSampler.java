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

import io.druid.timeline.DataSegment;

import java.util.List;
import java.util.Random;

public class ReservoirSegmentSampler
{

  public BalancerSegmentHolder getRandomBalancerSegmentHolder(final List<ServerHolder> serverHolders)
  {
    final Random rand = new Random();
    ServerHolder fromServerHolder = null;
    DataSegment proposalSegment = null;
    int numSoFar = 0;

    for (ServerHolder server : serverHolders) {
      for (DataSegment segment : server.getServer().getSegments().values()) {
        int randNum = rand.nextInt(numSoFar + 1);
        // w.p. 1 / (numSoFar+1), swap out the server and segment
        if (randNum == numSoFar) {
          fromServerHolder = server;
          proposalSegment = segment;
        }
        numSoFar++;
      }
    }
    if (fromServerHolder != null) {
      return new BalancerSegmentHolder(fromServerHolder.getServer(), proposalSegment);
    } else {
      return null;
    }
  }
}
