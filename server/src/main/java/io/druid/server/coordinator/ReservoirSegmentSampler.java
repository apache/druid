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
