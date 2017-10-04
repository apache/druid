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

public class RandomBalancerStrategy implements BalancerStrategy
{
  private final ReservoirSegmentSampler sampler = new ReservoirSegmentSampler();

  @Override
  public ServerHolder findNewSegmentHomeReplicator(DataSegment proposalSegment, List<ServerHolder> serverHolders)
  {
    if (serverHolders.size() == 1) {
      return null;
    } else {
      ServerHolder holder = serverHolders.get(new Random().nextInt(serverHolders.size()));
      while (holder.isServingSegment(proposalSegment)) {
        holder = serverHolders.get(new Random().nextInt(serverHolders.size()));
      }
      return holder;
    }
  }

  @Override
  public ServerHolder findNewSegmentHomeBalancer(DataSegment proposalSegment, List<ServerHolder> serverHolders)
  {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public BalancerSegmentHolder pickSegmentToMove(List<ServerHolder> serverHolders)
  {
    return sampler.getRandomBalancerSegmentHolder(serverHolders);
  }

  @Override
  public void emitStats(String tier, CoordinatorStats stats, List<ServerHolder> serverHolderList)
  {
  }
}
