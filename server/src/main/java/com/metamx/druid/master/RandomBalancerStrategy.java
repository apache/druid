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

import com.metamx.druid.client.DataSegment;

import java.util.List;
import java.util.Random;

public class RandomBalancerStrategy implements BalancerStrategy
{
  private final ReservoirSegmentSampler sampler = new ReservoirSegmentSampler();

  @Override
  public ServerHolder findNewSegmentHomeReplicator(
      DataSegment proposalSegment, List<ServerHolder> serverHolders
  )
  {
    if (serverHolders.size()==1)
    {
      return null;
    }
    else
    {
      ServerHolder holder = serverHolders.get(new Random().nextInt(serverHolders.size()));
      while (holder.isServingSegment(proposalSegment))
      {
        holder = serverHolders.get(new Random().nextInt(serverHolders.size()));
      }
      return holder;
    }
  }

  @Override
  public ServerHolder findNewSegmentHomeBalancer(
      DataSegment proposalSegment, List<ServerHolder> serverHolders
  )
  {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public BalancerSegmentHolder pickSegmentToMove(List<ServerHolder> serverHolders)
  {
    return sampler.getRandomBalancerSegmentHolder(serverHolders);
  }

  @Override
  public void emitStats(
      String tier, MasterStats stats, List<ServerHolder> serverHolderList
  )
  {
  }
}
