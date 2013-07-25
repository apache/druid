package com.metamx.druid.master;

import com.google.common.collect.Lists;
import com.metamx.druid.client.DataSegment;
import org.joda.time.DateTime;

import java.util.List;

public class RandomBalancerStrategy extends BalancerStrategy
{
  private final ReservoirSegmentSampler sampler = new ReservoirSegmentSampler();

  public RandomBalancerStrategy(DateTime referenceTimestamp)
  {
    super(referenceTimestamp);
  }

  @Override
  public ServerHolder findNewSegmentHome(
      DataSegment proposalSegment, Iterable<ServerHolder> serverHolders
  )
  {
     return sampler.getRandomServerHolder(Lists.newArrayList(serverHolders));
  }

  @Override
  public BalancerSegmentHolder pickSegmentToMove(List<ServerHolder> serverHolders)
  {
    return sampler.getRandomBalancerSegmentHolder(serverHolders);
  }
}
