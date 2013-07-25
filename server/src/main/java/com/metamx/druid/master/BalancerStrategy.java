package com.metamx.druid.master;

import com.metamx.druid.client.DataSegment;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Random;

public abstract class BalancerStrategy
{
  protected final DateTime referenceTimestamp;
  protected final Random rand;
  public BalancerStrategy(DateTime referenceTimestamp){
    this.referenceTimestamp=referenceTimestamp;
    rand=new Random(0);
  }
  public abstract ServerHolder findNewSegmentHome(final DataSegment proposalSegment,final Iterable<ServerHolder> serverHolders);
  public abstract BalancerSegmentHolder pickSegmentToMove(final List<ServerHolder> serverHolders);
}
