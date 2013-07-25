package com.metamx.druid.master;

import com.metamx.druid.client.DataSegment;

import java.util.List;
import java.util.Random;

public class ReservoirSegmentSampler
{
  public BalancerSegmentHolder getRandomBalancerSegmentHolder(final List<ServerHolder> serverHolders)
  {
    Random rand = new Random(0);
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
    return new BalancerSegmentHolder(fromServerHolder.getServer(), proposalSegment);
  }


  public ServerHolder getRandomServerHolder(final List<ServerHolder> serverHolders)
  {
    ServerHolder fromServerHolder = null;
    Random rand = new Random(0);
    int numSoFar = 0;
    for (ServerHolder server : serverHolders) {
      int randNum = rand.nextInt(numSoFar + 1);

      if(randNum==numSoFar) {
        fromServerHolder=server;
      }
    numSoFar++;
    }
    return fromServerHolder;
  }
}
