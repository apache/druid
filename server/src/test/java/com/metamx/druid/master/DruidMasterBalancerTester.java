package com.metamx.druid.master;

import com.google.common.collect.Maps;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;

import java.util.Map;

public class DruidMasterBalancerTester extends DruidMasterBalancer
{
  private final Map<String,Integer> serverMap = Maps.newHashMap();
  public DruidMasterBalancerTester(DruidMaster master)
  {
    super(master);
  }

  @Override
  protected void moveSegment(
      final BalancerSegmentHolder segment,
      final DruidServer toServer,
      final DruidMasterRuntimeParams params
  )
  {
    final String toServerName = toServer.getName();
    final LoadQueuePeon toPeon = params.getLoadManagementPeons().get(toServerName);

    final String fromServerName = segment.getFromServer().getName();
    final DataSegment segmentToMove = segment.getSegment();
    final String segmentName = segmentToMove.getIdentifier();

    if (!toPeon.getSegmentsToLoad().contains(segmentToMove) &&
        !currentlyMovingSegments.get("normal").containsKey(segmentName) &&
        !toServer.getSegments().containsKey(segmentName) &&
        new ServerHolder(toServer, toPeon).getAvailableSize() > segmentToMove.getSize()) {
      log.info(
          "Moving [%s] from [%s] to [%s]",
          segmentName,
          fromServerName,
          toServerName
      );
      try {
        final LoadQueuePeon loadPeon = params.getLoadManagementPeons().get(toServerName);

        loadPeon.loadSegment(segment.getSegment(), new LoadPeonCallback()
        {
          @Override
          protected void execute()
          {
          }
        });

        if (serverMap.get(fromServerName)==null)
        {
          serverMap.put(fromServerName,segment.getFromServer().getSegments().size());
        }
        if (serverMap.get(toServerName)==null)
        {
          serverMap.put(toServerName,0);
        }
//        serverMap.put(fromServerName,serverMap.get(fromServerName)-1);
        serverMap.put(toServerName, serverMap.get(toServerName)+1);
      }
      catch (Exception e) {
        log.info(e, String.format("[%s] : Moving exception", segmentName));
      }
    } else {
      currentlyMovingSegments.get("normal").remove(segment);
    }
  }

  public Map<String,Integer> getServerMap()
  {
    return this.serverMap;
  }

  public boolean isBalanced(int min, int numServers)
  {
    if (serverMap.size()==numServers)
    {
      for (int numSegments: serverMap.values())
      {
        if (numSegments<min)
        {
          return false;
        }
      }
      return true;
    }
    return false;
  }
}
