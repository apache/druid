package com.metamx.druid.master;

import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;

public class DruidMasterBalancerTester extends DruidMasterBalancer
{
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

        currentlyMovingSegments.get("normal").put(segmentName, segment);
      }
      catch (Exception e) {
        log.info(e, String.format("[%s] : Moving exception", segmentName));
      }
    } else {
      currentlyMovingSegments.get("normal").remove(segment);
    }
  }
}
