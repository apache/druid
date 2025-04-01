package org.apache.druid.server.coordinator.duty;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.SegmentAction;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class HandleClones implements CoordinatorDuty
{
  private static final Logger log = new Logger(HandleClones.class);

  @Nullable
  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    Map<String, String> cloneServers = params.getCoordinatorDynamicConfig().getCloneServers();
    Map<String, ServerHolder> historicalMap = new HashMap<>(); // TODO: redo
    params.getDruidCluster().getHistoricals().forEach((tier, historicals) -> {
      historicals.forEach(
          historical -> {
            historicalMap.put(historical.getServer().getHost(), historical);
          }
      );
    });

    for (Map.Entry<String, String> entry : cloneServers.entrySet()) {
      String sourceHistoricalName = entry.getKey();
      ServerHolder sourceServer = historicalMap.get(sourceHistoricalName);

      String targetHistorical = entry.getValue();
      ServerHolder targetServer = historicalMap.get(targetHistorical);

      for (DataSegment segment : sourceServer.getServedSegments()) {
        if (!targetServer.getServedSegments().contains(segment)) {
          log.warn("Cloning load of [%s] from [%s] to [%s]", segment, sourceServer, targetServer);
          targetServer.getPeon().loadSegment(segment, SegmentAction.LOAD, null);
        }
      }

      for (DataSegment segment : targetServer.getServedSegments()) {
        if (!sourceServer.getServedSegments().contains(segment)) {
          log.warn("Cloning drop of [%s] from [%s] to [%s]", segment, sourceServer, targetServer);
          targetServer.getPeon().dropSegment(segment, null);
        }
      }
    }
    return params;
  }
}
