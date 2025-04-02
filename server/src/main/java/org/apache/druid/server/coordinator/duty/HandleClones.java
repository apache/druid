package org.apache.druid.server.coordinator.duty;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.SegmentAction;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class HandleClones implements CoordinatorDuty
{
  private static final Logger log = new Logger(HandleClones.class);

  @Nullable
  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final Map<String, String> cloneServers = params.getCoordinatorDynamicConfig().getCloneServers();
    // TODO: clean up
    final Map<String, ServerHolder> historicalMap = params.getDruidCluster()
                                                          .getHistoricals()
                                                          .values()
                                                          .stream()
                                                          .flatMap(Collection::stream)
                                                          .collect(Collectors.toMap(
                                                              serverHolder -> serverHolder.getServer().getHost(),
                                                              serverHolder -> serverHolder
                                                          ));

    for (Map.Entry<String, String> entry : cloneServers.entrySet()) {
      String sourceHistoricalName = entry.getKey();
      ServerHolder sourceServer = historicalMap.get(sourceHistoricalName);

      String targetHistorical = entry.getValue();
      ServerHolder targetServer = historicalMap.get(targetHistorical);

      for (DataSegment segment : sourceServer.getProjectedSegments().getSegments()) {
        if (!targetServer.getServedSegments().contains(segment)) {
          log.info("Cloning load of [%s] from [%s] to [%s]", segment, sourceServer, targetServer);
          targetServer.getPeon().loadSegment(segment, SegmentAction.LOAD, null);
        }
      }

      for (DataSegment segment : targetServer.getProjectedSegments().getSegments()) {
        if (!sourceServer.getServedSegments().contains(segment)) {
          log.info("Cloning drop of [%s] from [%s] to [%s]", segment, sourceServer, targetServer);
          targetServer.getPeon().dropSegment(segment, null);
        }
      }
    }
    return params;
  }
}
