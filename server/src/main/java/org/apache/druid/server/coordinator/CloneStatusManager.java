package org.apache.druid.server.coordinator;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.loading.SegmentAction;
import org.apache.druid.timeline.DataSegment;

import java.util.HashMap;
import java.util.Map;

public class CloneStatusManager
{
  private static final Logger log = new Logger(CloneStatusManager.class);
  @GuardedBy("this")
  private final Map<String, CloneDetails> cloneStatusMap;

  public CloneStatusManager() {
    cloneStatusMap = new HashMap<>();
  }

  public Map<String, CloneDetails> getCloneStatusMap()
  {
    synchronized (this) {
      return cloneStatusMap;
    }
  }

  public void updateStats(Map<String, ServerHolder> historicalMap, Map<String, String> cloneServers)
  {
    synchronized (this) {
      cloneStatusMap.clear();

      for (Map.Entry<String, String> entry : cloneServers.entrySet()) {
        String targetServerName = entry.getKey();
        ServerHolder targetServer = historicalMap.get(entry.getKey());
        String sourceServerName = entry.getValue();
        ServerHolder sourceServer = historicalMap.get(entry.getValue());

        int segmentsLeft = 0;
        long bytesLeft = 0;

        if (targetServer == null) {
          cloneStatusMap.put(targetServerName, new CloneDetails(sourceServerName, 0, 0));
          continue;
        }

        for (Map.Entry<DataSegment, SegmentAction> queuedSegment: targetServer.getQueuedSegments().entrySet()) {
          if (queuedSegment.getValue().isLoad()) {
            segmentsLeft++;
            bytesLeft += queuedSegment.getKey().getSize();
          }
        }

        cloneStatusMap.put(targetServerName, new CloneDetails(sourceServerName, segmentsLeft, bytesLeft));
      }
    }
  }
}

