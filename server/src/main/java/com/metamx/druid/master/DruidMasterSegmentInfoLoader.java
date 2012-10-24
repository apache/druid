package com.metamx.druid.master;

import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;

import java.util.Set;

public class DruidMasterSegmentInfoLoader implements DruidMasterHelper
{
  private final DruidMaster master;

  private static final Logger log = new Logger(DruidMasterSegmentInfoLoader.class);

  public DruidMasterSegmentInfoLoader(DruidMaster master)
  {
    this.master = master;
  }

  @Override
  public DruidMasterRuntimeParams run(DruidMasterRuntimeParams params)
  {
    // Display info about all available segments
    final Set<DataSegment> availableSegments = master.getAvailableDataSegments();
    if (log.isDebugEnabled()) {
      log.debug("Available DataSegments");
      for (DataSegment dataSegment : availableSegments) {
        log.debug("  %s", dataSegment);
      }
    }

    return params.buildFromExisting()
                 .withAvailableSegments(availableSegments)
                 .build();
  }
}
