package org.apache.druid.server.coordinator.duty;

import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;

import javax.annotation.Nullable;

/**
 *
 */
public class KillUnreferencedSegmentSchemas implements CoordinatorDuty
{

  @Nullable
  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    return null;
  }
}
