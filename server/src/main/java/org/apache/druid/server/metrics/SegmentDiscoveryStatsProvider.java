package org.apache.druid.server.metrics;

import org.apache.druid.server.coordinator.stats.RowKey;

import java.util.Map;

public interface SegmentDiscoveryStatsProvider
{
  /**
   * Return the number of successful segment loads for each datasource during the emission period.
   */
  Map<RowKey, Long> getTotalSuccessfulSegmentLoadCount();
}
