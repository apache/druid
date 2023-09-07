package org.apache.druid.client;

import org.apache.druid.query.DataSource;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;

import java.util.Map;

/**
 * Segment timeline maintained in the coordinator.
 */
public interface CoordinatorTimeline extends InventoryView
{
  /**
   * Retrieve timeline for a dataSource.
   */
  VersionedIntervalTimeline<String, SegmentLoadInfo> getTimeline(DataSource dataSource);

  /**
   * Server information for all segments in the timeline.
   */
  Map<SegmentId, SegmentLoadInfo> getSegmentLoadInfos();


}
