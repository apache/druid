package org.apache.druid.client;

import org.apache.druid.query.DataSource;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;

import java.util.Map;

public interface CoordinatorInventoryView extends InventoryView
{
  VersionedIntervalTimeline<String, SegmentLoadInfo> getTimeline(DataSource dataSource);
  Map<SegmentId, SegmentLoadInfo> getSegmentLoadInfos();
}
