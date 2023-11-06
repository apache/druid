package org.apache.druid.segment.metadata;

import org.apache.druid.timeline.SegmentId;

public class SegmentSchemaCache
{
  public SegmentSchema getSchemaForSegment(SegmentId segmentId)
  {
    return null;
  }

  public void updateCache(SegmentSchema segmentSchema)
  {

  }

  public boolean isSchemaCached(SegmentId id)
  {
    return false;
  }
}
