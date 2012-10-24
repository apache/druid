package com.metamx.druid.coordination;

import com.metamx.druid.client.DataSegment;

/**
 */
public interface DataSegmentChangeHandler
{
  public void addSegment(DataSegment segment);
  public void removeSegment(DataSegment segment);
}
