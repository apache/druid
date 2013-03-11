package com.metamx.druid.realtime;

import com.metamx.druid.client.DataSegment;

import java.io.IOException;

public interface SegmentPublisher
{
  public void publishSegment(DataSegment segment) throws IOException;
}
