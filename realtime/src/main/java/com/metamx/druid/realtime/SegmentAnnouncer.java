package com.metamx.druid.realtime;

import com.metamx.druid.client.DataSegment;

import java.io.IOException;

public interface SegmentAnnouncer
{
  public void announceSegment(DataSegment segment) throws IOException;
  public void unannounceSegment(DataSegment segment) throws IOException;
}
