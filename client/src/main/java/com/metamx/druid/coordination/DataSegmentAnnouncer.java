package com.metamx.druid.coordination;

import com.metamx.druid.client.DataSegment;

import java.io.IOException;

public interface DataSegmentAnnouncer
{
  public void announceSegment(DataSegment segment) throws IOException;
  public void unannounceSegment(DataSegment segment) throws IOException;
}
