package com.metamx.druid.realtime;

import com.metamx.druid.client.DataSegment;

import java.io.File;
import java.io.IOException;

/**
 */
public interface SegmentPusher
{
  public DataSegment push(File file, DataSegment segment) throws IOException;
}
