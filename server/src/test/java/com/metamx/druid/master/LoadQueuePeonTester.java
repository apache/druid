package com.metamx.druid.master;

import com.metamx.druid.client.DataSegment;

import java.util.concurrent.ConcurrentSkipListSet;

public class LoadQueuePeonTester extends LoadQueuePeon
{
  private final ConcurrentSkipListSet<DataSegment> segmentsToLoad = new ConcurrentSkipListSet<DataSegment>();

  public LoadQueuePeonTester()
  {
    super(null, null, null, null, null);
  }

  @Override
  public void loadSegment(
      DataSegment segment,
      LoadPeonCallback callback
  )
  {
    segmentsToLoad.add(segment);
  }

  public ConcurrentSkipListSet<DataSegment> getSegmentsToLoad()
  {
    return segmentsToLoad;
  }
}
