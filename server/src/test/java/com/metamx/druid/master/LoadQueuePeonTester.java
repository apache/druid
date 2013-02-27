package com.metamx.druid.master;

import com.metamx.druid.client.DataSegment;
import com.metamx.phonebook.PhoneBook;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;

public class LoadQueuePeonTester extends LoadQueuePeon
{
  private ConcurrentSkipListSet<DataSegment> segmentsToLoad;

  public LoadQueuePeonTester(
      PhoneBook yp,
      String basePath,
      ScheduledExecutorService zkWritingExecutor
  )
  {
    super(yp, basePath, zkWritingExecutor);
  }

  @Override
  public void loadSegment(
      DataSegment segment,
      LoadPeonCallback callback
  )
  {
    if(segmentsToLoad == null) segmentsToLoad = new ConcurrentSkipListSet<DataSegment>();
    segmentsToLoad.add(segment);
  }

  public ConcurrentSkipListSet<DataSegment> getSegmentsToLoad()
  {
    if(segmentsToLoad == null) segmentsToLoad = new ConcurrentSkipListSet<DataSegment>();
      return segmentsToLoad;
  }
}
