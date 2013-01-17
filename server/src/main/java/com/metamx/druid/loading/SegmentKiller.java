package com.metamx.druid.loading;

import com.metamx.druid.client.DataSegment;
import org.jets3t.service.ServiceException;
import org.joda.time.Interval;

import java.util.List;

/**
 */
public interface SegmentKiller
{
  public List<DataSegment> kill(String datasource, Interval interval) throws ServiceException;
}
