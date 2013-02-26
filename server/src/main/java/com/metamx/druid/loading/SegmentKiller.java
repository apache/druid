package com.metamx.druid.loading;

import com.metamx.druid.client.DataSegment;
import org.jets3t.service.ServiceException;

import java.util.Collection;
import java.util.List;

/**
 */
public interface SegmentKiller
{
  public void kill(Collection<DataSegment> segments) throws ServiceException;
}
