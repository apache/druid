package com.metamx.druid.master;

import com.metamx.druid.client.DataSegment;

import java.util.List;

/**
 */
public interface MergerClient
{
  public void runRequest(String dataSource, List<DataSegment> segments);
}
