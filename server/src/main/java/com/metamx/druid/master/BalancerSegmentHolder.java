package com.metamx.druid.master;

import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;

/**
 */
public class BalancerSegmentHolder
{
  private final DruidServer server;
  private final DataSegment segment;

  private volatile int lifetime = 15;

  public BalancerSegmentHolder(
      DruidServer server,
      DataSegment segment
  )
  {
    this.server = server;
    this.segment = segment;
  }

  public DruidServer getServer()
  {
    return server;
  }

  public DataSegment getSegment()
  {
    return segment;
  }

  public int getLifetime()
  {
    return lifetime;
  }

  public void reduceLifetime()
  {
    lifetime--;
  }
}
