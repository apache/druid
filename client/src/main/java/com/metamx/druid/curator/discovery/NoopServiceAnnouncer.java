package com.metamx.druid.curator.discovery;

import com.metamx.druid.initialization.DruidNode;

/**
 * Does nothing.
 */
public class NoopServiceAnnouncer implements ServiceAnnouncer
{
  @Override
  public void announce(DruidNode node)
  {

  }

  @Override
  public void unannounce(DruidNode node)
  {

  }
}
