package com.metamx.druid.curator.discovery;

/**
 * Does nothing.
 */
public class NoopServiceAnnouncer implements ServiceAnnouncer
{
  @Override
  public void unannounce(String service)
  {

  }

  @Override
  public void announce(String service)
  {

  }
}
