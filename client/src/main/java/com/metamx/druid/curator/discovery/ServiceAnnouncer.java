package com.metamx.druid.curator.discovery;

import com.metamx.druid.initialization.DruidNode;

/**
 * Announces our ability to serve a particular function. Multiple users may announce the same service, in which
 * case they are treated as interchangeable instances of that service.
 */
public interface ServiceAnnouncer
{
  public void announce(DruidNode node);

  public void unannounce(DruidNode node);
}
