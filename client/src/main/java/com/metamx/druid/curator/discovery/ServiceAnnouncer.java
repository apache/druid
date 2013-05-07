package com.metamx.druid.curator.discovery;

/**
 * Announces our ability to serve a particular function. Multiple users may announce the same service, in which
 * case they are treated as interchangeable instances of that service.
 */
public interface ServiceAnnouncer
{
  public void announce(String service) throws Exception;
  public void unannounce(String service) throws Exception;
}
