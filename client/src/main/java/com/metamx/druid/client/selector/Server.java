package com.metamx.druid.client.selector;

/**
 */
public interface Server
{
  public String getScheme();
  public String getHost();
  public int getPort();
}
