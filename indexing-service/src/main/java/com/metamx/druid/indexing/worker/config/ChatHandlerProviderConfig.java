package com.metamx.druid.indexing.worker.config;

import org.skife.config.Config;

public abstract class ChatHandlerProviderConfig
{
  @Config("druid.indexer.chathandler.publishDiscovery")
  public boolean isPublishDiscovery()
  {
    return false;
  }

  @Config("druid.host")
  public abstract String getHost();

  @Config("druid.port")
  public abstract int getPort();
}
