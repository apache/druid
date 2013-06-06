package com.metamx.druid.indexing.worker.config;

import org.skife.config.Config;
import org.skife.config.DefaultNull;

public abstract class ChatHandlerProviderConfig
{
  @Config("druid.indexer.chathandler.service")
  @DefaultNull
  public abstract String getServiceFormat();

  @Config("druid.host")
  public abstract String getHost();

  @Config("druid.port")
  public abstract int getPort();
}
