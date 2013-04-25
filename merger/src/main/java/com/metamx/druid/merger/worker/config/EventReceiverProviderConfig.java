package com.metamx.druid.merger.worker.config;

import org.skife.config.Config;
import org.skife.config.DefaultNull;

public abstract class EventReceiverProviderConfig
{
  @Config("druid.indexer.eventreceiver.service")
  @DefaultNull
  public abstract String getServiceFormat();

  @Config("druid.host")
  public abstract String getHost();

  @Config("druid.port")
  public abstract int getPort();
}
