package io.druid.server.bridge;

import io.druid.curator.CuratorConfig;
import org.skife.config.Config;

/**
 */
public abstract class BridgeCuratorConfig extends CuratorConfig
{
  @Config("druid.bridge.zk.service.host")
  public abstract String getParentZkHosts();
}
