package io.druid.server.bridge;

import io.druid.client.DruidServer;
import io.druid.server.initialization.ZkPathsConfig;
import org.joda.time.Duration;
import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
// TODO: make sure that this uses sub cluster zk paths
public abstract class DruidClusterBridgeConfig extends ZkPathsConfig
{
  @Config("druid.server.tier")
  @Default(DruidServer.DEFAULT_TIER)
  public abstract String getTier();

  @Config("druid.bridge.startDelay")
  @Default("PT300s")
  public abstract Duration getStartDelay();

  @Config("druid.bridge.period")
  @Default("PT60s")
  public abstract Duration getPeriod();
}
