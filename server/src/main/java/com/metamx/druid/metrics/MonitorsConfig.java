package com.metamx.druid.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.metamx.metrics.JvmMonitor;
import com.metamx.metrics.Monitor;
import com.metamx.metrics.SysMonitor;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 */
public class MonitorsConfig
{
  @JsonProperty("monitorExclusions")
  @NotNull
  private List<Class<? extends Monitor>> monitors = ImmutableList.<Class<? extends Monitor>>builder()
                                                                 .add(JvmMonitor.class)
                                                                 .add(SysMonitor.class)
                                                                 .build();

  public List<Class<? extends Monitor>> getMonitors()
  {
    return monitors;
  }

  @Override
  public String toString()
  {
    return "MonitorsConfig{" +
           "monitors=" + monitors +
           '}';
  }
}
