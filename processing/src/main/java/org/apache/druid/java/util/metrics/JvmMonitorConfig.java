package org.apache.druid.java.util.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JvmMonitorConfig
{
  // The JVMMonitor is really more like a JVM memory + GC monitor
  public static final String PREFIX = "druid.monitoring.jvm";

  @JsonProperty("duration")
  private boolean duration = false;

  public boolean recordDuration() {
    return duration;
  }

  public JvmMonitorConfig(@JsonProperty("duration") final boolean duration)
  {
    this.duration = duration;
  }
}
