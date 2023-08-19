package org.apache.druid.server.metrics;

import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.AbstractMonitor;

public class SubqueryCountStatsMonitor extends AbstractMonitor
{
  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    return true;
  }
}
