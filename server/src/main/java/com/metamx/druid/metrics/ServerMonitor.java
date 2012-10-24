package com.metamx.druid.metrics;

import com.metamx.druid.client.DruidServer;
import com.metamx.druid.coordination.ServerManager;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.metrics.AbstractMonitor;

import java.util.Map;

public class ServerMonitor extends AbstractMonitor
{
  private final DruidServer druidServer;
  private final ServerManager serverManager;

  public ServerMonitor(
      DruidServer druidServer,
      ServerManager serverManager
  )
  {
    this.druidServer = druidServer;
    this.serverManager = serverManager;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    emitter.emit(new ServiceMetricEvent.Builder().build("server/segment/max", druidServer.getMaxSize()));
    for (Map.Entry<String, Long> entry : serverManager.getDataSourceSizes().entrySet()) {
      String dataSource = entry.getKey();
      long used = entry.getValue();
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setUser1(dataSource)
              .build("server/segment/used", used)
      );
    }
    for (Map.Entry<String, Long> entry : serverManager.getDataSourceCounts().entrySet()) {
      String dataSource = entry.getKey();
      long count = entry.getValue();
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setUser1(dataSource)
              .build("server/segment/count", count)
      );
    }

    return true;
  }
}
