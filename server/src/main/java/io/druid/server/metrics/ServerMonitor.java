/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.metrics;

import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.metrics.AbstractMonitor;
import io.druid.client.DruidServerConfig;
import io.druid.server.coordination.ServerManager;

import java.util.Map;

public class ServerMonitor extends AbstractMonitor
{
  private final DruidServerConfig serverConfig;
  private final ServerManager serverManager;

  @Inject
  public ServerMonitor(
      DruidServerConfig serverConfig,
      ServerManager serverManager
  )
  {
    this.serverConfig = serverConfig;
    this.serverManager = serverManager;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    emitter.emit(new ServiceMetricEvent.Builder().build("server/segment/max", serverConfig.getMaxSize()));
    long totalUsed = 0;
    long totalCount = 0;

    for (Map.Entry<String, Long> entry : serverManager.getDataSourceSizes().entrySet()) {
      String dataSource = entry.getKey();
      long used = entry.getValue();
      totalUsed += used;

      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder().setUser1(dataSource)
                                                                                 .setUser2(serverConfig.getTier());

      emitter.emit(builder.build("server/segment/used", used));
      final double usedPercent = serverConfig.getMaxSize() == 0 ? 0 : used / (double) serverConfig.getMaxSize();
      emitter.emit(builder.build("server/segment/usedPercent", usedPercent));
    }

    for (Map.Entry<String, Long> entry : serverManager.getDataSourceCounts().entrySet()) {
      String dataSource = entry.getKey();
      long count = entry.getValue();
      totalCount += count;
      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder().setUser1(dataSource)
                                                                                 .setUser2(serverConfig.getTier());

      emitter.emit(builder.build("server/segment/count", count));
    }

    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder().setUser2(serverConfig.getTier());
    emitter.emit(builder.build("server/segment/totalUsed", totalUsed));
    final double totalUsedPercent = serverConfig.getMaxSize() == 0 ? 0 : totalUsed / (double) serverConfig.getMaxSize();
    emitter.emit(builder.build("server/segment/totalUsedPercent", totalUsedPercent));
    emitter.emit(builder.build("server/segment/totalCount", totalCount));

    return true;
  }
}
