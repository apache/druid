/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.metrics;

import com.google.inject.Inject;
import com.metamx.druid.client.DruidServerConfig;
import com.metamx.druid.coordination.ServerManager;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.metrics.AbstractMonitor;

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
    for (Map.Entry<String, Long> entry : serverManager.getDataSourceSizes().entrySet()) {
      String dataSource = entry.getKey();
      long used = entry.getValue();
      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder().setUser1(dataSource)
                                                                                 .setUser2(serverConfig.getTier());

      emitter.emit(builder.build("server/segment/used", used));
      emitter.emit(builder.build("server/segment/usedPercent", used / (double) serverConfig.getMaxSize()));
    }

    for (Map.Entry<String, Long> entry : serverManager.getDataSourceCounts().entrySet()) {
      String dataSource = entry.getKey();
      long count = entry.getValue();
      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder().setUser1(dataSource)
                                                                                 .setUser2(serverConfig.getTier());

      emitter.emit(builder.build("server/segment/count", count));
    }

    return true;
  }
}
