/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
