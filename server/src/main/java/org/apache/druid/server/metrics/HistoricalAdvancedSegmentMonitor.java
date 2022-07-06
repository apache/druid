/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.apache.druid.server.metrics;


import com.google.inject.Inject;
import org.apache.druid.client.DruidServerConfig;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.coordination.SegmentLoadDropHandler;

import java.util.Map;

public class HistoricalAdvancedSegmentMonitor extends AbstractMonitor
{
  private final DruidServerConfig serverConfig;
  private final SegmentLoadDropHandler segmentLoadDropMgr;

  @Inject
  public HistoricalAdvancedSegmentMonitor(DruidServerConfig serverConfig, SegmentLoadDropHandler segmentLoadDropMgr)
  {
    this.serverConfig = serverConfig;
    this.segmentLoadDropMgr = segmentLoadDropMgr;
  }


  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    for (Map.Entry<String, Long> entry : segmentLoadDropMgr.getAverageNumOfRowsPerSegmentForDatasource().entrySet()) {
      String dataSource = entry.getKey();
      long averageSize = entry.getValue();
      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
          .setDimension(DruidMetrics.DATASOURCE, dataSource)
          .setDimension("tier", serverConfig.getTier())
          .setDimension("priority", String.valueOf(serverConfig.getPriority()));
      emitter.emit(builder.build("segment/rowCount/average", averageSize));
    }

    for (Map.Entry<String, SegmentRowCountDistribution> entry : segmentLoadDropMgr.getRowCountDistributionPerDatasource()
                                                                                  .entrySet()) {
      String dataSource = entry.getKey();
      SegmentRowCountDistribution rowCountBucket = entry.getValue();

      rowCountBucket.forEachDimension((final String bucketDimension, final int count) -> {
        final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
            .setDimension(DruidMetrics.DATASOURCE, dataSource)
            .setDimension("tier", serverConfig.getTier())
            .setDimension("priority", String.valueOf(serverConfig.getPriority()));
        builder.setDimension("range", bucketDimension);
        ServiceEventBuilder<ServiceMetricEvent> output = builder.build("segment/rowCount/range/count", count);
        emitter.emit(output);
      });
    }

    return true;
  }
}
