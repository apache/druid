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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.server.coordination.SegmentLoadDropHandler;

import java.util.Map;

/**
 * An experimental monitor used to keep track of segment stats. Can only be used on a historical and cannot be used with lazy loading.
 *
 * It keeps track of the average number of rows in a segment and the distribution of segments according to rowCount.
 */
public class SegmentStatsMonitor extends AbstractMonitor
{
  private final DruidServerConfig serverConfig;
  private final SegmentLoadDropHandler segmentLoadDropHandler;

  private static final Logger log = new Logger(SegmentStatsMonitor.class);

  /**
   * Constructor for this monitor. Will throw IllegalStateException if lazy load on start is set to true.
   *
   * @param serverConfig
   * @param segmentLoadDropHandler
   * @param segmentLoaderConfig
   */
  @Inject
  public SegmentStatsMonitor(
      DruidServerConfig serverConfig,
      SegmentLoadDropHandler segmentLoadDropHandler,
      SegmentLoaderConfig segmentLoaderConfig
  )
  {
    if (segmentLoaderConfig.isLazyLoadOnStart()) {
      // log message ensures there is an error displayed at startup if this fails as the exception isn't logged.
      log.error("Monitor doesn't support working with lazy loading on start");
      // throw this exception it kill the process at startup
      throw new IllegalStateException("Monitor doesn't support working with lazy loading on start");
    }
    this.serverConfig = serverConfig;
    this.segmentLoadDropHandler = segmentLoadDropHandler;

  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    for (Map.Entry<String, Long> entry : segmentLoadDropHandler.getAverageNumOfRowsPerSegmentForDatasource().entrySet()) {
      String dataSource = entry.getKey();
      long averageSize = entry.getValue();
      final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
          .setDimension(DruidMetrics.DATASOURCE, dataSource)
          .setDimension("tier", serverConfig.getTier())
          .setDimension("priority", String.valueOf(serverConfig.getPriority()));
      emitter.emit(builder.build("segment/rowCount/avg", averageSize));
    }

    for (Map.Entry<String, SegmentRowCountDistribution> entry : segmentLoadDropHandler.getRowCountDistributionPerDatasource()
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
