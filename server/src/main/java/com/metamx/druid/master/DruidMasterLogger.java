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

package com.metamx.druid.master;

import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidDataSource;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.collect.CountingMap;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

import java.util.Map;
import java.util.Set;

/**
 */
public class DruidMasterLogger implements DruidMasterHelper
{
  private static final Logger log = new Logger(DruidMasterLogger.class);

  @Override
  public DruidMasterRuntimeParams run(DruidMasterRuntimeParams params)
  {
    final Set<Map.Entry<String, LoadQueuePeon>> peonEntries = params.getLoadManagementPeons().entrySet();

    for (String msg : params.getMessages()) {
      log.info(msg);
    }

    log.info("Load Queues:");
    for (Map.Entry<String, LoadQueuePeon> entry : peonEntries) {
      LoadQueuePeon queuePeon = entry.getValue();
      DruidServer server = params.getAvailableServerMap().get(entry.getKey());
      log.info(
          "Server[%s, %s, %s] has %,d left to load, %,d left to drop, %,d bytes queued, %,d bytes served.",
          server.getName(),
          server.getType(),
          server.getSubType(),
          queuePeon.getSegmentsToLoad().size(),
          queuePeon.getSegmentsToDrop().size(),
          queuePeon.getLoadQueueSize(),
          server.getCurrSize()
      );
      if (log.isDebugEnabled()) {
        for (DataSegment segment : queuePeon.getSegmentsToLoad()) {
          log.debug("Segment to load[%s]", segment);
        }
        for (DataSegment segment : queuePeon.getSegmentsToDrop()) {
          log.debug("Segment to drop[%s]", segment);
        }
      }
    }

    final ServiceEmitter emitter = params.getEmitter();

    // Emit master metrics
    for (Map.Entry<String, LoadQueuePeon> entry : peonEntries) {
      String serverName = entry.getKey();
      LoadQueuePeon queuePeon = entry.getValue();
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setUser1(serverName).build(
              "master/loadQueue/size", queuePeon.getLoadQueueSize()
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setUser1(serverName).build(
              "master/loadQueue/count", queuePeon.getSegmentsToLoad().size()
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setUser1(serverName).build(
              "master/dropQueue/count", queuePeon.getSegmentsToDrop().size()
          )
      );
    }
    emitter.emit(new ServiceMetricEvent.Builder().build("master/unassigned/count", params.getUnassignedCount()));
    emitter.emit(new ServiceMetricEvent.Builder().build("master/unassigned/size", params.getUnassignedSize()));

    // Emit segment metrics
    CountingMap<String> segmentSizes = new CountingMap<String>();
    CountingMap<String> segmentCounts = new CountingMap<String>();
    for (DruidDataSource dataSource : params.getDataSources()) {
      for (DataSegment segment : dataSource.getSegments()) {
        segmentSizes.add(dataSource.getName(), segment.getSize());
        segmentCounts.add(dataSource.getName(), 1L);
      }
    }
    for (Map.Entry<String, Long> entry : segmentSizes.snapshot().entrySet()) {
      String dataSource = entry.getKey();
      Long size = entry.getValue();
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setUser1(dataSource).build(
              "master/segment/size", size
          )
      );
    }
    for (Map.Entry<String, Long> entry : segmentCounts.snapshot().entrySet()) {
      String dataSource = entry.getKey();
      Long count = entry.getValue();
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setUser1(dataSource).build(
              "master/segment/count", count
          )
      );
    }

    return params;
  }
}
