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

package org.apache.druid.sql.calcite.schema;

import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.timeline.SegmentId;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A monitor that provides stats on segments that are visible to the {@link DruidSchema}
 */
public class SchemaStatsMonitor extends AbstractMonitor
{
  private final DruidSchema druidSchema;

  @Inject
  SchemaStatsMonitor(DruidSchema druidSchema)
  {
    this.druidSchema = druidSchema;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    for (Map.Entry<String, ConcurrentSkipListMap<SegmentId, AvailableSegmentMetadata>> datasourceAndSegmentsMetadata : druidSchema.getSegmentMetadataInfoUnsafe().entrySet()) {
      builder.setDimension(DruidMetrics.DATASOURCE, datasourceAndSegmentsMetadata.getKey());
      for (AvailableSegmentMetadata metadata : datasourceAndSegmentsMetadata.getValue().values()) {
        builder.setDimension("segment/binaryVersion", metadata.getSegment().getBinaryVersion());
        builder.setDimension("segment/hasLastCompactedState", metadata.getSegment().getLastCompactionState() != null);

        emitter.emit(builder.build("segment/detailed/numRows", metadata.getNumRows()));
        emitter.emit(builder.build("segment/detailed/numReplicas", metadata.getNumReplicas()));
        emitter.emit(builder.build("segment/detailed/size", metadata.getSegment().getSize()));
        emitter.emit(builder.build("segment/detailed/numDimensions", metadata.getSegment().getDimensions().size()));
        emitter.emit(builder.build("segment/detailed/numMetrics", metadata.getSegment().getMetrics().size()));
        emitter.emit(builder.build("segment/detailed/durationMillis", metadata.getSegment().getInterval().toDurationMillis()));
      }
    }
    return true;
  }
}
