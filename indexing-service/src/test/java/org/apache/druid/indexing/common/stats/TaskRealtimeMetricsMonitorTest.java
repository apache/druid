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

package org.apache.druid.indexing.common.stats;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.incremental.NoopRowIngestionMeters;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TaskRealtimeMetricsMonitorTest
{
  private FireDepartment fireDepartment;
  private RowIngestionMeters rowIngestionMeters;
  private Map<String, String[]> dimensions;

  @Before
  public void setUp()
  {
    List<DimensionSchema> dimensionList = new ArrayList<>();
    fireDepartment = new FireDepartment(new DataSchema("testDataSource",
                                                       new TimestampSpec("timestamp", "iso", null),
                                                       new DimensionsSpec(
                                                           dimensionList,
                                                           null,
                                                           null
                                                       ),
                                                       new AggregatorFactory[]{new CountAggregatorFactory("rows")},
                                                       new UniformGranularitySpec(
                                                           Granularities.HOUR,
                                                           Granularities.NONE,
                                                           ImmutableList.of()
                                                       ),
                                                       null),
                                        new RealtimeIOConfig(null, null),
                                        null
    );
    rowIngestionMeters = new NoopRowIngestionMeters();

    dimensions = new HashMap<>();

    FireDepartmentMetrics metrics = fireDepartment.getMetrics();
    metrics.setRowsInMemory(1000);
    metrics.setMaxRowsInMemory(2000);
    metrics.setBytesInMemory(10000);
    metrics.setMaxBytesInMemory(20000);

  }

  @Test
  public void testMonitor()
  {
    TaskRealtimeMetricsMonitor monitor = new TaskRealtimeMetricsMonitor(fireDepartment, rowIngestionMeters, dimensions);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);
    Map<String, Long> resultMap = emitter.getEvents()
                                         .stream()
                                         .collect(Collectors.toMap(
                                             event -> (String) event.toMap().get("metric"),
                                             event -> (Long) event.toMap().get("value")
                                         ));
    Assert.assertEquals(21, resultMap.size());
    Assert.assertEquals(1000L, (long) resultMap.get("ingest/rows/inMemory"));
    Assert.assertEquals(2000L, (long) resultMap.get("ingest/rows/maxInMemory"));
    Assert.assertEquals(10000L, (long) resultMap.get("ingest/bytes/inMemory"));
    Assert.assertEquals(20000L, (long) resultMap.get("ingest/bytes/maxInMemory"));
  }
}
