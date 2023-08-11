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

package org.apache.druid.indexing.common;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.common.stats.TaskRealtimeMetricsMonitor;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.realtime.FireDepartment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class TaskRealtimeMetricsMonitorTest
{
  private static final Map<String, String[]> DIMENSIONS = ImmutableMap.of(
      "dim1",
      new String[]{"v1", "v2"},
      "dim2",
      new String[]{"vv"}
  );

  private static final Map<String, Object> TAGS = ImmutableMap.of("author", "Author Name", "version", 10);

  @Mock(answer = Answers.RETURNS_MOCKS)
  private FireDepartment fireDepartment;
  @Mock(answer = Answers.RETURNS_MOCKS)
  private RowIngestionMeters rowIngestionMeters;
  @Mock
  private ServiceEmitter emitter;
  private Map<String, ServiceMetricEvent> emittedEvents;
  private TaskRealtimeMetricsMonitor target;

  @Before
  public void setUp()
  {
    emittedEvents = new HashMap<>();
    Mockito.doCallRealMethod().when(emitter).emit(ArgumentMatchers.any(ServiceEventBuilder.class));
    Mockito
        .doAnswer(invocation -> {
          ServiceMetricEvent e = invocation.getArgument(0);
          emittedEvents.put(e.getMetric(), e);
          return null;
        })
        .when(emitter).emit(ArgumentMatchers.any(Event.class));
    target = new TaskRealtimeMetricsMonitor(fireDepartment, rowIngestionMeters, DIMENSIONS, TAGS);
  }

  @Test
  public void testdoMonitorShouldEmitUserProvidedTags()
  {
    target.doMonitor(emitter);
    for (ServiceMetricEvent sme : emittedEvents.values()) {
      Assert.assertEquals(TAGS, sme.getUserDims().get(DruidMetrics.TAGS));
    }
  }

  @Test
  public void testdoMonitorWithoutTagsShouldNotEmitTags()
  {
    target = new TaskRealtimeMetricsMonitor(fireDepartment, rowIngestionMeters, DIMENSIONS, null);
    for (ServiceMetricEvent sme : emittedEvents.values()) {
      Assert.assertFalse(sme.getUserDims().containsKey(DruidMetrics.TAGS));
    }
  }
}
