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

package org.apache.druid.indexing.worker.shuffle;

import org.apache.druid.com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.worker.shuffle.ShuffleMetrics.PerDatasourceShuffleMetrics;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;

public class ShuffleMonitorTest
{
  @Test
  public void testDoMonitor()
  {
    final ShuffleMetrics shuffleMetrics = Mockito.mock(ShuffleMetrics.class);
    final PerDatasourceShuffleMetrics perDatasourceShuffleMetrics = new PerDatasourceShuffleMetrics();
    perDatasourceShuffleMetrics.accumulate(100);
    perDatasourceShuffleMetrics.accumulate(200);
    perDatasourceShuffleMetrics.accumulate(10);
    Mockito.when(shuffleMetrics.snapshotAndReset())
           .thenReturn(ImmutableMap.of("supervisor", perDatasourceShuffleMetrics));
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    final ShuffleMonitor monitor = new ShuffleMonitor();
    monitor.setShuffleMetrics(shuffleMetrics);
    Assert.assertTrue(monitor.doMonitor(emitter));
    final List<Event> events = emitter.getEvents();
    Assert.assertEquals(2, events.size());
    Assert.assertSame(ServiceMetricEvent.class, events.get(0).getClass());
    ServiceMetricEvent event = (ServiceMetricEvent) events.get(0);
    Assert.assertEquals(ShuffleMonitor.SHUFFLE_BYTES_KEY, event.getMetric());
    Assert.assertEquals(310L, event.getValue());
    Assert.assertEquals(
        ImmutableMap.of(ShuffleMonitor.SUPERVISOR_TASK_ID_DIMENSION, "supervisor"),
        event.getUserDims()
    );
    Assert.assertSame(ServiceMetricEvent.class, events.get(1).getClass());
    event = (ServiceMetricEvent) events.get(1);
    Assert.assertEquals(ShuffleMonitor.SHUFFLE_REQUESTS_KEY, event.getMetric());
    Assert.assertEquals(3, event.getValue());
    Assert.assertEquals(
        ImmutableMap.of(ShuffleMonitor.SUPERVISOR_TASK_ID_DIMENSION, "supervisor"),
        event.getUserDims()
    );
  }
}
