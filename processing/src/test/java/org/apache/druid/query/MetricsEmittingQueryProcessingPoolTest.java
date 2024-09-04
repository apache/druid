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

package org.apache.druid.query;

import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class MetricsEmittingQueryProcessingPoolTest
{
  @Test
  public void testPrioritizedExecutorDelegate()
  {
    PrioritizedExecutorService service = Mockito.mock(PrioritizedExecutorService.class);
    Mockito.when(service.getQueueSize()).thenReturn(10);
    Mockito.when(service.getActiveTasks()).thenReturn(2);
    ExecutorServiceMonitor monitor = new ExecutorServiceMonitor();
    List<Event> events = new ArrayList<>();
    MetricsEmittingQueryProcessingPool processingPool = new MetricsEmittingQueryProcessingPool(service, monitor);
    Assert.assertSame(service, processingPool.delegate());

    ServiceEmitter serviceEmitter = new ServiceEmitter("service", "host", Mockito.mock(Emitter.class))
    {
      @Override
      public void emit(Event event)
      {
        events.add(event);
      }
    };
    monitor.doMonitor(serviceEmitter);
    Assert.assertEquals(2, events.size());
    Assert.assertEquals(((ServiceMetricEvent) (events.get(0))).getMetric(), "segment/scan/pending");
    Assert.assertEquals(((ServiceMetricEvent) (events.get(0))).getValue(), 10);
    Assert.assertEquals(((ServiceMetricEvent) (events.get(1))).getMetric(), "segment/scan/active");
    Assert.assertEquals(((ServiceMetricEvent) (events.get(1))).getValue(), 2);
  }

  @Test
  public void testNonPrioritizedExecutorDelegate()
  {
    ListeningExecutorService service = Mockito.mock(ListeningExecutorService.class);
    ExecutorServiceMonitor monitor = new ExecutorServiceMonitor();
    MetricsEmittingQueryProcessingPool processingPool = new MetricsEmittingQueryProcessingPool(service, monitor);
    Assert.assertSame(service, processingPool.delegate());

    ServiceEmitter serviceEmitter = Mockito.mock(ServiceEmitter.class);
    monitor.doMonitor(serviceEmitter);
    Mockito.verifyNoInteractions(serviceEmitter);
  }
}
