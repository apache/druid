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

package org.apache.druid.server.initialization.jetty;

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JettyServerModuleTest
{
  @Test
  public void testJettyServerModule()
  {
    List<Event> events = new ArrayList<>();
    ServiceEmitter serviceEmitter = new ServiceEmitter("service", "host", Mockito.mock(Emitter.class))
    {
      @Override
      public void emit(Event event)
      {
        events.add(event);
      }
    };
    QueuedThreadPool jettyServerThreadPool = Mockito.mock(QueuedThreadPool.class);
    JettyServerModule.setJettyServerThreadPool(jettyServerThreadPool);
    Mockito.when(jettyServerThreadPool.getThreads()).thenReturn(100);
    Mockito.when(jettyServerThreadPool.getIdleThreads()).thenReturn(40);
    Mockito.when(jettyServerThreadPool.isLowOnThreads()).thenReturn(true);
    Mockito.when(jettyServerThreadPool.getMinThreads()).thenReturn(30);
    Mockito.when(jettyServerThreadPool.getMaxThreads()).thenReturn(100);
    Mockito.when(jettyServerThreadPool.getQueueSize()).thenReturn(50);
    Mockito.when(jettyServerThreadPool.getBusyThreads()).thenReturn(60);

    JettyServerModule.JettyMonitor jettyMonitor = new JettyServerModule.JettyMonitor("ds", "t0");
    jettyMonitor.doMonitor(serviceEmitter);

    Assert.assertEquals(8, events.size());
    List<Pair<String, Number>> expectedEvents = Arrays.asList(
        new Pair<>("jetty/numOpenConnections", 0),
        new Pair<>("jetty/threadPool/total", 100),
        new Pair<>("jetty/threadPool/idle", 40),
        new Pair<>("jetty/threadPool/isLowOnThreads", 1),
        new Pair<>("jetty/threadPool/min", 30),
        new Pair<>("jetty/threadPool/max", 100),
        new Pair<>("jetty/threadPool/queueSize", 50),
        new Pair<>("jetty/threadPool/busy", 60)
    );

    for (int i = 0; i < expectedEvents.size(); i++) {
      Pair<String, Number> expected = expectedEvents.get(i);
      ServiceMetricEvent actual = (ServiceMetricEvent) (events.get(i));
      Assert.assertEquals(expected.lhs, actual.getMetric());
      Assert.assertEquals(expected.rhs, actual.getValue());
    }
  }
}
