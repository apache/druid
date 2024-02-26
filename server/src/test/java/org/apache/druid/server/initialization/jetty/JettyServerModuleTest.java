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

import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.Test;
import org.mockito.Mockito;

public class JettyServerModuleTest
{
  @Test
  public void testJettyServerModule()
  {
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

    final StubServiceEmitter serviceEmitter = new StubServiceEmitter("service", "host");
    jettyMonitor.doMonitor(serviceEmitter);

    serviceEmitter.verifyValue("jetty/numOpenConnections", 0);
    serviceEmitter.verifyValue("jetty/threadPool/total", 100);
    serviceEmitter.verifyValue("jetty/threadPool/idle", 40);
    serviceEmitter.verifyValue("jetty/threadPool/isLowOnThreads", 1);
    serviceEmitter.verifyValue("jetty/threadPool/min", 30);
    serviceEmitter.verifyValue("jetty/threadPool/max", 100);
    serviceEmitter.verifyValue("jetty/threadPool/queueSize", 50);
    serviceEmitter.verifyValue("jetty/threadPool/busy", 60);
  }
}
