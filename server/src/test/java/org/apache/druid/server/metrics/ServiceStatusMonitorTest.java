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

import com.google.common.base.Supplier;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ServiceStatusMonitorTest {

  private ServiceStatusMonitor monitor;
  private Map<String, Number> heartbeatTags = new HashMap<>();
  private Supplier<Map<String, Number>> heartbeatTagsSupplier = () -> heartbeatTags;

  @Before
  public void setUp() throws Exception {
    monitor = new ServiceStatusMonitor();
    monitor.heartbeatTagsSupplier = heartbeatTagsSupplier;
  }

  @Test
  public void testLeaderCount() {
    heartbeatTags.put("leader", 0);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);

    Assert.assertEquals(1, emitter.getEvents().size());
    Assert.assertEquals("druid/heartbeat", emitter.getEvents().get(0).toMap().get("metric"));
    Assert.assertEquals(0, emitter.getEvents().get(0).toMap().get("value"));

    heartbeatTags.put("leader", 1);
    emitter.flush();
    monitor.doMonitor(emitter);
    Assert.assertEquals(1, emitter.getEvents().size());
    Assert.assertEquals("druid/heartbeat", emitter.getEvents().get(0).toMap().get("metric"));
    Assert.assertEquals(1, emitter.getEvents().get(0).toMap().get("value"));
  }
}