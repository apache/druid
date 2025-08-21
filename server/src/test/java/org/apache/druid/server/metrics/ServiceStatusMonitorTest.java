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

public class ServiceStatusMonitorTest
{

  private ServiceStatusMonitor monitor;
  private StubServiceEmitter emitter;
  private Map<String, Object> heartbeatTags;
  private final Supplier<Map<String, Object>> heartbeatTagsSupplier = () -> heartbeatTags;
  private static final String HEARTBEAT_METRIC_KEY = "service/heartbeat";

  @Before
  public void setUp()
  {
    emitter = new StubServiceEmitter();
    monitor = new ServiceStatusMonitor();
    heartbeatTags = new HashMap<>();
    monitor.heartbeatTagsSupplier = heartbeatTagsSupplier;
  }

  @Test
  public void testDefaultHeartbeatReported()
  {
    monitor.doMonitor(emitter);
    Assert.assertEquals(1, emitter.getNumEmittedEvents());
    emitter.verifyValue(HEARTBEAT_METRIC_KEY, 1);
  }

  @Test
  public void testLeaderTag()
  {
    heartbeatTags.put("leader", 1);
    monitor.doMonitor(emitter);
    Assert.assertEquals(1, emitter.getNumEmittedEvents());
    emitter.verifyValue(HEARTBEAT_METRIC_KEY, Map.of("leader", 1), 1);
  }

  @Test
  public void testMoreThanOneTag()
  {
    heartbeatTags.put("leader", 1);
    heartbeatTags.put("taskRunner", "http");
    monitor.doMonitor(emitter);
    Assert.assertEquals(1, emitter.getNumEmittedEvents());
    emitter.verifyValue(HEARTBEAT_METRIC_KEY, Map.of("leader", 1, "taskRunner", "http"), 1);
  }
}
