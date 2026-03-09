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

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class SupervisorStatsMonitorTest
{
  private SupervisorStatsProvider statsProvider;

  @Before
  public void setUp()
  {
    statsProvider = () -> ImmutableList.of(
        new SupervisorStatsProvider.SupervisorStats("events-kafka", "kafka", "RUNNING"),
        new SupervisorStatsProvider.SupervisorStats("logs-kafka", "kafka", "SUSPENDED"),
        new SupervisorStatsProvider.SupervisorStats("alerts-kafka", "kafka", "UNHEALTHY_SUPERVISOR"),
        new SupervisorStatsProvider.SupervisorStats("metrics-kinesis", "kinesis", "UNHEALTHY_TASKS")
    );
  }

  @Test
  public void testMonitorEmitsCorrectMetrics()
  {
    final SupervisorStatsMonitor monitor = new SupervisorStatsMonitor(statsProvider);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);

    Assert.assertEquals(4, emitter.getNumEmittedEvents());

    emitter.verifyValue(
        "supervisor/count",
        Map.of("supervisorId", "events-kafka", "type", "kafka", "state", "RUNNING"),
        1
    );
    emitter.verifyValue(
        "supervisor/count",
        Map.of("supervisorId", "logs-kafka", "type", "kafka", "state", "SUSPENDED"),
        1
    );
    emitter.verifyValue(
        "supervisor/count",
        Map.of("supervisorId", "alerts-kafka", "type", "kafka", "state", "UNHEALTHY_SUPERVISOR"),
        1
    );
    emitter.verifyValue(
        "supervisor/count",
        Map.of("supervisorId", "metrics-kinesis", "type", "kinesis", "state", "UNHEALTHY_TASKS"),
        1
    );
  }

  @Test
  public void testMonitorWithEmptyStats()
  {
    final SupervisorStatsProvider emptyProvider = () -> ImmutableList.of();
    final SupervisorStatsMonitor monitor = new SupervisorStatsMonitor(emptyProvider);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);

    Assert.assertEquals(0, emitter.getNumEmittedEvents());
  }

  @Test
  public void testMonitorWithNullStats()
  {
    final SupervisorStatsProvider nullProvider = () -> null;
    final SupervisorStatsMonitor monitor = new SupervisorStatsMonitor(nullProvider);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);

    Assert.assertEquals(0, emitter.getNumEmittedEvents());
  }

  @Test
  public void testMonitorWithUnknownState()
  {
    final SupervisorStatsProvider provider = () -> ImmutableList.of(
        new SupervisorStatsProvider.SupervisorStats("unknown-supervisor", "scheduled_batch", "UNKNOWN")
    );
    final SupervisorStatsMonitor monitor = new SupervisorStatsMonitor(provider);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);

    Assert.assertEquals(1, emitter.getNumEmittedEvents());
    emitter.verifyValue(
        "supervisor/count",
        Map.of("supervisorId", "unknown-supervisor", "type", "scheduled_batch", "state", "UNKNOWN"),
        1
    );
  }
}
