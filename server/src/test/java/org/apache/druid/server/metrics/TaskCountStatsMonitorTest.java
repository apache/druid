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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class TaskCountStatsMonitorTest
{
  private TaskCountStatsProvider statsProvider;

  @Before
  public void setUp()
  {
    statsProvider = new TaskCountStatsProvider()
    {
      @Override
      public Map<String, Long> getSuccessfulTaskCount()
      {
        return null;
      }

      @Override
      public Map<String, Long> getFailedTaskCount()
      {
        return null;
      }

      @Override
      public Map<String, Long> getRunningTaskCount()
      {
        return ImmutableMap.of("d1", 1L);
      }

      @Override
      public Map<String, Long> getPendingTaskCount()
      {
        return null;
      }

      @Override
      public Map<String, Long> getWaitingTaskCount()
      {
        return null;
      }
    };
  }

  @Test
  public void testMonitor()
  {
    final TaskCountStatsMonitor monitor = new TaskCountStatsMonitor(statsProvider);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);
    Assert.assertEquals(1, emitter.getEvents().size());
    Assert.assertEquals("task/running/count", emitter.getEvents().get(0).toMap().get("metric"));
    Assert.assertEquals(1L, emitter.getEvents().get(0).toMap().get("value"));
  }
}
