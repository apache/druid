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

import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TaskSlotCountStatsMonitorTest
{
  private TaskSlotCountStatsProvider statsProvider;

  @Before
  public void setUp()
  {
    statsProvider = new TaskSlotCountStatsProvider()
    {
      @Override
      public Long getTotalTaskSlotCount()
      {
        return 1L;
      }

      @Override
      public Long getIdleTaskSlotCount()
      {
        return 1L;
      }

      @Override
      public Long getUsedTaskSlotCount()
      {
        return 1L;
      }

      @Override
      public Long getLazyTaskSlotCount()
      {
        return 1L;
      }

      @Override
      public Long getBlacklistedTaskSlotCount()
      {
        return 1L;
      }
    };
  }

  @Test
  public void testMonitor()
  {
    final TaskSlotCountStatsMonitor monitor = new TaskSlotCountStatsMonitor(statsProvider);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);
    Assert.assertEquals(5, emitter.getEvents().size());
    Assert.assertEquals("taskSlot/total/count", emitter.getEvents().get(0).toMap().get("metric"));
    Assert.assertEquals(1L, emitter.getEvents().get(0).toMap().get("value"));
    Assert.assertEquals("taskSlot/idle/count", emitter.getEvents().get(1).toMap().get("metric"));
    Assert.assertEquals(1L, emitter.getEvents().get(1).toMap().get("value"));
    Assert.assertEquals("taskSlot/used/count", emitter.getEvents().get(2).toMap().get("metric"));
    Assert.assertEquals(1L, emitter.getEvents().get(2).toMap().get("value"));
    Assert.assertEquals("taskSlot/lazy/count", emitter.getEvents().get(3).toMap().get("metric"));
    Assert.assertEquals(1L, emitter.getEvents().get(3).toMap().get("value"));
    Assert.assertEquals("taskSlot/blacklisted/count", emitter.getEvents().get(4).toMap().get("metric"));
    Assert.assertEquals(1L, emitter.getEvents().get(4).toMap().get("value"));
  }
}
