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

public class WorkerTaskCountStatsMonitorTest
{
  private WorkerTaskCountStatsProvider statsProvider;

  @Before
  public void setUp()
  {
    statsProvider = new WorkerTaskCountStatsProvider()
    {
      @Override
      public Long getWorkerTotalTaskSlotCount()
      {
        return 5L;
      }

      @Override
      public Long getWorkerFailedTaskCount()
      {
        return 4L;
      }

      @Override
      public Long getWorkerIdleTaskSlotCount()
      {
        return 3L;
      }

      @Override
      public Long getWorkerSuccessfulTaskCount()
      {
        return 2L;
      }

      @Override
      public Long getWorkerUsedTaskSlotCount()
      {
        return 1L;
      }

      @Override
      public String getWorkerCategory()
      {
        return "workerCategory";
      }

      @Override
      public String getWorkerVersion()
      {
        return "workerVersion";
      }
    };
  }

  @Test
  public void testMonitor()
  {
    final WorkerTaskCountStatsMonitor monitor = new WorkerTaskCountStatsMonitor(statsProvider);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);
    Assert.assertEquals(5, emitter.getEvents().size());
    Assert.assertEquals("worker/task/failed/count", emitter.getEvents().get(0).toMap().get("metric"));
    Assert.assertEquals("workerCategory", emitter.getEvents().get(0).toMap().get("category"));
    Assert.assertEquals("workerVersion", emitter.getEvents().get(0).toMap().get("version"));
    Assert.assertEquals(4L, emitter.getEvents().get(0).toMap().get("value"));
    Assert.assertEquals("worker/task/success/count", emitter.getEvents().get(1).toMap().get("metric"));
    Assert.assertEquals("workerCategory", emitter.getEvents().get(1).toMap().get("category"));
    Assert.assertEquals("workerVersion", emitter.getEvents().get(1).toMap().get("version"));
    Assert.assertEquals(2L, emitter.getEvents().get(1).toMap().get("value"));
    Assert.assertEquals("worker/taskSlot/idle/count", emitter.getEvents().get(2).toMap().get("metric"));
    Assert.assertEquals("workerCategory", emitter.getEvents().get(2).toMap().get("category"));
    Assert.assertEquals("workerVersion", emitter.getEvents().get(2).toMap().get("version"));
    Assert.assertEquals(3L, emitter.getEvents().get(2).toMap().get("value"));
    Assert.assertEquals("worker/taskSlot/total/count", emitter.getEvents().get(3).toMap().get("metric"));
    Assert.assertEquals("workerCategory", emitter.getEvents().get(3).toMap().get("category"));
    Assert.assertEquals("workerVersion", emitter.getEvents().get(3).toMap().get("version"));
    Assert.assertEquals(5L, emitter.getEvents().get(3).toMap().get("value"));
    Assert.assertEquals("worker/taskSlot/used/count", emitter.getEvents().get(4).toMap().get("metric"));
    Assert.assertEquals("workerCategory", emitter.getEvents().get(4).toMap().get("category"));
    Assert.assertEquals("workerVersion", emitter.getEvents().get(4).toMap().get("version"));
    Assert.assertEquals(1L, emitter.getEvents().get(4).toMap().get("value"));
  }
}
