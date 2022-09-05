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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

public class WorkerTaskCountStatsMonitorTest
{
  private Injector injectorForMiddleManager;
  private Injector injectorForMiddleManagerNullStats;
  private Injector injectorForPeon;

  private WorkerTaskCountStatsProvider statsProvider;
  private WorkerTaskCountStatsProvider nullStatsProvider;

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

    nullStatsProvider = new WorkerTaskCountStatsProvider()
    {
      @Nullable
      @Override
      public Long getWorkerTotalTaskSlotCount()
      {
        return null;
      }

      @Nullable
      @Override
      public Long getWorkerFailedTaskCount()
      {
        return null;
      }

      @Nullable
      @Override
      public Long getWorkerIdleTaskSlotCount()
      {
        return null;
      }

      @Nullable
      @Override
      public Long getWorkerSuccessfulTaskCount()
      {
        return null;
      }

      @Nullable
      @Override
      public Long getWorkerUsedTaskSlotCount()
      {
        return null;
      }

      @Nullable
      @Override
      public String getWorkerCategory()
      {
        return null;
      }

      @Nullable
      @Override
      public String getWorkerVersion()
      {
        return null;
      }
    };

    injectorForMiddleManager = Guice.createInjector(
        ImmutableList.of(
            (Module) binder -> {
              binder.bind(WorkerTaskCountStatsProvider.class).toInstance(statsProvider);
            }
        )
    );

    injectorForMiddleManagerNullStats = Guice.createInjector(
        ImmutableList.of(
            (Module) binder -> {
              binder.bind(WorkerTaskCountStatsProvider.class).toInstance(nullStatsProvider);
            }
        )
    );

    injectorForPeon = Guice.createInjector(
        ImmutableList.of(
            (Module) binder -> {}
        )
    );
  }

  @Test
  public void testMonitor()
  {
    final WorkerTaskCountStatsMonitor monitor =
        new WorkerTaskCountStatsMonitor(injectorForMiddleManager, ImmutableSet.of(NodeRole.MIDDLE_MANAGER));
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);
    Assert.assertEquals(5, emitter.getEvents().size());
    Assert.assertEquals("worker/task/failed/count", emitter.getEvents().get(0).toMap().get("metric"));
    Assert.assertEquals("workerCategory", emitter.getEvents().get(0).toMap().get("category"));
    Assert.assertEquals("workerVersion", emitter.getEvents().get(0).toMap().get("workerVersion"));
    Assert.assertEquals(4L, emitter.getEvents().get(0).toMap().get("value"));
    Assert.assertEquals("worker/task/success/count", emitter.getEvents().get(1).toMap().get("metric"));
    Assert.assertEquals("workerCategory", emitter.getEvents().get(1).toMap().get("category"));
    Assert.assertEquals("workerVersion", emitter.getEvents().get(1).toMap().get("workerVersion"));
    Assert.assertEquals(2L, emitter.getEvents().get(1).toMap().get("value"));
    Assert.assertEquals("worker/taskSlot/idle/count", emitter.getEvents().get(2).toMap().get("metric"));
    Assert.assertEquals("workerCategory", emitter.getEvents().get(2).toMap().get("category"));
    Assert.assertEquals("workerVersion", emitter.getEvents().get(2).toMap().get("workerVersion"));
    Assert.assertEquals(3L, emitter.getEvents().get(2).toMap().get("value"));
    Assert.assertEquals("worker/taskSlot/total/count", emitter.getEvents().get(3).toMap().get("metric"));
    Assert.assertEquals("workerCategory", emitter.getEvents().get(3).toMap().get("category"));
    Assert.assertEquals("workerVersion", emitter.getEvents().get(3).toMap().get("workerVersion"));
    Assert.assertEquals(5L, emitter.getEvents().get(3).toMap().get("value"));
    Assert.assertEquals("worker/taskSlot/used/count", emitter.getEvents().get(4).toMap().get("metric"));
    Assert.assertEquals("workerCategory", emitter.getEvents().get(4).toMap().get("category"));
    Assert.assertEquals("workerVersion", emitter.getEvents().get(4).toMap().get("workerVersion"));
    Assert.assertEquals(1L, emitter.getEvents().get(4).toMap().get("value"));
  }

  @Test
  public void testMonitorWithNulls()
  {
    final WorkerTaskCountStatsMonitor monitor =
        new WorkerTaskCountStatsMonitor(injectorForMiddleManagerNullStats, ImmutableSet.of(NodeRole.MIDDLE_MANAGER));
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);
    Assert.assertEquals(0, emitter.getEvents().size());
  }

  @Test
  public void testMonitorNotMiddleManager()
  {
    final WorkerTaskCountStatsMonitor monitor =
        new WorkerTaskCountStatsMonitor(injectorForPeon, ImmutableSet.of(NodeRole.PEON));
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);
    Assert.assertEquals(0, emitter.getEvents().size());
  }
}
