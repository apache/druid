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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Map;

public class WorkerTaskCountStatsMonitorTest
{
  private Injector injectorForMiddleManager;
  private Injector injectorForMiddleManagerNullStats;
  private Injector injectorForPeon;
  private Injector injectorForIndexer;

  private WorkerTaskCountStatsProvider statsProvider;
  private IndexerTaskCountStatsProvider indexerTaskStatsProvider;
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

    indexerTaskStatsProvider = new IndexerTaskCountStatsProvider()
    {
      @Override
      public Map<String, Long> getWorkerRunningTasks()
      {
        return ImmutableMap.of(
            "wikipedia", 2L,
            "animals", 3L
        );
      }

      @Override
      public Map<String, Long> getWorkerAssignedTasks()
      {
        return ImmutableMap.of(
            "products", 3L,
            "orders", 7L
        );
      }

      @Override
      public Map<String, Long> getWorkerCompletedTasks()
      {
        return ImmutableMap.of(
            "inventory", 8L,
            "metrics", 9L
        );
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
            binder -> binder.bind(WorkerTaskCountStatsProvider.class).toInstance(statsProvider)
        )
    );

    injectorForMiddleManagerNullStats = Guice.createInjector(
        ImmutableList.of(
            binder -> binder.bind(WorkerTaskCountStatsProvider.class).toInstance(nullStatsProvider)
        )
    );

    injectorForPeon = Guice.createInjector(
        ImmutableList.of(binder -> {})
    );

    injectorForIndexer = Guice.createInjector(
        ImmutableList.of(
            binder -> binder.bind(IndexerTaskCountStatsProvider.class).toInstance(indexerTaskStatsProvider)
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
    emitter.verifyValue(
        "worker/task/failed/count",
        ImmutableMap.of("category", "workerCategory", "workerVersion", "workerVersion"),
        4L
    );
    emitter.verifyValue(
        "worker/task/success/count",
        ImmutableMap.of("category", "workerCategory", "workerVersion", "workerVersion"),
        2L
    );
    emitter.verifyValue(
        "worker/taskSlot/idle/count",
        ImmutableMap.of("category", "workerCategory", "workerVersion", "workerVersion"),
        3L
    );
    emitter.verifyValue(
        "worker/taskSlot/total/count",
        ImmutableMap.of("category", "workerCategory", "workerVersion", "workerVersion"),
        5L
    );
    emitter.verifyValue(
        "worker/taskSlot/used/count",
        ImmutableMap.of("category", "workerCategory", "workerVersion", "workerVersion"),
        1L
    );
  }

  @Test
  public void testMonitorIndexer()
  {
    final WorkerTaskCountStatsMonitor monitor =
        new WorkerTaskCountStatsMonitor(injectorForIndexer, ImmutableSet.of(NodeRole.INDEXER));
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);
    Assert.assertEquals(6, emitter.getEvents().size());
    emitter.verifyValue(
        "worker/task/running/count",
        ImmutableMap.of("dataSource", "wikipedia"),
        2L
    );
    emitter.verifyValue(
        "worker/task/running/count",
        ImmutableMap.of("dataSource", "animals"),
        3L
    );
    emitter.verifyValue(
        "worker/task/assigned/count",
        ImmutableMap.of("dataSource", "products"),
        3L
    );
    emitter.verifyValue(
        "worker/task/assigned/count",
        ImmutableMap.of("dataSource", "orders"),
        7L
    );
    emitter.verifyValue(
        "worker/task/completed/count",
        ImmutableMap.of("dataSource", "inventory"),
        8L
    );
    emitter.verifyValue(
        "worker/task/completed/count",
        ImmutableMap.of("dataSource", "metrics"),
        9L
    );
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
}
