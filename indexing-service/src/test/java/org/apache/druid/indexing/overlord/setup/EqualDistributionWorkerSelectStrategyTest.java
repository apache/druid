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

package org.apache.druid.indexing.overlord.setup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;

public class EqualDistributionWorkerSelectStrategyTest
{
  private static final ImmutableMap<String, ImmutableWorkerInfo> WORKERS_FOR_AFFINITY_TESTS =
      ImmutableMap.of(
          "localhost0",
          new ImmutableWorkerInfo(
              new Worker("http", "localhost0", "localhost0", 2, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
              new HashSet<>(),
              new HashSet<>(),
              DateTimes.nowUtc()
          ),
          "localhost1",
          new ImmutableWorkerInfo(
              new Worker("http", "localhost1", "localhost1", 2, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
              new HashSet<>(),
              new HashSet<>(),
              DateTimes.nowUtc()
          ),
          "localhost2",
          new ImmutableWorkerInfo(
              new Worker("http", "localhost2", "localhost2", 2, "v1", WorkerConfig.DEFAULT_CATEGORY), 1,
              new HashSet<>(),
              new HashSet<>(),
              DateTimes.nowUtc()
          ),
          "localhost3",
          new ImmutableWorkerInfo(
              new Worker("http", "localhost3", "localhost3", 2, "v1", WorkerConfig.DEFAULT_CATEGORY), 1,
              new HashSet<>(),
              new HashSet<>(),
              DateTimes.nowUtc()
          )
      );

  @Test
  public void testFindWorkerForTask()
  {
    final EqualDistributionWorkerSelectStrategy strategy = new EqualDistributionWorkerSelectStrategy(null);

    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        ImmutableMap.of(
            "lhost",
            new ImmutableWorkerInfo(
                new Worker("http", "lhost", "lhost", 1, "v1", WorkerConfig.DEFAULT_CATEGORY), 0,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            ),
            "localhost",
            new ImmutableWorkerInfo(
                new Worker("http", "localhost", "localhost", 1, "v1", WorkerConfig.DEFAULT_CATEGORY), 1,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            )
        ),
        new NoopTask(null, null, null, 1, 0, null, null, null)
        {
          @Override
          public String getDataSource()
          {
            return "foo";
          }
        }
    );
    Assert.assertEquals("lhost", worker.getWorker().getHost());
  }

  @Test
  public void testFindWorkerForTaskWhenSameCurrCapacityUsed()
  {
    final EqualDistributionWorkerSelectStrategy strategy = new EqualDistributionWorkerSelectStrategy(null);

    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        ImmutableMap.of(
            "lhost",
            new ImmutableWorkerInfo(
                new Worker("http", "lhost", "lhost", 5, "v1", WorkerConfig.DEFAULT_CATEGORY), 5,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            ),
            "localhost",
            new ImmutableWorkerInfo(
                new Worker("http", "localhost", "localhost", 10, "v1", WorkerConfig.DEFAULT_CATEGORY), 5,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            )
        ),
        new NoopTask(null, null, null, 1, 0, null, null, null)
        {
          @Override
          public String getDataSource()
          {
            return "foo";
          }
        }
    );
    Assert.assertEquals("localhost", worker.getWorker().getHost());
  }

  @Test
  public void testOneDisableWorkerDifferentUsedCapacity()
  {
    String disabledVersion = "";
    final EqualDistributionWorkerSelectStrategy strategy = new EqualDistributionWorkerSelectStrategy(null);

    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        ImmutableMap.of(
            "lhost",
            new ImmutableWorkerInfo(
                new Worker("http", "disableHost", "disableHost", 10, disabledVersion, WorkerConfig.DEFAULT_CATEGORY), 2,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            ),
            "localhost",
            new ImmutableWorkerInfo(
                new Worker("http", "enableHost", "enableHost", 10, "v1", WorkerConfig.DEFAULT_CATEGORY), 5,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            )
        ),
        new NoopTask(null, null, null, 1, 0, null, null, null)
        {
          @Override
          public String getDataSource()
          {
            return "foo";
          }
        }
    );
    Assert.assertEquals("enableHost", worker.getWorker().getHost());
  }

  @Test
  public void testOneDisableWorkerSameUsedCapacity()
  {
    String disabledVersion = "";
    final EqualDistributionWorkerSelectStrategy strategy = new EqualDistributionWorkerSelectStrategy(null);

    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        ImmutableMap.of(
            "lhost",
            new ImmutableWorkerInfo(
                new Worker("http", "disableHost", "disableHost", 10, disabledVersion, WorkerConfig.DEFAULT_CATEGORY), 5,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            ),
            "localhost",
            new ImmutableWorkerInfo(
                new Worker("http", "enableHost", "enableHost", 10, "v1", WorkerConfig.DEFAULT_CATEGORY), 5,
                new HashSet<>(),
                new HashSet<>(),
                DateTimes.nowUtc()
            )
        ),
        new NoopTask(null, null, null, 1, 0, null, null, null)
        {
          @Override
          public String getDataSource()
          {
            return "foo";
          }
        }
    );
    Assert.assertEquals("enableHost", worker.getWorker().getHost());
  }

  @Test
  public void testWeakAffinity()
  {
    EqualDistributionWorkerSelectStrategy strategy = new EqualDistributionWorkerSelectStrategy(
        new AffinityConfig(
            ImmutableMap.of(
                "foo", ImmutableSet.of("localhost1", "localhost2", "localhost3"),
                "bar", ImmutableSet.of("nonexistent-worker")
            ),
            false
        )
    );

    ImmutableWorkerInfo workerFoo = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        WORKERS_FOR_AFFINITY_TESTS,
        createDummyTask("foo")
    );
    Assert.assertEquals("localhost1", workerFoo.getWorker().getHost());

    // With weak affinity, bar (which has no affinity workers available) can use a non-affinity worker.
    ImmutableWorkerInfo workerBar = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        WORKERS_FOR_AFFINITY_TESTS,
        createDummyTask("bar")
    );
    Assert.assertEquals("localhost0", workerBar.getWorker().getHost());

    ImmutableWorkerInfo workerBaz = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        WORKERS_FOR_AFFINITY_TESTS,
        createDummyTask("baz")
    );
    Assert.assertEquals("localhost0", workerBaz.getWorker().getHost());
  }

  @Test
  public void testStrongAffinity()
  {
    EqualDistributionWorkerSelectStrategy strategy = new EqualDistributionWorkerSelectStrategy(
        new AffinityConfig(
            ImmutableMap.of(
                "foo", ImmutableSet.of("localhost1", "localhost2", "localhost3"),
                "bar", ImmutableSet.of("nonexistent-worker")
            ),
            true
        )
    );

    ImmutableWorkerInfo workerFoo = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        WORKERS_FOR_AFFINITY_TESTS,
        createDummyTask("foo")
    );
    Assert.assertEquals("localhost1", workerFoo.getWorker().getHost());

    // With strong affinity, no workers can be found for bar.
    ImmutableWorkerInfo workerBar = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        WORKERS_FOR_AFFINITY_TESTS,
        createDummyTask("bar")
    );
    Assert.assertNull(workerBar);

    ImmutableWorkerInfo workerBaz = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        WORKERS_FOR_AFFINITY_TESTS,
        createDummyTask("baz")
    );
    Assert.assertEquals("localhost0", workerBaz.getWorker().getHost());
  }

  private static NoopTask createDummyTask(final String dataSource)
  {
    return new NoopTask(null, null, null, 1, 0, null, null, null)
    {
      @Override
      public String getDataSource()
      {
        return dataSource;
      }
    };
  }
}
