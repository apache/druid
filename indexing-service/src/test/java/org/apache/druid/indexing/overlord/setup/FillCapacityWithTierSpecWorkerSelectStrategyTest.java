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
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.java.util.common.DateTimes;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;

public class FillCapacityWithTierSpecWorkerSelectStrategyTest
{
  private static final ImmutableMap<String, ImmutableWorkerInfo> WORKERS_FOR_TIER_TESTS =
      ImmutableMap.of(
          "localhost0",
          new ImmutableWorkerInfo(
              new Worker("http", "localhost0", "localhost0", 5, "v1", "t1"), 1,
              new HashSet<>(),
              new HashSet<>(),
              DateTimes.nowUtc()
          ),
          "localhost1",
          new ImmutableWorkerInfo(
              new Worker("http", "localhost1", "localhost1", 5, "v1", "t1"), 2,
              new HashSet<>(),
              new HashSet<>(),
              DateTimes.nowUtc()
          ),
          "localhost2",
          new ImmutableWorkerInfo(
              new Worker("http", "localhost2", "localhost2", 5, "v1", "t2"), 3,
              new HashSet<>(),
              new HashSet<>(),
              DateTimes.nowUtc()
          ),
          "localhost3",
          new ImmutableWorkerInfo(
              new Worker("http", "localhost3", "localhost3", 5, "v1", "t2"), 4,
              new HashSet<>(),
              new HashSet<>(),
              DateTimes.nowUtc()
          )
      );

  @Test
  public void testFindWorkerForTask()
  {
    final WorkerTierSpec workerTierSpec = new WorkerTierSpec(
        ImmutableMap.of(
            "noop",
            new WorkerTierSpec.TierConfig(
                "t1",
                ImmutableMap.of("ds1", "t1")
            )
        ),
        false
    );
    final FillCapacityWithTierSpecWorkerSelectStrategy strategy = new FillCapacityWithTierSpecWorkerSelectStrategy(
        workerTierSpec);

    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        WORKERS_FOR_TIER_TESTS,
        new NoopTask(null, "ds1", 1, 0, null, null, null)
    );
    Assert.assertEquals("localhost1", worker.getWorker().getHost());
  }

  @Test
  public void testFindWorkerForTaskWithNullWorkerTierSpec()
  {
    final FillCapacityWithTierSpecWorkerSelectStrategy strategy = new FillCapacityWithTierSpecWorkerSelectStrategy(
        null);

    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        WORKERS_FOR_TIER_TESTS,
        new NoopTask(null, "ds1", 1, 0, null, null, null)
    );
    Assert.assertEquals("localhost3", worker.getWorker().getHost());
  }

  @Test
  public void testFindWorkerForTaskWithNullPreferredTier()
  {
    final WorkerTierSpec workerTierSpec = new WorkerTierSpec(
        ImmutableMap.of(
            "noop",
            new WorkerTierSpec.TierConfig(
                null,
                null
            )
        ),
        false
    );
    final FillCapacityWithTierSpecWorkerSelectStrategy strategy = new FillCapacityWithTierSpecWorkerSelectStrategy(
        workerTierSpec);

    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        WORKERS_FOR_TIER_TESTS,
        new NoopTask(null, "ds1", 1, 0, null, null, null)
    );
    Assert.assertEquals("localhost3", worker.getWorker().getHost());
  }

  @Test
  public void testWeakTierSpec()
  {
    final WorkerTierSpec workerTierSpec = new WorkerTierSpec(
        ImmutableMap.of(
            "noop",
            new WorkerTierSpec.TierConfig(
                "t1",
                ImmutableMap.of("ds1", "t3")
            )
        ),
        false
    );
    final FillCapacityWithTierSpecWorkerSelectStrategy strategy = new FillCapacityWithTierSpecWorkerSelectStrategy(
        workerTierSpec);

    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        WORKERS_FOR_TIER_TESTS,
        new NoopTask(null, "ds1", 1, 0, null, null, null)
    );
    Assert.assertEquals("localhost3", worker.getWorker().getHost());
  }

  @Test
  public void testStrongTierSpec()
  {
    final WorkerTierSpec workerTierSpec = new WorkerTierSpec(
        ImmutableMap.of(
            "noop",
            new WorkerTierSpec.TierConfig(
                "t1",
                ImmutableMap.of("ds1", "t3")
            )
        ),
        true
    );
    final FillCapacityWithTierSpecWorkerSelectStrategy strategy = new FillCapacityWithTierSpecWorkerSelectStrategy(
        workerTierSpec);

    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        WORKERS_FOR_TIER_TESTS,
        new NoopTask(null, "ds1", 1, 0, null, null, null)
    );
    Assert.assertNull(worker);
  }
}
