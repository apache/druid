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

public class FillCapacityWithCategorySpecWorkerSelectStrategyTest
{
  private static final ImmutableMap<String, ImmutableWorkerInfo> WORKERS_FOR_TIER_TESTS =
      ImmutableMap.of(
          "localhost0",
          new ImmutableWorkerInfo(
              new Worker("http", "localhost0", "localhost0", 5, "v1", "c1"), 1,
              new HashSet<>(),
              new HashSet<>(),
              DateTimes.nowUtc()
          ),
          "localhost1",
          new ImmutableWorkerInfo(
              new Worker("http", "localhost1", "localhost1", 5, "v1", "c1"), 2,
              new HashSet<>(),
              new HashSet<>(),
              DateTimes.nowUtc()
          ),
          "localhost2",
          new ImmutableWorkerInfo(
              new Worker("http", "localhost2", "localhost2", 5, "v1", "c2"), 3,
              new HashSet<>(),
              new HashSet<>(),
              DateTimes.nowUtc()
          ),
          "localhost3",
          new ImmutableWorkerInfo(
              new Worker("http", "localhost3", "localhost3", 5, "v1", "c2"), 4,
              new HashSet<>(),
              new HashSet<>(),
              DateTimes.nowUtc()
          )
      );

  @Test
  public void testFindWorkerForTaskWithNullWorkerTierSpec()
  {
    ImmutableWorkerInfo worker = selectWorker(null);
    Assert.assertEquals("localhost3", worker.getWorker().getHost());
  }

  @Test
  public void testFindWorkerForTaskWithPreferredTier()
  {
    // test defaultTier != null and tierAffinity is not empty
    final WorkerCategorySpec workerCategorySpec1 = new WorkerCategorySpec(
        ImmutableMap.of(
            "noop",
            new WorkerCategorySpec.CategoryConfig(
                "c1",
                ImmutableMap.of("ds1", "c1")
            )
        ),
        false
    );

    ImmutableWorkerInfo worker1 = selectWorker(workerCategorySpec1);
    Assert.assertEquals("localhost1", worker1.getWorker().getHost());

    // test defaultTier == null and tierAffinity is not empty
    final WorkerCategorySpec workerCategorySpec2 = new WorkerCategorySpec(
        ImmutableMap.of(
            "noop",
            new WorkerCategorySpec.CategoryConfig(
                null,
                ImmutableMap.of("ds1", "c1")
            )
        ),
        false
    );

    ImmutableWorkerInfo worker2 = selectWorker(workerCategorySpec2);
    Assert.assertEquals("localhost1", worker2.getWorker().getHost());

    // test defaultTier != null and tierAffinity is empty
    final WorkerCategorySpec workerCategorySpec3 = new WorkerCategorySpec(
        ImmutableMap.of(
            "noop",
            new WorkerCategorySpec.CategoryConfig(
                "c1",
                null
            )
        ),
        false
    );

    ImmutableWorkerInfo worker3 = selectWorker(workerCategorySpec3);
    Assert.assertEquals("localhost1", worker3.getWorker().getHost());
  }

  @Test
  public void testFindWorkerForTaskWithNullPreferredTier()
  {
    final WorkerCategorySpec workerCategorySpec = new WorkerCategorySpec(
        ImmutableMap.of(
            "noop",
            new WorkerCategorySpec.CategoryConfig(
                null,
                null
            )
        ),
        false
    );

    ImmutableWorkerInfo worker = selectWorker(workerCategorySpec);
    Assert.assertEquals("localhost3", worker.getWorker().getHost());
  }

  @Test
  public void testWeakTierSpec()
  {
    final WorkerCategorySpec workerCategorySpec = new WorkerCategorySpec(
        ImmutableMap.of(
            "noop",
            new WorkerCategorySpec.CategoryConfig(
                "c1",
                ImmutableMap.of("ds1", "c3")
            )
        ),
        false
    );

    ImmutableWorkerInfo worker = selectWorker(workerCategorySpec);
    Assert.assertEquals("localhost3", worker.getWorker().getHost());
  }

  @Test
  public void testStrongTierSpec()
  {
    final WorkerCategorySpec workerCategorySpec = new WorkerCategorySpec(
        ImmutableMap.of(
            "noop",
            new WorkerCategorySpec.CategoryConfig(
                "c1",
                ImmutableMap.of("ds1", "c3")
            )
        ),
        true
    );

    ImmutableWorkerInfo worker = selectWorker(workerCategorySpec);
    Assert.assertNull(worker);
  }

  private ImmutableWorkerInfo selectWorker(WorkerCategorySpec workerCategorySpec)
  {
    final FillCapacityWithCategorySpecWorkerSelectStrategy strategy = new FillCapacityWithCategorySpecWorkerSelectStrategy(
        workerCategorySpec);

    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        WORKERS_FOR_TIER_TESTS,
        new NoopTask(null, null, "ds1", 1, 0, null, null, null)
    );

    return worker;
  }
}
