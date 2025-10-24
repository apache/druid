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
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.ArbitraryGranularitySpec;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.TestSeekableStreamIndexTask;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.AllGranularity;
import org.apache.druid.segment.indexing.DataSchema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.util.Collections;
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
                ImmutableMap.of("ds1", "c1"),
                null
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
                ImmutableMap.of("ds1", "c1"),
                null
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
                null,
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
                ImmutableMap.of("ds1", "c3"),
                null
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
                ImmutableMap.of("ds1", "c3"),
                null
            )
        ),
        true
    );

    ImmutableWorkerInfo worker = selectWorker(workerCategorySpec);
    Assert.assertNull(worker);
  }

  @Test
  public void testSupervisorIdCategoryAffinity()
  {
    // Test that supervisor ID affinity takes precedence over datasource affinity
    final WorkerCategorySpec workerCategorySpec = new WorkerCategorySpec(
        ImmutableMap.of(
            "test_seekable_stream",
            new WorkerCategorySpec.CategoryConfig(
                "c1",  // default category
                ImmutableMap.of("ds1", "c1"),  // datasource affinity
                ImmutableMap.of("supervisor1", "c2")  // supervisor ID affinity
            )
        ),
        false
    );

    // Create a test task with supervisor ID "supervisor1"
    final Task taskWithSupervisor = createTestTask("task1", "supervisor1", "ds1");
    
    final FillCapacityWithCategorySpecWorkerSelectStrategy strategy =
        new FillCapacityWithCategorySpecWorkerSelectStrategy(workerCategorySpec, null);

    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        WORKERS_FOR_TIER_TESTS,
        taskWithSupervisor
    );

    // Should select c2 worker (localhost3) because supervisor ID affinity takes precedence
    Assert.assertNotNull(worker);
    Assert.assertEquals("c2", worker.getWorker().getCategory());
    Assert.assertEquals("localhost3", worker.getWorker().getHost());
  }

  @Test
  public void testSupervisorIdCategoryAffinityFallbackToDatasource()
  {
    // Test that it falls back to datasource affinity when supervisor ID affinity is not found
    final WorkerCategorySpec workerCategorySpec = new WorkerCategorySpec(
        ImmutableMap.of(
            "test_seekable_stream",
            new WorkerCategorySpec.CategoryConfig(
                "c2",  // default category
                ImmutableMap.of("ds1", "c1"),  // datasource affinity
                ImmutableMap.of("supervisor2", "c2")  // supervisor ID affinity (different supervisor)
            )
        ),
        false
    );

    // Create a test task with supervisor ID "supervisor1" (not in supervisorIdCategoryAffinity map)
    final Task taskWithSupervisor = createTestTask("task1", "supervisor1", "ds1");
    
    final FillCapacityWithCategorySpecWorkerSelectStrategy strategy =
        new FillCapacityWithCategorySpecWorkerSelectStrategy(workerCategorySpec, null);

    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        WORKERS_FOR_TIER_TESTS,
        taskWithSupervisor
    );

    // Should fall back to datasource affinity and select c1 worker
    Assert.assertNotNull(worker);
    Assert.assertEquals("c1", worker.getWorker().getCategory());
    Assert.assertEquals("localhost1", worker.getWorker().getHost());
  }

  @Test
  public void testSupervisorIdCategoryAffinityFallbackToDefault()
  {
    // Test that it falls back to default category when neither supervisor ID nor datasource affinity is found
    final WorkerCategorySpec workerCategorySpec = new WorkerCategorySpec(
        ImmutableMap.of(
            "test_seekable_stream",
            new WorkerCategorySpec.CategoryConfig(
                "c2",  // default category
                ImmutableMap.of("ds2", "c1"),  // datasource affinity (different datasource)
                ImmutableMap.of("supervisor2", "c1")  // supervisor ID affinity (different supervisor)
            )
        ),
        false
    );

    // Create a test task with supervisor ID "supervisor1" and datasource "ds1"
    final Task taskWithSupervisor = createTestTask("task1", "supervisor1", "ds1");
    
    final FillCapacityWithCategorySpecWorkerSelectStrategy strategy =
        new FillCapacityWithCategorySpecWorkerSelectStrategy(workerCategorySpec, null);

    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        WORKERS_FOR_TIER_TESTS,
        taskWithSupervisor
    );

    // Should fall back to default category c2
    Assert.assertNotNull(worker);
    Assert.assertEquals("c2", worker.getWorker().getCategory());
    Assert.assertEquals("localhost3", worker.getWorker().getHost());
  }

  private ImmutableWorkerInfo selectWorker(WorkerCategorySpec workerCategorySpec)
  {
    final FillCapacityWithCategorySpecWorkerSelectStrategy strategy =
        new FillCapacityWithCategorySpecWorkerSelectStrategy(workerCategorySpec, null);

    ImmutableWorkerInfo worker = strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        WORKERS_FOR_TIER_TESTS,
        NoopTask.forDatasource("ds1")
    );

    return worker;
  }

  /**
   * Helper method to create a test task with supervisor ID for testing
   */
  @SuppressWarnings("unchecked")
  private static Task createTestTask(String id, @Nullable String supervisorId, String datasource)
  {
    return new TestSeekableStreamIndexTask(
        id,
        supervisorId,
        null,
        DataSchema.builder()
            .withDataSource(datasource)
            .withTimestamp(new TimestampSpec(null, null, null))
            .withDimensions(new DimensionsSpec(Collections.emptyList()))
            .withGranularity(new ArbitraryGranularitySpec(new AllGranularity(), Collections.emptyList()))
            .build(),
        Mockito.mock(SeekableStreamIndexTaskTuningConfig.class),
        Mockito.mock(SeekableStreamIndexTaskIOConfig.class),
        null,
        null
    );
  }
}
