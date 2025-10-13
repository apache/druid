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
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.ArbitraryGranularitySpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
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
import java.util.Map;


public class EqualDistributionWithSupervisorCategorySpecWorkerSelectStrategyTest
{
  private static final ImmutableMap<String, ImmutableWorkerInfo> WORKERS_FOR_TIER_TESTS =
      ImmutableMap.of(
          "localhost0",
          new ImmutableWorkerInfo(
              new Worker("http", "localhost0", "localhost0", 1, "v1", "c1"), 0,
              new HashSet<>(),
              new HashSet<>(),
              DateTimes.nowUtc()
          ),
          "localhost1",
          new ImmutableWorkerInfo(
              new Worker("http", "localhost1", "localhost1", 2, "v1", "c1"), 0,
              new HashSet<>(),
              new HashSet<>(),
              DateTimes.nowUtc()
          ),
          "localhost2",
          new ImmutableWorkerInfo(
              new Worker("http", "localhost2", "localhost2", 3, "v1", "c2"), 0,
              new HashSet<>(),
              new HashSet<>(),
              DateTimes.nowUtc()
          ),
          "localhost3",
          new ImmutableWorkerInfo(
              new Worker("http", "localhost3", "localhost3", 4, "v1", "c2"), 0,
              new HashSet<>(),
              new HashSet<>(),
              DateTimes.nowUtc()
          )
      );

  @Test
  public void testFindWorkerForTaskWithNullWorkerTierSpec()
  {
    ImmutableWorkerInfo worker = selectWorker(null, new TestStreamingTask("id1", "sup-1", "ds1", null, null));
    Assert.assertEquals("localhost3", worker.getWorker().getHost());
  }

  @Test
  public void testFindWorkerForTaskWithPreferredTierBySupervisor()
  {
    final WorkerCategorySpec workerCategorySpec = new WorkerCategorySpec(
        ImmutableMap.of(
            "noop",
            new WorkerCategorySpec.CategoryConfig(
                "c1",
                ImmutableMap.of("sup-2", "c2")
            )
        ),
        false
    );

    // supervisor sup-2 prefers c2, which should pick highest-capacity in c2 -> localhost3
    ImmutableWorkerInfo worker1 = selectWorker(workerCategorySpec, new TestStreamingTask("id2", "sup-2", "ds1", null, null));
    Assert.assertEquals("localhost3", worker1.getWorker().getHost());

    // not specified, defaultCategory c1 -> pick highest in c1 -> localhost1
    ImmutableWorkerInfo worker2 = selectWorker(workerCategorySpec, new TestStreamingTask("id3", "sup-1", "ds1", null, null));
    Assert.assertEquals("localhost1", worker2.getWorker().getHost());
  }

  @Test
  public void testWeakTierSpecFallsBack()
  {
    final WorkerCategorySpec workerCategorySpec = new WorkerCategorySpec(
        ImmutableMap.of(
            "noop",
            new WorkerCategorySpec.CategoryConfig(
                "c1",
                ImmutableMap.of("sup-x", "c3")
            )
        ),
        false
    );

    // preferred category c3 doesn't exist; weak spec -> choose from all runnable (highest capacity overall -> localhost3)
    ImmutableWorkerInfo worker = selectWorker(workerCategorySpec, new TestStreamingTask("id4", "sup-x", "ds1", null, null));
    Assert.assertEquals("localhost3", worker.getWorker().getHost());
  }

  @Test
  public void testStrongTierSpecReturnsNullIfUnavailable()
  {
    final WorkerCategorySpec workerCategorySpec = new WorkerCategorySpec(
        ImmutableMap.of(
            "noop",
            new WorkerCategorySpec.CategoryConfig(
                "c1",
                ImmutableMap.of("sup-x", "c3")
            )
        ),
        true
    );

    ImmutableWorkerInfo worker = selectWorker(workerCategorySpec, new TestStreamingTask("id5", "sup-x", "ds1", null, null));
    Assert.assertNull(worker);
  }

  @Test
  public void testNonSeekableTaskFallsBackToDatasource()
  {
    final WorkerCategorySpec workerCategorySpec = new WorkerCategorySpec(
        ImmutableMap.of(
            "noop",
            new WorkerCategorySpec.CategoryConfig(
                "c1",
                ImmutableMap.of("ds1", "c2")
            )
        ),
        false
    );

    // No supervisorId available -> uses datasource mapping to c2 -> highest in c2 -> localhost3
    ImmutableWorkerInfo worker = selectWorker(workerCategorySpec, NoopTask.forDatasource("ds1"));
    Assert.assertEquals("localhost3", worker.getWorker().getHost());
  }

  private ImmutableWorkerInfo selectWorker(WorkerCategorySpec workerCategorySpec, AbstractTask task)
  {
    final EqualDistributionWithSupervisorCategorySpecWorkerSelectStrategy strategy =
        new EqualDistributionWithSupervisorCategorySpecWorkerSelectStrategy(workerCategorySpec, null);

    return strategy.findWorkerForTask(
        new RemoteTaskRunnerConfig(),
        WORKERS_FOR_TIER_TESTS,
        task
    );
  }

  private static class TestStreamingTask extends SeekableStreamIndexTask<String, String, ByteEntity>
  {
    TestStreamingTask(
        String id,
        @Nullable String supervisorId,
        String datasource,
        @Nullable Map context,
        @Nullable String groupId
    )
    {
      this(
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
          context,
          groupId
      );
    }

    private TestStreamingTask(
        String id,
        @Nullable String supervisorId,
        @Nullable TaskResource taskResource,
        DataSchema dataSchema,
        SeekableStreamIndexTaskTuningConfig tuningConfig,
        SeekableStreamIndexTaskIOConfig<String, String> ioConfig,
        @Nullable Map context,
        @Nullable String groupId
    )
    {
      super(id, supervisorId, taskResource, dataSchema, tuningConfig, ioConfig, context, groupId);
    }

    @Override
    protected SeekableStreamIndexTaskRunner<String, String, ByteEntity> createTaskRunner()
    {
      return null;
    }

    @Override
    protected RecordSupplier<String, String, ByteEntity> newTaskRecordSupplier(final TaskToolbox toolbox)
    {
      return null;
    }

    @Override
    public String getType()
    {
      return "noop";
    }
  }
}


