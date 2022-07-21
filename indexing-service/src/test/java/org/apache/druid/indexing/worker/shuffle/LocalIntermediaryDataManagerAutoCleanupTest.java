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

package org.apache.druid.indexing.worker.shuffle;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.io.FileUtils;
import org.apache.druid.client.indexing.NoopOverlordClient;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.BucketNumberedShardSpec;
import org.apache.druid.timeline.partition.BuildingShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.ShardSpecLookup;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LocalIntermediaryDataManagerAutoCleanupTest
{
  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  private TaskConfig taskConfig;
  private OverlordClient overlordClient;

  @Before
  public void setup() throws IOException
  {
    this.taskConfig = new TaskConfig(
        null,
        null,
        null,
        null,
        null,
        false,
        null,
        null,
        ImmutableList.of(new StorageLocationConfig(tempDir.newFolder(), null, null)),
        false,
        false,
        TaskConfig.BATCH_PROCESSING_MODE_DEFAULT.name(),
        null
    );
    this.overlordClient = new NoopOverlordClient()
    {
      @Override
      public ListenableFuture<Map<String, TaskStatus>> taskStatuses(Set<String> taskIds)
      {
        final Map<String, TaskStatus> result = new HashMap<>();
        for (String taskId : taskIds) {
          TaskState state = taskId.startsWith("running_") ? TaskState.RUNNING : TaskState.SUCCESS;
          result.put(taskId, new TaskStatus(taskId, state, 10, null, null));
        }
        return Futures.immediateFuture(result);
      }
    };
  }

  @Test
  public void testCompletedExpiredSupervisor() throws IOException, InterruptedException
  {
    Assert.assertTrue(
        isCleanedUpAfter3s("supervisor_1", new Period("PT1S"))
    );
  }

  @Test
  public void testCompletedNotExpiredSupervisor() throws IOException, InterruptedException
  {
    Assert.assertFalse(
        isCleanedUpAfter3s("supervisor_2", new Period("PT10S"))
    );
  }

  @Test
  public void testRunningSupervisor() throws IOException, InterruptedException
  {
    Assert.assertFalse(
        isCleanedUpAfter3s("running_supervisor_1", new Period("PT1S"))
    );
  }

  /**
   * Creates a LocalIntermediaryDataManager and adds a segment to it.
   * Also checks the cleanup status after 3s.
   * We use 3 seconds to avoid race condition between clean up in LocalIntermediaryDataManager
   * and checking of status in test.
   *
   * @return true if the cleanup has happened after 3s, false otherwise.
   */
  private boolean isCleanedUpAfter3s(String supervisorTaskId, Period timeoutPeriod)
      throws IOException, InterruptedException
  {
    final String subTaskId = "subTaskId";
    final Interval interval = Intervals.of("2018/2019");
    final File segmentFile = generateSegmentDir("test");
    final DataSegment segment = newSegment(interval);

    // Setup data manager with expiry timeout 1s and initial delay of 1 second
    WorkerConfig workerConfig = new TestWorkerConfig(1, 1, timeoutPeriod);
    LocalIntermediaryDataManager intermediaryDataManager =
        new LocalIntermediaryDataManager(workerConfig, taskConfig, overlordClient);
    intermediaryDataManager.addSegment(supervisorTaskId, subTaskId, segment, segmentFile);

    intermediaryDataManager
        .findPartitionFile(supervisorTaskId, subTaskId, interval, 0);

    // Start the data manager and the cleanup cycle
    intermediaryDataManager.start();

    // Check the state of the partition after 3s
    Thread.sleep(3000);
    boolean partitionFileExists = intermediaryDataManager
        .findPartitionFile(supervisorTaskId, subTaskId, interval, 0)
        .isPresent();

    intermediaryDataManager.stop();
    return !partitionFileExists;
  }

  private File generateSegmentDir(String fileName) throws IOException
  {
    // Each file size is 138 bytes after compression
    final File segmentDir = tempDir.newFolder();
    FileUtils.write(new File(segmentDir, fileName), "test data.", StandardCharsets.UTF_8);
    FileUtils.writeByteArrayToFile(new File(segmentDir, "version.bin"), Ints.toByteArray(9));
    return segmentDir;
  }

  private DataSegment newSegment(Interval interval)
  {
    return new DataSegment(
        "dataSource",
        interval,
        "version",
        null,
        null,
        null,
        new TestShardSpec(),
        9,
        10
    );
  }

  private static class TestShardSpec implements BucketNumberedShardSpec<BuildingShardSpec<ShardSpec>>
  {
    @Override
    public int getBucketId()
    {
      return 0;
    }

    @Override
    public BuildingShardSpec<ShardSpec> convert(int partitionId)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public ShardSpecLookup getLookup(List<? extends ShardSpec> shardSpecs)
    {
      throw new UnsupportedOperationException();
    }
  }

  private static class TestWorkerConfig extends WorkerConfig
  {
    private final long cleanupPeriodSeconds;
    private final long discoveryPeriodSeconds;
    private final Period timeoutPeriod;

    private TestWorkerConfig(long cleanupPeriodSeconds, long discoveryPeriodSeconds, Period timeoutPeriod)
    {
      this.cleanupPeriodSeconds = cleanupPeriodSeconds;
      this.discoveryPeriodSeconds = discoveryPeriodSeconds;
      this.timeoutPeriod = timeoutPeriod;
    }

    @Override
    public long getIntermediaryPartitionCleanupPeriodSec()
    {
      return cleanupPeriodSeconds;
    }

    @Override
    public long getIntermediaryPartitionDiscoveryPeriodSec()
    {
      return discoveryPeriodSeconds;
    }

    @Override
    public Period getIntermediaryPartitionTimeout()
    {
      return timeoutPeriod;
    }
  }
}
