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
import org.apache.commons.io.FileUtils;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.NoopIndexingServiceClient;
import org.apache.druid.client.indexing.TaskStatus;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.BucketNumberedShardSpec;
import org.apache.druid.timeline.partition.BuildingShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.ShardSpecLookup;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
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

  private LocalIntermediaryDataManager intermediaryDataManager;

  @Before
  public void setup() throws IOException
  {
    final WorkerConfig workerConfig = new WorkerConfig()
    {
      @Override
      public long getIntermediaryPartitionDiscoveryPeriodSec()
      {
        return 1;
      }

      @Override
      public long getIntermediaryPartitionCleanupPeriodSec()
      {
        return 2;
      }

      @Override
      public Period getIntermediaryPartitionTimeout()
      {
        return new Period("PT2S");
      }

    };
    final TaskConfig taskConfig = new TaskConfig(
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
        false
    );
    final IndexingServiceClient indexingServiceClient = new NoopIndexingServiceClient()
    {
      @Override
      public Map<String, TaskStatus> getTaskStatuses(Set<String> taskIds)
      {
        final Map<String, TaskStatus> result = new HashMap<>();
        for (String taskId : taskIds) {
          result.put(taskId, new TaskStatus(taskId, TaskState.SUCCESS, 10));
        }
        return result;
      }
    };
    intermediaryDataManager = new LocalIntermediaryDataManager(workerConfig, taskConfig, indexingServiceClient);
    intermediaryDataManager.start();
  }

  @After
  public void teardown() throws InterruptedException
  {
    intermediaryDataManager.stop();
  }

  @Test
  public void testCleanup() throws IOException, InterruptedException
  {
    final String supervisorTaskId = "supervisorTaskId";
    final String subTaskId = "subTaskId";
    final Interval interval = Intervals.of("2018/2019");
    final File segmentFile = generateSegmentDir("test");
    final DataSegment segment = newSegment(interval);
    intermediaryDataManager.addSegment(supervisorTaskId, subTaskId, segment, segmentFile);

    Thread.sleep(3000);
    Assert.assertFalse(intermediaryDataManager.findPartitionFile(supervisorTaskId, subTaskId, interval, 0).isPresent());
  }

  private File generateSegmentDir(String fileName) throws IOException
  {
    // Each file size is 138 bytes after compression
    final File segmentDir = tempDir.newFolder();
    FileUtils.write(new File(segmentDir, fileName), "test data.", StandardCharsets.UTF_8);
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
}
