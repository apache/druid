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

package org.apache.druid.indexing.worker;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.NoopIndexingServiceClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class IntermediaryDataManagerManualAddAndDeleteTest
{
  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private IntermediaryDataManager intermediaryDataManager;

  @Before
  public void setup() throws IOException
  {
    final WorkerConfig workerConfig = new WorkerConfig();
    final TaskConfig taskConfig = new TaskConfig(
        null,
        null,
        null,
        null,
        null,
        false,
        null,
        null,
        ImmutableList.of(new StorageLocationConfig(tempDir.newFolder(), 600L, null))
    );
    final IndexingServiceClient indexingServiceClient = new NoopIndexingServiceClient();
    intermediaryDataManager = new IntermediaryDataManager(workerConfig, taskConfig, indexingServiceClient);
    intermediaryDataManager.start();
  }

  @After
  public void teardown() throws InterruptedException
  {
    intermediaryDataManager.stop();
  }

  @Test
  public void testAddSegmentFailure() throws IOException
  {
    int i = 0;
    for (; i < 4; i++) {
      File segmentFile = generateSegmentDir("file_" + i);
      DataSegment segment = newSegment(Intervals.of("2018/2019"), i);
      intermediaryDataManager.addSegment("supervisorTaskId", "subTaskId", segment, segmentFile);
    }
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Can't find location to handle segment");
    File segmentFile = generateSegmentDir("file_" + i);
    DataSegment segment = newSegment(Intervals.of("2018/2019"), 4);
    intermediaryDataManager.addSegment("supervisorTaskId", "subTaskId", segment, segmentFile);
  }

  @Test
  public void testFindPartitionFiles() throws IOException
  {
    final String supervisorTaskId = "supervisorTaskId";
    final Interval interval = Intervals.of("2018/2019");
    final int partitionId = 0;
    for (int i = 0; i < 4; i++) {
      final File segmentFile = generateSegmentDir("file_" + i);
      final DataSegment segment = newSegment(interval, partitionId);
      intermediaryDataManager.addSegment(supervisorTaskId, "subTaskId_" + i, segment, segmentFile);
    }
    for (int i = 0; i < 4; i++) {
      final File file = intermediaryDataManager.findPartitionFile(
          supervisorTaskId,
          "subTaskId_" + i,
          interval,
          partitionId
      );
      Assert.assertNotNull(file);
    }
  }

  @Test
  public void deletePartitions() throws IOException
  {
    final String supervisorTaskId = "supervisorTaskId";
    final Interval interval = Intervals.of("2018/2019");
    for (int partitionId = 0; partitionId < 2; partitionId++) {
      for (int subTaskId = 0; subTaskId < 2; subTaskId++) {
        final File segmentFile = generateSegmentDir("file_" + partitionId + "_" + subTaskId);
        final DataSegment segment = newSegment(interval, partitionId);
        intermediaryDataManager.addSegment(supervisorTaskId, "subTaskId_" + subTaskId, segment, segmentFile);
      }
    }

    intermediaryDataManager.deletePartitions(supervisorTaskId);

    for (int partitionId = 0; partitionId < 2; partitionId++) {
      for (int subTaskId = 0; subTaskId < 2; subTaskId++) {
        Assert.assertNull(
            intermediaryDataManager.findPartitionFile(supervisorTaskId, "subTaskId_" + subTaskId, interval, partitionId)
        );
      }
    }
  }

  @Test
  public void testAddRemoveAdd() throws IOException
  {
    final String supervisorTaskId = "supervisorTaskId";
    final Interval interval = Intervals.of("2018/2019");
    int i = 0;
    for (; i < 4; i++) {
      File segmentFile = generateSegmentDir("file_" + i);
      DataSegment segment = newSegment(interval, i);
      intermediaryDataManager.addSegment("supervisorTaskId", "subTaskId", segment, segmentFile);
    }
    intermediaryDataManager.deletePartitions(supervisorTaskId);
    File segmentFile = generateSegmentDir("file_" + i);
    DataSegment segment = newSegment(interval, i);
    intermediaryDataManager.addSegment(supervisorTaskId, "subTaskId", segment, segmentFile);
  }

  private File generateSegmentDir(String fileName) throws IOException
  {
    // Each file size is 138 bytes after compression
    final File segmentDir = tempDir.newFolder();
    FileUtils.write(new File(segmentDir, fileName), "test data.", StandardCharsets.UTF_8);
    return segmentDir;
  }

  private DataSegment newSegment(Interval interval, int partitionId)
  {
    return new DataSegment(
        "dataSource",
        interval,
        "version",
        null,
        null,
        null,
        new NumberedShardSpec(partitionId, 0),
        9,
        10
    );
  }
}
