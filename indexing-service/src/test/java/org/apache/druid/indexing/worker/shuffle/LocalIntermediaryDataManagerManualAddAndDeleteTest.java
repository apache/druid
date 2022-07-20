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
import com.google.common.io.ByteSource;
import com.google.common.primitives.Ints;
import org.apache.commons.io.FileUtils;
import org.apache.druid.client.indexing.NoopOverlordClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.BucketNumberedShardSpec;
import org.apache.druid.timeline.partition.BuildingShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.ShardSpecLookup;
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
import java.util.List;
import java.util.Optional;

public class LocalIntermediaryDataManagerManualAddAndDeleteTest
{
  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private LocalIntermediaryDataManager intermediaryDataManager;
  private File intermediarySegmentsLocation;
  private File siblingLocation;

  @Before
  public void setup() throws IOException
  {
    final WorkerConfig workerConfig = new WorkerConfig();
    intermediarySegmentsLocation = tempDir.newFolder();
    siblingLocation = tempDir.newFolder();
    final TaskConfig taskConfig = new TaskConfig(
        null,
        null,
        null,
        null,
        null,
        false,
        null,
        null,
        ImmutableList.of(new StorageLocationConfig(intermediarySegmentsLocation, 1200L, null)),
        false,
        false,
        TaskConfig.BATCH_PROCESSING_MODE_DEFAULT.name(),
        null
    );
    final OverlordClient overlordClient = new NoopOverlordClient();
    intermediaryDataManager = new LocalIntermediaryDataManager(workerConfig, taskConfig, overlordClient);
    intermediaryDataManager.start();
  }

  @After
  public void teardown()
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
      final Optional<ByteSource> file = intermediaryDataManager.findPartitionFile(
          supervisorTaskId,
          "subTaskId_" + i,
          interval,
          partitionId
      );
      Assert.assertTrue(file.isPresent());
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
        Assert.assertFalse(
            intermediaryDataManager.findPartitionFile(supervisorTaskId, "subTaskId_" + subTaskId, interval, partitionId).isPresent()
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

  @Test
  public void testFailsWithCraftyFabricatedNamesForDelete() throws IOException
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("supervisorTaskId cannot start with the '.' character.");
    final String supervisorTaskId = "../" + siblingLocation.getName();
    final String someFile = "sneaky-snake.txt";
    File dataFile = new File(siblingLocation, someFile);
    FileUtils.write(
        dataFile,
        "test data",
        StandardCharsets.UTF_8
    );
    Assert.assertTrue(new File(intermediarySegmentsLocation, supervisorTaskId).exists());
    Assert.assertTrue(dataFile.exists());
    intermediaryDataManager.deletePartitions(supervisorTaskId);
    Assert.assertTrue(new File(intermediarySegmentsLocation, supervisorTaskId).exists());
    Assert.assertTrue(dataFile.exists());
  }

  @Test
  public void testFailsWithCraftyFabricatedNamesForFind() throws IOException
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("supervisorTaskId cannot start with the '.' character.");
    final String supervisorTaskId = "../" + siblingLocation.getName();
    final Interval interval = Intervals.of("2018/2019");
    final int partitionId = 0;
    final String intervalAndPart =
        StringUtils.format("%s/%s/%s", interval.getStart().toString(), interval.getEnd().toString(), partitionId);

    final String someFile = "sneaky-snake.txt";

    final String someFilePath = intervalAndPart + "/" + someFile;

    // can only traverse to find files that are in a path of the form {start}/{end}/{partitionId}, so write a data file
    // in a location like that
    File dataFile = new File(siblingLocation, someFilePath);
    FileUtils.write(
        dataFile,
        "test data",
        StandardCharsets.UTF_8
    );

    Assert.assertTrue(new File(intermediarySegmentsLocation, supervisorTaskId).exists());
    Assert.assertTrue(
        new File(intermediarySegmentsLocation, supervisorTaskId + "/" + someFilePath).exists());

    final Optional<ByteSource> foundFile1 = intermediaryDataManager.findPartitionFile(
        supervisorTaskId,
        someFile,
        interval,
        partitionId
    );
    Assert.assertFalse(foundFile1.isPresent());
  }

  private File generateSegmentDir(String fileName) throws IOException
  {
    // Each file size is 138 bytes after compression
    final File segmentDir = tempDir.newFolder();
    FileUtils.write(new File(segmentDir, fileName), "test data.", StandardCharsets.UTF_8);
    FileUtils.writeByteArrayToFile(new File(segmentDir, "version.bin"), Ints.toByteArray(9));
    return segmentDir;
  }

  private DataSegment newSegment(Interval interval, int bucketId)
  {
    return new DataSegment(
        "dataSource",
        interval,
        "version",
        null,
        null,
        null,
        new TestShardSpec(bucketId),
        9,
        10
    );
  }

  private static class TestShardSpec implements BucketNumberedShardSpec<BuildingShardSpec<ShardSpec>>
  {
    private final int bucketId;

    private TestShardSpec(int bucketId)
    {
      this.bucketId = bucketId;
    }

    @Override
    public int getBucketId()
    {
      return bucketId;
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
