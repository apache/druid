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
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import org.apache.commons.io.FileUtils;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.NoopIndexingServiceClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.FileUtils.FileCopyResult;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.utils.CompressionUtils;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ShuffleDataSegmentPusherTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private IntermediaryDataManager intermediaryDataManager;
  private ShuffleDataSegmentPusher segmentPusher;

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
        ImmutableList.of(new StorageLocationConfig(temporaryFolder.newFolder(), null, null))
    );
    final IndexingServiceClient indexingServiceClient = new NoopIndexingServiceClient();
    intermediaryDataManager = new IntermediaryDataManager(workerConfig, taskConfig, indexingServiceClient);
    intermediaryDataManager.start();
    segmentPusher = new ShuffleDataSegmentPusher("supervisorTaskId", "subTaskId", intermediaryDataManager);
  }

  @After
  public void teardown() throws InterruptedException
  {
    intermediaryDataManager.stop();
  }

  @Test
  public void testPush() throws IOException
  {
    final File segmentDir = generateSegmentDir();
    final DataSegment segment = newSegment(Intervals.of("2018/2019"));
    final DataSegment pushed = segmentPusher.push(segmentDir, segment, true);

    Assert.assertEquals(9, pushed.getBinaryVersion().intValue());
    Assert.assertEquals(14, pushed.getSize()); // 10 bytes data + 4 bytes version

    final File zippedSegment = intermediaryDataManager.findPartitionFile(
        "supervisorTaskId",
        "subTaskId",
        segment.getInterval(),
        segment.getShardSpec().getPartitionNum()
    );
    Assert.assertNotNull(zippedSegment);
    final File tempDir = temporaryFolder.newFolder();
    final FileCopyResult result = CompressionUtils.unzip(zippedSegment, tempDir);
    final List<File> unzippedFiles = new ArrayList<>(result.getFiles());
    unzippedFiles.sort(Comparator.comparing(File::getName));
    final File dataFile = unzippedFiles.get(0);
    Assert.assertEquals("test", dataFile.getName());
    Assert.assertEquals("test data.", Files.readFirstLine(dataFile, StandardCharsets.UTF_8));
    final File versionFile = unzippedFiles.get(1);
    Assert.assertEquals("version.bin", versionFile.getName());
    Assert.assertArrayEquals(Ints.toByteArray(0x9), Files.toByteArray(versionFile));
  }

  private File generateSegmentDir() throws IOException
  {
    // Each file size is 138 bytes after compression
    final File segmentDir = temporaryFolder.newFolder();
    Files.asByteSink(new File(segmentDir, "version.bin")).write(Ints.toByteArray(0x9));
    FileUtils.write(new File(segmentDir, "test"), "test data.", StandardCharsets.UTF_8);
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
        new NumberedShardSpec(0, 0),
        9,
        0
    );
  }
}
