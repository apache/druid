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
import org.apache.druid.indexing.worker.shuffle.ShuffleMetrics.PerDatasourceShuffleMetrics;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.BucketNumberedShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ShuffleResourceTest
{
  private static final String DATASOURCE = "datasource";

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  private LocalIntermediaryDataManager intermediaryDataManager;
  private ShuffleMetrics shuffleMetrics;
  private ShuffleResource shuffleResource;

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
        false,
        TaskConfig.BATCH_PROCESSING_MODE_DEFAULT.name(),
        null
    );
    final OverlordClient overlordClient = new NoopOverlordClient()
    {
      @Override
      public ListenableFuture<Map<String, TaskStatus>> taskStatuses(Set<String> taskIds)
      {
        final Map<String, TaskStatus> result = new HashMap<>();
        for (String taskId : taskIds) {
          result.put(taskId, new TaskStatus(taskId, TaskState.SUCCESS, 10, null, null));
        }
        return Futures.immediateFuture(result);
      }
    };
    intermediaryDataManager = new LocalIntermediaryDataManager(workerConfig, taskConfig, overlordClient);
    shuffleMetrics = new ShuffleMetrics();
    shuffleResource = new ShuffleResource(intermediaryDataManager, Optional.of(shuffleMetrics));
  }

  @Test
  public void testGetUnknownPartitionReturnNotFound()
  {
    final Response response = shuffleResource.getPartition(
        "unknownSupervisorTask",
        "unknownSubtask",
        "2020-01-01",
        "2020-01-02",
        0
    );
    Assert.assertEquals(Status.NOT_FOUND.getStatusCode(), response.getStatus());
    Assert.assertNotNull(response.getEntity());
    final String errorMessage = (String) response.getEntity();
    Assert.assertTrue(errorMessage.contains("Can't find the partition for supervisorTask"));
  }

  @Test
  public void testGetPartitionWithValidParamsReturnOk() throws IOException
  {
    final String supervisorTaskId = "supervisorTask";
    final String subtaskId = "subtaskId";
    final Interval interval = Intervals.of("2020-01-01/P1D");
    final DataSegment segment = newSegment(interval);
    final File segmentDir = generateSegmentDir("test");
    intermediaryDataManager.addSegment(supervisorTaskId, subtaskId, segment, segmentDir);

    final Response response = shuffleResource.getPartition(
        supervisorTaskId,
        subtaskId,
        interval.getStart().toString(),
        interval.getEnd().toString(),
        segment.getId().getPartitionNum()
    );
    final Map<String, PerDatasourceShuffleMetrics> snapshot = shuffleMetrics.snapshotAndReset();
    Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    Assert.assertEquals(1, snapshot.get(supervisorTaskId).getShuffleRequests());
    Assert.assertEquals(254, snapshot.get(supervisorTaskId).getShuffleBytes());
  }

  @Test
  public void testDeleteUnknownPartitionReturnOk()
  {
    final Response response = shuffleResource.deletePartitions("unknownSupervisorTask");
    Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }

  @Test
  public void testDeletePartitionWithValidParamsReturnOk() throws IOException
  {
    final String supervisorTaskId = "supervisorTask";
    final String subtaskId = "subtaskId";
    final Interval interval = Intervals.of("2020-01-01/P1D");
    final DataSegment segment = newSegment(interval);
    final File segmentDir = generateSegmentDir("test");
    intermediaryDataManager.addSegment(supervisorTaskId, subtaskId, segment, segmentDir);

    final Response response = shuffleResource.deletePartitions(supervisorTaskId);
    Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }

  @Test
  public void testDeletePartitionThrowingExceptionReturnIntervalServerError() throws IOException
  {
    final IntermediaryDataManager exceptionThrowingManager = EasyMock.niceMock(IntermediaryDataManager.class);
    exceptionThrowingManager.deletePartitions(EasyMock.anyString());
    EasyMock.expectLastCall().andThrow(new IOException("test"));
    EasyMock.replay(exceptionThrowingManager);
    final ShuffleResource shuffleResource = new ShuffleResource(exceptionThrowingManager, Optional.of(shuffleMetrics));

    final Response response = shuffleResource.deletePartitions("supervisorTask");
    Assert.assertEquals(Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
  }

  private static DataSegment newSegment(Interval interval)
  {
    BucketNumberedShardSpec<?> shardSpec = Mockito.mock(BucketNumberedShardSpec.class);
    Mockito.when(shardSpec.getBucketId()).thenReturn(0);

    return new DataSegment(
        DATASOURCE,
        interval,
        "version",
        null,
        null,
        null,
        shardSpec,
        0,
        10
    );
  }

  private File generateSegmentDir(String fileName) throws IOException
  {
    // Each file size is 138 bytes after compression
    final File segmentDir = tempDir.newFolder();
    FileUtils.write(new File(segmentDir, fileName), "test data.", StandardCharsets.UTF_8);
    FileUtils.writeByteArrayToFile(new File(segmentDir, "version.bin"), Ints.toByteArray(9));
    return segmentDir;
  }
}
