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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.ClusterByPartition;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.msq.indexing.MSQWorkerTaskLauncher;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernel;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.msq.statistics.CompleteKeyStatisticsInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.mock;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class WorkerSketchFetcherTest
{
  @Mock
  private CompleteKeyStatisticsInformation completeKeyStatisticsInformation;

  @Mock
  private MSQWorkerTaskLauncher workerTaskLauncher;

  @Mock
  private ControllerQueryKernel kernel;

  @Mock
  private StageDefinition stageDefinition;
  @Mock
  private ClusterBy clusterBy;
  @Mock
  private ClusterByStatisticsCollector mergedClusterByStatisticsCollector1;
  @Mock
  private ClusterByStatisticsCollector mergedClusterByStatisticsCollector2;
  @Mock
  private WorkerClient workerClient;
  private ClusterByPartitions expectedPartitions1;
  private ClusterByPartitions expectedPartitions2;
  private AutoCloseable mocks;
  private WorkerSketchFetcher target;

  @Before
  public void setUp()
  {
    mocks = MockitoAnnotations.openMocks(this);
    doReturn(StageId.fromString("1_1")).when(stageDefinition).getId();
    doReturn(true).when(completeKeyStatisticsInformation).isComplete();
    doReturn(ImmutableSortedMap.of(123L, ImmutableSet.of(1, 2))).when(completeKeyStatisticsInformation)
                                                                .getTimeSegmentVsWorkerMap();

    expectedPartitions1 = new ClusterByPartitions(ImmutableList.of(new ClusterByPartition(
        mock(RowKey.class),
        mock(RowKey.class)
    )));
    expectedPartitions2 = new ClusterByPartitions(ImmutableList.of(new ClusterByPartition(
        mock(RowKey.class),
        mock(RowKey.class)
    )));

    doReturn(Either.value(expectedPartitions1)).when(stageDefinition)
                                               .generatePartitionsForShuffle(eq(mergedClusterByStatisticsCollector1));
    doReturn(Either.value(expectedPartitions2)).when(stageDefinition)
                                               .generatePartitionsForShuffle(eq(mergedClusterByStatisticsCollector2));

    doReturn(
        mergedClusterByStatisticsCollector1,
        mergedClusterByStatisticsCollector2
    ).when(stageDefinition).createResultKeyStatisticsCollector(anyInt());
  }

  @After
  public void tearDown() throws Exception
  {
    mocks.close();
    if (target != null) {
      target.close();
    }
  }

  @Test
  public void test_submitFetcherTask_parallelFetch()
      throws InterruptedException
  {

    final List<String> taskIds = ImmutableList.of("task-worker0_0", "task-worker1_0", "task-worker2_1");
    final CountDownLatch latch = new CountDownLatch(taskIds.size());

    target = spy(new WorkerSketchFetcher(workerClient, workerTaskLauncher, true));

    // When fetching snapshots, return a mock and add it to queue
    doAnswer(invocation -> {
      ClusterByStatisticsSnapshot snapshot = mock(ClusterByStatisticsSnapshot.class);
      return Futures.immediateFuture(snapshot);
    }).when(workerClient).fetchClusterByStatisticsSnapshot(any(), any(), anyInt());

    target.inMemoryFullSketchMerging(
        (kernelConsumer) ->
        {
          kernelConsumer.accept(kernel);
          latch.countDown();
        }
        ,
        stageDefinition.getId(),
        ImmutableSet.copyOf(taskIds),
        ((queryKernel, integer, msqFault) -> {})
    );

    latch.await(1, TimeUnit.MINUTES);
    Assert.assertEquals(0, latch.getCount());

  }

  @Test
  public void test_submitFetcherTask_sequentialFetch() throws InterruptedException
  {
    final List<String> taskIds = ImmutableList.of("task-worker0_0", "task-worker1_1", "task-worker2_1");
    final CountDownLatch latch = new CountDownLatch(taskIds.size() - 1);

    target = spy(new WorkerSketchFetcher(workerClient, workerTaskLauncher, true));

    // When fetching snapshots, return a mock and add it to queue
    doAnswer(invocation -> {
      ClusterByStatisticsSnapshot snapshot = mock(ClusterByStatisticsSnapshot.class);
      return Futures.immediateFuture(snapshot);
    }).when(workerClient).fetchClusterByStatisticsSnapshotForTimeChunk(any(), any(), anyInt(), anyLong());

    target.sequentialTimeChunkMerging(
        (kernelConsumer) ->
        {
          kernelConsumer.accept(kernel);
          latch.countDown();
        },
        completeKeyStatisticsInformation
        ,
        stageDefinition.getId(),
        ImmutableSet.copyOf(taskIds),
        ((queryKernel, integer, msqFault) -> {})
    );

    latch.await(1, TimeUnit.MINUTES);
    Assert.assertEquals(0, latch.getCount());
  }
}
