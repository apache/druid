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
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;
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
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;

import static org.easymock.EasyMock.mock;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class WorkerSketchFetcherTest
{
  @Mock
  private CompleteKeyStatisticsInformation completeKeyStatisticsInformation;
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
    doReturn(clusterBy).when(stageDefinition).getClusterBy();
    doReturn(25_000).when(stageDefinition).getMaxPartitionCount();

    expectedPartitions1 = new ClusterByPartitions(ImmutableList.of(new ClusterByPartition(mock(RowKey.class), mock(RowKey.class))));
    expectedPartitions2 = new ClusterByPartitions(ImmutableList.of(new ClusterByPartition(mock(RowKey.class), mock(RowKey.class))));

    doReturn(Either.value(expectedPartitions1)).when(stageDefinition).generatePartitionsForShuffle(eq(mergedClusterByStatisticsCollector1));
    doReturn(Either.value(expectedPartitions2)).when(stageDefinition).generatePartitionsForShuffle(eq(mergedClusterByStatisticsCollector2));

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
  public void test_submitFetcherTask_parallelFetch_mergePerformedCorrectly()
      throws ExecutionException, InterruptedException
  {
    // Store snapshots in a queue
    final Queue<ClusterByStatisticsSnapshot> snapshotQueue = new ConcurrentLinkedQueue<>();
    final List<String> workerIds = ImmutableList.of("0", "1", "2", "3", "4");
    final CountDownLatch latch = new CountDownLatch(workerIds.size());

    target = spy(new WorkerSketchFetcher(workerClient, ClusterStatisticsMergeMode.PARALLEL, 300_000_000));

    // When fetching snapshots, return a mock and add it to queue
    doAnswer(invocation -> {
      ClusterByStatisticsSnapshot snapshot = mock(ClusterByStatisticsSnapshot.class);
      snapshotQueue.add(snapshot);
      latch.countDown();
      return Futures.immediateFuture(snapshot);
    }).when(workerClient).fetchClusterByStatisticsSnapshot(any(), any(), anyInt());

    CompletableFuture<Either<Long, ClusterByPartitions>> eitherCompletableFuture = target.submitFetcherTask(
        completeKeyStatisticsInformation,
        workerIds,
        stageDefinition
    );

    // Assert that the final result is complete and all other sketches returned have been merged.
    eitherCompletableFuture.join();
    Thread.sleep(1000);
    Assert.assertTrue(eitherCompletableFuture.isDone() && !eitherCompletableFuture.isCompletedExceptionally());
    Assert.assertFalse(snapshotQueue.isEmpty());
    // Verify that all statistics were added to controller.
    for (ClusterByStatisticsSnapshot snapshot : snapshotQueue) {
      verify(mergedClusterByStatisticsCollector1, times(1)).addAll(eq(snapshot));
    }
    // Check that the partitions returned by the merged collector is returned by the final future.
    Assert.assertEquals(expectedPartitions1, eitherCompletableFuture.get().valueOrThrow());
  }

  @Test
  public void test_submitFetcherTask_sequentialFetch_mergePerformedCorrectly()
      throws ExecutionException, InterruptedException
  {
    // Store snapshots in a queue
    final Queue<ClusterByStatisticsSnapshot> snapshotQueue = new ConcurrentLinkedQueue<>();

    SortedMap<Long, Set<Integer>> timeSegmentVsWorkerMap = ImmutableSortedMap.of(1L, ImmutableSet.of(0, 1, 2), 2L, ImmutableSet.of(0, 1, 4));
    doReturn(timeSegmentVsWorkerMap).when(completeKeyStatisticsInformation).getTimeSegmentVsWorkerMap();

    final CyclicBarrier barrier = new CyclicBarrier(3);
    target = spy(new WorkerSketchFetcher(workerClient, ClusterStatisticsMergeMode.SEQUENTIAL, 300_000_000));

    // When fetching snapshots, return a mock and add it to queue
    doAnswer(invocation -> {
      ClusterByStatisticsSnapshot snapshot = mock(ClusterByStatisticsSnapshot.class);
      snapshotQueue.add(snapshot);
      barrier.await();
      return Futures.immediateFuture(snapshot);
    }).when(workerClient).fetchClusterByStatisticsSnapshotForTimeChunk(any(), any(), anyInt(), anyLong());

    CompletableFuture<Either<Long, ClusterByPartitions>> eitherCompletableFuture = target.submitFetcherTask(
        completeKeyStatisticsInformation,
        ImmutableList.of("0", "1", "2", "3", "4"),
        stageDefinition
    );

    // Assert that the final result is complete and all other sketches returned have been merged.
    eitherCompletableFuture.join();
    Thread.sleep(1000);

    Assert.assertTrue(eitherCompletableFuture.isDone() && !eitherCompletableFuture.isCompletedExceptionally());
    Assert.assertFalse(snapshotQueue.isEmpty());
    // Verify that all statistics were added to controller.
    snapshotQueue.stream().limit(3).forEach(snapshot -> {
      verify(mergedClusterByStatisticsCollector1, times(1)).addAll(eq(snapshot));
    });
    snapshotQueue.stream().skip(3).limit(3).forEach(snapshot -> {
      verify(mergedClusterByStatisticsCollector2, times(1)).addAll(eq(snapshot));
    });
    ClusterByPartitions expectedResult =
        new ClusterByPartitions(
            ImmutableList.of(
                new ClusterByPartition(expectedPartitions1.get(0).getStart(), expectedPartitions2.get(0).getStart()),
                new ClusterByPartition(expectedPartitions2.get(0).getStart(), expectedPartitions2.get(0).getEnd())
            )
        );
    // Check that the partitions returned by the merged collector is returned by the final future.
    Assert.assertEquals(expectedResult, eitherCompletableFuture.get().valueOrThrow());
  }
}
