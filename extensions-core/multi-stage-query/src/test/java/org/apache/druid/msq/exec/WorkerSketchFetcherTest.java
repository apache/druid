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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.ClusterByPartitions;
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

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.easymock.EasyMock.mock;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
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
  private ClusterByStatisticsCollector mergedClusterByStatisticsCollector;
  @Mock
  private WorkerClient workerClient;
  private AutoCloseable mocks;
  private WorkerSketchFetcher target;

  @Before
  public void setUp()
  {
    mocks = MockitoAnnotations.openMocks(this);
    target = spy(new WorkerSketchFetcher(workerClient, ClusterStatisticsMergeMode.PARALLEL, 300_000_000));

    doReturn(StageId.fromString("1_1")).when(stageDefinition).getId();
    doReturn(clusterBy).when(stageDefinition).getClusterBy();
    doReturn(mergedClusterByStatisticsCollector).when(stageDefinition).createResultKeyStatisticsCollector(anyInt());
  }

  @After
  public void tearDown() throws Exception
  {
    mocks.close();
  }

  @Test
  public void test_submitFetcherTask_parallelFetch_workerThrowsException_shouldCancelOtherTasks()
  {
    // Store futures in a queue
    final Queue<ListenableFuture<ClusterByStatisticsSnapshot>> futureQueue = new ConcurrentLinkedQueue<>();
    final CountDownLatch latch = new CountDownLatch(5);

    // When fetching snapshots, return a mock and add future to queue
    doAnswer(invocation -> {
      ListenableFuture<ClusterByStatisticsSnapshot> future = spy(Futures.immediateFuture(mock(ClusterByStatisticsSnapshot.class)));
      futureQueue.add(future);
      latch.countDown();
      return future;
    }).when(workerClient).fetchClusterByStatisticsSnapshot(any(), any(), anyInt());

    // Cause a worker to fail instead of returning the result
    doAnswer(invocation -> {
      latch.countDown();
      return Futures.immediateFailedFuture(new InterruptedException("interrupted"));
    }).when(workerClient).fetchClusterByStatisticsSnapshot(eq("2"), any(), anyInt());

    CompletableFuture<Either<Long, ClusterByPartitions>> eitherCompletableFuture = target.submitFetcherTask(
        completeKeyStatisticsInformation,
        ImmutableList.of("1", "2", "3", "4", "5"),
        stageDefinition
    );

    // Assert that the final result is failed and all other task futures are also cancelled.
    Assert.assertThrows(CompletionException.class, eitherCompletableFuture::join);
    Assert.assertTrue(eitherCompletableFuture.isCompletedExceptionally());
    for (ListenableFuture<ClusterByStatisticsSnapshot> snapshotFuture : futureQueue) {
      verify(snapshotFuture, times(1)).cancel(eq(true));
    }
  }

  @Test
  public void test_submitFetcherTask_parallelFetch_mergePerformedCorrectly()
      throws ExecutionException, InterruptedException
  {
    // Store snapshots in a queue
    final Queue<ClusterByStatisticsSnapshot> snapshotQueue = new ConcurrentLinkedQueue<>();
    final CountDownLatch latch = new CountDownLatch(5);

    final Either<Long, ClusterByPartitions> expectedPartitions = mock(Either.class);
    doReturn(expectedPartitions).when(stageDefinition).generatePartitionsForShuffle(eq(mergedClusterByStatisticsCollector));

    // When fetching snapshots, return a mock and add it to queue
    doAnswer(invocation -> {
      ClusterByStatisticsSnapshot snapshot = mock(ClusterByStatisticsSnapshot.class);
      snapshotQueue.add(snapshot);
      latch.countDown();
      return Futures.immediateFuture(snapshot);
    }).when(workerClient).fetchClusterByStatisticsSnapshot(any(), any(), anyInt());

    CompletableFuture<Either<Long, ClusterByPartitions>> eitherCompletableFuture = target.submitFetcherTask(
        completeKeyStatisticsInformation,
        ImmutableList.of("1", "2", "3", "4", "5"),
        stageDefinition
    );

    // Assert that the final result is complete and all other sketches returned have been merged.
    eitherCompletableFuture.join();
    Assert.assertTrue(eitherCompletableFuture.isDone() && !eitherCompletableFuture.isCompletedExceptionally());
    for (ClusterByStatisticsSnapshot snapshot : snapshotQueue) {
      verify(mergedClusterByStatisticsCollector, times(1)).addAll(eq(snapshot));
    }
    Assert.assertEquals(expectedPartitions, eitherCompletableFuture.get());
  }
}
