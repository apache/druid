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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernel;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.msq.statistics.CompleteKeyStatisticsInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.mock;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class WorkerSketchFetcherTest
{
  @Mock
  private CompleteKeyStatisticsInformation completeKeyStatisticsInformation;

  @Mock
  private WorkerManager workerManager;

  @Mock
  private ControllerQueryKernel kernel;

  @Mock
  private StageDefinition stageDefinition;
  @Mock
  private WorkerClient workerClient;
  private AutoCloseable mocks;
  private WorkerSketchFetcher target;

  private static final String TASK_0 = "task-worker0_0";
  private static final String TASK_1 = "task-worker1_0";
  private static final String TASK_2 = "task-worker2_1";
  private static final List<String> TASK_IDS = ImmutableList.of(TASK_0, TASK_1, TASK_2);

  @Before
  public void setUp()
  {
    mocks = MockitoAnnotations.openMocks(this);
    doReturn(StageId.fromString("1_1")).when(stageDefinition).getId();

    doReturn(ImmutableSortedMap.of(123L, ImmutableSet.of(1, 2))).when(completeKeyStatisticsInformation)
                                                                .getTimeSegmentVsWorkerMap();

    doReturn(0).when(workerManager).getWorkerNumber(TASK_0);
    doReturn(1).when(workerManager).getWorkerNumber(TASK_1);
    doReturn(2).when(workerManager).getWorkerNumber(TASK_2);
    doReturn(true).when(workerManager).isWorkerActive(any());
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
  public void test_submitFetcherTask_parallelFetch() throws InterruptedException
  {

    final CountDownLatch latch = new CountDownLatch(TASK_IDS.size());

    target = spy(new WorkerSketchFetcher(workerClient, workerManager, true));

    // When fetching snapshots, return a mock and add it to queue
    doAnswer(invocation -> {
      ClusterByStatisticsSnapshot snapshot = mock(ClusterByStatisticsSnapshot.class);
      return Futures.immediateFuture(snapshot);
    }).when(workerClient).fetchClusterByStatisticsSnapshot(any(), any());

    target.inMemoryFullSketchMerging((kernelConsumer) -> {
      kernelConsumer.accept(kernel);
      latch.countDown();
    }, stageDefinition.getId(), ImmutableSet.copyOf(TASK_IDS), ((queryKernel, integer, msqFault) -> {}));

    Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));

  }

  @Test
  public void test_submitFetcherTask_sequentialFetch() throws InterruptedException
  {
    doReturn(true).when(completeKeyStatisticsInformation).isComplete();
    final CountDownLatch latch = new CountDownLatch(TASK_IDS.size());

    target = spy(new WorkerSketchFetcher(workerClient, workerManager, true));

    // When fetching snapshots, return a mock and add it to queue
    doAnswer(invocation -> {
      ClusterByStatisticsSnapshot snapshot = mock(ClusterByStatisticsSnapshot.class);
      return Futures.immediateFuture(snapshot);
    }).when(workerClient).fetchClusterByStatisticsSnapshotForTimeChunk(any(), any(), anyLong());

    target.sequentialTimeChunkMerging(
        (kernelConsumer) -> {
          kernelConsumer.accept(kernel);
          latch.countDown();
        },
        completeKeyStatisticsInformation,
        stageDefinition.getId(),
        ImmutableSet.copyOf(TASK_IDS),
        ((queryKernel, integer, msqFault) -> {})
    );

    Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));

  }

  @Test
  public void test_sequentialMerge_nonCompleteInformation()
  {

    doReturn(false).when(completeKeyStatisticsInformation).isComplete();
    target = spy(new WorkerSketchFetcher(workerClient, workerManager, true));
    Assert.assertThrows(ISE.class, () -> target.sequentialTimeChunkMerging(
        (ignore) -> {},
        completeKeyStatisticsInformation,
        stageDefinition.getId(),
        ImmutableSet.of(""),
        ((queryKernel, integer, msqFault) -> {})
    ));
  }

  @Test
  public void test_inMemoryRetryEnabled_retryInvoked() throws InterruptedException
  {
    final CountDownLatch latch = new CountDownLatch(TASK_IDS.size());

    target = spy(new WorkerSketchFetcher(workerClient, workerManager, true));

    workersWithFailedFetchParallel(ImmutableSet.of(TASK_1));

    CountDownLatch retryLatch = new CountDownLatch(1);
    target.inMemoryFullSketchMerging(
        (kernelConsumer) -> {
          kernelConsumer.accept(kernel);
          latch.countDown();
        },
        stageDefinition.getId(),
        ImmutableSet.copyOf(TASK_IDS),
        ((queryKernel, integer, msqFault) -> {
          if (integer.equals(1) && msqFault.getErrorMessage().contains(TASK_1)) {
            retryLatch.countDown();
          }
        })
    );

    Assert.assertTrue(latch.await(500, TimeUnit.SECONDS));
    Assert.assertTrue(retryLatch.await(500, TimeUnit.SECONDS));
  }

  @Test
  public void test_SequentialRetryEnabled_retryInvoked() throws InterruptedException
  {
    doReturn(true).when(completeKeyStatisticsInformation).isComplete();
    final CountDownLatch latch = new CountDownLatch(TASK_IDS.size());

    target = spy(new WorkerSketchFetcher(workerClient, workerManager, true));

    workersWithFailedFetchSequential(ImmutableSet.of(TASK_1));
    CountDownLatch retryLatch = new CountDownLatch(1);
    target.sequentialTimeChunkMerging(
        (kernelConsumer) -> {
          kernelConsumer.accept(kernel);
          latch.countDown();
        },
        completeKeyStatisticsInformation,
        stageDefinition.getId(),
        ImmutableSet.copyOf(TASK_IDS),
        ((queryKernel, integer, msqFault) -> {
          if (integer.equals(1) && msqFault.getErrorMessage().contains(TASK_1)) {
            retryLatch.countDown();
          }
        })
    );

    Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
    Assert.assertTrue(retryLatch.await(5, TimeUnit.SECONDS));
  }

  @Test
  public void test_InMemoryRetryDisabled_multipleFailures() throws InterruptedException
  {

    target = spy(new WorkerSketchFetcher(workerClient, workerManager, false));

    workersWithFailedFetchParallel(ImmutableSet.of(TASK_1, TASK_0));

    try {
      target.inMemoryFullSketchMerging(
          (kernelConsumer) -> kernelConsumer.accept(kernel),
          stageDefinition.getId(),
          ImmutableSet.copyOf(TASK_IDS),
          ((queryKernel, integer, msqFault) -> {
            throw new ISE("Should not be here");
          })
      );
    }
    catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Task fetch failed"));
    }

    while (!target.executorService.isShutdown()) {
      Thread.sleep(100);
    }
    Assert.assertTrue((target.getError().getMessage().contains("Task fetch failed")));

  }

  @Test
  public void test_InMemoryRetryDisabled_singleFailure() throws InterruptedException
  {

    target = spy(new WorkerSketchFetcher(workerClient, workerManager, false));

    workersWithFailedFetchParallel(ImmutableSet.of(TASK_1));

    try {
      target.inMemoryFullSketchMerging(
          (kernelConsumer) -> kernelConsumer.accept(kernel),
          stageDefinition.getId(),
          ImmutableSet.copyOf(TASK_IDS),
          ((queryKernel, integer, msqFault) -> {
            throw new ISE("Should not be here");
          })
      );
    }
    catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Task fetch failed"));
    }

    while (!target.executorService.isShutdown()) {
      Thread.sleep(100);
    }
    Assert.assertTrue((target.getError().getMessage().contains("Task fetch failed")));

  }


  @Test
  public void test_SequentialRetryDisabled_multipleFailures() throws InterruptedException
  {

    doReturn(true).when(completeKeyStatisticsInformation).isComplete();
    target = spy(new WorkerSketchFetcher(workerClient, workerManager, false));

    workersWithFailedFetchSequential(ImmutableSet.of(TASK_1, TASK_0));

    try {
      target.sequentialTimeChunkMerging(
          (kernelConsumer) -> {
            kernelConsumer.accept(kernel);
          },
          completeKeyStatisticsInformation,
          stageDefinition.getId(),
          ImmutableSet.copyOf(TASK_IDS),
          ((queryKernel, integer, msqFault) -> {
            throw new ISE("Should not be here");
          })
      );
    }
    catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Task fetch failed"));
    }

    while (!target.executorService.isShutdown()) {
      Thread.sleep(100);
    }
    Assert.assertTrue(target.getError().getMessage().contains("Task fetch failed"));

  }

  @Test
  public void test_SequentialRetryDisabled_singleFailure() throws InterruptedException
  {
    doReturn(true).when(completeKeyStatisticsInformation).isComplete();
    target = spy(new WorkerSketchFetcher(workerClient, workerManager, false));

    workersWithFailedFetchSequential(ImmutableSet.of(TASK_1));

    try {
      target.sequentialTimeChunkMerging(
          (kernelConsumer) -> {
            kernelConsumer.accept(kernel);
          },
          completeKeyStatisticsInformation,
          stageDefinition.getId(),
          ImmutableSet.copyOf(TASK_IDS),
          ((queryKernel, integer, msqFault) -> {
            throw new ISE("Should not be here");
          })
      );
    }
    catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Task fetch failed"));
    }

    while (!target.executorService.isShutdown()) {
      Thread.sleep(100);
    }
    Assert.assertTrue(target.getError().getMessage().contains(TASK_1));

  }


  private void workersWithFailedFetchSequential(Set<String> failedTasks)
  {
    doAnswer(invocation -> {
      ClusterByStatisticsSnapshot snapshot = mock(ClusterByStatisticsSnapshot.class);
      if (failedTasks.contains((String) invocation.getArgument(0))) {
        return Futures.immediateFailedFuture(new Exception("Task fetch failed :" + invocation.getArgument(0)));
      }
      return Futures.immediateFuture(snapshot);
    }).when(workerClient).fetchClusterByStatisticsSnapshotForTimeChunk(any(), any(), anyLong());
  }

  private void workersWithFailedFetchParallel(Set<String> failedTasks)
  {
    doAnswer(invocation -> {
      ClusterByStatisticsSnapshot snapshot = mock(ClusterByStatisticsSnapshot.class);
      if (failedTasks.contains((String) invocation.getArgument(0))) {
        return Futures.immediateFailedFuture(new Exception("Task fetch failed :" + invocation.getArgument(0)));
      }
      return Futures.immediateFuture(snapshot);
    }).when(workerClient).fetchClusterByStatisticsSnapshot(any(), any());
  }

}
