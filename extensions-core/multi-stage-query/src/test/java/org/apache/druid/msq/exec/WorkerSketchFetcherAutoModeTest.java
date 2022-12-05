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

import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.statistics.CompleteKeyStatisticsInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class WorkerSketchFetcherAutoModeTest
{
  @Mock
  private CompleteKeyStatisticsInformation completeKeyStatisticsInformation;
  @Mock
  private StageDefinition stageDefinition;
  @Mock
  private ClusterBy clusterBy;
  private AutoCloseable mocks;
  private WorkerSketchFetcher target;

  @Before
  public void setUp()
  {
    mocks = MockitoAnnotations.openMocks(this);

    target = spy(new WorkerSketchFetcher(mock(WorkerClient.class), ClusterStatisticsMergeMode.AUTO, 300_000_000));
    // Don't actually try to fetch sketches
    doReturn(null).when(target).inMemoryFullSketchMerging(any(), any());
    doReturn(null).when(target).sequentialTimeChunkMerging(any(), any(), any());

    doReturn(StageId.fromString("1_1")).when(stageDefinition).getId();
    doReturn(clusterBy).when(stageDefinition).getClusterBy();
  }

  @After
  public void tearDown() throws Exception
  {
    mocks.close();
  }

  @Test
  public void test_submitFetcherTask_belowThresholds_ShouldBeParallel()
  {
    // Bytes below threshold
    doReturn(10.0).when(completeKeyStatisticsInformation).getBytesRetained();

    // Cluster by bucket count not 0
    doReturn(1).when(clusterBy).getBucketByCount();

    // Worker count below threshold
    doReturn(1).when(stageDefinition).getMaxWorkerCount();

    target.submitFetcherTask(completeKeyStatisticsInformation, Collections.emptyList(), stageDefinition);
    verify(target, times(1)).inMemoryFullSketchMerging(any(), any());
    verify(target, times(0)).sequentialTimeChunkMerging(any(), any(), any());
  }

  @Test
  public void test_submitFetcherTask_workerCountAboveThreshold_shouldBeSequential()
  {
    // Bytes below threshold
    doReturn(10.0).when(completeKeyStatisticsInformation).getBytesRetained();

    // Cluster by bucket count not 0
    doReturn(1).when(clusterBy).getBucketByCount();

    // Worker count below threshold
    doReturn((int) WorkerSketchFetcher.WORKER_THRESHOLD + 1).when(stageDefinition).getMaxWorkerCount();

    target.submitFetcherTask(completeKeyStatisticsInformation, Collections.emptyList(), stageDefinition);
    verify(target, times(0)).inMemoryFullSketchMerging(any(), any());
    verify(target, times(1)).sequentialTimeChunkMerging(any(), any(), any());
  }

  @Test
  public void test_submitFetcherTask_noClusterByColumns_shouldBeParallel()
  {
    // Bytes above threshold
    doReturn(WorkerSketchFetcher.BYTES_THRESHOLD + 10.0).when(completeKeyStatisticsInformation).getBytesRetained();

    // Cluster by bucket count 0
    doReturn(ClusterBy.none()).when(stageDefinition).getClusterBy();

    // Worker count above threshold
    doReturn((int) WorkerSketchFetcher.WORKER_THRESHOLD + 1).when(stageDefinition).getMaxWorkerCount();

    target.submitFetcherTask(completeKeyStatisticsInformation, Collections.emptyList(), stageDefinition);
    verify(target, times(1)).inMemoryFullSketchMerging(any(), any());
    verify(target, times(0)).sequentialTimeChunkMerging(any(), any(), any());
  }

  @Test
  public void test_submitFetcherTask_bytesRetainedAboveThreshold_shouldBeSequential()
  {
    // Bytes above threshold
    doReturn(WorkerSketchFetcher.BYTES_THRESHOLD + 10.0).when(completeKeyStatisticsInformation).getBytesRetained();

    // Cluster by bucket count not 0
    doReturn(1).when(clusterBy).getBucketByCount();

    // Worker count below threshold
    doReturn(1).when(stageDefinition).getMaxWorkerCount();

    target.submitFetcherTask(completeKeyStatisticsInformation, Collections.emptyList(), stageDefinition);
    verify(target, times(0)).inMemoryFullSketchMerging(any(), any());
    verify(target, times(1)).sequentialTimeChunkMerging(any(), any(), any());
  }
}
