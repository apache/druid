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

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.msq.statistics.ClusterByStatisticsWorkerReport;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Queues up fetching sketches from workers and progressively generates partitions boundaries.
 * TODO: still in progress. Some cleanup required as things will likely move around with sequantial merging.
 */
public class WorkerSketchFetcher
{
  private static final int DEFAULT_THREAD_COUNT = 10;

  private final WorkerClient workerClient;
  private final ExecutorService executorService;

  public WorkerSketchFetcher(WorkerClient workerClient)
  {
    this.workerClient = workerClient;
    this.executorService = Executors.newFixedThreadPool(DEFAULT_THREAD_COUNT);
  }

  public CompletableFuture<Either<Long, ClusterByPartitions>> submitFetcherTask(
      ClusterByStatisticsWorkerReport workerReport,
      List<String> workerTaskIds,
      StageDefinition stageDefinition)
  {
    CompletableFuture<Either<Long, ClusterByPartitions>> partitionFuture = new CompletableFuture<>();
    ClusterByStatisticsCollector finalClusterByStatisticsCollector = stageDefinition.createResultKeyStatisticsCollector();

    int workerNumber = stageDefinition.getMaxWorkerCount();
    IntSet finishedWorkers = new IntAVLTreeSet();

    for (int i = 0; i < workerNumber; i++) {
      final int workerNo = i;
      executorService.submit(() -> {
        try {
          ClusterByStatisticsSnapshot clusterByStatisticsSnapshot = workerClient.fetchClusterByStatisticsSnapshot(
              workerTaskIds.get(workerNo),
              stageDefinition.getId().getQueryId(),
              stageDefinition.getStageNumber()
          );
          // If the future already failed for some reason, skip ahead.
          if (!partitionFuture.isDone()) {
            finalClusterByStatisticsCollector.addAll(clusterByStatisticsSnapshot);
            finishedWorkers.add(workerNo);
            if (finishedWorkers.size() == workerNumber) {
              partitionFuture.complete(stageDefinition.generatePartitionsForShuffle(finalClusterByStatisticsCollector));
            }
          }
        }
        catch (Exception e) {
          finalClusterByStatisticsCollector.clear();
          partitionFuture.completeExceptionally(e);
        }
      });
    }
    return partitionFuture;
  }
}
