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
import org.apache.druid.frame.key.ClusterByPartition;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.msq.statistics.ClusterByStatisticsWorkerReport;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Queues up fetching sketches from workers and progressively generates partitions boundaries.
 */
public class WorkerSketchFetcher
{
  private static final int DEFAULT_THREAD_COUNT = 10;
  private static final long BYTES_THRESHOLD = 1_000_000_000L;
  private static final long WORKER_THRESHOLD = 100;

  private final boolean forceNonSequentialMerging;
  private final WorkerClient workerClient;
  private final ExecutorService executorService;

  public WorkerSketchFetcher(WorkerClient workerClient, boolean forceNonSequentialMerging)
  {
    this.workerClient = workerClient;
    this.forceNonSequentialMerging = forceNonSequentialMerging;
    this.executorService = Executors.newFixedThreadPool(DEFAULT_THREAD_COUNT);
  }

  /**
   * Submits a request to fetch and generate partitions for the given worker report and returns a future for it. It
   * decides based on the report if it should fetch sketches one by one or together.
   */
  public CompletableFuture<Either<Long, ClusterByPartitions>> submitFetcherTask(
      ClusterByStatisticsWorkerReport workerReport,
      List<String> workerTaskIds,
      StageDefinition stageDefinition
  )
  {
    ClusterBy clusterBy = stageDefinition.getClusterBy();

    if (forceNonSequentialMerging || clusterBy.getBucketByCount() == 0) {
      return inMemoryFullSketchMerging(stageDefinition, workerTaskIds);
    } else if (stageDefinition.getMaxWorkerCount() > WORKER_THRESHOLD || workerReport.getBytesRetained() > BYTES_THRESHOLD) {
      return inMemoryFullSketchMerging(stageDefinition, workerTaskIds);
    } else {
      return sequentialTimeChunkMerging(workerReport, stageDefinition, workerTaskIds);
    }
  }

  /**
   * Fetches the full {@link ClusterByStatisticsCollector} from all workers and generates partition boundaries from them.
   * This is faster than fetching them timechunk by timechunk but the collector will be downsampled till it can fit
   * on the controller, resulting in less accurate partition boundries.
   */
  private CompletableFuture<Either<Long, ClusterByPartitions>> inMemoryFullSketchMerging(
      StageDefinition stageDefinition,
      List<String> workerTaskIds
  )
  {
    CompletableFuture<Either<Long, ClusterByPartitions>> partitionFuture = new CompletableFuture<>();

    final ClusterByStatisticsCollector mergedStatisticsCollector = stageDefinition.createResultKeyStatisticsCollector();
    final int workerCount = stageDefinition.getMaxWorkerCount();
    final Set<Integer> finishedWorkers = new HashSet<>();

    for (int i = 0; i < workerCount; i++) {
      final int workerNo = i;
      executorService.submit(() -> {
        try {
          ClusterByStatisticsSnapshot clusterByStatisticsSnapshot = workerClient.fetchClusterByStatisticsSnapshot(
              workerTaskIds.get(workerNo),
              stageDefinition.getId().getQueryId(),
              stageDefinition.getStageNumber()
          );

          // If the future already failed for some reason, stop the task.
          if (partitionFuture.isDone()) {
            return;
          }

          synchronized (mergedStatisticsCollector) {
            mergedStatisticsCollector.addAll(clusterByStatisticsSnapshot);
            finishedWorkers.add(workerNo);

            if (finishedWorkers.size() == workerCount) {
              partitionFuture.complete(stageDefinition.generatePartitionsForShuffle(mergedStatisticsCollector));
            }
          }
        }
        catch (Exception e) {
          partitionFuture.completeExceptionally(e);
        }
      });
    }
    return partitionFuture;
  }

  /**
   * Fetches cluster statistics from all workers and generates partition boundaries from them one time chunk at a time.
   * This takes longer due to the overhead of fetching sketches, however, this prevents any loss in accuracy from
   * downsampling on the controller.
   */
  private CompletableFuture<Either<Long, ClusterByPartitions>> sequentialTimeChunkMerging(
      ClusterByStatisticsWorkerReport workerReport,
      StageDefinition stageDefinition,
      List<String> workerTaskIds
  )
  {
    SequentialFetchStage sequentialFetchStage = new SequentialFetchStage(
        stageDefinition,
        workerTaskIds,
        workerReport.getTimeSegmentVsWorkerIdMap().entrySet().iterator()
    );
    sequentialFetchStage.submitFetchingTasksForNextTimeChunk();
    return sequentialFetchStage.getPartitionFuture();
  }

  private class SequentialFetchStage
  {
    private final StageDefinition stageDefinition;
    private final List<String> workerTaskIds;
    private final Iterator<Map.Entry<Long, Set<Integer>>> timeSegmentVsWorkerIdIterator;
    private final CompletableFuture<Either<Long, ClusterByPartitions>> partitionFuture;
    private final List<ClusterByPartition> finalPartitionBoundries;

    public SequentialFetchStage(
        StageDefinition stageDefinition,
        List<String> workerTaskIds,
        Iterator<Map.Entry<Long, Set<Integer>>> timeSegmentVsWorkerIdIterator
    )
    {
      this.finalPartitionBoundries = new ArrayList<>();
      this.stageDefinition = stageDefinition;
      this.workerTaskIds = workerTaskIds;
      this.timeSegmentVsWorkerIdIterator = timeSegmentVsWorkerIdIterator;
      this.partitionFuture = new CompletableFuture<>();
    }

    public void submitFetchingTasksForNextTimeChunk()
    {
      if (!timeSegmentVsWorkerIdIterator.hasNext()) {
        partitionFuture.complete(Either.value(new ClusterByPartitions(finalPartitionBoundries)));
      } else {
        Map.Entry<Long, Set<Integer>> entry = timeSegmentVsWorkerIdIterator.next();
        Long timeChunk = entry.getKey();
        Set<Integer> workerIdsWithTimeChunk = entry.getValue();
        ClusterByStatisticsCollector mergedStatisticsCollector = stageDefinition.createResultKeyStatisticsCollector();
        Set<Integer> finishedWorkers = new HashSet<>();

        for (int workerNo : workerIdsWithTimeChunk) {
          executorService.submit(() -> {
            try {
              ClusterByStatisticsSnapshot singletonStatisticsSnapshot =
                  workerClient.fetchSingletonStatisticsSnapshot(
                      workerTaskIds.get(workerNo),
                      stageDefinition.getId().getQueryId(),
                      stageDefinition.getStageNumber(),
                      timeChunk
                  );
              // If the future already failed for some reason, stop the task.
              if (partitionFuture.isDone()) {
                return;
              }
              synchronized (mergedStatisticsCollector) {
                mergedStatisticsCollector.addAll(singletonStatisticsSnapshot);
                finishedWorkers.add(workerNo);

                if (finishedWorkers.size() == workerIdsWithTimeChunk.size()) {
                  Either<Long, ClusterByPartitions> longClusterByPartitionsEither =
                      stageDefinition.generatePartitionsForShuffle(mergedStatisticsCollector);

                  if (longClusterByPartitionsEither.isError()) {
                    partitionFuture.complete(longClusterByPartitionsEither);
                  }

                  List<ClusterByPartition> timeSketchPartitions =
                      stageDefinition.generatePartitionsForShuffle(mergedStatisticsCollector)
                                     .valueOrThrow()
                                     .ranges();
                  abutAndAppendPartitionBoundries(finalPartitionBoundries, timeSketchPartitions);

                  submitFetchingTasksForNextTimeChunk();
                }
              }
            }
            catch (Exception e) {
              partitionFuture.completeExceptionally(e);
            }
          });
        }
      }
    }

    private void abutAndAppendPartitionBoundries(
        List<ClusterByPartition> finalPartitionBoundries,
        List<ClusterByPartition> timeSketchPartitions
    )
    {
      if (!finalPartitionBoundries.isEmpty()) {
        ClusterByPartition clusterByPartition = finalPartitionBoundries.remove(finalPartitionBoundries.size() - 1);
        finalPartitionBoundries.add(new ClusterByPartition(clusterByPartition.getStart(), timeSketchPartitions.get(0).getStart()));
      }
      finalPartitionBoundries.addAll(timeSketchPartitions);
    }

    public CompletableFuture<Either<Long, ClusterByPartitions>> getPartitionFuture()
    {
      return partitionFuture;
    }
  }
}
