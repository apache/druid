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
import org.apache.druid.msq.statistics.CompleteKeyStatisticsInformation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * Queues up fetching sketches from workers and progressively generates partitions boundaries.
 */
public class WorkerSketchFetcher
{
  private static final int DEFAULT_THREAD_COUNT = 4;
  // If the combined size of worker sketches is more than this threshold, SEQUENTIAL merging mode is used.
  private static final long BYTES_THRESHOLD = 1_000_000_000L;
  // If there are more workers than this threshold, SEQUENTIAL merging mode is used.
  private static final long WORKER_THRESHOLD = 100;

  private final ClusterStatisticsMergeMode clusterStatisticsMergeMode;
  private final int statisticsMaxRetainedBytes;
  private final WorkerClient workerClient;
  private final ExecutorService executorService;

  public WorkerSketchFetcher(WorkerClient workerClient, ClusterStatisticsMergeMode clusterStatisticsMergeMode, int statisticsMaxRetainedBytes)
  {
    this.workerClient = workerClient;
    this.clusterStatisticsMergeMode = clusterStatisticsMergeMode;
    this.executorService = Executors.newFixedThreadPool(DEFAULT_THREAD_COUNT);
    this.statisticsMaxRetainedBytes = statisticsMaxRetainedBytes;
  }

  /**
   * Submits a request to fetch and generate partitions for the given worker statistics and returns a future for it. It
   * decides based on the statistics if it should fetch sketches one by one or together.
   */
  public CompletableFuture<Either<Long, ClusterByPartitions>> submitFetcherTask(
      CompleteKeyStatisticsInformation completeKeyStatisticsInformation,
      List<String> workerTaskIds,
      StageDefinition stageDefinition
  )
  {
    ClusterBy clusterBy = stageDefinition.getClusterBy();

    switch (clusterStatisticsMergeMode) {
      case SEQUENTIAL:
        return sequentialTimeChunkMerging(completeKeyStatisticsInformation, stageDefinition, workerTaskIds);
      case PARALLEL:
        return inMemoryFullSketchMerging(stageDefinition, workerTaskIds);
      case AUTO:
        if (clusterBy.getBucketByCount() == 0) {
          // If there is no time cluserting, there is no scope for sequential merge
          return inMemoryFullSketchMerging(stageDefinition, workerTaskIds);
        } else if (stageDefinition.getMaxWorkerCount() > WORKER_THRESHOLD || completeKeyStatisticsInformation.getBytesRetained() > BYTES_THRESHOLD) {
          return sequentialTimeChunkMerging(completeKeyStatisticsInformation, stageDefinition, workerTaskIds);
        } else {
          return inMemoryFullSketchMerging(stageDefinition, workerTaskIds);
        }
      default:
        throw new IllegalStateException("No fetching strategy found for mode: " + clusterStatisticsMergeMode);
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

    // Create a new key statistics collector to merge worker sketches into
    final ClusterByStatisticsCollector mergedStatisticsCollector =
        stageDefinition.createResultKeyStatisticsCollector(statisticsMaxRetainedBytes);
    final int workerCount = workerTaskIds.size();
    // Guarded by synchronized mergedStatisticsCollector
    final Set<Integer> finishedWorkers = new HashSet<>();

    // Submit a task for each worker to fetch statistics
    IntStream.range(0, workerCount).forEach(workerNo -> {
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
    });
    return partitionFuture;
  }

  /**
   * Fetches cluster statistics from all workers and generates partition boundaries from them one time chunk at a time.
   * This takes longer due to the overhead of fetching sketches, however, this prevents any loss in accuracy from
   * downsampling on the controller.
   */
  private CompletableFuture<Either<Long, ClusterByPartitions>> sequentialTimeChunkMerging(
      CompleteKeyStatisticsInformation completeKeyStatisticsInformation,
      StageDefinition stageDefinition,
      List<String> workerTaskIds
  )
  {
    SequentialFetchStage sequentialFetchStage = new SequentialFetchStage(
        stageDefinition,
        workerTaskIds,
        completeKeyStatisticsInformation.getTimeSegmentVsWorkerMap().entrySet().iterator()
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
    // Final sorted list of partition boundaries. This is appended to after statistics for each time chunk are gathered.
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
        // Time chunk for which partition boundries are going to be generated for
        Long timeChunk = entry.getKey();
        Set<Integer> workerIdsWithTimeChunk = entry.getValue();
        // Create a new key statistics collector to merge worker sketches into
        ClusterByStatisticsCollector mergedStatisticsCollector =
            stageDefinition.createResultKeyStatisticsCollector(statisticsMaxRetainedBytes);
        // Guarded by synchronized mergedStatisticsCollector
        Set<Integer> finishedWorkers = new HashSet<>();

        // Submits a task for every worker which has a certain time chunk
        for (int workerNo : workerIdsWithTimeChunk) {
          executorService.submit(() -> {
            try {
              ClusterByStatisticsSnapshot snapshotForTimeChunk =
                  workerClient.fetchClusterByStatisticsSnapshotForTimeChunk(
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
                mergedStatisticsCollector.addAll(snapshotForTimeChunk);
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

    /**
     * Takes a list of sorted {@link ClusterByPartitions} {@param timeSketchPartitions} and adds it to a sorted list
     * {@param finalPartitionBoundries}. If {@param finalPartitionBoundries} is not empty, the end time of the last
     * partition of {@param finalPartitionBoundries} is changed to abut with the starting time of the first partition
     * of {@param timeSketchPartitions}.
     *
     * This is used to make the partitions generated continuous.
     */
    private void abutAndAppendPartitionBoundries(
        List<ClusterByPartition> finalPartitionBoundries,
        List<ClusterByPartition> timeSketchPartitions
    )
    {
      if (!finalPartitionBoundries.isEmpty()) {
        // Stitch up the end time of the last partition with the start time of the first partition.
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
