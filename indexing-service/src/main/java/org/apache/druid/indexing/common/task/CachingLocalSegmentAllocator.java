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

package org.apache.druid.indexing.common.task;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.SurrogateAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.task.batch.parallel.SupervisorTaskAccess;
import org.apache.druid.indexing.common.task.batch.partition.CompletePartitionAnalysis;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.partition.BucketNumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Allocates all necessary segments locally at the beginning and reuses them.
 */
public class CachingLocalSegmentAllocator implements SegmentAllocatorForBatch
{
  private final String dataSource;
  private final Map<String, Pair<Interval, BucketNumberedShardSpec>> sequenceNameToBucket;
  private final Function<Interval, String> versionFinder;
  private final NonLinearlyPartitionedSequenceNameFunction sequenceNameFunction;
  private final boolean isParallel;

  private final Map<String, SegmentIdWithShardSpec> sequenceNameToSegmentId = new HashMap<>();
  private final Object2IntMap<Interval> intervalToNextPartitionId = new Object2IntOpenHashMap<>();

  CachingLocalSegmentAllocator(
      TaskToolbox toolbox,
      String dataSource,
      String taskId,
      GranularitySpec granularitySpec,
      @Nullable SupervisorTaskAccess supervisorTaskAccess,
      CompletePartitionAnalysis<?, ?> partitionAnalysis
  ) throws IOException
  {
    this.dataSource = dataSource;
    this.sequenceNameToBucket = new HashMap<>();

    final TaskAction<List<TaskLock>> action;
    if (supervisorTaskAccess == null) {
      action = new LockListAction();
      isParallel = false;
    } else {
      action = new SurrogateAction<>(supervisorTaskAccess.getSupervisorTaskId(), new LockListAction());
      isParallel = true;
    }

    this.versionFinder = createVersionFinder(toolbox, action);
    final Map<Interval, List<BucketNumberedShardSpec<?>>> intervalToShardSpecs = partitionAnalysis.createBuckets(
        toolbox
    );

    sequenceNameFunction = new NonLinearlyPartitionedSequenceNameFunction(
        taskId,
        new ShardSpecs(intervalToShardSpecs, granularitySpec.getQueryGranularity())
    );

    for (Entry<Interval, List<BucketNumberedShardSpec<?>>> entry : intervalToShardSpecs.entrySet()) {
      final Interval interval = entry.getKey();
      final List<BucketNumberedShardSpec<?>> buckets = entry.getValue();

      buckets.forEach(bucket -> {
        sequenceNameToBucket.put(sequenceNameFunction.getSequenceName(interval, bucket), Pair.of(interval, bucket));
      });
    }
  }

  static Function<Interval, String> createVersionFinder(
      TaskToolbox toolbox,
      TaskAction<List<TaskLock>> lockListAction
  ) throws IOException
  {
    final Map<Interval, String> intervalToVersion =
        toolbox.getTaskActionClient()
               .submit(lockListAction)
               .stream()
               .collect(Collectors.toMap(TaskLock::getInterval, TaskLock::getVersion));

    return interval -> findVersion(intervalToVersion, interval);
  }

  private static String findVersion(Map<Interval, String> intervalToVersion, Interval interval)
  {
    return intervalToVersion.entrySet().stream()
                            .filter(entry -> entry.getKey().contains(interval))
                            .map(Entry::getValue)
                            .findFirst()
                            .orElseThrow(() -> new ISE("Cannot find a version for interval[%s]", interval));
  }

  @Override
  public SegmentIdWithShardSpec allocate(
      InputRow row,
      String sequenceName,
      String previousSegmentId,
      boolean skipSegmentLineageCheck
  )
  {
    return sequenceNameToSegmentId.computeIfAbsent(
        sequenceName,
        k -> {
          final Pair<Interval, BucketNumberedShardSpec> pair = Preconditions.checkNotNull(
              sequenceNameToBucket.get(sequenceName),
              "Missing bucket for sequence[%s]",
              sequenceName
          );
          final Interval interval = pair.lhs;
          // Determines the partitionId if this segment allocator is used by the single-threaded task.
          // In parallel ingestion, the partitionId is determined in the supervisor task.
          // See ParallelIndexSupervisorTask.groupGenericPartitionLocationsPerPartition().
          // This code... isn't pretty, but should be simple enough to understand.
          final ShardSpec shardSpec = isParallel
                                      ? pair.rhs
                                      : pair.rhs.convert(
                                          intervalToNextPartitionId.computeInt(
                                              interval,
                                              (i, nextPartitionId) -> nextPartitionId == null ? 0 : nextPartitionId + 1
                                          )
                                      );
          final String version = versionFinder.apply(interval);
          return new SegmentIdWithShardSpec(dataSource, interval, version, shardSpec);
        }
    );
  }

  @Override
  public SequenceNameFunction getSequenceNameFunction()
  {
    return sequenceNameFunction;
  }
}
