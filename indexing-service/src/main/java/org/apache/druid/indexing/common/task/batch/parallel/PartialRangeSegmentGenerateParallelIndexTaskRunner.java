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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.Partitions;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.joda.time.Interval;

import java.util.Map;

/**
 * {@link ParallelIndexTaskRunner} for the phase to create range partitioned segments in multi-phase parallel indexing.
 *
 * @see PartialHashSegmentMergeParallelIndexTaskRunner
 */
class PartialRangeSegmentGenerateParallelIndexTaskRunner
    extends InputSourceSplitParallelIndexTaskRunner<PartialRangeSegmentGenerateTask, GeneratedPartitionsReport<PartitionMetadata>>
{
  private final IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> taskClientFactory;
  private final AppenderatorsManager appenderatorsManager;
  private final Map<Interval, Partitions> intervalToPartitions;

  PartialRangeSegmentGenerateParallelIndexTaskRunner(
      TaskToolbox toolbox,
      String taskId,
      String groupId,
      ParallelIndexIngestionSpec ingestionSchema,
      Map<String, Object> context,
      IndexingServiceClient indexingServiceClient,
      Map<Interval, Partitions> intervalToPartitions
  )
  {
    this(
        toolbox,
        taskId,
        groupId,
        ingestionSchema,
        context,
        indexingServiceClient,
        intervalToPartitions,
        null,
        null
    );
  }

  @VisibleForTesting
  PartialRangeSegmentGenerateParallelIndexTaskRunner(
      TaskToolbox toolbox,
      String taskId,
      String groupId,
      ParallelIndexIngestionSpec ingestionSchema,
      Map<String, Object> context,
      IndexingServiceClient indexingServiceClient,
      Map<Interval, Partitions> intervalToPartitions,
      IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> taskClientFactory,
      AppenderatorsManager appenderatorsManager
  )
  {
    super(toolbox, taskId, groupId, ingestionSchema, context, indexingServiceClient);
    this.taskClientFactory = taskClientFactory;
    this.appenderatorsManager = appenderatorsManager;
    this.intervalToPartitions = intervalToPartitions;
  }

  @Override
  public String getName()
  {
    return PartialRangeSegmentGenerateTask.TYPE;
  }

  @Override
  SubTaskSpec<PartialRangeSegmentGenerateTask> createSubTaskSpec(
      String id,
      String groupId,
      String supervisorTaskId,
      Map<String, Object> context,
      InputSplit split,
      ParallelIndexIngestionSpec subTaskIngestionSpec,
      IndexingServiceClient indexingServiceClient
  )
  {
    return new SubTaskSpec<PartialRangeSegmentGenerateTask>(
        id,
        groupId,
        supervisorTaskId,
        context,
        split
    )
    {
      @Override
      public PartialRangeSegmentGenerateTask newSubTask(int numAttempts)
      {
        return new PartialRangeSegmentGenerateTask(
            null,
            groupId,
            null,
            supervisorTaskId,
            numAttempts,
            subTaskIngestionSpec,
            context,
            intervalToPartitions,
            indexingServiceClient,
            taskClientFactory,
            appenderatorsManager
        );
      }
    };
  }
}
