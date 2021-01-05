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

import org.apache.druid.data.input.InputSplit;
import org.apache.druid.indexing.common.TaskToolbox;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * {@link ParallelIndexTaskRunner} for the phase to create hash partitioned segments in multi-phase parallel indexing.
 */
class PartialHashSegmentGenerateParallelIndexTaskRunner
    extends InputSourceSplitParallelIndexTaskRunner<PartialHashSegmentGenerateTask, GeneratedPartitionsReport<GenericPartitionStat>>
{
  private static final String PHASE_NAME = "partial segment generation";

  @Nullable
  private final Map<Interval, Integer> intervalToNumShardsOverride;

  PartialHashSegmentGenerateParallelIndexTaskRunner(
      TaskToolbox toolbox,
      String taskId,
      String groupId,
      ParallelIndexIngestionSpec ingestionSchema,
      Map<String, Object> context,
      @Nullable Map<Interval, Integer> intervalToNumShardsOverride
  )
  {
    super(toolbox, taskId, groupId, ingestionSchema, context);
    this.intervalToNumShardsOverride = intervalToNumShardsOverride;
  }

  @Override
  public String getName()
  {
    return PHASE_NAME;
  }

  @Override
  SubTaskSpec<PartialHashSegmentGenerateTask> createSubTaskSpec(
      String id,
      String groupId,
      String supervisorTaskId,
      Map<String, Object> context,
      InputSplit split,
      ParallelIndexIngestionSpec subTaskIngestionSpec
  )
  {
    return new SubTaskSpec<PartialHashSegmentGenerateTask>(
        id,
        groupId,
        supervisorTaskId,
        context,
        split
    )
    {
      @Override
      public PartialHashSegmentGenerateTask newSubTask(int numAttempts)
      {
        return new PartialHashSegmentGenerateTask(
            null,
            groupId,
            null,
            supervisorTaskId,
            numAttempts,
            subTaskIngestionSpec,
            context,
            intervalToNumShardsOverride
        );
      }
    };
  }
}
