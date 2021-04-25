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

import java.util.Map;

/**
 * {@link ParallelIndexTaskRunner} for the phase to determine cardinalities of dimension values in
 * multi-phase parallel indexing.
 */
class PartialDimensionCardinalityParallelIndexTaskRunner
    extends InputSourceSplitParallelIndexTaskRunner<PartialDimensionCardinalityTask, DimensionCardinalityReport>
{
  private static final String PHASE_NAME = "partial dimension cardinality";

  PartialDimensionCardinalityParallelIndexTaskRunner(
      TaskToolbox toolbox,
      String taskId,
      String groupId,
      ParallelIndexIngestionSpec ingestionSchema,
      Map<String, Object> context
  )
  {
    super(
        toolbox,
        taskId,
        groupId,
        ingestionSchema,
        context
    );
  }

  @Override
  public String getName()
  {
    return PHASE_NAME;
  }

  @Override
  SubTaskSpec<PartialDimensionCardinalityTask> createSubTaskSpec(
      String id,
      String groupId,
      String supervisorTaskId,
      Map<String, Object> context,
      InputSplit split,
      ParallelIndexIngestionSpec subTaskIngestionSpec
  )
  {
    return new SubTaskSpec<PartialDimensionCardinalityTask>(
        id,
        groupId,
        supervisorTaskId,
        context,
        split
    )
    {
      @Override
      public PartialDimensionCardinalityTask newSubTask(int numAttempts)
      {
        return new PartialDimensionCardinalityTask(
            null,
            getGroupId(),
            null,
            getSupervisorTaskId(),
            numAttempts,
            subTaskIngestionSpec,
            getContext(),
            getToolbox().getJsonMapper()
        );
      }
    };
  }
}
