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
import org.apache.druid.segment.indexing.DataSchema;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * {@link ParallelIndexTaskRunner} for the phase to merge partitioned segments in multi-phase parallel indexing.
 */
class PartialGenericSegmentMergeParallelIndexTaskRunner
    extends ParallelIndexPhaseRunner<PartialGenericSegmentMergeTask, PushedSegmentsReport>
{
  private static final String PHASE_NAME = "partial segment merge";

  private final DataSchema dataSchema;
  private final List<PartialGenericSegmentMergeIOConfig> mergeIOConfigs;

  PartialGenericSegmentMergeParallelIndexTaskRunner(
      TaskToolbox toolbox,
      String taskId,
      String groupId,
      DataSchema dataSchema,
      List<PartialGenericSegmentMergeIOConfig> mergeIOConfigs,
      ParallelIndexTuningConfig tuningConfig,
      Map<String, Object> context,
      IndexingServiceClient indexingServiceClient
  )
  {
    super(toolbox, taskId, groupId, tuningConfig, context, indexingServiceClient);

    this.dataSchema = dataSchema;
    this.mergeIOConfigs = mergeIOConfigs;
  }

  @Override
  public String getName()
  {
    return PHASE_NAME;
  }

  @Override
  Iterator<SubTaskSpec<PartialGenericSegmentMergeTask>> subTaskSpecIterator()
  {
    return mergeIOConfigs.stream().map(this::newTaskSpec).iterator();
  }

  @Override
  int estimateTotalNumSubTasks()
  {
    return mergeIOConfigs.size();
  }

  @VisibleForTesting
  SubTaskSpec<PartialGenericSegmentMergeTask> newTaskSpec(PartialGenericSegmentMergeIOConfig ioConfig)
  {
    final PartialGenericSegmentMergeIngestionSpec ingestionSpec = new PartialGenericSegmentMergeIngestionSpec(
        dataSchema,
        ioConfig,
        getTuningConfig()
    );
    return new SubTaskSpec<PartialGenericSegmentMergeTask>(
        getTaskId() + "_" + getAndIncrementNextSpecId(),
        getGroupId(),
        getTaskId(),
        getContext(),
        new InputSplit<>(ioConfig.getPartitionLocations())
    )
    {
      @Override
      public PartialGenericSegmentMergeTask newSubTask(int numAttempts)
      {
        return new PartialGenericSegmentMergeTask(
            null,
            getGroupId(),
            null,
            getSupervisorTaskId(),
            numAttempts,
            ingestionSpec,
            getContext(),
            null,
            null,
            null
        );
      }
    };
  }
}
