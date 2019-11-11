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

import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.Task;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Base class for different implementations of {@link ParallelIndexTaskRunner} that operate on
 * {@link org.apache.druid.data.input.Firehose} splits.
 */
abstract class FirehoseSplitParallelIndexTaskRunner<T extends Task, R extends SubTaskReport>
    extends ParallelIndexPhaseRunner<T, R>
{
  private final ParallelIndexIngestionSpec ingestionSchema;
  private final FiniteFirehoseFactory<?, ?> baseFirehoseFactory;

  FirehoseSplitParallelIndexTaskRunner(
      TaskToolbox toolbox,
      String taskId,
      String groupId,
      ParallelIndexIngestionSpec ingestionSchema,
      Map<String, Object> context,
      IndexingServiceClient indexingServiceClient
  )
  {
    super(
        toolbox,
        taskId,
        groupId,
        ingestionSchema.getTuningConfig(),
        context,
        indexingServiceClient
    );
    this.ingestionSchema = ingestionSchema;
    this.baseFirehoseFactory = (FiniteFirehoseFactory) ingestionSchema.getIOConfig().getFirehoseFactory();
  }

  @Override
  Iterator<SubTaskSpec<T>> subTaskSpecIterator() throws IOException
  {
    return baseFirehoseFactory.getSplits(getTuningConfig().getSplitHintSpec()).map(this::newTaskSpec).iterator();
  }

  @Override
  final int getTotalNumSubTasks() throws IOException
  {
    return baseFirehoseFactory.getNumSplits(getTuningConfig().getSplitHintSpec());
  }

  final SubTaskSpec<T> newTaskSpec(InputSplit split)
  {
    final ParallelIndexIngestionSpec subTaskIngestionSpec = new ParallelIndexIngestionSpec(
        ingestionSchema.getDataSchema(),
        new ParallelIndexIOConfig(
            baseFirehoseFactory.withSplit(split),
            ingestionSchema.getIOConfig().isAppendToExisting()
        ),
        ingestionSchema.getTuningConfig()
    );

    return createSubTaskSpec(
        getTaskId() + "_" + getAndIncrementNextSpecId(),
        getGroupId(),
        getTaskId(),
        getContext(),
        split,
        subTaskIngestionSpec,
        getIndexingServiceClient()
    );
  }

  abstract SubTaskSpec<T> createSubTaskSpec(
      String id,
      String groupId,
      String supervisorTaskId,
      Map<String, Object> context,
      InputSplit split,
      ParallelIndexIngestionSpec subTaskIngestionSpec,
      IndexingServiceClient indexingServiceClient
  );
}
