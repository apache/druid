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
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.FirehoseFactoryToInputSourceAdaptor;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.indexing.common.TaskToolbox;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * {@link ParallelIndexTaskRunner} for the phase to create partitioned segments in multi-phase parallel indexing.
 *
 * @see PartialSegmentMergeParallelIndexTaskRunner
 */
class PartialSegmentGenerateParallelIndexTaskRunner
    extends ParallelIndexPhaseRunner<PartialSegmentGenerateTask, GeneratedPartitionsReport>
{
  private final ParallelIndexIngestionSpec ingestionSchema;
  private final SplittableInputSource<?> baseInputSource;

  PartialSegmentGenerateParallelIndexTaskRunner(
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
    this.baseInputSource = (SplittableInputSource) ingestionSchema.getIOConfig().getNonNullInputSource(
        ingestionSchema.getDataSchema().getParser()
    );
  }

  @Override
  public String getName()
  {
    return PartialSegmentGenerateTask.TYPE;
  }

  @Override
  Iterator<SubTaskSpec<PartialSegmentGenerateTask>> subTaskSpecIterator() throws IOException
  {
    return baseInputSource.createSplits(
        ingestionSchema.getIOConfig().getInputFormat(),
        getTuningConfig().getSplitHintSpec()
    ).map(this::newTaskSpec).iterator();
  }

  @Override
  int getTotalNumSubTasks() throws IOException
  {
    return baseInputSource.getNumSplits(
        ingestionSchema.getIOConfig().getInputFormat(),
        getTuningConfig().getSplitHintSpec()
    );
  }

  @VisibleForTesting
  ParallelIndexIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @VisibleForTesting
  SplittableInputSource<?> getBaseInputSource()
  {
    return baseInputSource;
  }

  SubTaskSpec<PartialSegmentGenerateTask> newTaskSpec(InputSplit split)
  {
    final FirehoseFactory firehoseFactory;
    final SplittableInputSource inputSource;
    if (baseInputSource instanceof FirehoseFactoryToInputSourceAdaptor) {
      firehoseFactory = ((FirehoseFactoryToInputSourceAdaptor) baseInputSource).getFirehoseFactory().withSplit(split);
      inputSource = null;
    } else {
      firehoseFactory = null;
      inputSource = baseInputSource.withSplit(split);
    }
    final ParallelIndexIngestionSpec subTaskIngestionSpec = new ParallelIndexIngestionSpec(
        ingestionSchema.getDataSchema(),
        new ParallelIndexIOConfig(
            firehoseFactory,
            inputSource,
            ingestionSchema.getIOConfig().getInputFormat(),
            ingestionSchema.getIOConfig().isAppendToExisting()
        ),
        ingestionSchema.getTuningConfig()
    );
    return new SubTaskSpec<PartialSegmentGenerateTask>(
        getTaskId() + "_" + getAndIncrementNextSpecId(),
        getGroupId(),
        getTaskId(),
        getContext(),
        split
    )
    {
      @Override
      public PartialSegmentGenerateTask newSubTask(int numAttempts)
      {
        return new PartialSegmentGenerateTask(
            null,
            getGroupId(),
            null,
            getSupervisorTaskId(),
            numAttempts,
            subTaskIngestionSpec,
            getContext(),
            getIndexingServiceClient(),
            null,
            null
        );
      }
    };
  }
}
