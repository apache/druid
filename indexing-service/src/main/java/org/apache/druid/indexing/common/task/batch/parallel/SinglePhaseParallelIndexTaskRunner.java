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
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.indexing.common.TaskToolbox;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * An implementation of {@link ParallelIndexTaskRunner} to support best-effort roll-up. This runner can submit and
 * monitor multiple {@link SinglePhaseSubTask}s.
 * <p>
 * As its name indicates, distributed indexing is done in a single phase, i.e., without shuffling intermediate data. As
 * a result, this task can't be used for perfect rollup.
 */
class SinglePhaseParallelIndexTaskRunner extends ParallelIndexPhaseRunner<SinglePhaseSubTask, PushedSegmentsReport>
{
  private static final String PHASE_NAME = "segment generation";

  private final ParallelIndexIngestionSpec ingestionSchema;
  private final SplittableInputSource<?> baseInputSource;

  SinglePhaseParallelIndexTaskRunner(
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
    return PHASE_NAME;
  }

  @VisibleForTesting
  ParallelIndexIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @VisibleForTesting
  @Override
  Iterator<SubTaskSpec<SinglePhaseSubTask>> subTaskSpecIterator() throws IOException
  {
    return baseInputSource.createSplits(
        ingestionSchema.getIOConfig().getInputFormat(),
        getTuningConfig().getSplitHintSpec()
    ).map(this::newTaskSpec).iterator();
  }

  @Override
  int estimateTotalNumSubTasks() throws IOException
  {
    return baseInputSource.estimateNumSplits(
        ingestionSchema.getIOConfig().getInputFormat(),
        getTuningConfig().getSplitHintSpec()
    );
  }

  @VisibleForTesting
  SubTaskSpec<SinglePhaseSubTask> newTaskSpec(InputSplit split)
  {
    final FirehoseFactory firehoseFactory;
    final InputSource inputSource;
    if (baseInputSource instanceof FirehoseFactoryToInputSourceAdaptor) {
      firehoseFactory = ((FirehoseFactoryToInputSourceAdaptor) baseInputSource).getFirehoseFactory().withSplit(split);
      inputSource = null;
    } else {
      firehoseFactory = null;
      inputSource = baseInputSource.withSplit(split);
    }
    return new SinglePhaseSubTaskSpec(
        getTaskId() + "_" + getAndIncrementNextSpecId(),
        getGroupId(),
        getTaskId(),
        new ParallelIndexIngestionSpec(
            ingestionSchema.getDataSchema(),
            new ParallelIndexIOConfig(
                firehoseFactory,
                inputSource,
                ingestionSchema.getIOConfig().getInputFormat(),
                ingestionSchema.getIOConfig().isAppendToExisting()
            ),
            ingestionSchema.getTuningConfig()
        ),
        getContext(),
        split
    );
  }
}
