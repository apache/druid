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

package org.apache.druid.msq.exec.std;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.exec.ExecutionContext;
import org.apache.druid.msq.exec.FrameContext;
import org.apache.druid.msq.exec.StageProcessor;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.util.MultiStageQueryContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

/**
 * Base class for {@link StageProcessor} that want to build a {@link ProcessorsAndChannels} for some shuffle-agnostic
 * work, then have the shuffle work taken care of by {@link StandardStageRunner}. In general, this allows the
 * {@link StageProcessor} implementation to be simpler, and comes at the cost of not being able to do shuffle-specific
 * optimizations.
 *
 * This abstract class may be removed someday, in favor of its subclasses using {@link StandardStageRunner} directly.
 * It was introduced mainly to minimize code changes in a refactor.
 */
public abstract class StandardStageProcessor<T, R, ExtraInfoType> implements StageProcessor<R, ExtraInfoType>
{
  /**
   * Create processors for a particular worker in a particular stage. The processors will be run on a thread pool,
   * with at most "maxOutstandingProcessors" number of processors outstanding at once.
   *
   * The Sequence returned by {@link ProcessorsAndChannels#getProcessorManager()} is passed directly to
   * {@link FrameProcessorExecutor#runAllFully}.
   *
   * @param stageDefinition          stage definition
   * @param workerNumber             current worker number; some factories use this to determine what work to do
   * @param inputSlices              input slices for this worker, indexed by input number (one for each
   *                                 {@link StageDefinition#getInputSpecs()})
   * @param inputSliceReader         reader for the input slices
   * @param extra                    any extra, out-of-band information associated with this particular worker; some
   *                                 factories use this to determine what work to do
   * @param outputChannelFactory     factory for generating output channels.
   * @param frameContext             Context which provides services needed by frame processors
   * @param maxOutstandingProcessors maximum number of processors that will be active at once
   * @param counters                 allows creation of custom processor counters
   * @param warningPublisher         publisher for warnings encountered during execution
   *
   * @return a processor sequence, which may be computed lazily; and a list of output channels.
   */
  public abstract ProcessorsAndChannels<T, R> makeProcessors(
      StageDefinition stageDefinition,
      int workerNumber,
      List<InputSlice> inputSlices,
      InputSliceReader inputSliceReader,
      @Nullable ExtraInfoType extra,
      OutputChannelFactory outputChannelFactory,
      FrameContext frameContext,
      int maxOutstandingProcessors,
      CounterTracker counters,
      Consumer<Throwable> warningPublisher,
      boolean removeNullBytes
  ) throws IOException;

  @Override
  public ListenableFuture<R> execute(ExecutionContext context)
  {
    try {
      final StandardStageRunner<T, R> stageRunner = new StandardStageRunner<>(context);

      @SuppressWarnings("unchecked")
      final ProcessorsAndChannels<T, R> processors = makeProcessors(
          context.workOrder().getStageDefinition(),
          context.workOrder().getWorkerNumber(),
          context.workOrder().getInputs(),
          context.inputSliceReader(),
          (ExtraInfoType) context.workOrder().getExtraInfo(),
          stageRunner.workOutputChannelFactory(),
          context.frameContext(),
          context.threadCount(),
          context.counters(),
          context::onWarning,
          MultiStageQueryContext.removeNullBytes(context.workOrder().getWorkerContext())
      );

      return stageRunner.run(processors);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
