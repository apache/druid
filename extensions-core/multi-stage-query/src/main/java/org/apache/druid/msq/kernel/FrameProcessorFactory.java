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

package org.apache.druid.msq.kernel;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

/**
 * Property of {@link StageDefinition} that describes its computation logic.
 *
 * Workers call {@link #makeProcessors} to generate the processors that perform computations within that worker's
 * {@link org.apache.druid.frame.processor.FrameProcessorExecutor}. Additionally, provides methods for accumulating
 * the results of the processors: {@link #newAccumulatedResult()}, {@link #accumulateResult}, and
 * {@link #mergeAccumulatedResult}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface FrameProcessorFactory<ExtraInfoType, ProcessorType extends FrameProcessor<T>, T, R>
{
  /**
   * Create processors for a particular worker in a particular stage. The processors will be run on a thread pool,
   * with at most "maxOutstandingProcessors" number of processors outstanding at once.
   *
   * The Sequence returned by {@link ProcessorsAndChannels#processors()} is passed directly to
   * {@link org.apache.druid.frame.processor.FrameProcessorExecutor#runAllFully}.
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
  ProcessorsAndChannels<ProcessorType, T> makeProcessors(
      StageDefinition stageDefinition,
      int workerNumber,
      List<InputSlice> inputSlices,
      InputSliceReader inputSliceReader,
      @Nullable ExtraInfoType extra,
      OutputChannelFactory outputChannelFactory,
      FrameContext frameContext,
      int maxOutstandingProcessors,
      CounterTracker counters,
      Consumer<Throwable> warningPublisher
  ) throws IOException;

  TypeReference<R> getAccumulatedResultTypeReference();

  /**
   * Produces a "blank slate" result.
   */
  R newAccumulatedResult();

  /**
   * Accumulates an additional result. May modify the left-hand side {@code accumulated}. Does not modify the
   * right-hand side {@code current}.
   */
  R accumulateResult(R accumulated, T current);

  /**
   * Merges two accumulated results. May modify the left-hand side {@code accumulated}. Does not modify the right-hand
   * side {@code current}.
   */
  R mergeAccumulatedResult(R accumulated, R otherAccumulated);

  /**
   * Produces an {@link ExtraInfoHolder} wrapper that allows serialization of {@code ExtraInfoType}.
   */
  @SuppressWarnings("rawtypes")
  ExtraInfoHolder makeExtraInfoHolder(@Nullable ExtraInfoType extra);
}
