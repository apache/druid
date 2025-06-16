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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.frame.processor.BlockingQueueOutputChannelFactory;
import org.apache.druid.frame.processor.Bouncer;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.frame.processor.manager.ProcessorManager;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.msq.counters.CounterNames;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.counters.CpuCounters;
import org.apache.druid.msq.exec.ExecutionContext;
import org.apache.druid.msq.exec.FrameContext;
import org.apache.druid.msq.exec.OutputChannelMode;
import org.apache.druid.msq.exec.StageProcessor;
import org.apache.druid.msq.indexing.CountingOutputChannelFactory;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.WorkOrder;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

/**
 * Runner for {@link StageProcessor} that want to build a {@link ProcessorsAndChannels} for some shuffle-agnostic
 * work, then have the shuffle work taken care of generically by {@link StandardStageRunner}.
 *
 * Using this class allows the {@link StageProcessor} implementation to be simpler, since it doesn't need to worry
 * about how to generate channels "properly" for the shuffle. However, it comes at the cost of not being able
 * to do shuffle-specific optimizations.
 */
public class StandardStageRunner<T, R>
{
  private final ExecutionContext executionContext;
  private final WorkOrder workOrder;
  private final CounterTracker counterTracker;
  private final FrameProcessorExecutor exec;
  private final String cancellationId;
  private final int threadCount;
  private final FrameContext frameContext;

  @MonotonicNonNull
  private OutputChannelFactory workOutputChannelFactory;
  @MonotonicNonNull
  private ListenableFuture<R> workResultFuture;
  @MonotonicNonNull
  private ListenableFuture<ResultAndChannels<Object>> pipelineFuture;

  public StandardStageRunner(final ExecutionContext executionContext)
  {
    this.executionContext = executionContext;
    this.workOrder = executionContext.workOrder();
    this.counterTracker = executionContext.counters();
    this.exec = executionContext.executor();
    this.cancellationId = executionContext.cancellationId();
    this.threadCount = executionContext.threadCount();
    this.frameContext = executionContext.frameContext();
  }

  /**
   * Start execution.
   */
  public ListenableFuture<R> run(final ProcessorsAndChannels<T, R> processors)
  {
    final StageDefinition stageDef = workOrder.getStageDefinition();

    makeAndRunWorkProcessors(processors);

    if (stageDef.doesShuffle()) {
      makeAndRunShuffleProcessors();
    }

    // Return a future that resolves to the work result, but only when the final stage result and output channels are
    // *also* ready (from pipelineFuture).
    return FutureUtils.transformAsync(
        Futures.allAsList(
            workResultFuture,
            FutureUtils.transformAsync(pipelineFuture, ResultAndChannels::resultFuture)
        ),
        ignored -> workResultFuture
    );
  }

  /**
   * Returns the {@link OutputChannelFactory} that the processors passed to {@link #run(ProcessorsAndChannels)}
   * are expected to use.
   */
  public OutputChannelFactory workOutputChannelFactory()
  {
    if (workOutputChannelFactory != null) {
      return workOutputChannelFactory;
    }

    final OutputChannelFactory baseOutputChannelFactory;

    if (workOrder.getStageDefinition().doesShuffle()) {
      // Writing to a consumer in the same JVM (which will be set up later on in this method).
      baseOutputChannelFactory = new BlockingQueueOutputChannelFactory(frameContext.memoryParameters().getFrameSize());
    } else {
      // Writing stage output.
      baseOutputChannelFactory = executionContext.outputChannelFactory();
    }

    workOutputChannelFactory = new CountingOutputChannelFactory(
        baseOutputChannelFactory,
        counterTracker.channel(CounterNames.outputChannel())
    );

    return workOutputChannelFactory;
  }

  /**
   * Executes processors using {@link #exec}. Saves the result future in {@link #workResultFuture} and saves the
   * current pipeline state (result and output channels) in {@link #pipelineFuture}.
   */
  private void makeAndRunWorkProcessors(final ProcessorsAndChannels<T, R> processors)
  {
    final ProcessorManager<T, R> processorManager = processors.getProcessorManager();

    final int maxOutstandingProcessors;

    if (processors.getOutputChannels().getAllChannels().isEmpty()) {
      // No output channels: run up to "threadCount" processors at once.
      maxOutstandingProcessors = Math.max(1, threadCount);
    } else {
      // If there are output channels, that acts as a ceiling on the number of processors that can run at once.
      maxOutstandingProcessors =
          Math.max(1, Math.min(threadCount, processors.getOutputChannels().getAllChannels().size()));
    }

    final boolean usesProcessingBuffers = workOrder.getStageDefinition().getProcessor().usesProcessingBuffers();

    workResultFuture = exec.runAllFully(
        counterTracker.trackCpu(processorManager, CpuCounters.LABEL_MAIN),
        maxOutstandingProcessors,
        usesProcessingBuffers ? frameContext.processingBuffers().getBouncer() : Bouncer.unlimited(),
        cancellationId
    );

    final ResultAndChannels<R> workResultAndChannels = new ResultAndChannels<>(
        workResultFuture,
        processors.getOutputChannels()
    );

    //noinspection unchecked
    pipelineFuture = Futures.immediateFuture((ResultAndChannels<Object>) workResultAndChannels);
  }

  /**
   * Executes the shuffle pipeline and sets the result future in {@link #pipelineFuture}.
   */
  private void makeAndRunShuffleProcessors()
  {
    final ShuffleSpec shuffleSpec = workOrder.getStageDefinition().getShuffleSpec();
    final StandardShuffleOperations stageOperations = new StandardShuffleOperations(executionContext);

    pipelineFuture = stageOperations.gatherResultKeyStatisticsIfNeeded(pipelineFuture);

    final OutputChannelFactory stageOutputChannelFactory = new CountingOutputChannelFactory(
        executionContext.outputChannelFactory(),
        counterTracker.channel(CounterNames.shuffleChannel())
    );

    switch (shuffleSpec.kind()) {
      case MIX:
        pipelineFuture = stageOperations.mix(pipelineFuture, stageOutputChannelFactory);
        break;

      case HASH:
        pipelineFuture = stageOperations.hashPartition(
            pipelineFuture,
            stageOutputChannelFactory,
            executionContext.workOrder().getOutputChannelMode() != OutputChannelMode.MEMORY
        );
        break;

      case HASH_LOCAL_SORT:
        final OutputChannelFactory hashOutputChannelFactory;
        final boolean hashOutputBuffered;

        if (shuffleSpec.partitionCount() == 1) {
          // Single partition; no need to write temporary files.
          hashOutputChannelFactory =
              new BlockingQueueOutputChannelFactory(frameContext.memoryParameters().getFrameSize());
          hashOutputBuffered = false;
        } else {
          // Multi-partition; write temporary files and then sort each one file-by-file.
          hashOutputChannelFactory = executionContext.makeIntermediateOutputChannelFactory("hash-parts");
          hashOutputBuffered = true;
        }

        pipelineFuture = stageOperations.hashPartition(pipelineFuture, hashOutputChannelFactory, hashOutputBuffered);
        pipelineFuture = stageOperations.localSort(pipelineFuture, stageOutputChannelFactory);
        break;

      case GLOBAL_SORT:
        pipelineFuture = stageOperations.globalSort(pipelineFuture, stageOutputChannelFactory);
        break;

      default:
        throw new UOE("Cannot handle shuffle kind [%s]", shuffleSpec.kind());
    }
  }
}
