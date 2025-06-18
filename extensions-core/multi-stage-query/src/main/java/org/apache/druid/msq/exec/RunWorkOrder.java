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

package org.apache.druid.msq.exec;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.ByteTracker;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.processor.BlockingQueueOutputChannelFactory;
import org.apache.druid.frame.processor.ComposingOutputChannelFactory;
import org.apache.druid.frame.processor.FileOutputChannelFactory;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.frame.processor.OutputChannels;
import org.apache.druid.frame.util.DurableStorageUtils;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.indexing.InputChannelFactory;
import org.apache.druid.msq.indexing.InputChannelsImpl;
import org.apache.druid.msq.indexing.error.CanceledFault;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.InputSlices;
import org.apache.druid.msq.input.MapInputSliceReader;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.msq.input.NilInputSliceReader;
import org.apache.druid.msq.input.external.ExternalInputSlice;
import org.apache.druid.msq.input.external.ExternalInputSliceReader;
import org.apache.druid.msq.input.inline.InlineInputSlice;
import org.apache.druid.msq.input.inline.InlineInputSliceReader;
import org.apache.druid.msq.input.lookup.LookupInputSlice;
import org.apache.druid.msq.input.lookup.LookupInputSliceReader;
import org.apache.druid.msq.input.stage.InputChannels;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.input.stage.StageInputSliceReader;
import org.apache.druid.msq.input.table.SegmentsInputSlice;
import org.apache.druid.msq.input.table.SegmentsInputSliceReader;
import org.apache.druid.msq.kernel.ShuffleKind;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.shuffle.output.DurableStorageOutputChannelFactory;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Main worker logic for executing a {@link WorkOrder} in a {@link FrameProcessorExecutor}.
 */
public class RunWorkOrder
{
  enum State
  {
    /**
     * Initial state. Must be in this state to call {@link #startAsync()}.
     */
    INIT,

    /**
     * State entered upon calling {@link #startAsync()}.
     */
    STARTED,

    /**
     * State entered upon failure of some work.
     */
    FAILED,

    /**
     * State entered upon calling {@link #stop(Throwable)}.
     */
    STOPPING,

    /**
     * State entered when a call to {@link #stop(Throwable)} concludes.
     */
    STOPPED
  }

  private final WorkOrder workOrder;
  private final InputChannelFactory inputChannelFactory;
  private final CounterTracker counterTracker;
  private final FrameProcessorExecutor exec;
  private final String cancellationId;
  private final WorkerContext workerContext;
  private final FrameContext frameContext;
  private final RunWorkOrderListener listener;
  private final ByteTracker intermediateByteTracker;
  @GuardedBy("stageOutputChannels")
  private final List<OutputChannel> stageOutputChannels = new ArrayList<>();
  private final SettableFuture<ClusterByPartitions> stagePartitionBoundariesFuture = SettableFuture.create();
  private final AtomicReference<State> state = new AtomicReference<>(State.INIT);
  private final CountDownLatch stopLatch = new CountDownLatch(1);
  private final AtomicReference<Either<Throwable, Object>> resultForListener = new AtomicReference<>();

  @MonotonicNonNull
  private ListenableFuture<?> stageResultFuture;

  public RunWorkOrder(
      final WorkOrder workOrder,
      final InputChannelFactory inputChannelFactory,
      final CounterTracker counterTracker,
      final FrameProcessorExecutor exec,
      final String cancellationId,
      final WorkerContext workerContext,
      final FrameContext frameContext,
      final RunWorkOrderListener listener
  )
  {
    this.workOrder = workOrder;
    this.inputChannelFactory = inputChannelFactory;
    this.counterTracker = counterTracker;
    this.exec = exec;
    this.cancellationId = cancellationId;
    this.workerContext = workerContext;
    this.frameContext = frameContext;
    this.listener = listener;
    this.intermediateByteTracker =
        new ByteTracker(
            frameContext.storageParameters().isIntermediateStorageLimitConfigured()
            ? frameContext.storageParameters().getIntermediateSuperSorterStorageMaxLocalBytes()
            : Long.MAX_VALUE
        );
  }

  /**
   * Start execution of the provided {@link WorkOrder} in the provided {@link FrameProcessorExecutor}.
   *
   * Execution proceeds asynchronously after this method returns. The {@link RunWorkOrderListener} passed to the
   * constructor of this instance can be used to track progress.
   */
  public void startAsync()
  {
    if (!state.compareAndSet(State.INIT, State.STARTED)) {
      throw new ISE("Cannot start from state[%s]", state);
    }

    try {
      exec.registerCancellationId(cancellationId);
      initGlobalSortPartitionBoundariesIfNeeded();
      startStageProcessor();
      setUpCompletionCallbacks();
    }
    catch (Throwable t) {
      stopUnchecked(t);
    }
  }

  /**
   * Stops an execution that was previously initiated through {@link #startAsync()} and closes the {@link FrameContext}.
   * May be called to cancel execution. Must also be called after successful execution in order to ensure that resources
   * are all properly cleaned up.
   *
   * Blocks until execution is fully stopped.
   *
   * @param t error to send to {@link RunWorkOrderListener#onFailure}, if success/failure has not already been sent.
   *          Will also be thrown at the end of this method.
   */
  public void stop(@Nullable Throwable t) throws InterruptedException
  {
    if (state.compareAndSet(State.INIT, State.STOPPING)
        || state.compareAndSet(State.STARTED, State.STOPPING)
        || state.compareAndSet(State.FAILED, State.STOPPING)) {
      // Initiate stopping.
      try {
        exec.cancel(cancellationId);
      }
      catch (Throwable e2) {
        if (t == null) {
          t = e2;
        } else {
          t.addSuppressed(e2);
        }
      }

      try {
        frameContext.close();
      }
      catch (Throwable e2) {
        if (t == null) {
          t = e2;
        } else {
          t.addSuppressed(e2);
        }
      }

      try {
        // notifyListener will ignore this error if work has already succeeded.
        notifyListener(Either.error(t != null ? t : new MSQException(CanceledFault.unknown())));
      }
      catch (Throwable e2) {
        if (t == null) {
          t = e2;
        } else {
          t.addSuppressed(e2);
        }
      }

      stopLatch.countDown();
    }

    stopLatch.await();

    if (t != null) {
      Throwables.throwIfInstanceOf(t, InterruptedException.class);
      Throwables.throwIfUnchecked(t);
      throw new RuntimeException(t);
    }
  }

  /**
   * Calls {@link #stop(Throwable)}. If the call to {@link #stop(Throwable)} throws {@link InterruptedException},
   * this method sets the interrupt flag and throws an unchecked exception.
   *
   * @param t error to send to {@link RunWorkOrderListener#onFailure}, if success/failure has not already been sent.
   *          Will also be thrown at the end of this method.
   */
  public void stopUnchecked(@Nullable final Throwable t)
  {
    try {
      stop(t);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /**
   * Settable {@link ClusterByPartitions} future for global sort. Necessary because we don't know ahead of time
   * what the boundaries will be. The controller decides based on statistics from all workers. Once the controller
   * decides, its decision is written to this future, which allows sorting on workers to proceed.
   */
  public SettableFuture<ClusterByPartitions> getStagePartitionBoundariesFuture()
  {
    return stagePartitionBoundariesFuture;
  }

  /**
   * Uses {@link StageProcessor#execute(ExecutionContext)} to run work, and assigns the result to
   * {@link #stageResultFuture}.
   */
  private void startStageProcessor()
  {
    if (stageResultFuture != null) {
      throw new ISE("stageResultAndChannelsFuture already set");
    }

    final StageProcessor<?, ?> processor = workOrder.getStageDefinition().getProcessor();
    final ExecutionContext executionContext = makeExecutionContext();

    stageResultFuture = processor.execute(executionContext);
  }

  /**
   * Initialize {@link #stagePartitionBoundariesFuture} if it will be needed (i.e. if {@link ShuffleKind#GLOBAL_SORT})
   * but does not need statistics. In this case, it is known upfront, before the job starts.
   */
  private void initGlobalSortPartitionBoundariesIfNeeded()
  {
    if (workOrder.getStageDefinition().doesShuffle()
        && workOrder.getStageDefinition().getShuffleSpec().kind() == ShuffleKind.GLOBAL_SORT
        && !workOrder.getStageDefinition().mustGatherResultKeyStatistics()) {
      // Result key stats aren't needed, so the partition boundaries are knowable ahead of time. Compute them now.
      final ClusterByPartitions boundaries =
          workOrder.getStageDefinition()
                   .generatePartitionBoundariesForShuffle(null)
                   .valueOrThrow();

      stagePartitionBoundariesFuture.set(boundaries);
    }
  }

  /**
   * Callbacks that fire when all work for the stage is done (i.e. when {@link #stageResultFuture} resolves).
   */
  private void setUpCompletionCallbacks()
  {
    Futures.addCallback(
        stageResultFuture,
        new FutureCallback<Object>()
        {
          @Override
          public void onSuccess(Object resultObject)
          {
            try {
              final OutputChannels outputChannels;

              synchronized (stageOutputChannels) {
                stageOutputChannels.sort(Comparator.comparing(OutputChannel::getPartitionNumber));
                outputChannels = OutputChannels.wrap(
                    stageOutputChannels.stream()
                                       .map(OutputChannel::readOnly)
                                       .sorted(Comparator.comparing(OutputChannel::getPartitionNumber))
                                       .collect(Collectors.toList()));
                stageOutputChannels.clear();
              }

              if (workOrder.getOutputChannelMode() != OutputChannelMode.MEMORY) {
                // In non-MEMORY output channel modes, call onOutputChannelAvailable when all work is done.
                // (In MEMORY mode, we would have called onOutputChannelAvailable when the channels were created.)
                for (final OutputChannel channel : outputChannels.getAllChannels()) {
                  listener.onOutputChannelAvailable(channel);
                }
              }

              if (workOrder.getOutputChannelMode().isDurable()) {
                // In DURABLE_STORAGE output channel mode, write a success file once all work is done.
                writeDurableStorageSuccessFile();
              }

              notifyListener(Either.value(resultObject));
            }
            catch (Throwable t) {
              onFailure(t);
            }
          }

          @Override
          public void onFailure(final Throwable t)
          {
            if (state.compareAndSet(State.STARTED, State.FAILED)) {
              // Call notifyListener only if we were STARTED. In particular, if we were STOPPING, skip this and allow
              // the stop() method to set its own Canceled error.
              notifyListener(Either.error(t));
            }
          }
        },
        Execs.directExecutor()
    );
  }

  /**
   * Notify {@link RunWorkOrderListener} that the job is done, if not already notified.
   */
  private void notifyListener(final Either<Throwable, Object> result)
  {
    if (resultForListener.compareAndSet(null, result)) {
      if (result.isError()) {
        listener.onFailure(result.error());
      } else {
        listener.onSuccess(result.valueOrThrow());
      }
    }
  }

  /**
   * Write {@link DurableStorageUtils#SUCCESS_MARKER_FILENAME} for a particular stage, if durable storage is enabled.
   */
  private void writeDurableStorageSuccessFile()
  {
    final DurableStorageOutputChannelFactory durableStorageOutputChannelFactory =
        makeDurableStorageOutputChannelFactory(
            frameContext.tempDir("durable"),
            frameContext.memoryParameters().getFrameSize(),
            workOrder.getOutputChannelMode() == OutputChannelMode.DURABLE_STORAGE_QUERY_RESULTS
        );

    try {
      durableStorageOutputChannelFactory.createSuccessFile(workerContext.workerId());
    }
    catch (IOException e) {
      throw new ISE(
          e,
          "Unable to create success file at location[%s]",
          durableStorageOutputChannelFactory.getSuccessFilePath()
      );
    }
  }

  private ExecutionContext makeExecutionContext()
  {
    return new ExecutionContextImpl(
        workOrder,
        exec,
        makeInputSliceReader(),
        this::makeIntermediateOutputChannelFactory,
        makeStageOutputChannelFactory(),
        stagePartitionBoundariesFuture,
        frameContext,
        counterTracker,
        workerContext.threadCount(),
        cancellationId,
        listener
    );
  }

  private InputSliceReader makeInputSliceReader()
  {
    final String queryId = workOrder.getQueryDefinition().getQueryId();
    final boolean reindex = MultiStageQueryContext.isReindex(workOrder.getWorkerContext());
    final boolean removeNullBytes = MultiStageQueryContext.removeNullBytes(workOrder.getWorkerContext());

    final InputChannels inputChannels =
        new InputChannelsImpl(
            workOrder.getQueryDefinition(),
            InputSlices.allReadablePartitions(workOrder.getInputs()),
            inputChannelFactory,
            () -> ArenaMemoryAllocator.createOnHeap(frameContext.memoryParameters().getFrameSize()),
            exec,
            cancellationId,
            counterTracker,
            removeNullBytes
        );

    return new MapInputSliceReader(
        ImmutableMap.<Class<? extends InputSlice>, InputSliceReader>builder()
                    .put(NilInputSlice.class, NilInputSliceReader.INSTANCE)
                    .put(StageInputSlice.class, new StageInputSliceReader(queryId, inputChannels))
                    .put(ExternalInputSlice.class, new ExternalInputSliceReader(frameContext.tempDir("external")))
                    .put(InlineInputSlice.class, new InlineInputSliceReader(frameContext.segmentWrangler()))
                    .put(LookupInputSlice.class, new LookupInputSliceReader(frameContext.segmentWrangler()))
                    .put(SegmentsInputSlice.class, new SegmentsInputSliceReader(frameContext, reindex))
                    .build()
    );
  }

  private OutputChannelFactory makeStageOutputChannelFactory()
  {
    // Use the standard frame size, since we assume this size when computing how much is needed to merge output
    // files from different workers.
    final int frameSize = frameContext.memoryParameters().getFrameSize();
    final OutputChannelMode outputChannelMode = workOrder.getOutputChannelMode();
    final OutputChannelFactory outputChannelFactory;

    switch (outputChannelMode) {
      case MEMORY:
        // Use ListeningOutputChannelFactory to capture output channels as they are created, rather than when
        // work is complete.
        outputChannelFactory = new ListeningOutputChannelFactory(
            new BlockingQueueOutputChannelFactory(frameSize),
            listener::onOutputChannelAvailable
        );
        break;

      case LOCAL_STORAGE:
        final File fileChannelDirectory =
            frameContext.tempDir(StringUtils.format("output_stage_%06d", workOrder.getStageNumber()));
        outputChannelFactory = new FileOutputChannelFactory(fileChannelDirectory, frameSize, null);
        break;

      case DURABLE_STORAGE_INTERMEDIATE:
      case DURABLE_STORAGE_QUERY_RESULTS:
        outputChannelFactory = makeDurableStorageOutputChannelFactory(
            frameContext.tempDir("durable"),
            frameSize,
            outputChannelMode == OutputChannelMode.DURABLE_STORAGE_QUERY_RESULTS
        );
        break;

      default:
        throw DruidException.defensive("No handling for outputChannelMode[%s]", outputChannelMode);
    }

    // Capture output channels as they are created.
    return new ListeningOutputChannelFactory(
        outputChannelFactory,
        channel -> {
          synchronized (stageOutputChannels) {
            stageOutputChannels.add(channel);
          }
        }
    );
  }

  private OutputChannelFactory makeIntermediateOutputChannelFactory(final String name)
  {
    final File tempDir = frameContext.tempDir(name);
    final int frameSize = frameContext.memoryParameters().getFrameSize();
    final File fileChannelDirectory =
        new File(tempDir, StringUtils.format("intermediate-stage-%06d", workOrder.getStageNumber()));
    final FileOutputChannelFactory fileOutputChannelFactory =
        new FileOutputChannelFactory(fileChannelDirectory, frameSize, intermediateByteTracker);

    if (workOrder.getOutputChannelMode().isDurable()
        && frameContext.storageParameters().isIntermediateStorageLimitConfigured()) {
      final boolean isQueryResults =
          workOrder.getOutputChannelMode() == OutputChannelMode.DURABLE_STORAGE_QUERY_RESULTS;
      return new ComposingOutputChannelFactory(
          ImmutableList.of(
              fileOutputChannelFactory,
              makeDurableStorageOutputChannelFactory(tempDir, frameSize, isQueryResults)
          ),
          frameSize
      );
    } else {
      return fileOutputChannelFactory;
    }
  }

  private DurableStorageOutputChannelFactory makeDurableStorageOutputChannelFactory(
      final File tmpDir,
      final int frameSize,
      final boolean isQueryResults
  )
  {
    return DurableStorageOutputChannelFactory.createStandardImplementation(
        workerContext.queryId(),
        workOrder.getWorkerNumber(),
        workOrder.getStageNumber(),
        workerContext.workerId(),
        frameSize,
        MSQTasks.makeStorageConnector(workerContext.injector()),
        tmpDir,
        isQueryResults
    );
  }
}
