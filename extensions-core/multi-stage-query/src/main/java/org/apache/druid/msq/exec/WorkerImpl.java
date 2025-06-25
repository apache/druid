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

import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntObjectPair;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.util.DurableStorageUtils;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.indexing.InputChannelFactory;
import org.apache.druid.msq.indexing.MSQWorkerTask;
import org.apache.druid.msq.indexing.error.CanceledFault;
import org.apache.druid.msq.indexing.error.CancellationReason;
import org.apache.druid.msq.indexing.error.CannotParseExternalDataFault;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.MSQWarningReportLimiterPublisher;
import org.apache.druid.msq.indexing.error.MSQWarningReportPublisher;
import org.apache.druid.msq.indexing.error.MSQWarningReportSimplePublisher;
import org.apache.druid.msq.indexing.error.MSQWarnings;
import org.apache.druid.msq.input.InputSlices;
import org.apache.druid.msq.input.stage.ReadablePartition;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernelUtils;
import org.apache.druid.msq.kernel.worker.WorkerStageKernel;
import org.apache.druid.msq.kernel.worker.WorkerStagePhase;
import org.apache.druid.msq.shuffle.input.DurableStorageInputChannelFactory;
import org.apache.druid.msq.shuffle.input.MetaInputChannelFactory;
import org.apache.druid.msq.shuffle.input.WorkerInputChannelFactory;
import org.apache.druid.msq.shuffle.input.WorkerOrLocalInputChannelFactory;
import org.apache.druid.msq.shuffle.output.StageOutputHolder;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.msq.statistics.PartialKeyStatisticsInformation;
import org.apache.druid.msq.util.DecoratedExecutorService;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.PrioritizedCallable;
import org.apache.druid.query.PrioritizedRunnable;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.server.DruidNode;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Interface for a worker of a multi-stage query.
 *
 * Not scoped to any particular query. There is one of these per {@link MSQWorkerTask}, and one per server for
 * long-lived workers.
 */
public class WorkerImpl implements Worker
{
  private static final Logger log = new Logger(WorkerImpl.class);

  /**
   * Task object, if this {@link WorkerImpl} was launched from a task. Ideally, this would not be needed, and we
   * would be able to get everything we need from {@link WorkerContext}.
   */
  @Nullable
  private final MSQWorkerTask task;
  private final WorkerContext context;
  private final DruidNode selfDruidNode;

  private final BlockingQueue<Consumer<KernelHolders>> kernelManipulationQueue = new LinkedBlockingDeque<>();
  private final ConcurrentHashMap<StageId, ConcurrentHashMap<Integer, StageOutputHolder>> stageOutputs = new ConcurrentHashMap<>();

  /**
   * Pair of {workerNumber, stageId} -> counters.
   */
  private final ConcurrentHashMap<IntObjectPair<StageId>, CounterTracker> stageCounters = new ConcurrentHashMap<>();

  /**
   * Atomic that is set to true when {@link #run()} starts (or when {@link #stop(CancellationReason reason)} is called before {@link #run()}).
   */
  private final AtomicBoolean didRun = new AtomicBoolean();

  /**
   * Future that resolves when {@link #run()} completes.
   */
  private final SettableFuture<Void> runFuture = SettableFuture.create();

  /**
   * Set once in {@link #run} and never reassigned. This is in a field so {@link #doCancel(CancellationReason reason)} can close it.
   */
  private volatile ControllerClient controllerClient;

  /**
   * Set once in {@link #runInternal} and never reassigned. Used by processing threads so we can contact other workers
   * during a shuffle.
   */
  private volatile WorkerClient workerClient;

  /**
   * Set to false by {@link #controllerFailed()} as a way of enticing the {@link #runInternal} method to exit promptly.
   */
  private volatile boolean controllerAlive = true;

  public WorkerImpl(@Nullable final MSQWorkerTask task, final WorkerContext context)
  {
    this.task = task;
    this.context = context;
    this.selfDruidNode = context.selfNode();
  }

  @Override
  public String id()
  {
    return context.workerId();
  }

  @Override
  public void run()
  {
    if (!didRun.compareAndSet(false, true)) {
      throw new ISE("already run");
    }

    try (final Closer closer = Closer.create()) {
      final KernelHolders kernelHolders = KernelHolders.create(context, closer);
      controllerClient = kernelHolders.getControllerClient();

      Throwable t = null;
      Optional<MSQErrorReport> maybeErrorReport;

      try {
        maybeErrorReport = runInternal(kernelHolders, closer);
      }
      catch (Throwable e) {
        t = e;
        maybeErrorReport = Optional.of(
            MSQErrorReport.fromException(
                context.workerId(),
                MSQTasks.getHostFromSelfNode(context.selfNode()),
                null,
                e
            )
        );
      }

      if (maybeErrorReport.isPresent()) {
        final MSQErrorReport errorReport = maybeErrorReport.get();
        final String logMessage = MSQTasks.errorReportToLogMessage(errorReport);
        log.warn("%s", logMessage);

        if (controllerAlive) {
          controllerClient.postWorkerError(errorReport);
        }

        if (t != null) {
          Throwables.throwIfInstanceOf(t, MSQException.class);
          throw new MSQException(t, maybeErrorReport.get().getFault());
        } else {
          throw new MSQException(maybeErrorReport.get().getFault());
        }
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    finally {
      runFuture.set(null);
    }
  }

  /**
   * Runs worker logic. Returns an empty Optional on success. On failure, returns an error report for errors that
   * happened in other threads; throws exceptions for errors that happened in the main worker loop.
   */
  private Optional<MSQErrorReport> runInternal(final KernelHolders kernelHolders, final Closer workerCloser)
      throws Exception
  {
    context.registerWorker(this, workerCloser);
    workerCloser.register(context.dataServerQueryHandlerFactory());
    this.workerClient = workerCloser.register(new ExceptionWrappingWorkerClient(context.makeWorkerClient()));
    final FrameProcessorExecutor workerExec = new FrameProcessorExecutor(makeProcessingPool());

    final long maxAllowedParseExceptions;

    if (task != null) {
      maxAllowedParseExceptions =
          Long.parseLong(task.getContext()
                             .getOrDefault(MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, Long.MAX_VALUE)
                             .toString());
    } else {
      maxAllowedParseExceptions = 0;
    }

    final long maxVerboseParseExceptions;
    if (maxAllowedParseExceptions == -1L) {
      maxVerboseParseExceptions = Limits.MAX_VERBOSE_PARSE_EXCEPTIONS;
    } else {
      maxVerboseParseExceptions = Math.min(maxAllowedParseExceptions, Limits.MAX_VERBOSE_PARSE_EXCEPTIONS);
    }

    final Set<String> criticalWarningCodes;
    if (maxAllowedParseExceptions == 0) {
      criticalWarningCodes = ImmutableSet.of(CannotParseExternalDataFault.CODE);
    } else {
      criticalWarningCodes = ImmutableSet.of();
    }

    // Delay removal of kernels so we don't interfere with iteration of kernelHolders.getAllKernelHolders().
    final Set<StageId> kernelsToRemove = new HashSet<>();

    while (!kernelHolders.isDone()) {
      boolean didSomething = false;

      for (final KernelHolder kernelHolder : kernelHolders.getAllKernelHolders()) {
        final WorkerStageKernel kernel = kernelHolder.kernel;
        final StageDefinition stageDefinition = kernel.getStageDefinition();

        // Workers run all work orders they get. There is not (currently) any limit on the number of concurrent work
        // orders; we rely on the controller to avoid overloading workers.
        if (kernel.getPhase() == WorkerStagePhase.NEW
            && kernelHolders.runningKernelCount() < context.maxConcurrentStages()) {
          handleNewWorkOrder(
              kernelHolder,
              controllerClient,
              workerExec,
              criticalWarningCodes,
              maxVerboseParseExceptions
          );
          logKernelStatus(kernelHolders.getAllKernels());
          didSomething = true;
        }

        if (kernel.getPhase() == WorkerStagePhase.READING_INPUT
            && handleReadingInput(kernelHolder, controllerClient)) {
          didSomething = true;
          logKernelStatus(kernelHolders.getAllKernels());
        }

        if (kernel.getPhase() == WorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES
            && handleWaitingForResultPartitionBoundaries(kernelHolder)) {
          didSomething = true;
          logKernelStatus(kernelHolders.getAllKernels());
        }

        if (kernel.getPhase() == WorkerStagePhase.RESULTS_COMPLETE
            && handleResultsReady(kernelHolder, controllerClient)) {
          didSomething = true;
          logKernelStatus(kernelHolders.getAllKernels());
        }

        if (kernel.getPhase() == WorkerStagePhase.FAILED) {
          // Return an error report when a work order fails. This is better than throwing an exception, because we can
          // include the stage number.
          return Optional.of(
              MSQErrorReport.fromException(
                  id(),
                  MSQTasks.getHostFromSelfNode(selfDruidNode),
                  stageDefinition.getId().getStageNumber(),
                  kernel.getException()
              )
          );
        }

        if (kernel.getPhase().isTerminal()) {
          handleTerminated(kernelHolder);
          kernelsToRemove.add(stageDefinition.getId());
        }
      }

      for (final StageId stageId : kernelsToRemove) {
        kernelHolders.removeKernel(stageId);
      }

      kernelsToRemove.clear();

      if (!didSomething && !kernelHolders.isDone()) {
        Consumer<KernelHolders> nextCommand;

        // Run the next command, waiting for it if necessary. Post counters to the controller every 5 seconds
        // while waiting.
        do {
          postCountersToController(kernelHolders.getControllerClient());
        } while ((nextCommand = kernelManipulationQueue.poll(5, TimeUnit.SECONDS)) == null);

        nextCommand.accept(kernelHolders);

        // Run all pending commands after that one. Helps avoid deep queues.
        // After draining the command queue, move on to the next iteration of the worker loop.
        while ((nextCommand = kernelManipulationQueue.poll()) != null) {
          nextCommand.accept(kernelHolders);
        }

        logKernelStatus(kernelHolders.getAllKernels());
      }
    }

    // Empty means success.
    return Optional.empty();
  }

  /**
   * Handle a kernel in state {@link WorkerStagePhase#NEW}. The kernel is transitioned to
   * {@link WorkerStagePhase#READING_INPUT} and a {@link RunWorkOrder} instance is created to start executing work.
   */
  private void handleNewWorkOrder(
      final KernelHolder kernelHolder,
      final ControllerClient controllerClient,
      final FrameProcessorExecutor workerExec,
      final Set<String> criticalWarningCodes,
      final long maxVerboseParseExceptions
  ) throws IOException
  {
    final WorkerStageKernel kernel = kernelHolder.kernel;
    final WorkOrder workOrder = kernel.getWorkOrder();
    final StageDefinition stageDefinition = workOrder.getStageDefinition();
    final String cancellationId = cancellationIdFor(stageDefinition.getId(), workOrder.getWorkerNumber());

    log.info(
        "Starting work order for stage[%s], workerNumber[%d]%s",
        stageDefinition.getId(),
        workOrder.getWorkerNumber(),
        (log.isDebugEnabled()
         ? StringUtils.format(", payload[%s]", context.jsonMapper().writeValueAsString(workOrder)) : "")
    );

    final FrameContext frameContext = context.frameContext(workOrder);

    // Set up resultsCloser (called when we are done reading results).
    kernelHolder.resultsCloser.register(() -> FileUtils.deleteDirectory(frameContext.tempDir()));
    kernelHolder.resultsCloser.register(() -> removeStageOutputChannels(stageDefinition.getId()));

    // Create separate inputChannelFactory per stage, because the list of tasks can grow between stages, and
    // so we need to avoid the memoization of controllerClient.getWorkerIds() in baseInputChannelFactory.
    final InputChannelFactory inputChannelFactory =
        makeBaseInputChannelFactory(workOrder, controllerClient, kernelHolder.processorCloser);

    final boolean includeAllCounters = context.includeAllCounters();
    final RunWorkOrder runWorkOrder = new RunWorkOrder(
        workOrder,
        inputChannelFactory,
        stageCounters.computeIfAbsent(
            IntObjectPair.of(workOrder.getWorkerNumber(), stageDefinition.getId()),
            ignored -> new CounterTracker(includeAllCounters)
        ),
        workerExec,
        cancellationId,
        context,
        frameContext,
        makeRunWorkOrderListener(workOrder, controllerClient, criticalWarningCodes, maxVerboseParseExceptions)
    );

    // Set up processorCloser (called when processing is done).
    kernelHolder.processorCloser.register(() -> runWorkOrder.stopUnchecked(null));

    // Start working on this stage immediately.
    kernel.startReading();
    runWorkOrder.startAsync();
    kernelHolder.partitionBoundariesFuture = runWorkOrder.getStagePartitionBoundariesFuture();
  }

  /**
   * Handle a kernel in state {@link WorkerStagePhase#READING_INPUT}.
   *
   * If the worker has finished generating result key statistics, they are posted to the controller and the kernel is
   * transitioned to {@link WorkerStagePhase#PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES}.
   *
   * @return whether kernel state changed
   */
  private boolean handleReadingInput(
      final KernelHolder kernelHolder,
      final ControllerClient controllerClient
  ) throws IOException
  {
    final WorkerStageKernel kernel = kernelHolder.kernel;
    if (kernel.hasResultKeyStatisticsSnapshot()) {
      if (controllerAlive) {
        PartialKeyStatisticsInformation partialKeyStatisticsInformation =
            kernel.getResultKeyStatisticsSnapshot()
                  .partialKeyStatistics();

        controllerClient.postPartialKeyStatistics(
            kernel.getStageDefinition().getId(),
            kernel.getWorkOrder().getWorkerNumber(),
            partialKeyStatisticsInformation
        );
      }

      kernel.startPreshuffleWaitingForResultPartitionBoundaries();
      return true;
    } else if (kernel.isDoneReadingInput()
               && kernel.getStageDefinition().doesSortDuringShuffle()
               && !kernel.getStageDefinition().mustGatherResultKeyStatistics()) {
      // Skip postDoneReadingInput when context.maxConcurrentStages() == 1, for backwards compatibility.
      // See Javadoc comment on ControllerClient#postDoneReadingInput.
      if (controllerAlive && context.maxConcurrentStages() > 1) {
        controllerClient.postDoneReadingInput(
            kernel.getStageDefinition().getId(),
            kernel.getWorkOrder().getWorkerNumber()
        );
      }

      kernel.startPreshuffleWritingOutput();
      return true;
    } else {
      return false;
    }
  }

  /**
   * Handle a kernel in state {@link WorkerStagePhase#PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES}.
   *
   * If partition boundaries have become available, the {@link KernelHolder#partitionBoundariesFuture} is updated and
   * the kernel is transitioned to state {@link WorkerStagePhase#PRESHUFFLE_WRITING_OUTPUT}.
   *
   * @return whether kernel state changed
   */
  private boolean handleWaitingForResultPartitionBoundaries(final KernelHolder kernelHolder)
  {
    if (kernelHolder.kernel.hasResultPartitionBoundaries()) {
      kernelHolder.partitionBoundariesFuture.set(kernelHolder.kernel.getResultPartitionBoundaries());
      kernelHolder.kernel.startPreshuffleWritingOutput();
      return true;
    } else {
      return false;
    }
  }

  /**
   * Handle a kernel in state {@link WorkerStagePhase#RESULTS_COMPLETE}. If {@link ControllerClient#postResultsComplete}
   * has not yet been posted to the controller, it is posted at this time. Otherwise nothing happens.
   *
   * @return whether kernel state changed
   */
  private boolean handleResultsReady(final KernelHolder kernelHolder, final ControllerClient controllerClient)
      throws IOException
  {
    final WorkerStageKernel kernel = kernelHolder.kernel;
    final boolean didNotPostYet =
        kernel.addPostedResultsComplete(kernel.getStageDefinition().getId(), kernel.getWorkOrder().getWorkerNumber());

    if (controllerAlive && didNotPostYet) {
      controllerClient.postResultsComplete(
          kernel.getStageDefinition().getId(),
          kernel.getWorkOrder().getWorkerNumber(),
          kernel.getResultObject()
      );
    }

    return didNotPostYet;
  }

  /**
   * Handle a kernel in state where {@link WorkerStagePhase#isTerminal()} is true.
   */
  private void handleTerminated(final KernelHolder kernelHolder)
  {
    final WorkerStageKernel kernel = kernelHolder.kernel;
    removeStageOutputChannels(kernel.getStageDefinition().getId());

    if (kernelHolder.kernel.getWorkOrder().getOutputChannelMode().isDurable()) {
      removeStageDurableStorageOutput(kernel.getStageDefinition().getId());
    }
  }

  @Override
  public void stop(CancellationReason reason)
  {
    // stopGracefully() is called when the containing process is terminated, or when the task is canceled.
    log.info("Worker id[%s] canceled.", context.workerId());

    if (didRun.compareAndSet(false, true)) {
      // run() hasn't been called yet. Set runFuture so awaitStop() still works.
      runFuture.set(null);
    } else {
      doCancel(reason);
    }
  }

  @Override
  public void awaitStop()
  {
    FutureUtils.getUnchecked(runFuture, false);
  }

  @Override
  public void controllerFailed()
  {
    log.info(
        "Controller task[%s] for worker[%s] failed. Canceling.",
        task != null ? task.getControllerTaskId() : null,
        id()
    );
    doCancel(CancellationReason.TASK_SHUTDOWN);
  }

  @Override
  public ListenableFuture<InputStream> readStageOutput(
      final StageId stageId,
      final int partitionNumber,
      final long offset
  )
  {
    return getOrCreateStageOutputHolder(stageId, partitionNumber).readRemotelyFrom(offset);
  }

  /**
   * Accept a new {@link WorkOrder} for execution.
   *
   * For backwards-compatibility purposes, this method populates {@link WorkOrder#getOutputChannelMode()}
   * and {@link WorkOrder#getWorkerContext()} if the controller did not set them. (They are there for newer controllers,
   * but not older ones.)
   */
  @Override
  public void postWorkOrder(final WorkOrder workOrder)
  {
    log.info(
        "Got work order for stage[%s], workerNumber[%s]",
        workOrder.getStageDefinition().getId(),
        workOrder.getWorkerNumber()
    );

    if (task != null && task.getWorkerNumber() != workOrder.getWorkerNumber()) {
      throw new ISE(
          "Worker number mismatch: expected workerNumber[%d], got[%d]",
          task.getWorkerNumber(),
          workOrder.getWorkerNumber()
      );
    }

    final WorkOrder workOrderToUse = makeWorkOrderToUse(
        workOrder,
        task != null && task.getContext() != null ? QueryContext.of(task.getContext()) : QueryContext.empty()
    );

    kernelManipulationQueue.add(
        kernelHolders ->
            kernelHolders.addKernel(WorkerStageKernel.create(workOrderToUse))
    );
  }

  @Override
  public boolean postResultPartitionBoundaries(
      final StageId stageId,
      final ClusterByPartitions stagePartitionBoundaries
  )
  {
    kernelManipulationQueue.add(
        kernelHolders -> {
          final WorkerStageKernel stageKernel = kernelHolders.getKernelFor(stageId);

          if (stageKernel != null) {
            if (!stageKernel.hasResultPartitionBoundaries()) {
              stageKernel.setResultPartitionBoundaries(stagePartitionBoundaries);
            } else {
              // Ignore if partition boundaries are already set.
              log.warn("Stage[%s] already has result partition boundaries set. Ignoring new ones.", stageId);
            }
          }
        }
    );
    return true;
  }

  @Override
  public void postCleanupStage(final StageId stageId)
  {
    log.debug("Received cleanup order for stage[%s].", stageId);
    kernelManipulationQueue.add(holder -> {
      holder.finishProcessing(stageId);
      final WorkerStageKernel kernel = holder.getKernelFor(stageId);
      if (kernel != null) {
        // Calling setStageFinished places the kernel into FINISHED state, which also means we'll ignore any
        // "Canceled" errors generated by "holder.finishProcessing(stageId)". (See WorkerStageKernel.fail)
        kernel.setStageFinished();
      }
    });
  }

  @Override
  public void postFinish()
  {
    log.debug("Received finish call.");
    kernelManipulationQueue.add(KernelHolders::setDone);
  }

  @Override
  public ClusterByStatisticsSnapshot fetchStatisticsSnapshot(StageId stageId)
  {
    log.debug("Fetching statistics for stage[%s]", stageId);
    final SettableFuture<ClusterByStatisticsSnapshot> snapshotFuture = SettableFuture.create();
    kernelManipulationQueue.add(
        holder -> {
          try {
            final WorkerStageKernel kernel = holder.getKernelFor(stageId);
            if (kernel != null) {
              final ClusterByStatisticsSnapshot snapshot = kernel.getResultKeyStatisticsSnapshot();
              if (snapshot == null) {
                throw new ISE("Requested statistics snapshot is not generated yet for stage [%s]", stageId);
              }

              snapshotFuture.set(snapshot);
            } else {
              snapshotFuture.setException(new ISE("Stage[%s] has terminated", stageId));
            }
          }
          catch (Throwable t) {
            snapshotFuture.setException(t);
          }
        }
    );
    return FutureUtils.getUnchecked(snapshotFuture, true);
  }

  @Override
  public ClusterByStatisticsSnapshot fetchStatisticsSnapshotForTimeChunk(StageId stageId, long timeChunk)
  {
    return fetchStatisticsSnapshot(stageId).getSnapshotForTimeChunk(timeChunk);
  }

  @Override
  public CounterSnapshotsTree getCounters()
  {
    final CounterSnapshotsTree retVal = new CounterSnapshotsTree();

    for (final Map.Entry<IntObjectPair<StageId>, CounterTracker> entry : stageCounters.entrySet()) {
      retVal.put(
          entry.getKey().right().getStageNumber(),
          entry.getKey().leftInt(),
          entry.getValue().snapshot()
      );
    }

    return retVal;
  }

  /**
   * Create a {@link RunWorkOrderListener} for {@link RunWorkOrder} that hooks back into the {@link KernelHolders}
   * in the main loop.
   */
  private RunWorkOrderListener makeRunWorkOrderListener(
      final WorkOrder workOrder,
      final ControllerClient controllerClient,
      final Set<String> criticalWarningCodes,
      final long maxVerboseParseExceptions
  )
  {
    final StageId stageId = workOrder.getStageDefinition().getId();
    final MSQWarningReportPublisher msqWarningReportPublisher = new MSQWarningReportLimiterPublisher(
        new MSQWarningReportSimplePublisher(
            id(),
            controllerClient,
            id(),
            MSQTasks.getHostFromSelfNode(selfDruidNode)
        ),
        Limits.MAX_VERBOSE_WARNINGS,
        ImmutableMap.of(CannotParseExternalDataFault.CODE, maxVerboseParseExceptions),
        criticalWarningCodes,
        controllerClient,
        id(),
        MSQTasks.getHostFromSelfNode(selfDruidNode)
    );

    return new RunWorkOrderListener()
    {
      @Override
      public void onDoneReadingInput(@Nullable ClusterByStatisticsSnapshot snapshot)
      {
        kernelManipulationQueue.add(
            holder -> {
              final WorkerStageKernel kernel = holder.getKernelFor(stageId);
              if (kernel != null) {
                kernel.setResultKeyStatisticsSnapshot(snapshot);
              }
            }
        );
      }

      @Override
      public void onOutputChannelAvailable(OutputChannel channel)
      {
        ReadableFrameChannel readableChannel = null;

        try {
          readableChannel = channel.getReadableChannel();
          getOrCreateStageOutputHolder(stageId, channel.getPartitionNumber())
              .setChannel(readableChannel);
        }
        catch (Exception e) {
          if (readableChannel != null) {
            try {
              readableChannel.close();
            }
            catch (Throwable e2) {
              e.addSuppressed(e2);
            }
          }

          kernelManipulationQueue.add(holder -> {
            throw new RE(e, "Worker completion callback error for stage [%s]", stageId);
          });
        }
      }

      @Override
      public void onSuccess(Object resultObject)
      {
        kernelManipulationQueue.add(
            holder -> {
              // Call finishProcessing prior to transitioning to RESULTS_COMPLETE, so the FrameContext is closed
              // and resources are released.
              holder.finishProcessing(stageId);

              final WorkerStageKernel kernel = holder.getKernelFor(stageId);
              if (kernel != null) {
                kernel.setResultsComplete(resultObject);
              }
            }
        );
      }

      @Override
      public void onWarning(Throwable t)
      {
        msqWarningReportPublisher.publishException(stageId.getStageNumber(), t);
      }

      @Override
      public void onFailure(Throwable t)
      {
        kernelManipulationQueue.add(
            holder -> {
              final WorkerStageKernel kernel = holder.getKernelFor(stageId);
              if (kernel != null) {
                kernel.fail(t);
              }
            }
        );
      }
    };
  }

  private InputChannelFactory makeBaseInputChannelFactory(
      final WorkOrder workOrder,
      final ControllerClient controllerClient,
      final Closer closer
  )
  {
    return MetaInputChannelFactory.create(
        InputSlices.allStageSlices(workOrder.getInputs()),
        workOrder.getOutputChannelMode(),
        outputChannelMode -> {
          switch (outputChannelMode) {
            case MEMORY:
            case LOCAL_STORAGE:
              final Supplier<List<String>> workerIds;

              if (workOrder.getWorkerIds() != null) {
                workerIds = workOrder::getWorkerIds;
              } else {
                workerIds = Suppliers.memoize(
                    () -> {
                      try {
                        return controllerClient.getWorkerIds();
                      }
                      catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                    }
                );
              }

              return new WorkerOrLocalInputChannelFactory(
                  id(),
                  workerIds,
                  new WorkerInputChannelFactory(workerClient, workerIds),
                  this::getOrCreateStageOutputHolder
              );

            case DURABLE_STORAGE_INTERMEDIATE:
            case DURABLE_STORAGE_QUERY_RESULTS:
              return DurableStorageInputChannelFactory.createStandardImplementation(
                  task.getControllerTaskId(),
                  MSQTasks.makeStorageConnector(context.injector()),
                  closer,
                  outputChannelMode == OutputChannelMode.DURABLE_STORAGE_QUERY_RESULTS
              );

            default:
              throw DruidException.defensive("No handling for output channel mode[%s]", outputChannelMode);
          }
        }
    );
  }

  /**
   * Decorates the server-wide {@link QueryProcessingPool} such that any Callables and Runnables, not just
   * {@link PrioritizedCallable} and {@link PrioritizedRunnable}, may be added to it.
   * <p>
   * In production, the underlying {@link QueryProcessingPool} pool is set up by
   * {@link org.apache.druid.guice.DruidProcessingModule}.
   */
  private ListeningExecutorService makeProcessingPool()
  {
    final QueryProcessingPool queryProcessingPool = context.injector().getInstance(QueryProcessingPool.class);
    final int priority = 0;

    return new DecoratedExecutorService(
        queryProcessingPool,
        new DecoratedExecutorService.Decorator()
        {
          @Override
          public <T> Callable<T> decorateCallable(Callable<T> callable)
          {
            return new PrioritizedCallable<>()
            {
              @Override
              public int getPriority()
              {
                return priority;
              }

              @Override
              public T call() throws Exception
              {
                return callable.call();
              }
            };
          }

          @Override
          public Runnable decorateRunnable(Runnable runnable)
          {
            return new PrioritizedRunnable()
            {
              @Override
              public int getPriority()
              {
                return priority;
              }

              @Override
              public void run()
              {
                runnable.run();
              }
            };
          }
        }
    );
  }

  /**
   * Posts all counters for this worker to the controller.
   */
  private void postCountersToController(final ControllerClient controllerClient) throws IOException
  {
    final CounterSnapshotsTree snapshotsTree = getCounters();

    if (controllerAlive && !snapshotsTree.isEmpty()) {
      controllerClient.postCounters(id(), snapshotsTree);
    }
  }

  /**
   * Removes and closes all output channels for a stage from {@link #stageOutputs}.
   */
  private void removeStageOutputChannels(final StageId stageId)
  {
    // This code is thread-safe because remove() on ConcurrentHashMap will remove and return the removed channel only for
    // one thread. For the other threads it will return null, therefore we will call doneReading for a channel only once
    final ConcurrentHashMap<Integer, StageOutputHolder> partitionOutputsForStage = stageOutputs.remove(stageId);
    // Check for null, this can be the case if this method is called simultaneously from multiple threads.
    if (partitionOutputsForStage == null) {
      return;
    }
    for (final int partition : partitionOutputsForStage.keySet()) {
      final StageOutputHolder output = partitionOutputsForStage.remove(partition);
      if (output != null) {
        output.close();
      }
    }
  }

  /**
   * Remove outputs from durable storage for a particular stage.
   */
  private void removeStageDurableStorageOutput(final StageId stageId)
  {
    // One caveat with this approach is that in case of a worker crash, while the MM/Indexer systems will delete their
    // temp directories where intermediate results were stored, it won't be the case for the external storage.
    // Therefore, the logic for cleaning the stage output in case of a worker/machine crash has to be external.
    // We currently take care of this in the controller.
    final String folderName = DurableStorageUtils.getTaskIdOutputsFolderName(
        task.getControllerTaskId(),
        stageId.getStageNumber(),
        task.getWorkerNumber(),
        context.workerId()
    );
    try {
      MSQTasks.makeStorageConnector(context.injector()).deleteRecursively(folderName);
    }
    catch (Exception e) {
      // If an error is thrown while cleaning up a file, log it and try to continue with the cleanup
      log.warn(e, "Error while cleaning up durable storage path[%s].", folderName);
    }
  }

  private StageOutputHolder getOrCreateStageOutputHolder(final StageId stageId, final int partitionNumber)
  {
    return stageOutputs.computeIfAbsent(stageId, ignored1 -> new ConcurrentHashMap<>())
                       .computeIfAbsent(partitionNumber, ignored -> new StageOutputHolder());
  }

  /**
   * Returns cancellation ID for a particular stage, to be used in {@link FrameProcessorExecutor#cancel(String)}.
   * In addition to being a token for cancellation, this also appears in thread dumps, so make it a little descriptive.
   */
  private static String cancellationIdFor(final StageId stageId, final int workerNumber)
  {
    return StringUtils.format("msq-worker[%s_%s]", stageId, workerNumber);
  }

  /**
   * Called by {@link #stop(CancellationReason reason)} (task canceled, or containing process shut down) and
   * {@link #controllerFailed()}.
   */
  private void doCancel(CancellationReason reason)
  {
    // Set controllerAlive = false so we don't try to contact the controller after being canceled. If it canceled us,
    // it doesn't need to know that we were canceled. If we were canceled by something else, the controller will
    // detect this as part of its monitoring of workers.
    controllerAlive = false;

    // Close controller client to cancel any currently in-flight calls to the controller.
    if (controllerClient != null) {
      controllerClient.close();
    }

    // Close worker client to cancel any currently in-flight calls to other workers.
    if (workerClient != null) {
      CloseableUtils.closeAndSuppressExceptions(workerClient, e -> log.warn("Failed to close workerClient"));
    }

    // Clear the main loop event queue, then throw a CanceledFault into the loop to exit it promptly.
    kernelManipulationQueue.clear();
    kernelManipulationQueue.add(
        kernel -> {
          throw new MSQException(new CanceledFault(reason));
        }
    );
  }

  /**
   * Returns a work order based on the provided "originalWorkOrder", but where {@link WorkOrder#hasOutputChannelMode()}
   * and {@link WorkOrder#hasWorkerContext()} are both true. If the original work order didn't have those fields, they
   * are populated from the "taskContext". Otherwise the "taskContext" is ignored.
   *
   * This method can be removed once we can rely on these fields always being set in the WorkOrder.
   * (They will be there for newer controllers; this is a backwards-compatibility method.)
   *
   * @param originalWorkOrder work order from controller
   * @param taskContext       task context
   */
  static WorkOrder makeWorkOrderToUse(final WorkOrder originalWorkOrder, @Nullable final QueryContext taskContext)
  {
    // This condition can be removed once we can rely on QueryContext always being in the WorkOrder.
    // (It will be there for newer controllers; this is a backwards-compatibility thing.)
    final QueryContext queryContext;
    if (originalWorkOrder.hasWorkerContext()) {
      queryContext = originalWorkOrder.getWorkerContext();
    } else if (taskContext != null) {
      queryContext = taskContext;
    } else {
      queryContext = QueryContext.empty();
    }

    // This stack of conditions can be removed once we can rely on OutputChannelMode always being in the WorkOrder.
    // (It will be there for newer controllers; this is a backwards-compatibility thing.)
    final OutputChannelMode outputChannelMode;
    if (originalWorkOrder.hasOutputChannelMode()) {
      outputChannelMode = originalWorkOrder.getOutputChannelMode();
    } else {
      outputChannelMode = ControllerQueryKernelUtils.getOutputChannelMode(
          originalWorkOrder.getQueryDefinition(),
          originalWorkOrder.getStageNumber(),
          MultiStageQueryContext.getSelectDestination(queryContext),
          MultiStageQueryContext.isDurableStorageEnabled(queryContext),
          false
      );
    }

    return originalWorkOrder.withWorkerContext(queryContext).withOutputChannelMode(outputChannelMode);
  }

  /**
   * Log (at DEBUG level) a string explaining the status of all work assigned to this worker.
   */
  private static void logKernelStatus(final Iterable<WorkerStageKernel> kernels)
  {
    if (log.isDebugEnabled()) {
      log.debug(
          "Stages: %s",
          StreamSupport.stream(kernels.spliterator(), false)
                       .sorted(Comparator.comparing(k -> k.getStageDefinition().getStageNumber()))
                       .map(WorkerImpl::makeKernelStageStatusString)
                       .collect(Collectors.joining("; "))
      );
    }
  }

  /**
   * Helper used by {@link #logKernelStatus}.
   */
  private static String makeKernelStageStatusString(final WorkerStageKernel kernel)
  {
    final String inputPartitionNumbers =
        StreamSupport.stream(InputSlices.allReadablePartitions(kernel.getWorkOrder().getInputs()).spliterator(), false)
                     .map(ReadablePartition::getPartitionNumber)
                     .sorted()
                     .map(String::valueOf)
                     .collect(Collectors.joining(","));

    // String like ">50" if shuffling to 50 partitions, ">?" if shuffling to unknown number of partitions.
    final String shuffleStatus =
        kernel.getStageDefinition().doesShuffle()
        ? ">" + (kernel.hasResultPartitionBoundaries() ? kernel.getResultPartitionBoundaries().size() : "?")
        : "";

    return StringUtils.format(
        "S%d:W%d:P[%s]%s:%s:%s",
        kernel.getStageDefinition().getStageNumber(),
        kernel.getWorkOrder().getWorkerNumber(),
        inputPartitionNumbers,
        shuffleStatus,
        kernel.getStageDefinition().doesShuffle() ? "SHUFFLE" : "RETAIN",
        kernel.getPhase()
    );
  }

  /**
   * Holds {@link WorkerStageKernel} and {@link Closer}, one per {@link WorkOrder}. Also holds {@link ControllerClient}.
   * Only manipulated by the main loop. Other threads that need to manipulate kernels must do so through
   * {@link #kernelManipulationQueue}.
   */
  private static class KernelHolders implements Closeable
  {
    private final WorkerContext workerContext;
    private final ControllerClient controllerClient;

    /**
     * Stage number -> kernel holder.
     */
    private final Int2ObjectMap<KernelHolder> holderMap = new Int2ObjectOpenHashMap<>();

    private boolean done = false;

    private KernelHolders(final WorkerContext workerContext, final ControllerClient controllerClient)
    {
      this.workerContext = workerContext;
      this.controllerClient = controllerClient;
    }

    public static KernelHolders create(final WorkerContext workerContext, final Closer closer)
    {
      return closer.register(new KernelHolders(workerContext, closer.register(workerContext.makeControllerClient())));
    }

    /**
     * Add a {@link WorkerStageKernel} to this holder. Also creates a {@link ControllerClient} for the query ID
     * if one does not yet exist. Does nothing if a kernel with the same {@link StageId} is already being tracked.
     */
    public void addKernel(final WorkerStageKernel kernel)
    {
      final StageId stageId = kernel.getWorkOrder().getStageDefinition().getId();

      if (holderMap.putIfAbsent(stageId.getStageNumber(), new KernelHolder(kernel)) != null) {
        // Already added. Do nothing.
      }
    }

    /**
     * Called when processing for a stage is complete. Releases processing resources associated with the stage, i.e.,
     * those that are part of {@link KernelHolder#processorCloser}.
     *
     * Does not release results-fetching resources, i.e., does not release {@link KernelHolder#resultsCloser}. Those
     * resources are released on {@link #removeKernel(StageId)} only.
     */
    public void finishProcessing(final StageId stageId)
    {
      final KernelHolder kernel = holderMap.get(stageId.getStageNumber());

      if (kernel != null) {
        try {
          kernel.processorCloser.close();
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    /**
     * Remove the {@link WorkerStageKernel} for a given {@link StageId} from this holder. Closes all the associated
     * {@link Closeable}. Removes and closes the {@link ControllerClient} for this query ID, if there are no longer
     * any active work orders for that query ID
     *
     * @throws IllegalStateException if there is no active kernel for this stage
     */
    public void removeKernel(final StageId stageId)
    {
      final KernelHolder removed = holderMap.remove(stageId.getStageNumber());

      if (removed == null) {
        throw new ISE("No kernel for stage[%s]", stageId);
      }

      try {
        removed.processorCloser.close();
        removed.resultsCloser.close();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Returns all currently-active kernel holders.
     */
    public Iterable<KernelHolder> getAllKernelHolders()
    {
      return holderMap.values();
    }

    /**
     * Returns all currently-active kernels.
     */
    public Iterable<WorkerStageKernel> getAllKernels()
    {
      return Iterables.transform(holderMap.values(), holder -> holder.kernel);
    }

    /**
     * Returns the number of kernels that are in running states, where {@link WorkerStagePhase#isRunning()}.
     */
    public int runningKernelCount()
    {
      int retVal = 0;
      for (final KernelHolder holder : holderMap.values()) {
        if (holder.kernel.getPhase().isRunning()) {
          retVal++;
        }
      }

      return retVal;
    }

    /**
     * Return the kernel for a particular {@link StageId}.
     *
     * @return kernel, or null if there is no active kernel for this stage
     */
    @Nullable
    public WorkerStageKernel getKernelFor(final StageId stageId)
    {
      final KernelHolder holder = holderMap.get(stageId.getStageNumber());
      if (holder != null) {
        return holder.kernel;
      } else {
        return null;
      }
    }

    /**
     * Retrieves the {@link ControllerClient}, which is shared across all {@link WorkOrder} for this worker.
     */
    public ControllerClient getControllerClient()
    {
      return controllerClient;
    }

    /**
     * Remove all {@link WorkerStageKernel} and close all {@link ControllerClient}.
     */
    @Override
    public void close()
    {
      for (final int stageNumber : ImmutableList.copyOf(holderMap.keySet())) {
        final StageId stageId = new StageId(workerContext.queryId(), stageNumber);

        try {
          removeKernel(stageId);
        }
        catch (Exception e) {
          log.warn(e, "Failed to remove kernel for stage[%s].", stageId);
        }
      }
    }

    /**
     * Check whether {@link #setDone()} has been called.
     */
    public boolean isDone()
    {
      return done;
    }

    /**
     * Mark the holder as "done", signaling to the main loop that it should clean up and exit as soon as possible.
     */
    public void setDone()
    {
      this.done = true;
    }
  }

  /**
   * Holder for a single {@link WorkerStageKernel} and associated items, contained within {@link KernelHolders}.
   */
  private static class KernelHolder
  {
    private final WorkerStageKernel kernel;
    private SettableFuture<ClusterByPartitions> partitionBoundariesFuture;

    /**
     * Closer for processing. This is closed when all processing for a stage has completed.
     */
    private final Closer processorCloser;

    /**
     * Closer for results. This is closed when results for a stage are no longer needed. Always closed
     * *after* {@link #processorCloser} is done closing.
     */
    private final Closer resultsCloser;

    public KernelHolder(WorkerStageKernel kernel)
    {
      this.kernel = kernel;
      this.processorCloser = Closer.create();
      this.resultsCloser = Closer.create();
    }
  }
}
