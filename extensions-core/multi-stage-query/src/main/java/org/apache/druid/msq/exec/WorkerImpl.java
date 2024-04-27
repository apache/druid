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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.allocation.ArenaMemoryAllocatorFactory;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.ByteTracker;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.frame.channel.ReadableFileFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableNilFrameChannel;
import org.apache.druid.frame.file.FrameFile;
import org.apache.druid.frame.file.FrameFileWriter;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.processor.BlockingQueueOutputChannelFactory;
import org.apache.druid.frame.processor.Bouncer;
import org.apache.druid.frame.processor.ComposingOutputChannelFactory;
import org.apache.druid.frame.processor.FileOutputChannelFactory;
import org.apache.druid.frame.processor.FrameChannelHashPartitioner;
import org.apache.druid.frame.processor.FrameChannelMixer;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.frame.processor.OutputChannels;
import org.apache.druid.frame.processor.PartitionedOutputChannel;
import org.apache.druid.frame.processor.SuperSorter;
import org.apache.druid.frame.processor.SuperSorterProgressTracker;
import org.apache.druid.frame.processor.manager.ProcessorManager;
import org.apache.druid.frame.processor.manager.ProcessorManagers;
import org.apache.druid.frame.util.DurableStorageUtils;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.counters.CounterNames;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.indexing.CountingOutputChannelFactory;
import org.apache.druid.msq.indexing.InputChannelFactory;
import org.apache.druid.msq.indexing.InputChannelsImpl;
import org.apache.druid.msq.indexing.MSQWorkerTask;
import org.apache.druid.msq.indexing.destination.MSQSelectDestination;
import org.apache.druid.msq.indexing.error.CanceledFault;
import org.apache.druid.msq.indexing.error.CannotParseExternalDataFault;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.MSQFaultUtils;
import org.apache.druid.msq.indexing.error.MSQWarningReportLimiterPublisher;
import org.apache.druid.msq.indexing.error.MSQWarningReportPublisher;
import org.apache.druid.msq.indexing.error.MSQWarningReportSimplePublisher;
import org.apache.druid.msq.indexing.error.MSQWarnings;
import org.apache.druid.msq.indexing.processor.KeyStatisticsCollectionProcessor;
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
import org.apache.druid.msq.input.stage.ReadablePartition;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.input.stage.StageInputSliceReader;
import org.apache.druid.msq.input.table.SegmentsInputSlice;
import org.apache.druid.msq.input.table.SegmentsInputSliceReader;
import org.apache.druid.msq.kernel.FrameContext;
import org.apache.druid.msq.kernel.FrameProcessorFactory;
import org.apache.druid.msq.kernel.ProcessorsAndChannels;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.StagePartition;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.kernel.worker.WorkerStageKernel;
import org.apache.druid.msq.kernel.worker.WorkerStagePhase;
import org.apache.druid.msq.shuffle.input.DurableStorageInputChannelFactory;
import org.apache.druid.msq.shuffle.input.WorkerInputChannelFactory;
import org.apache.druid.msq.shuffle.output.DurableStorageOutputChannelFactory;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.msq.statistics.PartialKeyStatisticsInformation;
import org.apache.druid.msq.util.DecoratedExecutorService;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.PrioritizedCallable;
import org.apache.druid.query.PrioritizedRunnable;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.rpc.ServiceClosedException;
import org.apache.druid.server.DruidNode;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Interface for a worker of a multi-stage query.
 */
public class WorkerImpl implements Worker
{
  private static final Logger log = new Logger(WorkerImpl.class);

  private final MSQWorkerTask task;
  private final WorkerContext context;
  private final DruidNode selfDruidNode;
  private final Bouncer processorBouncer;

  private final BlockingQueue<Consumer<KernelHolder>> kernelManipulationQueue = new LinkedBlockingDeque<>();
  private final ConcurrentHashMap<StageId, ConcurrentHashMap<Integer, ReadableFrameChannel>> stageOutputs = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<StageId, CounterTracker> stageCounters = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<StageId, WorkerStageKernel> stageKernelMap = new ConcurrentHashMap<>();
  private final ByteTracker intermediateSuperSorterLocalStorageTracker;
  private final boolean durableStageStorageEnabled;
  private final WorkerStorageParameters workerStorageParameters;
  /**
   * Only set for select jobs.
   */
  @Nullable
  private final MSQSelectDestination selectDestination;

  /**
   * Set once in {@link #runTask} and never reassigned.
   */
  private volatile ControllerClient controllerClient;

  /**
   * Set once in {@link #runTask} and never reassigned. Used by processing threads so we can contact other workers
   * during a shuffle.
   */
  private volatile WorkerClient workerClient;

  /**
   * Set to false by {@link #controllerFailed()} as a way of enticing the {@link #runTask} method to exit promptly.
   */
  private volatile boolean controllerAlive = true;

  public WorkerImpl(MSQWorkerTask task, WorkerContext context)
  {
    this(
        task,
        context,
        WorkerStorageParameters.createProductionInstance(
            context.injector(),
            MultiStageQueryContext.isDurableStorageEnabled(QueryContext.of(task.getContext()))
            // If Durable Storage is enabled, then super sorter intermediate storage can be enabled.
        )
    );
  }

  @VisibleForTesting
  public WorkerImpl(MSQWorkerTask task, WorkerContext context, WorkerStorageParameters workerStorageParameters)
  {
    this.task = task;
    this.context = context;
    this.selfDruidNode = context.selfNode();
    this.processorBouncer = context.processorBouncer();
    QueryContext queryContext = QueryContext.of(task.getContext());
    this.durableStageStorageEnabled = MultiStageQueryContext.isDurableStorageEnabled(queryContext);
    this.selectDestination = MultiStageQueryContext.getSelectDestinationOrNull(queryContext);
    this.workerStorageParameters = workerStorageParameters;

    long maxBytes = workerStorageParameters.isIntermediateStorageLimitConfigured()
                    ? workerStorageParameters.getIntermediateSuperSorterStorageMaxLocalBytes()
                    : Long.MAX_VALUE;
    this.intermediateSuperSorterLocalStorageTracker = new ByteTracker(maxBytes);
  }

  @Override
  public String id()
  {
    return task.getId();
  }

  @Override
  public MSQWorkerTask task()
  {
    return task;
  }

  @Override
  public TaskStatus run() throws Exception
  {
    try (final Closer closer = Closer.create()) {
      Optional<MSQErrorReport> maybeErrorReport;

      try {
        maybeErrorReport = runTask(closer);
      }
      catch (Throwable e) {
        maybeErrorReport = Optional.of(
            MSQErrorReport.fromException(
                id(),
                MSQTasks.getHostFromSelfNode(selfDruidNode),
                null,
                e
            )
        );
      }

      if (maybeErrorReport.isPresent()) {
        final MSQErrorReport errorReport = maybeErrorReport.get();
        final String errorLogMessage = MSQTasks.errorReportToLogMessage(errorReport);
        log.warn(errorLogMessage);

        closer.register(() -> {
          if (controllerAlive && controllerClient != null && selfDruidNode != null) {
            controllerClient.postWorkerError(id(), errorReport);
          }
        });

        return TaskStatus.failure(id(), MSQFaultUtils.generateMessageWithErrorCode(errorReport.getFault()));
      } else {
        return TaskStatus.success(id());
      }
    }
  }

  /**
   * Runs worker logic. Returns an empty Optional on success. On failure, returns an error report for errors that
   * happened in other threads; throws exceptions for errors that happened in the main worker loop.
   */
  public Optional<MSQErrorReport> runTask(final Closer closer) throws Exception
  {
    this.controllerClient = context.makeControllerClient(task.getControllerTaskId());
    closer.register(controllerClient::close);
    closer.register(context.dataServerQueryHandlerFactory());
    context.registerWorker(this, closer); // Uses controllerClient, so must be called after that is initialized

    this.workerClient = new ExceptionWrappingWorkerClient(context.makeWorkerClient());
    closer.register(workerClient::close);

    final KernelHolder kernelHolder = new KernelHolder();
    final String cancellationId = id();

    final FrameProcessorExecutor workerExec = new FrameProcessorExecutor(makeProcessingPool());

    // Delete all the stage outputs
    closer.register(() -> {
      for (final StageId stageId : stageOutputs.keySet()) {
        cleanStageOutput(stageId, false);
      }
    });

    // Close stage output processors and running futures (if present)
    closer.register(() -> {
      try {
        workerExec.cancel(cancellationId);
      }
      catch (InterruptedException e) {
        // Strange that cancellation would itself be interrupted. Throw an exception, since this is unexpected.
        throw new RuntimeException(e);
      }
    });

    long maxAllowedParseExceptions = Long.parseLong(task.getContext().getOrDefault(
        MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED,
        Long.MAX_VALUE
    ).toString());

    long maxVerboseParseExceptions;
    if (maxAllowedParseExceptions == -1L) {
      maxVerboseParseExceptions = Limits.MAX_VERBOSE_PARSE_EXCEPTIONS;
    } else {
      maxVerboseParseExceptions = Math.min(maxAllowedParseExceptions, Limits.MAX_VERBOSE_PARSE_EXCEPTIONS);
    }

    Set<String> criticalWarningCodes;
    if (maxAllowedParseExceptions == 0) {
      criticalWarningCodes = ImmutableSet.of(CannotParseExternalDataFault.CODE);
    } else {
      criticalWarningCodes = ImmutableSet.of();
    }

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

    closer.register(msqWarningReportPublisher);

    final Map<StageId, SettableFuture<ClusterByPartitions>> partitionBoundariesFutureMap = new HashMap<>();

    final Map<StageId, FrameContext> stageFrameContexts = new HashMap<>();

    while (!kernelHolder.isDone()) {
      boolean didSomething = false;

      for (final WorkerStageKernel kernel : kernelHolder.getStageKernelMap().values()) {
        final StageDefinition stageDefinition = kernel.getStageDefinition();

        if (kernel.getPhase() == WorkerStagePhase.NEW) {

          log.info("Processing work order for stage [%d]" +
                   (log.isDebugEnabled()
                    ? StringUtils.format(
                       " with payload [%s]",
                       context.jsonMapper().writeValueAsString(kernel.getWorkOrder())
                   ) : ""), stageDefinition.getId().getStageNumber());

          // Create separate inputChannelFactory per stage, because the list of tasks can grow between stages, and
          // so we need to avoid the memoization in baseInputChannelFactory.
          final InputChannelFactory inputChannelFactory = makeBaseInputChannelFactory(closer);

          // Compute memory parameters for all stages, even ones that haven't been assigned yet, so we can fail-fast
          // if some won't work. (We expect that all stages will get assigned to the same pool of workers.)
          for (final StageDefinition stageDef : kernel.getWorkOrder().getQueryDefinition().getStageDefinitions()) {
            stageFrameContexts.computeIfAbsent(
                stageDef.getId(),
                stageId -> context.frameContext(
                    kernel.getWorkOrder().getQueryDefinition(),
                    stageId.getStageNumber()
                )
            );
          }

          // Start working on this stage immediately.
          kernel.startReading();

          final RunWorkOrder runWorkOrder = new RunWorkOrder(
              kernel,
              inputChannelFactory,
              stageCounters.computeIfAbsent(stageDefinition.getId(), ignored -> new CounterTracker()),
              workerExec,
              cancellationId,
              context.threadCount(),
              stageFrameContexts.get(stageDefinition.getId()),
              msqWarningReportPublisher
          );

          runWorkOrder.start();

          final SettableFuture<ClusterByPartitions> partitionBoundariesFuture =
              runWorkOrder.getStagePartitionBoundariesFuture();

          if (partitionBoundariesFuture != null) {
            if (partitionBoundariesFutureMap.put(stageDefinition.getId(), partitionBoundariesFuture) != null) {
              throw new ISE("Work order collision for stage [%s]", stageDefinition.getId());
            }
          }

          didSomething = true;
          logKernelStatus(kernelHolder.getStageKernelMap().values());
        }

        if (kernel.getPhase() == WorkerStagePhase.READING_INPUT && kernel.hasResultKeyStatisticsSnapshot()) {
          if (controllerAlive) {
            PartialKeyStatisticsInformation partialKeyStatisticsInformation =
                kernel.getResultKeyStatisticsSnapshot()
                      .partialKeyStatistics();

            controllerClient.postPartialKeyStatistics(
                stageDefinition.getId(),
                kernel.getWorkOrder().getWorkerNumber(),
                partialKeyStatisticsInformation
            );
          }
          kernel.startPreshuffleWaitingForResultPartitionBoundaries();

          didSomething = true;
          logKernelStatus(kernelHolder.getStageKernelMap().values());
        }

        logKernelStatus(kernelHolder.getStageKernelMap().values());
        if (kernel.getPhase() == WorkerStagePhase.PRESHUFFLE_WAITING_FOR_RESULT_PARTITION_BOUNDARIES
            && kernel.hasResultPartitionBoundaries()) {
          partitionBoundariesFutureMap.get(stageDefinition.getId()).set(kernel.getResultPartitionBoundaries());
          kernel.startPreshuffleWritingOutput();

          didSomething = true;
          logKernelStatus(kernelHolder.getStageKernelMap().values());
        }

        if (kernel.getPhase() == WorkerStagePhase.RESULTS_READY
            && kernel.addPostedResultsComplete(Pair.of(
            stageDefinition.getId(),
            kernel.getWorkOrder().getWorkerNumber()
        ))) {
          if (controllerAlive) {
            controllerClient.postResultsComplete(
                stageDefinition.getId(),
                kernel.getWorkOrder().getWorkerNumber(),
                kernel.getResultObject()
            );
          }
        }

        if (kernel.getPhase() == WorkerStagePhase.FAILED) {
          // Better than throwing an exception, because we can include the stage number.
          return Optional.of(
              MSQErrorReport.fromException(
                  id(),
                  MSQTasks.getHostFromSelfNode(selfDruidNode),
                  stageDefinition.getId().getStageNumber(),
                  kernel.getException()
              )
          );
        }
      }

      if (!didSomething && !kernelHolder.isDone()) {
        Consumer<KernelHolder> nextCommand;

        do {
          postCountersToController();
        } while ((nextCommand = kernelManipulationQueue.poll(5, TimeUnit.SECONDS)) == null);

        nextCommand.accept(kernelHolder);
        logKernelStatus(kernelHolder.getStageKernelMap().values());
      }
    }

    // Empty means success.
    return Optional.empty();
  }

  @Override
  public void stopGracefully()
  {
    // stopGracefully() is called when the containing process is terminated, or when the task is canceled.
    log.info("Worker task[%s] canceled.", task.getId());
    doCancel();
  }

  @Override
  public void controllerFailed()
  {
    log.info("Controller task[%s] for worker task[%s] failed. Canceling.", task.getControllerTaskId(), task.getId());
    doCancel();
  }

  @Override
  public InputStream readChannel(
      final String queryId,
      final int stageNumber,
      final int partitionNumber,
      final long offset
  ) throws IOException
  {
    final StageId stageId = new StageId(queryId, stageNumber);
    final StagePartition stagePartition = new StagePartition(stageId, partitionNumber);
    final ConcurrentHashMap<Integer, ReadableFrameChannel> partitionOutputsForStage = stageOutputs.get(stageId);

    if (partitionOutputsForStage == null) {
      return null;
    }
    final ReadableFrameChannel channel = partitionOutputsForStage.get(partitionNumber);

    if (channel == null) {
      return null;
    }

    if (channel instanceof ReadableNilFrameChannel) {
      // Build an empty frame file.
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      FrameFileWriter.open(Channels.newChannel(baos), null, ByteTracker.unboundedTracker()).close();

      final ByteArrayInputStream in = new ByteArrayInputStream(baos.toByteArray());

      //noinspection ResultOfMethodCallIgnored: OK to ignore since "skip" always works for ByteArrayInputStream.
      in.skip(offset);

      return in;
    } else if (channel instanceof ReadableFileFrameChannel) {
      // Close frameFile once we've returned an input stream: no need to retain a reference to the mmap after that,
      // since we aren't using it.
      try (final FrameFile frameFile = ((ReadableFileFrameChannel) channel).newFrameFileReference()) {
        final RandomAccessFile randomAccessFile = new RandomAccessFile(frameFile.file(), "r");

        if (offset >= randomAccessFile.length()) {
          randomAccessFile.close();
          return new ByteArrayInputStream(ByteArrays.EMPTY_ARRAY);
        } else {
          randomAccessFile.seek(offset);
          return Channels.newInputStream(randomAccessFile.getChannel());
        }
      }
    } else {
      String errorMsg = StringUtils.format(
          "Returned server error to client because channel for [%s] is not nil or file-based (class = %s)",
          stagePartition,
          channel.getClass().getName()
      );
      log.error(StringUtils.encodeForFormat(errorMsg));

      throw new IOException(errorMsg);
    }
  }

  @Override
  public void postWorkOrder(final WorkOrder workOrder)
  {
    log.info("Got work order for stage [%d]", workOrder.getStageNumber());
    if (task.getWorkerNumber() != workOrder.getWorkerNumber()) {
      throw new ISE("Worker number mismatch: expected [%d]", task.getWorkerNumber());
    }

    // Do not add to queue if workerOrder already present.
    kernelManipulationQueue.add(
        kernelHolder ->
            kernelHolder.getStageKernelMap().putIfAbsent(
                workOrder.getStageDefinition().getId(),
                WorkerStageKernel.create(workOrder)
            )
    );
  }

  @Override
  public boolean postResultPartitionBoundaries(
      final ClusterByPartitions stagePartitionBoundaries,
      final String queryId,
      final int stageNumber
  )
  {
    final StageId stageId = new StageId(queryId, stageNumber);

    kernelManipulationQueue.add(
        kernelHolder -> {
          final WorkerStageKernel stageKernel = kernelHolder.getStageKernelMap().get(stageId);

          if (stageKernel != null) {
            if (!stageKernel.hasResultPartitionBoundaries()) {
              stageKernel.setResultPartitionBoundaries(stagePartitionBoundaries);
            } else {
              // Ignore if partition boundaries are already set.
              log.warn(
                  "Stage[%s] already has result partition boundaries set. Ignoring the latest partition boundaries recieved.",
                  stageId
              );
            }
          } else {
            // Ignore the update if we don't have a kernel for this stage.
            log.warn("Ignored result partition boundaries call for unknown stage [%s]", stageId);
          }
        }
    );
    return true;
  }

  @Override
  public void postCleanupStage(final StageId stageId)
  {
    log.info("Cleanup order for stage [%s] received", stageId);
    kernelManipulationQueue.add(
        holder -> {
          cleanStageOutput(stageId, true);
          // Mark the stage as FINISHED
          WorkerStageKernel stageKernel = holder.getStageKernelMap().get(stageId);
          if (stageKernel == null) {
            log.warn("Stage id [%s] non existent. Unable to mark the stage kernel for it as FINISHED", stageId);
          } else {
            stageKernel.setStageFinished();
          }
        }
    );
  }

  @Override
  public void postFinish()
  {
    log.info("Finish received for task [%s]", task.getId());
    kernelManipulationQueue.add(KernelHolder::setDone);
  }

  @Override
  public ClusterByStatisticsSnapshot fetchStatisticsSnapshot(StageId stageId)
  {
    log.info("Fetching statistics for stage [%d]", stageId.getStageNumber());
    if (stageKernelMap.get(stageId) == null) {
      throw new ISE("Requested statistics snapshot for non-existent stageId %s.", stageId);
    } else if (stageKernelMap.get(stageId).getResultKeyStatisticsSnapshot() == null) {
      throw new ISE(
          "Requested statistics snapshot is not generated yet for stageId [%s]",
          stageId
      );
    } else {
      return stageKernelMap.get(stageId).getResultKeyStatisticsSnapshot();
    }
  }

  @Override
  public ClusterByStatisticsSnapshot fetchStatisticsSnapshotForTimeChunk(StageId stageId, long timeChunk)
  {
    log.debug(
        "Fetching statistics for stage [%d]  with time chunk [%d] ",
        stageId.getStageNumber(),
        timeChunk
    );
    if (stageKernelMap.get(stageId) == null) {
      throw new ISE("Requested statistics snapshot for non-existent stageId [%s].", stageId);
    } else if (stageKernelMap.get(stageId).getResultKeyStatisticsSnapshot() == null) {
      throw new ISE(
          "Requested statistics snapshot is not generated yet for stageId [%s]",
          stageId
      );
    } else {
      return stageKernelMap.get(stageId)
                           .getResultKeyStatisticsSnapshot()
                           .getSnapshotForTimeChunk(timeChunk);
    }

  }


  @Override
  public CounterSnapshotsTree getCounters()
  {
    final CounterSnapshotsTree retVal = new CounterSnapshotsTree();

    for (final Map.Entry<StageId, CounterTracker> entry : stageCounters.entrySet()) {
      retVal.put(entry.getKey().getStageNumber(), task().getWorkerNumber(), entry.getValue().snapshot());
    }

    return retVal;
  }

  private InputChannelFactory makeBaseInputChannelFactory(final Closer closer)
  {
    final Supplier<List<String>> workerTaskList = Suppliers.memoize(
        () -> {
          try {
            return controllerClient.getTaskList();
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
    )::get;

    if (durableStageStorageEnabled) {
      return DurableStorageInputChannelFactory.createStandardImplementation(
          task.getControllerTaskId(),
          MSQTasks.makeStorageConnector(context.injector()),
          closer,
          false
      );
    } else {
      return new WorkerOrLocalInputChannelFactory(workerTaskList);
    }
  }

  private OutputChannelFactory makeStageOutputChannelFactory(
      final FrameContext frameContext,
      final int stageNumber,
      boolean isFinalStage
  )
  {
    // Use the standard frame size, since we assume this size when computing how much is needed to merge output
    // files from different workers.
    final int frameSize = frameContext.memoryParameters().getStandardFrameSize();

    if (durableStageStorageEnabled || (isFinalStage
                                       && MSQSelectDestination.DURABLESTORAGE.equals(selectDestination))) {
      return DurableStorageOutputChannelFactory.createStandardImplementation(
          task.getControllerTaskId(),
          task().getWorkerNumber(),
          stageNumber,
          task().getId(),
          frameSize,
          MSQTasks.makeStorageConnector(context.injector()),
          context.tempDir(),
          (isFinalStage && MSQSelectDestination.DURABLESTORAGE.equals(selectDestination))
      );
    } else {
      final File fileChannelDirectory =
          new File(context.tempDir(), StringUtils.format("output_stage_%06d", stageNumber));

      return new FileOutputChannelFactory(fileChannelDirectory, frameSize, null);
    }
  }

  private OutputChannelFactory makeSuperSorterIntermediateOutputChannelFactory(
      final FrameContext frameContext,
      final int stageNumber,
      final File tmpDir
  )
  {
    final int frameSize = frameContext.memoryParameters().getLargeFrameSize();
    final File fileChannelDirectory =
        new File(tmpDir, StringUtils.format("intermediate_output_stage_%06d", stageNumber));
    final FileOutputChannelFactory fileOutputChannelFactory =
        new FileOutputChannelFactory(fileChannelDirectory, frameSize, intermediateSuperSorterLocalStorageTracker);

    if (durableStageStorageEnabled && workerStorageParameters.isIntermediateStorageLimitConfigured()) {
      return new ComposingOutputChannelFactory(
          ImmutableList.of(
              fileOutputChannelFactory,
              DurableStorageOutputChannelFactory.createStandardImplementation(
                  task.getControllerTaskId(),
                  task().getWorkerNumber(),
                  stageNumber,
                  task().getId(),
                  frameSize,
                  MSQTasks.makeStorageConnector(context.injector()),
                  tmpDir,
                  false
              )
          ),
          frameSize
      );
    } else {
      return fileOutputChannelFactory;
    }
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
            return new PrioritizedCallable<T>()
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
  private void postCountersToController() throws IOException
  {
    final CounterSnapshotsTree snapshotsTree = getCounters();

    if (controllerAlive && !snapshotsTree.isEmpty()) {
      try {
        controllerClient.postCounters(id(), snapshotsTree);
      }
      catch (IOException e) {
        if (e.getCause() instanceof ServiceClosedException) {
          // Suppress. This can happen if the controller goes away while a postCounters call is in flight.
          log.debug(e, "Ignoring failure on postCounters, because controller has gone away.");
        } else {
          throw e;
        }
      }
    }
  }

  /**
   * Cleans up the stage outputs corresponding to the provided stage id. It essentially calls {@code doneReading()} on
   * the readable channels corresponding to all the partitions for that stage, and removes it from the {@code stageOutputs}
   * map
   */
  private void cleanStageOutput(final StageId stageId, boolean removeDurableStorageFiles)
  {
    // This code is thread-safe because remove() on ConcurrentHashMap will remove and return the removed channel only for
    // one thread. For the other threads it will return null, therefore we will call doneReading for a channel only once
    final ConcurrentHashMap<Integer, ReadableFrameChannel> partitionOutputsForStage = stageOutputs.remove(stageId);
    // Check for null, this can be the case if this method is called simultaneously from multiple threads.
    if (partitionOutputsForStage == null) {
      return;
    }
    for (final int partition : partitionOutputsForStage.keySet()) {
      final ReadableFrameChannel output = partitionOutputsForStage.remove(partition);
      if (output == null) {
        continue;
      }
      output.close();
    }
    // One caveat with this approach is that in case of a worker crash, while the MM/Indexer systems will delete their
    // temp directories where intermediate results were stored, it won't be the case for the external storage.
    // Therefore, the logic for cleaning the stage output in case of a worker/machine crash has to be external.
    // We currently take care of this in the controller.
    if (durableStageStorageEnabled && removeDurableStorageFiles) {
      final String folderName = DurableStorageUtils.getTaskIdOutputsFolderName(
          task.getControllerTaskId(),
          stageId.getStageNumber(),
          task.getWorkerNumber(),
          task.getId()
      );
      try {
        MSQTasks.makeStorageConnector(context.injector()).deleteRecursively(folderName);
      }
      catch (Exception e) {
        // If an error is thrown while cleaning up a file, log it and try to continue with the cleanup
        log.warn(e, "Error while cleaning up folder at path " + folderName);
      }
    }
  }

  /**
   * Called by {@link #stopGracefully()} (task canceled, or containing process shut down) and
   * {@link #controllerFailed()}.
   */
  private void doCancel()
  {
    // Set controllerAlive = false so we don't try to contact the controller after being canceled. If it canceled us,
    // it doesn't need to know that we were canceled. If we were canceled by something else, the controller will
    // detect this as part of its monitoring of workers.
    controllerAlive = false;

    // Close controller client to cancel any currently in-flight calls to the controller.
    if (controllerClient != null) {
      controllerClient.close();
    }

    // Clear the main loop event queue, then throw a CanceledFault into the loop to exit it promptly.
    kernelManipulationQueue.clear();
    kernelManipulationQueue.add(
        kernel -> {
          throw new MSQException(CanceledFault.INSTANCE);
        }
    );
  }

  /**
   * Log (at DEBUG level) a string explaining the status of all work assigned to this worker.
   */
  private static void logKernelStatus(final Collection<WorkerStageKernel> kernels)
  {
    if (log.isDebugEnabled()) {
      log.debug(
          "Stages: %s",
          kernels.stream()
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
   * An {@link InputChannelFactory} that loads data locally when possible, and otherwise connects directly to other
   * workers. Used when durable shuffle storage is off.
   */
  private class WorkerOrLocalInputChannelFactory implements InputChannelFactory
  {
    private final Supplier<List<String>> taskList;
    private final WorkerInputChannelFactory workerInputChannelFactory;

    public WorkerOrLocalInputChannelFactory(final Supplier<List<String>> taskList)
    {
      this.workerInputChannelFactory = new WorkerInputChannelFactory(workerClient, taskList);
      this.taskList = taskList;
    }

    @Override
    public ReadableFrameChannel openChannel(StageId stageId, int workerNumber, int partitionNumber)
    {
      final String taskId = taskList.get().get(workerNumber);
      if (taskId.equals(id())) {
        final ConcurrentMap<Integer, ReadableFrameChannel> partitionOutputsForStage = stageOutputs.get(stageId);
        if (partitionOutputsForStage == null) {
          throw new ISE("Unable to find outputs for stage [%s]", stageId);
        }

        final ReadableFrameChannel myChannel = partitionOutputsForStage.get(partitionNumber);

        if (myChannel instanceof ReadableFileFrameChannel) {
          // Must duplicate the channel to avoid double-closure upon task cleanup.
          final FrameFile frameFile = ((ReadableFileFrameChannel) myChannel).newFrameFileReference();
          return new ReadableFileFrameChannel(frameFile);
        } else if (myChannel instanceof ReadableNilFrameChannel) {
          return myChannel;
        } else {
          throw new ISE("Output for stage [%s] are stored in an instance of %s which is not "
                        + "supported", stageId, myChannel.getClass());
        }
      } else {
        return workerInputChannelFactory.openChannel(stageId, workerNumber, partitionNumber);
      }
    }
  }

  /**
   * Main worker logic for executing a {@link WorkOrder}.
   */
  private class RunWorkOrder
  {
    private final WorkerStageKernel kernel;
    private final InputChannelFactory inputChannelFactory;
    private final CounterTracker counterTracker;
    private final FrameProcessorExecutor exec;
    private final String cancellationId;
    private final int parallelism;
    private final FrameContext frameContext;
    private final MSQWarningReportPublisher warningPublisher;

    private InputSliceReader inputSliceReader;
    private OutputChannelFactory workOutputChannelFactory;
    private OutputChannelFactory shuffleOutputChannelFactory;
    private ResultAndChannels<?> workResultAndOutputChannels;
    private SettableFuture<ClusterByPartitions> stagePartitionBoundariesFuture;
    private ListenableFuture<OutputChannels> shuffleOutputChannelsFuture;

    public RunWorkOrder(
        final WorkerStageKernel kernel,
        final InputChannelFactory inputChannelFactory,
        final CounterTracker counterTracker,
        final FrameProcessorExecutor exec,
        final String cancellationId,
        final int parallelism,
        final FrameContext frameContext,
        final MSQWarningReportPublisher warningPublisher
    )
    {
      this.kernel = kernel;
      this.inputChannelFactory = inputChannelFactory;
      this.counterTracker = counterTracker;
      this.exec = exec;
      this.cancellationId = cancellationId;
      this.parallelism = parallelism;
      this.frameContext = frameContext;
      this.warningPublisher = warningPublisher;
    }

    private void start() throws IOException
    {
      final WorkOrder workOrder = kernel.getWorkOrder();
      final StageDefinition stageDef = workOrder.getStageDefinition();

      final boolean isFinalStage = stageDef.getStageNumber() == workOrder.getQueryDefinition()
                                                                         .getFinalStageDefinition()
                                                                         .getStageNumber();

      makeInputSliceReader();
      makeWorkOutputChannelFactory(isFinalStage);
      makeShuffleOutputChannelFactory(isFinalStage);
      makeAndRunWorkProcessors();

      if (stageDef.doesShuffle()) {
        makeAndRunShuffleProcessors();
      } else {
        // No shuffling: work output _is_ shuffle output. Retain read-only versions to reduce memory footprint.
        shuffleOutputChannelsFuture =
            Futures.immediateFuture(workResultAndOutputChannels.getOutputChannels().readOnly());
      }

      setUpCompletionCallbacks(isFinalStage);
    }

    /**
     * Settable {@link ClusterByPartitions} future for global sort. Necessary because we don't know ahead of time
     * what the boundaries will be. The controller decides based on statistics from all workers. Once the controller
     * decides, its decision is written to this future, which allows sorting on workers to proceed.
     */
    @Nullable
    public SettableFuture<ClusterByPartitions> getStagePartitionBoundariesFuture()
    {
      return stagePartitionBoundariesFuture;
    }

    private void makeInputSliceReader()
    {
      if (inputSliceReader != null) {
        throw new ISE("inputSliceReader already created");
      }

      final WorkOrder workOrder = kernel.getWorkOrder();
      final String queryId = workOrder.getQueryDefinition().getQueryId();

      final InputChannels inputChannels =
          new InputChannelsImpl(
              workOrder.getQueryDefinition(),
              InputSlices.allReadablePartitions(workOrder.getInputs()),
              inputChannelFactory,
              () -> ArenaMemoryAllocator.createOnHeap(frameContext.memoryParameters().getStandardFrameSize()),
              exec,
              cancellationId
          );

      inputSliceReader = new MapInputSliceReader(
          ImmutableMap.<Class<? extends InputSlice>, InputSliceReader>builder()
                      .put(NilInputSlice.class, NilInputSliceReader.INSTANCE)
                      .put(StageInputSlice.class, new StageInputSliceReader(queryId, inputChannels))
                      .put(ExternalInputSlice.class, new ExternalInputSliceReader(frameContext.tempDir()))
                      .put(InlineInputSlice.class, new InlineInputSliceReader(frameContext.segmentWrangler()))
                      .put(LookupInputSlice.class, new LookupInputSliceReader(frameContext.segmentWrangler()))
                      .put(
                          SegmentsInputSlice.class,
                          new SegmentsInputSliceReader(
                              frameContext,
                              MultiStageQueryContext.isReindex(QueryContext.of(task().getContext()))
                          )
                      )
                      .build()
      );
    }

    private void makeWorkOutputChannelFactory(boolean isFinalStage)
    {
      if (workOutputChannelFactory != null) {
        throw new ISE("processorOutputChannelFactory already created");
      }

      final OutputChannelFactory baseOutputChannelFactory;

      if (kernel.getStageDefinition().doesShuffle()) {
        // Writing to a consumer in the same JVM (which will be set up later on in this method). Use the large frame
        // size if we're writing to a SuperSorter, since we'll generate fewer temp files if we use larger frames.
        // Otherwise, use the standard frame size.
        final int frameSize;

        if (kernel.getStageDefinition().getShuffleSpec().kind().isSort()) {
          frameSize = frameContext.memoryParameters().getLargeFrameSize();
        } else {
          frameSize = frameContext.memoryParameters().getStandardFrameSize();
        }

        baseOutputChannelFactory = new BlockingQueueOutputChannelFactory(frameSize);
      } else {
        // Writing stage output.
        baseOutputChannelFactory =
            makeStageOutputChannelFactory(frameContext, kernel.getStageDefinition().getStageNumber(), isFinalStage);
      }

      workOutputChannelFactory = new CountingOutputChannelFactory(
          baseOutputChannelFactory,
          counterTracker.channel(CounterNames.outputChannel())
      );
    }

    private void makeShuffleOutputChannelFactory(boolean isFinalStage)
    {
      shuffleOutputChannelFactory =
          new CountingOutputChannelFactory(
              makeStageOutputChannelFactory(frameContext, kernel.getStageDefinition().getStageNumber(), isFinalStage),
              counterTracker.channel(CounterNames.shuffleChannel())
          );
    }

    /**
     * Use {@link FrameProcessorFactory#makeProcessors} to create {@link ProcessorsAndChannels}. Executes the
     * processors using {@link #exec} and sets the output channels in {@link #workResultAndOutputChannels}.
     *
     * @param <FactoryType>         type of {@link StageDefinition#getProcessorFactory()}
     * @param <ProcessorReturnType> return type of {@link FrameProcessor} created by the manager
     * @param <ManagerReturnType>   result type of {@link ProcessorManager#result()}
     * @param <ExtraInfoType>       type of {@link WorkOrder#getExtraInfo()}
     */
    private <FactoryType extends FrameProcessorFactory<ProcessorReturnType, ManagerReturnType, ExtraInfoType>, ProcessorReturnType, ManagerReturnType, ExtraInfoType> void makeAndRunWorkProcessors()
        throws IOException
    {
      if (workResultAndOutputChannels != null) {
        throw new ISE("workResultAndOutputChannels already set");
      }

      @SuppressWarnings("unchecked")
      final FactoryType processorFactory = (FactoryType) kernel.getStageDefinition().getProcessorFactory();

      @SuppressWarnings("unchecked")
      final ProcessorsAndChannels<ProcessorReturnType, ManagerReturnType> processors =
          processorFactory.makeProcessors(
              kernel.getStageDefinition(),
              kernel.getWorkOrder().getWorkerNumber(),
              kernel.getWorkOrder().getInputs(),
              inputSliceReader,
              (ExtraInfoType) kernel.getWorkOrder().getExtraInfo(),
              workOutputChannelFactory,
              frameContext,
              parallelism,
              counterTracker,
              e -> warningPublisher.publishException(kernel.getStageDefinition().getStageNumber(), e)
          );

      final ProcessorManager<ProcessorReturnType, ManagerReturnType> processorManager = processors.getProcessorManager();

      final int maxOutstandingProcessors;

      if (processors.getOutputChannels().getAllChannels().isEmpty()) {
        // No output channels: run up to "parallelism" processors at once.
        maxOutstandingProcessors = Math.max(1, parallelism);
      } else {
        // If there are output channels, that acts as a ceiling on the number of processors that can run at once.
        maxOutstandingProcessors =
            Math.max(1, Math.min(parallelism, processors.getOutputChannels().getAllChannels().size()));
      }

      final ListenableFuture<ManagerReturnType> workResultFuture = exec.runAllFully(
          processorManager,
          maxOutstandingProcessors,
          processorBouncer,
          cancellationId
      );

      workResultAndOutputChannels = new ResultAndChannels<>(workResultFuture, processors.getOutputChannels());
    }

    private void makeAndRunShuffleProcessors()
    {
      if (shuffleOutputChannelsFuture != null) {
        throw new ISE("shuffleOutputChannelsFuture already set");
      }

      final ShuffleSpec shuffleSpec = kernel.getWorkOrder().getStageDefinition().getShuffleSpec();

      final ShufflePipelineBuilder shufflePipeline = new ShufflePipelineBuilder(
          kernel,
          counterTracker,
          exec,
          cancellationId,
          frameContext
      );

      shufflePipeline.initialize(workResultAndOutputChannels);

      switch (shuffleSpec.kind()) {
        case MIX:
          shufflePipeline.mix(shuffleOutputChannelFactory);
          break;

        case HASH:
          shufflePipeline.hashPartition(shuffleOutputChannelFactory);
          break;

        case HASH_LOCAL_SORT:
          final OutputChannelFactory hashOutputChannelFactory;

          if (shuffleSpec.partitionCount() == 1) {
            // Single partition; no need to write temporary files.
            hashOutputChannelFactory =
                new BlockingQueueOutputChannelFactory(frameContext.memoryParameters().getStandardFrameSize());
          } else {
            // Multi-partition; write temporary files and then sort each one file-by-file.
            hashOutputChannelFactory =
                new FileOutputChannelFactory(
                    context.tempDir(kernel.getStageDefinition().getStageNumber(), "hash-parts"),
                    frameContext.memoryParameters().getStandardFrameSize(),
                    null
                );
          }

          shufflePipeline.hashPartition(hashOutputChannelFactory);
          shufflePipeline.localSort(shuffleOutputChannelFactory);
          break;

        case GLOBAL_SORT:
          shufflePipeline.gatherResultKeyStatisticsIfNeeded();
          shufflePipeline.globalSort(shuffleOutputChannelFactory, makeGlobalSortPartitionBoundariesFuture());
          break;

        default:
          throw new UOE("Cannot handle shuffle kind [%s]", shuffleSpec.kind());
      }

      shuffleOutputChannelsFuture = shufflePipeline.build();
    }

    private ListenableFuture<ClusterByPartitions> makeGlobalSortPartitionBoundariesFuture()
    {
      if (kernel.getStageDefinition().mustGatherResultKeyStatistics()) {
        if (stagePartitionBoundariesFuture != null) {
          throw new ISE("Cannot call 'makeGlobalSortPartitionBoundariesFuture' twice");
        }

        return (stagePartitionBoundariesFuture = SettableFuture.create());
      } else {
        return Futures.immediateFuture(kernel.getResultPartitionBoundaries());
      }
    }

    private void setUpCompletionCallbacks(boolean isFinalStage)
    {
      final StageDefinition stageDef = kernel.getStageDefinition();

      Futures.addCallback(
          Futures.allAsList(
              Arrays.asList(
                  workResultAndOutputChannels.getResultFuture(),
                  shuffleOutputChannelsFuture
              )
          ),
          new FutureCallback<List<Object>>()
          {
            @Override
            public void onSuccess(final List<Object> workerResultAndOutputChannelsResolved)
            {
              final Object resultObject = workerResultAndOutputChannelsResolved.get(0);
              final OutputChannels outputChannels = (OutputChannels) workerResultAndOutputChannelsResolved.get(1);

              for (OutputChannel channel : outputChannels.getAllChannels()) {
                try {
                  stageOutputs.computeIfAbsent(stageDef.getId(), ignored1 -> new ConcurrentHashMap<>())
                              .computeIfAbsent(channel.getPartitionNumber(), ignored2 -> channel.getReadableChannel());
                }
                catch (Exception e) {
                  kernelManipulationQueue.add(holder -> {
                    throw new RE(e, "Worker completion callback error for stage [%s]", stageDef.getId());
                  });

                  // Don't make the "setResultsComplete" call below.
                  return;
                }
              }

              // Once the outputs channels have been resolved and are ready for reading, write success file, if
              // using durable storage.
              writeDurableStorageSuccessFileIfNeeded(stageDef.getStageNumber(), isFinalStage);

              kernelManipulationQueue.add(holder -> holder.getStageKernelMap()
                                                          .get(stageDef.getId())
                                                          .setResultsComplete(resultObject));
            }

            @Override
            public void onFailure(final Throwable t)
            {
              kernelManipulationQueue.add(
                  kernelHolder ->
                      kernelHolder.getStageKernelMap().get(stageDef.getId()).fail(t)
              );
            }
          },
          MoreExecutors.directExecutor()
      );
    }

    /**
     * Write {@link DurableStorageUtils#SUCCESS_MARKER_FILENAME} for a particular stage, if durable storage is enabled.
     */
    private void writeDurableStorageSuccessFileIfNeeded(final int stageNumber, boolean isFinalStage)
    {
      final DurableStorageOutputChannelFactory durableStorageOutputChannelFactory;
      if (durableStageStorageEnabled || (isFinalStage
                                         && MSQSelectDestination.DURABLESTORAGE.equals(selectDestination))) {
        durableStorageOutputChannelFactory = DurableStorageOutputChannelFactory.createStandardImplementation(
            task.getControllerTaskId(),
            task().getWorkerNumber(),
            stageNumber,
            task().getId(),
            frameContext.memoryParameters().getStandardFrameSize(),
            MSQTasks.makeStorageConnector(context.injector()),
            context.tempDir(),
            (isFinalStage && MSQSelectDestination.DURABLESTORAGE.equals(selectDestination))
        );
      } else {
        return;
      }
      try {
        durableStorageOutputChannelFactory.createSuccessFile(task.getId());
      }
      catch (IOException e) {
        throw new ISE(
            e,
            "Unable to create the success file [%s] at the location [%s]",
            DurableStorageUtils.SUCCESS_MARKER_FILENAME,
            durableStorageOutputChannelFactory.getSuccessFilePath()
        );
      }
    }
  }

  /**
   * Helper for {@link RunWorkOrder#makeAndRunShuffleProcessors()}. Builds a {@link FrameProcessor} pipeline to
   * handle the shuffle.
   */
  private class ShufflePipelineBuilder
  {
    private final WorkerStageKernel kernel;
    private final CounterTracker counterTracker;
    private final FrameProcessorExecutor exec;
    private final String cancellationId;
    private final FrameContext frameContext;

    // Current state of the pipeline. It's a future to allow pipeline construction to be deferred if necessary.
    private ListenableFuture<ResultAndChannels<?>> pipelineFuture;

    public ShufflePipelineBuilder(
        final WorkerStageKernel kernel,
        final CounterTracker counterTracker,
        final FrameProcessorExecutor exec,
        final String cancellationId,
        final FrameContext frameContext
    )
    {
      this.kernel = kernel;
      this.counterTracker = counterTracker;
      this.exec = exec;
      this.cancellationId = cancellationId;
      this.frameContext = frameContext;
    }

    /**
     * Start the pipeline with the outputs of the main processor.
     */
    public void initialize(final ResultAndChannels<?> resultAndChannels)
    {
      if (pipelineFuture != null) {
        throw new ISE("already initialized");
      }

      pipelineFuture = Futures.immediateFuture(resultAndChannels);
    }

    /**
     * Add {@link FrameChannelMixer}, which mixes all current outputs into a single channel from the provided factory.
     */
    public void mix(final OutputChannelFactory outputChannelFactory)
    {
      // No sorting or statistics gathering, just combining all outputs into one big partition. Use a mixer to get
      // everything into one file. Note: even if there is only one output channel, we'll run it through the mixer
      // anyway, to ensure the data gets written to a file. (httpGetChannelData requires files.)

      push(
          resultAndChannels -> {
            final OutputChannel outputChannel = outputChannelFactory.openChannel(0);

            final FrameChannelMixer mixer =
                new FrameChannelMixer(
                    resultAndChannels.getOutputChannels().getAllReadableChannels(),
                    outputChannel.getWritableChannel()
                );

            return new ResultAndChannels<>(
                exec.runFully(mixer, cancellationId),
                OutputChannels.wrap(Collections.singletonList(outputChannel.readOnly()))
            );
          }
      );
    }

    /**
     * Add {@link KeyStatisticsCollectionProcessor} if {@link StageDefinition#mustGatherResultKeyStatistics()}.
     */
    public void gatherResultKeyStatisticsIfNeeded()
    {
      push(
          resultAndChannels -> {
            final StageDefinition stageDefinition = kernel.getStageDefinition();
            final OutputChannels channels = resultAndChannels.getOutputChannels();

            if (channels.getAllChannels().isEmpty()) {
              // No data coming out of this processor. Report empty statistics, if the kernel is expecting statistics.
              if (stageDefinition.mustGatherResultKeyStatistics()) {
                kernelManipulationQueue.add(
                    holder ->
                        holder.getStageKernelMap().get(stageDefinition.getId())
                              .setResultKeyStatisticsSnapshot(ClusterByStatisticsSnapshot.empty())
                );
              }

              // Generate one empty channel so the SuperSorter has something to do.
              final BlockingQueueFrameChannel channel = BlockingQueueFrameChannel.minimal();
              channel.writable().close();

              final OutputChannel outputChannel = OutputChannel.readOnly(
                  channel.readable(),
                  FrameWithPartition.NO_PARTITION
              );

              return new ResultAndChannels<>(
                  Futures.immediateFuture(null),
                  OutputChannels.wrap(Collections.singletonList(outputChannel))
              );
            } else if (stageDefinition.mustGatherResultKeyStatistics()) {
              return gatherResultKeyStatistics(channels);
            } else {
              return resultAndChannels;
            }
          }
      );
    }

    /**
     * Add a {@link SuperSorter} using {@link StageDefinition#getSortKey()} and partition boundaries
     * from {@code partitionBoundariesFuture}.
     */
    public void globalSort(
        final OutputChannelFactory outputChannelFactory,
        final ListenableFuture<ClusterByPartitions> partitionBoundariesFuture
    )
    {
      pushAsync(
          resultAndChannels -> {
            final StageDefinition stageDefinition = kernel.getStageDefinition();

            final File sorterTmpDir = context.tempDir(stageDefinition.getStageNumber(), "super-sort");
            FileUtils.mkdirp(sorterTmpDir);
            if (!sorterTmpDir.isDirectory()) {
              throw new IOException("Cannot create directory: " + sorterTmpDir);
            }

            final WorkerMemoryParameters memoryParameters = frameContext.memoryParameters();
            final SuperSorter sorter = new SuperSorter(
                resultAndChannels.getOutputChannels().getAllReadableChannels(),
                stageDefinition.getFrameReader(),
                stageDefinition.getSortKey(),
                partitionBoundariesFuture,
                exec,
                outputChannelFactory,
                makeSuperSorterIntermediateOutputChannelFactory(
                    frameContext,
                    stageDefinition.getStageNumber(),
                    sorterTmpDir
                ),
                memoryParameters.getSuperSorterMaxActiveProcessors(),
                memoryParameters.getSuperSorterMaxChannelsPerProcessor(),
                -1,
                cancellationId,
                counterTracker.sortProgress()
            );

            return FutureUtils.transform(
                sorter.run(),
                sortedChannels -> new ResultAndChannels<>(Futures.immediateFuture(null), sortedChannels)
            );
          }
      );
    }

    /**
     * Add a {@link FrameChannelHashPartitioner} using {@link StageDefinition#getSortKey()}.
     */
    public void hashPartition(final OutputChannelFactory outputChannelFactory)
    {
      pushAsync(
          resultAndChannels -> {
            final ShuffleSpec shuffleSpec = kernel.getStageDefinition().getShuffleSpec();
            final int partitions = shuffleSpec.partitionCount();

            final List<OutputChannel> outputChannels = new ArrayList<>();

            for (int i = 0; i < partitions; i++) {
              outputChannels.add(outputChannelFactory.openChannel(i));
            }

            final FrameChannelHashPartitioner partitioner = new FrameChannelHashPartitioner(
                resultAndChannels.getOutputChannels().getAllReadableChannels(),
                outputChannels.stream().map(OutputChannel::getWritableChannel).collect(Collectors.toList()),
                kernel.getStageDefinition().getFrameReader(),
                kernel.getStageDefinition().getClusterBy().getColumns().size(),
                FrameWriters.makeFrameWriterFactory(
                    FrameType.ROW_BASED,
                    new ArenaMemoryAllocatorFactory(frameContext.memoryParameters().getStandardFrameSize()),
                    kernel.getStageDefinition().getSignature(),
                    kernel.getStageDefinition().getSortKey()
                )
            );

            final ListenableFuture<Long> partitionerFuture = exec.runFully(partitioner, cancellationId);

            final ResultAndChannels<Long> retVal =
                new ResultAndChannels<>(partitionerFuture, OutputChannels.wrap(outputChannels));

            if (retVal.getOutputChannels().areReadableChannelsReady()) {
              return Futures.immediateFuture(retVal);
            } else {
              return FutureUtils.transform(partitionerFuture, ignored -> retVal);
            }
          }
      );
    }

    /**
     * Add a sequence of {@link SuperSorter}, operating on each current output channel in order, one at a time.
     */
    public void localSort(final OutputChannelFactory outputChannelFactory)
    {
      pushAsync(
          resultAndChannels -> {
            final StageDefinition stageDefinition = kernel.getStageDefinition();
            final OutputChannels channels = resultAndChannels.getOutputChannels();
            final List<ListenableFuture<OutputChannel>> sortedChannelFutures = new ArrayList<>();

            ListenableFuture<OutputChannel> nextFuture = Futures.immediateFuture(null);

            for (final OutputChannel channel : channels.getAllChannels()) {
              final File sorterTmpDir = context.tempDir(
                  stageDefinition.getStageNumber(),
                  StringUtils.format("hash-parts-super-sort-%06d", channel.getPartitionNumber())
              );

              FileUtils.mkdirp(sorterTmpDir);

              // SuperSorter will try to write to output partition zero; we remap it to the correct partition number.
              final OutputChannelFactory partitionOverrideOutputChannelFactory = new OutputChannelFactory()
              {
                @Override
                public OutputChannel openChannel(int expectedZero) throws IOException
                {
                  if (expectedZero != 0) {
                    throw new ISE("Unexpected part [%s]", expectedZero);
                  }

                  return outputChannelFactory.openChannel(channel.getPartitionNumber());
                }

                @Override
                public PartitionedOutputChannel openPartitionedChannel(String name, boolean deleteAfterRead)
                {
                  throw new UnsupportedOperationException();
                }

                @Override
                public OutputChannel openNilChannel(int expectedZero)
                {
                  if (expectedZero != 0) {
                    throw new ISE("Unexpected part [%s]", expectedZero);
                  }

                  return outputChannelFactory.openNilChannel(channel.getPartitionNumber());
                }
              };

              // Chain futures so we only sort one partition at a time.
              nextFuture = Futures.transformAsync(
                  nextFuture,
                  (AsyncFunction<OutputChannel, OutputChannel>) ignored -> {
                    final SuperSorter sorter = new SuperSorter(
                        Collections.singletonList(channel.getReadableChannel()),
                        stageDefinition.getFrameReader(),
                        stageDefinition.getSortKey(),
                        Futures.immediateFuture(ClusterByPartitions.oneUniversalPartition()),
                        exec,
                        partitionOverrideOutputChannelFactory,
                        makeSuperSorterIntermediateOutputChannelFactory(
                            frameContext,
                            stageDefinition.getStageNumber(),
                            sorterTmpDir
                        ),
                        1,
                        2,
                        -1,
                        cancellationId,

                        // Tracker is not actually tracked, since it doesn't quite fit into the way we report counters.
                        // There's a single SuperSorterProgressTrackerCounter per worker, but workers that do local
                        // sorting have a SuperSorter per partition.
                        new SuperSorterProgressTracker()
                    );

                    return FutureUtils.transform(sorter.run(), r -> Iterables.getOnlyElement(r.getAllChannels()));
                  },
                  MoreExecutors.directExecutor()
              );

              sortedChannelFutures.add(nextFuture);
            }

            return FutureUtils.transform(
                Futures.allAsList(sortedChannelFutures),
                sortedChannels -> new ResultAndChannels<>(
                    Futures.immediateFuture(null),
                    OutputChannels.wrap(sortedChannels)
                )
            );
          }
      );
    }

    /**
     * Return the (future) output channels for this pipeline.
     */
    public ListenableFuture<OutputChannels> build()
    {
      if (pipelineFuture == null) {
        throw new ISE("Not initialized");
      }

      return Futures.transformAsync(
          pipelineFuture,
          (AsyncFunction<ResultAndChannels<?>, OutputChannels>) resultAndChannels ->
              Futures.transform(
                  resultAndChannels.getResultFuture(),
                  (Function<Object, OutputChannels>) input -> {
                    sanityCheckOutputChannels(resultAndChannels.getOutputChannels());
                    return resultAndChannels.getOutputChannels();
                  },
                  MoreExecutors.directExecutor()
              ),
          MoreExecutors.directExecutor()
      );
    }

    /**
     * Adds {@link KeyStatisticsCollectionProcessor}. Called by {@link #gatherResultKeyStatisticsIfNeeded()}.
     */
    private ResultAndChannels<?> gatherResultKeyStatistics(final OutputChannels channels)
    {
      final StageDefinition stageDefinition = kernel.getStageDefinition();
      final List<OutputChannel> retVal = new ArrayList<>();
      final List<KeyStatisticsCollectionProcessor> processors = new ArrayList<>();

      for (final OutputChannel outputChannel : channels.getAllChannels()) {
        final BlockingQueueFrameChannel channel = BlockingQueueFrameChannel.minimal();
        retVal.add(OutputChannel.readOnly(channel.readable(), outputChannel.getPartitionNumber()));

        processors.add(
            new KeyStatisticsCollectionProcessor(
                outputChannel.getReadableChannel(),
                channel.writable(),
                stageDefinition.getFrameReader(),
                stageDefinition.getClusterBy(),
                stageDefinition.createResultKeyStatisticsCollector(
                    frameContext.memoryParameters().getPartitionStatisticsMaxRetainedBytes()
                )
            )
        );
      }

      final ListenableFuture<ClusterByStatisticsCollector> clusterByStatisticsCollectorFuture =
          exec.runAllFully(
              ProcessorManagers.of(processors)
                               .withAccumulation(
                                   stageDefinition.createResultKeyStatisticsCollector(
                                       frameContext.memoryParameters().getPartitionStatisticsMaxRetainedBytes()
                                   ),
                                   ClusterByStatisticsCollector::addAll
                               ),
              // Run all processors simultaneously. They are lightweight and this keeps things moving.
              processors.size(),
              Bouncer.unlimited(),
              cancellationId
          );

      Futures.addCallback(
          clusterByStatisticsCollectorFuture,
          new FutureCallback<ClusterByStatisticsCollector>()
          {
            @Override
            public void onSuccess(final ClusterByStatisticsCollector result)
            {
              kernelManipulationQueue.add(
                  holder ->
                      holder.getStageKernelMap().get(stageDefinition.getId())
                            .setResultKeyStatisticsSnapshot(result.snapshot())
              );
            }

            @Override
            public void onFailure(Throwable t)
            {
              kernelManipulationQueue.add(
                  holder -> {
                    log.noStackTrace()
                       .warn(t, "Failed to gather clusterBy statistics for stage [%s]", stageDefinition.getId());
                    holder.getStageKernelMap().get(stageDefinition.getId()).fail(t);
                  }
              );
            }
          },
          MoreExecutors.directExecutor()
      );

      return new ResultAndChannels<>(
          clusterByStatisticsCollectorFuture,
          OutputChannels.wrap(retVal)
      );
    }

    /**
     * Update the {@link #pipelineFuture}.
     */
    private void push(final ExceptionalFunction<ResultAndChannels<?>, ResultAndChannels<?>> fn)
    {
      pushAsync(
          channels ->
              Futures.immediateFuture(fn.apply(channels))
      );
    }

    /**
     * Update the {@link #pipelineFuture} asynchronously.
     */
    private void pushAsync(final ExceptionalFunction<ResultAndChannels<?>, ListenableFuture<ResultAndChannels<?>>> fn)
    {
      if (pipelineFuture == null) {
        throw new ISE("Not initialized");
      }

      pipelineFuture = FutureUtils.transform(
          Futures.transformAsync(
              pipelineFuture,
              new AsyncFunction<ResultAndChannels<?>, ResultAndChannels<?>>()
              {
                @Override
                public ListenableFuture<ResultAndChannels<?>> apply(ResultAndChannels<?> t) throws Exception
                {
                  return fn.apply(t);
                }
              },
              MoreExecutors.directExecutor()
          ),
          resultAndChannels -> new ResultAndChannels<>(
              resultAndChannels.getResultFuture(),
              resultAndChannels.getOutputChannels().readOnly()
          )
      );
    }

    /**
     * Verifies there is exactly one channel per partition.
     */
    private void sanityCheckOutputChannels(final OutputChannels outputChannels)
    {
      for (int partitionNumber : outputChannels.getPartitionNumbers()) {
        final List<OutputChannel> outputChannelsForPartition =
            outputChannels.getChannelsForPartition(partitionNumber);

        Preconditions.checkState(partitionNumber >= 0, "Expected partitionNumber >= 0, but got [%s]", partitionNumber);
        Preconditions.checkState(
            outputChannelsForPartition.size() == 1,
            "Expected one channel for partition [%s], but got [%s]",
            partitionNumber,
            outputChannelsForPartition.size()
        );
      }
    }
  }

  private class KernelHolder
  {
    private boolean done = false;

    public Map<StageId, WorkerStageKernel> getStageKernelMap()
    {
      return stageKernelMap;
    }

    public boolean isDone()
    {
      return done;
    }

    public void setDone()
    {
      this.done = true;
    }
  }

  private static class ResultAndChannels<T>
  {
    private final ListenableFuture<T> resultFuture;
    private final OutputChannels outputChannels;

    public ResultAndChannels(
        ListenableFuture<T> resultFuture,
        OutputChannels outputChannels
    )
    {
      this.resultFuture = resultFuture;
      this.outputChannels = outputChannels;
    }

    public ListenableFuture<T> getResultFuture()
    {
      return resultFuture;
    }

    public OutputChannels getOutputChannels()
    {
      return outputChannels;
    }
  }

  private interface ExceptionalFunction<T, R>
  {
    R apply(T t) throws Exception;
  }
}
