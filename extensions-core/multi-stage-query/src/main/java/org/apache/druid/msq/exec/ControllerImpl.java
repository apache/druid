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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.ReadableConcatFrameChannel;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.ClusterByPartition;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.frame.key.RowKeyReader;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.frame.util.DurableStorageUtils;
import org.apache.druid.frame.write.InvalidFieldException;
import org.apache.druid.frame.write.InvalidNullByteException;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.granularity.GranularitySpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.LockReleaseAction;
import org.apache.druid.indexing.common.actions.MarkSegmentsAsUnusedAction;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalAppendAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalReplaceAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.TooManyBucketsException;
import org.apache.druid.indexing.common.task.batch.parallel.TombstoneHelper;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.counters.CounterSnapshots;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.indexing.InputChannelFactory;
import org.apache.druid.msq.indexing.InputChannelsImpl;
import org.apache.druid.msq.indexing.LegacyMSQSpec;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.QueryDefMSQSpec;
import org.apache.druid.msq.indexing.WorkerCount;
import org.apache.druid.msq.indexing.client.ControllerChatHandler;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.msq.indexing.destination.ExportMSQDestination;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.indexing.destination.SegmentGenerationStageSpec;
import org.apache.druid.msq.indexing.destination.TerminalStageSpec;
import org.apache.druid.msq.indexing.error.CanceledFault;
import org.apache.druid.msq.indexing.error.CancellationReason;
import org.apache.druid.msq.indexing.error.CannotParseExternalDataFault;
import org.apache.druid.msq.indexing.error.FaultsExceededChecker;
import org.apache.druid.msq.indexing.error.InsertCannotAllocateSegmentFault;
import org.apache.druid.msq.indexing.error.InsertCannotBeEmptyFault;
import org.apache.druid.msq.indexing.error.InsertLockPreemptedFault;
import org.apache.druid.msq.indexing.error.InsertTimeOutOfBoundsFault;
import org.apache.druid.msq.indexing.error.InvalidFieldFault;
import org.apache.druid.msq.indexing.error.InvalidNullByteFault;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.MSQFault;
import org.apache.druid.msq.indexing.error.MSQWarningReportLimiterPublisher;
import org.apache.druid.msq.indexing.error.TooManyBucketsFault;
import org.apache.druid.msq.indexing.error.TooManySegmentsInTimeChunkFault;
import org.apache.druid.msq.indexing.error.TooManyWarningsFault;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.indexing.error.WorkerFailedFault;
import org.apache.druid.msq.indexing.error.WorkerRpcFailedFault;
import org.apache.druid.msq.indexing.processor.SegmentGeneratorStageProcessor;
import org.apache.druid.msq.indexing.report.MSQSegmentReport;
import org.apache.druid.msq.indexing.report.MSQStagesReport;
import org.apache.druid.msq.indexing.report.MSQStatusReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.input.InputSpecSlicerFactory;
import org.apache.druid.msq.input.MapInputSpecSlicer;
import org.apache.druid.msq.input.external.ExternalInputSpec;
import org.apache.druid.msq.input.external.ExternalInputSpecSlicer;
import org.apache.druid.msq.input.inline.InlineInputSpec;
import org.apache.druid.msq.input.inline.InlineInputSpecSlicer;
import org.apache.druid.msq.input.lookup.LookupInputSpec;
import org.apache.druid.msq.input.lookup.LookupInputSpecSlicer;
import org.apache.druid.msq.input.stage.InputChannels;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.input.stage.StageInputSpecSlicer;
import org.apache.druid.msq.input.table.TableInputSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.StagePartition;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernel;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernelConfig;
import org.apache.druid.msq.kernel.controller.ControllerStagePhase;
import org.apache.druid.msq.kernel.controller.WorkerInputs;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.msq.shuffle.input.DurableStorageInputChannelFactory;
import org.apache.druid.msq.shuffle.input.WorkerInputChannelFactory;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;
import org.apache.druid.msq.statistics.PartialKeyStatisticsInformation;
import org.apache.druid.msq.util.IntervalUtils;
import org.apache.druid.msq.util.MSQFutureUtils;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.DruidNode;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.storage.ExportStorageProvider;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class ControllerImpl implements Controller
{
  private static final Logger log = new Logger(ControllerImpl.class);
  private static final String RESULT_READER_CANCELLATION_ID = "result-reader";

  private final String queryId;
  private final MSQSpec querySpec;
  private final Query<?> legacyQuery;
  private final ResultsContext resultsContext;
  private final ControllerContext context;
  private volatile ControllerQueryKernelConfig queryKernelConfig;

  /**
   * Queue of "commands" to run on the {@link ControllerQueryKernel}. Various threads insert into the queue
   * using {@link #addToKernelManipulationQueue}. The main thread running {@link RunQueryUntilDone#run()} reads
   * from the queue and executes the commands.
   * <p>
   * This ensures that all manipulations on {@link ControllerQueryKernel}, and all core logic, are run in
   * a single-threaded manner.
   */
  private final BlockingQueue<Consumer<ControllerQueryKernel>> kernelManipulationQueue =
      new ArrayBlockingQueue<>(Limits.MAX_KERNEL_MANIPULATION_QUEUE_SIZE);

  // For system error reporting. This is the very first error we got from a worker. (We only report that one.)
  private final AtomicReference<MSQErrorReport> workerErrorRef = new AtomicReference<>();

  // For system warning reporting
  private final ConcurrentLinkedQueue<MSQErrorReport> workerWarnings = new ConcurrentLinkedQueue<>();

  // Query definition.
  // For live reports. Written by the main controller thread, read by HTTP threads.
  private final AtomicReference<QueryDefinition> queryDefRef = new AtomicReference<>();

  // Last reported CounterSnapshots per stage per worker
  // For live reports. Written by the main controller thread, read by HTTP threads.
  private final CounterSnapshotsTree taskCountersForLiveReports = new CounterSnapshotsTree();

  // Stage number -> stage phase
  // For live reports. Written by the main controller thread, read by HTTP threads.
  private final ConcurrentHashMap<Integer, ControllerStagePhase> stagePhasesForLiveReports = new ConcurrentHashMap<>();

  // Stage number -> runtime interval. Endpoint is eternity's end if the stage is still running.
  // For live reports. Written by the main controller thread, read by HTTP threads.
  private final ConcurrentHashMap<Integer, Interval> stageRuntimesForLiveReports = new ConcurrentHashMap<>();

  // Stage number -> worker count. Only set for stages that have started.
  // For live reports. Written by the main controller thread, read by HTTP threads.
  private final ConcurrentHashMap<Integer, Integer> stageWorkerCountsForLiveReports = new ConcurrentHashMap<>();

  // Stage number -> partition count. Only set for stages that have started.
  // For live reports. Written by the main controller thread, read by HTTP threads.
  private final ConcurrentHashMap<Integer, Integer> stagePartitionCountsForLiveReports = new ConcurrentHashMap<>();

  // Stage number -> output channel mode. Only set for stages that have started.
  // For live reports. Written by the main controller thread, read by HTTP threads.
  private final ConcurrentHashMap<Integer, OutputChannelMode> stageOutputChannelModesForLiveReports =
      new ConcurrentHashMap<>();

  private final QueryKitSpecFactory queryKitSpecFactory;

  private WorkerSketchFetcher workerSketchFetcher;

  // WorkerNumber -> WorkOrders which need to be retried and our determined by the controller.
  // Map is always populated in the main controller thread by addToRetryQueue, and pruned in retryFailedTasks.
  private final Map<Integer, Set<WorkOrder>> workOrdersToRetry = new HashMap<>();

  // Time at which the query started.
  // For live reports. Written by the main controller thread, read by HTTP threads.
  private volatile DateTime queryStartTime = null;

  private volatile DruidNode selfDruidNode;
  private volatile WorkerManager workerManager;
  private volatile WorkerClient netClient;

  private volatile FaultsExceededChecker faultsExceededChecker = null;

  private Map<Integer, ClusterStatisticsMergeMode> stageToStatsMergingMode;
  private volatile SegmentLoadStatusFetcher segmentLoadWaiter;
  @Nullable
  private MSQSegmentReport segmentReport;

  public ControllerImpl(
      final String queryId,
      final LegacyMSQSpec querySpec,
      final ResultsContext resultsContext,
      final ControllerContext controllerContext,
      final QueryKitSpecFactory queryKitSpecFactory
  )
  {
    this.queryId = Preconditions.checkNotNull(queryId, "queryId");
    this.querySpec = Preconditions.checkNotNull(querySpec, "querySpec");
    this.legacyQuery = querySpec.getQuery();
    this.resultsContext = Preconditions.checkNotNull(resultsContext, "resultsContext");
    this.context = Preconditions.checkNotNull(controllerContext, "controllerContext");
    this.queryKitSpecFactory = queryKitSpecFactory;
  }


  public ControllerImpl(
      final String queryId,
      final QueryDefMSQSpec querySpec,
      final ResultsContext resultsContext,
      final ControllerContext controllerContext,
      final QueryKitSpecFactory queryKitSpecFactory
  )
  {
    this.queryId = Preconditions.checkNotNull(queryId, "queryId");
    this.querySpec = Preconditions.checkNotNull(querySpec, "querySpec");
    this.legacyQuery = null;
    this.resultsContext = Preconditions.checkNotNull(resultsContext, "resultsContext");
    this.context = Preconditions.checkNotNull(controllerContext, "controllerContext");
    this.queryKitSpecFactory = queryKitSpecFactory;
  }


  @Override
  public String queryId()
  {
    return queryId;
  }

  @Override
  public void run(final QueryListener queryListener) throws Exception
  {
    final MSQTaskReportPayload reportPayload;
    try (final Closer closer = Closer.create()) {
      reportPayload = runInternal(queryListener, closer);
    }
    // Call onQueryComplete after Closer is fully closed, ensuring no controller-related processing is ongoing.
    queryListener.onQueryComplete(reportPayload);
  }

  @Override
  public void stop(CancellationReason reason)
  {
    final QueryDefinition queryDef = queryDefRef.get();

    // stopGracefully() is called when the containing process is terminated, or when the task is canceled.
    log.info("Query [%s] canceled.", queryDef != null ? queryDef.getQueryId() : "<no id yet>");

    stopExternalFetchers();
    addToKernelManipulationQueue(
        kernel -> {
          throw new MSQException(new CanceledFault(reason));
        }
    );

    if (workerManager != null) {
      workerManager.stop(true);
    }
  }

  private MSQTaskReportPayload runInternal(final QueryListener queryListener, final Closer closer)
  {
    QueryDefinition queryDef = null;
    ControllerQueryKernel queryKernel = null;
    ListenableFuture<?> workerTaskRunnerFuture = null;
    CounterSnapshotsTree countersSnapshot = null;
    Throwable exceptionEncountered = null;

    final TaskState taskStateForReport;
    final MSQErrorReport errorForReport;

    try {
      // Planning-related: convert the native query from MSQSpec into a multi-stage QueryDefinition.
      this.queryStartTime = DateTimes.nowUtc();
      context.registerController(this, closer);
      queryDef = initializeQueryDefAndState();

      this.netClient = closer.register(new ExceptionWrappingWorkerClient(context.newWorkerClient()));
      this.workerSketchFetcher = new WorkerSketchFetcher(
          netClient,
          workerManager,
          queryKernelConfig.isFaultTolerant(),
          MultiStageQueryContext.getSketchEncoding(querySpec.getContext())
      );
      closer.register(workerSketchFetcher::close);

      // Execution-related: run the multi-stage QueryDefinition.
      final InputSpecSlicerFactory inputSpecSlicerFactory =
          makeInputSpecSlicerFactory(context.newTableInputSpecSlicer(workerManager));

      final Pair<ControllerQueryKernel, ListenableFuture<?>> queryRunResult =
          new RunQueryUntilDone(
              queryDef,
              queryKernelConfig,
              inputSpecSlicerFactory,
              queryListener,
              closer
          ).run();

      queryKernel = Preconditions.checkNotNull(queryRunResult.lhs);
      workerTaskRunnerFuture = Preconditions.checkNotNull(queryRunResult.rhs);
      handleQueryResults(queryDef, queryKernel);
    }
    catch (Throwable e) {
      exceptionEncountered = e;
    }

    // Fetch final counters in separate try, in case runQueryUntilDone threw an exception.
    try {
      countersSnapshot = getFinalCountersSnapshot(queryKernel);
    }
    catch (Throwable e) {
      if (exceptionEncountered != null) {
        exceptionEncountered.addSuppressed(e);
      } else {
        exceptionEncountered = e;
      }
    }

    if (queryKernel != null && queryKernel.isSuccess() && exceptionEncountered == null) {
      taskStateForReport = TaskState.SUCCESS;
      errorForReport = null;
    } else {
      // Query failure. Generate an error report and log the error(s) we encountered.
      final String selfHost = MSQTasks.getHostFromSelfNode(selfDruidNode);
      final MSQErrorReport controllerError;

      if (exceptionEncountered != null) {
        controllerError = MSQErrorReport.fromException(
            queryId(),
            selfHost,
            null,
            exceptionEncountered,
            querySpec.getColumnMappings()
        );
      } else {
        controllerError = null;
      }

      MSQErrorReport workerError = workerErrorRef.get();

      taskStateForReport = TaskState.FAILED;
      errorForReport = MSQTasks.makeErrorReport(queryId(), selfHost, controllerError, workerError);

      // Log the errors we encountered.
      if (controllerError != null) {
        log.warn("Controller: %s", MSQTasks.errorReportToLogMessage(controllerError));
      }

      if (workerError != null) {
        log.warn("Worker: %s", MSQTasks.errorReportToLogMessage(workerError));
      }
    }
    if (queryKernel != null && queryKernel.isSuccess()) {
      // If successful, encourage workers to exit successfully.
      // Only send this command to participating workers. For task-based queries, this is all tasks, since tasks
      // are launched only when needed. For Dart, this is any servers that were actually assigned work items.
      postFinishToWorkers(queryKernel.getAllParticipatingWorkers());
      workerManager.stop(false);
    } else {
      // If not successful, cancel running tasks.
      if (workerManager != null) {
        workerManager.stop(true);
      }
    }

    // Wait for worker tasks to exit. Ignore their return status. At this point, we've done everything we need to do,
    // so we don't care about the task exit status.
    if (workerTaskRunnerFuture != null) {
      try {
        workerTaskRunnerFuture.get();
      }
      catch (Exception ignored) {
        // Suppress.
      }
    }

    boolean shouldWaitForSegmentLoad = MultiStageQueryContext.shouldWaitForSegmentLoad(querySpec.getContext());

    try {
      if (MSQControllerTask.isIngestion(querySpec)) {
        releaseTaskLocks();
      }

      cleanUpDurableStorageIfNeeded();

      if (queryKernel != null && queryKernel.isSuccess()) {
        if (shouldWaitForSegmentLoad && segmentLoadWaiter != null) {
          // If successful, there are segments created and segment load is enabled, segmentLoadWaiter should wait
          // for them to become available.
          log.info("Controller will now wait for segments to be loaded. The query has already finished executing,"
                   + " and results will be included once the segments are loaded, even if this query is canceled now.");
          segmentLoadWaiter.waitForSegmentsToLoad();
        }
      }
      stopExternalFetchers();
    }
    catch (Exception e) {
      log.warn(e, "Exception thrown during cleanup. Ignoring it and writing task report.");
    }

    // Generate report even if something went wrong.
    final MSQStagesReport stagesReport;

    if (queryDef != null) {
      final Map<Integer, ControllerStagePhase> stagePhaseMap;

      if (queryKernel != null) {
        // Once the query finishes, cleanup would have happened for all the stages that were successful
        // Therefore we mark it as done to make the reports prettier and more accurate
        queryKernel.markSuccessfulTerminalStagesAsFinished();
        stagePhaseMap = queryKernel.getActiveStages()
                                   .stream()
                                   .collect(
                                       Collectors.toMap(StageId::getStageNumber, queryKernel::getStagePhase)
                                   );
      } else {
        stagePhaseMap = Collections.emptyMap();
      }

      stagesReport = makeStageReport(
          queryDef,
          stagePhaseMap,
          stageRuntimesForLiveReports,
          stageWorkerCountsForLiveReports,
          stagePartitionCountsForLiveReports,
          stageOutputChannelModesForLiveReports
      );
    } else {
      stagesReport = null;
    }

    final MSQTaskReportPayload msqTaskReportPayload = new MSQTaskReportPayload(
        makeStatusReport(
            taskStateForReport,
            errorForReport,
            workerWarnings,
            queryStartTime,
            new Interval(queryStartTime, DateTimes.nowUtc()).toDurationMillis(),
            workerManager,
            segmentLoadWaiter,
            segmentReport
        ),
        stagesReport,
        countersSnapshot,
        null
    );
    // Emit summary metrics
    emitSummaryMetrics(msqTaskReportPayload, querySpec);
    return msqTaskReportPayload;
  }

  private void emitSummaryMetrics(final MSQTaskReportPayload msqTaskReportPayload, final MSQSpec querySpec)
  {
    final Set<Integer> stagesToInclude = new HashSet<>();
    final MSQStagesReport stagesReport = msqTaskReportPayload.getStages();
    if (stagesReport != null) {
      for (MSQStagesReport.Stage stage : stagesReport.getStages()) {
        boolean hasParentStage = stage.getStageDefinition()
                                      .getInputSpecs()
                                      .stream()
                                      .anyMatch(stageInput -> stageInput instanceof StageInputSpec);
        if (!hasParentStage) {
          stagesToInclude.add(stage.getStageNumber());
        }
      }
    }
    long totalProcessedBytes = 0;

    if (msqTaskReportPayload.getCounters() != null) {
      totalProcessedBytes =
          msqTaskReportPayload.getCounters()
                              .copyMap()
                              .entrySet()
                              .stream()
                              .filter(entry -> stagesReport == null || stagesToInclude.contains(entry.getKey()))
                              .flatMap(counterSnapshotsMap -> counterSnapshotsMap.getValue().values().stream())
                              .flatMap(counterSnapshots -> counterSnapshots.getMap().entrySet().stream())
                              .filter(entry -> entry.getKey().startsWith("input"))
                              .mapToLong(entry -> {
                                ChannelCounters.Snapshot snapshot = (ChannelCounters.Snapshot) entry.getValue();
                                return snapshot.getBytes() == null ? 0L : Arrays.stream(snapshot.getBytes()).sum();
                              })
                              .sum();
    }

    log.debug("Processed bytes[%d] for query[%s].", totalProcessedBytes, querySpec.getId());
    context.emitMetric("ingest/input/bytes", totalProcessedBytes);
  }

  /**
   * Releases the locks obtained by the task.
   */
  private void releaseTaskLocks() throws IOException
  {
    final List<TaskLock> locks;
    try {
      locks = context.taskActionClient().submit(new LockListAction());
      for (final TaskLock lock : locks) {
        context.taskActionClient().submit(new LockReleaseAction(lock.getInterval()));
      }
    }
    catch (IOException e) {
      throw new IOException("Failed to release locks", e);
    }
  }

  /**
   * Adds some logic to {@link #kernelManipulationQueue}, where it will, in due time, be executed by the main
   * controller loop in {@link RunQueryUntilDone#run()}.
   * <p>
   * If the consumer throws an exception, the query fails.
   */
  public void addToKernelManipulationQueue(Consumer<ControllerQueryKernel> kernelConsumer)
  {
    if (!kernelManipulationQueue.offer(kernelConsumer)) {
      final String message = "Controller kernel queue is full. Main controller loop may be delayed or stuck.";
      log.warn(message);
      throw new IllegalStateException(message);
    }
  }

  public static void ensureExportLocationEmpty(final ControllerContext context, final MSQDestination destination)
  {
    if (MSQControllerTask.isExport(destination)) {
      final ExportMSQDestination exportMSQDestination = (ExportMSQDestination) destination;
      final ExportStorageProvider exportStorageProvider = exportMSQDestination.getExportStorageProvider();

      try {
        // Check that the export destination is empty as a sanity check. We want
        // to avoid modifying any other files with export.
        Iterator<String> filesIterator = exportStorageProvider.createStorageConnector(context.taskTempDir())
            .listDir("");
        if (filesIterator.hasNext()) {
          throw DruidException.forPersona(DruidException.Persona.USER)
              .ofCategory(DruidException.Category.RUNTIME_FAILURE)
              .build(
                  "Found files at provided export destination[%s]. Export is only allowed to "
                      + "an empty path. Please provide an empty path/subdirectory or move the existing files.",
                  exportStorageProvider.getBasePath()
              );
        }
      }
      catch (IOException e) {
        throw DruidException.forPersona(DruidException.Persona.USER)
            .ofCategory(DruidException.Category.RUNTIME_FAILURE)
            .build(e, "Exception occurred while connecting to export destination.");
      }
    }
  }

  private QueryDefinition initializeQueryDefAndState()
  {
    this.selfDruidNode = context.selfNode();
    this.queryKernelConfig = context.queryKernelConfig(queryId, querySpec);

    final QueryContext queryContext = querySpec.getContext();

    final QueryDefinition queryDef;
    if (legacyQuery != null) {
      QueryKitBasedMSQPlanner qkPlanner = new QueryKitBasedMSQPlanner(
          querySpec,
          resultsContext,
          legacyQuery,
          context.jsonMapper(),
          queryKitSpecFactory.makeQueryKitSpec(
              QueryKitBasedMSQPlanner.makeQueryControllerToolKit(querySpec.getContext(), context.jsonMapper()),
              queryId,
              querySpec.getTuningConfig(),
              querySpec.getContext()
          )
      );
      queryDef = qkPlanner.makeQueryDefinition();
    } else {
      queryDef = ((QueryDefMSQSpec) querySpec).getQueryDef();
    }

    ensureExportLocationEmpty(context, querySpec.getDestination());

    if (log.isDebugEnabled()) {
      try {
        log.debug(
            "Query[%s] definition: %s",
            queryDef.getQueryId(),
            context.jsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(queryDef)
        );
      }
      catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    QueryValidator.validateQueryDef(queryDef);
    queryDefRef.set(queryDef);

    workerManager = context.newWorkerManager(
        queryId,
        querySpec,
        queryKernelConfig,
        (failedTask, fault) -> {
          if (queryKernelConfig.isFaultTolerant() && ControllerQueryKernel.isRetriableFault(fault)) {
            addToKernelManipulationQueue(kernel -> {
              addToRetryQueue(kernel, failedTask.getWorkerNumber(), fault);
            });
          } else {
            throw new MSQException(fault);
          }
        }
    );

    if (queryKernelConfig.isFaultTolerant() && !(workerManager instanceof RetryCapableWorkerManager)) {
      // Not expected to happen, since all WorkerManager impls are currently retry-capable. Defensive check
      // for future-proofing.
      throw DruidException.defensive(
          "Cannot run with fault tolerance since workerManager class[%s] does not support retrying",
          workerManager.getClass().getName()
      );
    }

    final long maxParseExceptions = MultiStageQueryContext.getMaxParseExceptions(queryContext);
    this.faultsExceededChecker = new FaultsExceededChecker(
        ImmutableMap.of(CannotParseExternalDataFault.CODE, maxParseExceptions)
    );

    stageToStatsMergingMode = new HashMap<>();
    queryDef.getStageDefinitions().forEach(
        stageDefinition ->
            stageToStatsMergingMode.put(
                stageDefinition.getId().getStageNumber(),
                finalizeClusterStatisticsMergeMode(
                    stageDefinition,
                    MultiStageQueryContext.getClusterStatisticsMergeMode(queryContext)
                )
            )
    );


    return queryDef;
  }

  /**
   * Adds the work orders for worker to {@link ControllerImpl#workOrdersToRetry} if the {@link ControllerQueryKernel} determines that there
   * are work orders which needs reprocessing.
   * <br></br>
   * This method is not thread safe, so it should always be called inside the main controller thread.
   */
  private void addToRetryQueue(ControllerQueryKernel kernel, int worker, MSQFault fault)
  {
    // Blind cast to RetryCapableWorkerManager is safe, since we verified that workerManager is retry-capable
    // when initially creating it.
    final RetryCapableWorkerManager retryCapableWorkerManager = (RetryCapableWorkerManager) workerManager;

    List<WorkOrder> retriableWorkOrders = kernel.getWorkInCaseWorkerEligibleForRetryElseThrow(worker, fault);
    if (!retriableWorkOrders.isEmpty()) {
      log.info("Submitting worker[%s] for relaunch because of fault[%s]", worker, fault);
      retryCapableWorkerManager.submitForRelaunch(worker);
      workOrdersToRetry.compute(
          worker,
          (workerNumber, workOrders) -> {
            if (workOrders == null) {
              return new HashSet<>(retriableWorkOrders);
            } else {
              workOrders.addAll(retriableWorkOrders);
              return workOrders;
            }
          }
      );
    } else {
      log.debug(
          "Worker[%d] has no active workOrders that need relaunch therefore not relaunching",
          worker
      );
      retryCapableWorkerManager.reportFailedInactiveWorker(worker);
    }
  }

  /**
   * Accepts a {@link PartialKeyStatisticsInformation} and updates the controller key statistics information. If all key
   * statistics information has been gathered, enqueues the task with the {@link WorkerSketchFetcher} to generate
   * partition boundaries. This is intended to be called by the {@link ControllerChatHandler}.
   */
  @Override
  public void updatePartialKeyStatisticsInformation(
      int stageNumber,
      int workerNumber,
      Object partialKeyStatisticsInformationObject
  )
  {
    addToKernelManipulationQueue(
        queryKernel -> {
          final StageId stageId = queryKernel.getStageId(stageNumber);

          if (queryKernel.isStageFinished(stageId)) {
            return;
          }

          final PartialKeyStatisticsInformation partialKeyStatisticsInformation;

          try {
            partialKeyStatisticsInformation = context.jsonMapper().convertValue(
                partialKeyStatisticsInformationObject,
                PartialKeyStatisticsInformation.class
            );
          }
          catch (IllegalArgumentException e) {
            throw new IAE(
                e,
                "Unable to deserialize the key statistic for stage [%s] received from the worker [%d]",
                stageId,
                workerNumber
            );
          }

          queryKernel.addPartialKeyStatisticsForStageAndWorker(stageId, workerNumber, partialKeyStatisticsInformation);
        }
    );
  }

  @Override
  public void doneReadingInput(int stageNumber, int workerNumber)
  {
    addToKernelManipulationQueue(
        queryKernel -> {
          final StageId stageId = queryKernel.getStageId(stageNumber);

          if (queryKernel.isStageFinished(stageId)) {
            return;
          }

          queryKernel.setDoneReadingInputForStageAndWorker(stageId, workerNumber);
        }
    );
  }

  @Override
  public void workerError(MSQErrorReport errorReport)
  {
    if (queryKernelConfig.isFaultTolerant()) {
      // Blind cast to RetryCapableWorkerManager in fault-tolerant mode is safe, since when fault-tolerance is
      // enabled, we verify that workerManager is retry-capable when initially creating it.
      final RetryCapableWorkerManager retryCapableWorkerManager = (RetryCapableWorkerManager) workerManager;

      if (retryCapableWorkerManager.isTaskCanceledByController(errorReport.getTaskId()) ||
          !retryCapableWorkerManager.isWorkerActive(errorReport.getTaskId())) {
        log.debug(
            "Ignoring error report for worker[%s] because it was intentionally shut down.",
            errorReport.getTaskId()
        );
        return;
      }
    }

    workerErrorRef.compareAndSet(null, mapQueryColumnNameToOutputColumnName(errorReport));

    // Wake up the main controller thread.
    addToKernelManipulationQueue(kernel -> {
      throw new MSQException(new WorkerFailedFault(errorReport.getTaskId(), null));
    });
  }

  /**
   * This method intakes all the warnings that are generated by the worker. It is the responsibility of the
   * worker node to ensure that it doesn't spam the controller with unnecessary warning stack traces. Currently, that
   * limiting is implemented in {@link MSQWarningReportLimiterPublisher}
   */
  @Override
  public void workerWarning(List<MSQErrorReport> errorReports)
  {
    // This check safeguards that the controller doesn't run out of memory. Workers apply their own limiting to
    // protect their own memory, and to conserve worker -> controller bandwidth.
    long numReportsToAddCheck = Math.min(
        errorReports.size(),
        Limits.MAX_WORKERS * Limits.MAX_VERBOSE_WARNINGS - workerWarnings.size()
    );
    if (numReportsToAddCheck > 0) {
      synchronized (workerWarnings) {
        long numReportsToAdd = Math.min(
            errorReports.size(),
            Limits.MAX_WORKERS * Limits.MAX_VERBOSE_WARNINGS - workerWarnings.size()
        );
        for (int i = 0; i < numReportsToAdd; ++i) {
          workerWarnings.add(errorReports.get(i));
        }
      }
    }
  }

  /**
   * Periodic update of {@link CounterSnapshots} from subtasks.
   */
  @Override
  public void updateCounters(String taskId, CounterSnapshotsTree snapshotsTree)
  {
    taskCountersForLiveReports.putAll(snapshotsTree);
    Optional<Pair<String, Long>> warningsExceeded =
        faultsExceededChecker.addFaultsAndCheckIfExceeded(taskCountersForLiveReports);

    if (warningsExceeded.isPresent()) {
      // Present means the warning limit was exceeded, and warnings have therefore turned into an error.
      String errorCode = warningsExceeded.get().lhs;
      Long limit = warningsExceeded.get().rhs;

      workerError(MSQErrorReport.fromFault(
          taskId,
          selfDruidNode.getHost(),
          null,
          new TooManyWarningsFault(limit.intValue(), errorCode)
      ));
      addToKernelManipulationQueue(
          queryKernel ->
              queryKernel.getActiveStages().forEach(queryKernel::failStage)
      );
    }
  }

  /**
   * Reports that results are ready for a subtask.
   */
  @SuppressWarnings("unchecked")
  @Override
  public void resultsComplete(
      final String queryId,
      final int stageNumber,
      final int workerNumber,
      Object resultObject
  )
  {
    addToKernelManipulationQueue(
        queryKernel -> {
          final StageId stageId = new StageId(queryId, stageNumber);

          if (queryKernel.isStageFinished(stageId)) {
            return;
          }

          final Object convertedResultObject;
          try {
            convertedResultObject = context.jsonMapper().convertValue(
                resultObject,
                queryKernel.getStageDefinition(stageId).getProcessor().getResultTypeReference()
            );
          }
          catch (IllegalArgumentException e) {
            throw new IAE(
                e,
                "Unable to deserialize the result object for stage [%s] received from the worker [%d]",
                stageId,
                workerNumber
            );
          }

          queryKernel.setResultsCompleteForStageAndWorker(stageId, workerNumber, convertedResultObject);
        }
    );
  }

  @Override
  @Nullable
  public TaskReport.ReportMap liveReports()
  {
    final QueryDefinition queryDef = queryDefRef.get();

    if (queryDef == null) {
      return null;
    }

    return TaskReport.buildTaskReports(
        new MSQTaskReport(
            queryId(),
            new MSQTaskReportPayload(
                makeStatusReport(
                    TaskState.RUNNING,
                    null,
                    workerWarnings,
                    queryStartTime,
                    queryStartTime == null ? -1L : new Interval(queryStartTime, DateTimes.nowUtc()).toDurationMillis(),
                    workerManager,
                    segmentLoadWaiter,
                    segmentReport
                ),
                makeStageReport(
                    queryDef,
                    stagePhasesForLiveReports,
                    stageRuntimesForLiveReports,
                    stageWorkerCountsForLiveReports,
                    stagePartitionCountsForLiveReports,
                    stageOutputChannelModesForLiveReports
                ),
                makeCountersSnapshotForLiveReports(),
                null
            )
        )
    );
  }

  /**
   * @param isStageOutputEmpty {@code true} if the stage output is empty, {@code false} if the stage output is non-empty,
   *                           {@code null} for stages where cluster key statistics are not gathered or is incomplete.
   * @return the segments that will be generated by this job. Delegates to
   * {@link #generateSegmentIdsWithShardSpecsForAppend} or {@link #generateSegmentIdsWithShardSpecsForReplace} as
   * appropriate. This is a potentially expensive call, since it requires calling Overlord APIs.
   * @throws MSQException with {@link InsertCannotAllocateSegmentFault} if an allocation cannot be made
   */
  private List<SegmentIdWithShardSpec> generateSegmentIdsWithShardSpecs(
      final DataSourceMSQDestination destination,
      final RowSignature signature,
      final ClusterBy clusterBy,
      final ClusterByPartitions partitionBoundaries,
      final boolean mayHaveMultiValuedClusterByFields,
      @Nullable final Boolean isStageOutputEmpty
  ) throws IOException
  {
    if (destination.isReplaceTimeChunks()) {
      return generateSegmentIdsWithShardSpecsForReplace(
          destination,
          signature,
          clusterBy,
          partitionBoundaries,
          mayHaveMultiValuedClusterByFields,
          isStageOutputEmpty
      );
    } else {
      final RowKeyReader keyReader = clusterBy.keyReader(signature);
      return generateSegmentIdsWithShardSpecsForAppend(
          destination,
          partitionBoundaries,
          keyReader,
          context.taskLockType(),
          isStageOutputEmpty
      );
    }
  }

  /**
   * Used by {@link #generateSegmentIdsWithShardSpecs}.
   *
   * @param isStageOutputEmpty {@code true} if the stage output is empty, {@code false} if the stage output is non-empty,
   *                           {@code null} for stages where cluster key statistics are not gathered or is incomplete.
   */
  private List<SegmentIdWithShardSpec> generateSegmentIdsWithShardSpecsForAppend(
      final DataSourceMSQDestination destination,
      final ClusterByPartitions partitionBoundaries,
      final RowKeyReader keyReader,
      final TaskLockType taskLockType,
      @Nullable final Boolean isStageOutputEmpty
  ) throws IOException
  {
    if (Boolean.TRUE.equals(isStageOutputEmpty)) {
      return Collections.emptyList();
    }

    final List<SegmentIdWithShardSpec> retVal = new ArrayList<>(partitionBoundaries.size());

    final Granularity segmentGranularity = destination.getSegmentGranularity();

    // Compute & validate partitions by bucket (time chunk) if there is a maximum number of segments to be enforced per time chunk
    if (querySpec.getTuningConfig().getMaxNumSegments() != null) {
      final Map<DateTime, List<Pair<Integer, ClusterByPartition>>> partitionsByBucket =
          getPartitionsByBucket(partitionBoundaries, segmentGranularity, keyReader);

      validateNumSegmentsPerBucketOrThrow(partitionsByBucket, segmentGranularity);
    }

    String previousSegmentId = null;

    segmentReport = new MSQSegmentReport(
        NumberedShardSpec.class.getSimpleName(),
        "Using NumberedShardSpec to generate segments since the query is inserting rows."
    );

    for (ClusterByPartition partitionBoundary : partitionBoundaries) {
      final DateTime timestamp = getBucketDateTime(partitionBoundary, segmentGranularity, keyReader);
      final SegmentIdWithShardSpec allocation;
      try {
        allocation = context.taskActionClient().submit(
            new SegmentAllocateAction(
                destination.getDataSource(),
                timestamp,
                // Same granularity for queryGranularity, segmentGranularity because we don't have insight here
                // into what queryGranularity "actually" is. (It depends on what time floor function was used.)
                segmentGranularity,
                segmentGranularity,
                queryId(),
                previousSegmentId,
                false,
                NumberedPartialShardSpec.instance(),
                LockGranularity.TIME_CHUNK,
                taskLockType
            )
        );
      }
      catch (ISE e) {
        if (isTaskLockPreemptedException(e)) {
          throw new MSQException(e, InsertLockPreemptedFault.instance());
        } else {
          throw e;
        }
      }

      if (allocation == null) {
        throw new MSQException(
            new InsertCannotAllocateSegmentFault(
                destination.getDataSource(),
                segmentGranularity.bucket(timestamp),
                null
            )
        );
      }

      // Even if allocation isn't null, the overlord makes the best effort job of allocating a segment with the given
      // segmentGranularity. This is commonly seen in case when there is already a coarser segment in the interval where
      // the requested segment is present and that segment completely overlaps the request. Throw an error if the interval
      // doesn't match the granularity requested
      if (!IntervalUtils.isAligned(allocation.getInterval(), segmentGranularity)) {
        throw new MSQException(
            new InsertCannotAllocateSegmentFault(
                destination.getDataSource(),
                segmentGranularity.bucket(timestamp),
                allocation.getInterval()
            )
        );
      }

      retVal.add(allocation);
      previousSegmentId = allocation.asSegmentId().toString();
    }

    return retVal;
  }

  /**
   * Return partition ranges by bucket (time chunk).
   */
  private Map<DateTime, List<Pair<Integer, ClusterByPartition>>> getPartitionsByBucket(
      final ClusterByPartitions partitionBoundaries,
      final Granularity segmentGranularity,
      final RowKeyReader keyReader
  )
  {
    final Map<DateTime, List<Pair<Integer, ClusterByPartition>>> partitionsByBucket = new HashMap<>();
    for (int i = 0; i < partitionBoundaries.ranges().size(); i++) {
      final ClusterByPartition partitionBoundary = partitionBoundaries.ranges().get(i);
      final DateTime bucketDateTime = getBucketDateTime(partitionBoundary, segmentGranularity, keyReader);
      partitionsByBucket.computeIfAbsent(bucketDateTime, ignored -> new ArrayList<>())
                        .add(Pair.of(i, partitionBoundary));
    }
    return partitionsByBucket;
  }

  private void validateNumSegmentsPerBucketOrThrow(
      final Map<DateTime, List<Pair<Integer, ClusterByPartition>>> partitionsByBucket,
      final Granularity segmentGranularity
  )
  {
    final Integer maxNumSegments = querySpec.getTuningConfig().getMaxNumSegments();
    if (maxNumSegments == null) {
      // Return early because a null value indicates no maximum, i.e., a time chunk can have any number of segments.
      return;
    }
    for (final Map.Entry<DateTime, List<Pair<Integer, ClusterByPartition>>> bucketEntry : partitionsByBucket.entrySet()) {
      final int numSegmentsInTimeChunk = bucketEntry.getValue().size();
      if (numSegmentsInTimeChunk > maxNumSegments) {
        throw new MSQException(
            new TooManySegmentsInTimeChunkFault(
                bucketEntry.getKey(),
                numSegmentsInTimeChunk,
                maxNumSegments,
                segmentGranularity
            )
        );
      }
    }
  }

  /**
   * Used by {@link #generateSegmentIdsWithShardSpecs}.
   *
   * @param isStageOutputEmpty {@code true} if the stage output is empty, {@code false} if the stage output is non-empty,
   *                           {@code null} for stages where cluster key statistics are not gathered or is incomplete.
   */
  private List<SegmentIdWithShardSpec> generateSegmentIdsWithShardSpecsForReplace(
      final DataSourceMSQDestination destination,
      final RowSignature signature,
      final ClusterBy clusterBy,
      final ClusterByPartitions partitionBoundaries,
      final boolean mayHaveMultiValuedClusterByFields,
      @Nullable final Boolean isStageOutputEmpty
  ) throws IOException
  {
    if (Boolean.TRUE.equals(isStageOutputEmpty)) {
      return Collections.emptyList();
    }

    final RowKeyReader keyReader = clusterBy.keyReader(signature);
    final SegmentIdWithShardSpec[] retVal = new SegmentIdWithShardSpec[partitionBoundaries.size()];
    final Granularity segmentGranularity = destination.getSegmentGranularity();
    final Pair<List<String>, String> shardReasonPair = computeShardColumns(
        signature,
        clusterBy,
        querySpec.getColumnMappings(),
        mayHaveMultiValuedClusterByFields
    );
    final List<String> shardColumns = shardReasonPair.lhs;
    final String commentary = shardReasonPair.rhs;

    log.info("ShardSpec chosen: %s", commentary);
    if (shardColumns.isEmpty()) {
      segmentReport = new MSQSegmentReport(NumberedShardSpec.class.getSimpleName(), commentary);
    } else {
      segmentReport = new MSQSegmentReport(DimensionRangeShardSpec.class.getSimpleName(), commentary);
    }

    // Group partition ranges by bucket (time chunk), so we can generate shardSpecs for each bucket independently.
    final Map<DateTime, List<Pair<Integer, ClusterByPartition>>> partitionsByBucket =
        getPartitionsByBucket(partitionBoundaries, segmentGranularity, keyReader);

    // Validate the buckets.
    validateNumSegmentsPerBucketOrThrow(partitionsByBucket, segmentGranularity);

    // Process buckets (time chunks) one at a time.
    for (final Map.Entry<DateTime, List<Pair<Integer, ClusterByPartition>>> bucketEntry : partitionsByBucket.entrySet()) {
      final Interval interval = segmentGranularity.bucket(bucketEntry.getKey());

      // Validate interval against the replaceTimeChunks set of intervals.
      if (destination.getReplaceTimeChunks().stream().noneMatch(chunk -> chunk.contains(interval))) {
        throw new MSQException(new InsertTimeOutOfBoundsFault(interval, destination.getReplaceTimeChunks()));
      }

      final List<Pair<Integer, ClusterByPartition>> ranges = bucketEntry.getValue();

      String version = null;

      final List<TaskLock> locks = context.taskActionClient().submit(new LockListAction());
      for (final TaskLock lock : locks) {
        if (lock.getInterval().contains(interval)) {
          version = lock.getVersion();
        }
      }

      if (version == null) {
        // Lock was revoked, probably, because we should have originally acquired it in isReady.
        throw new MSQException(InsertLockPreemptedFault.INSTANCE);
      }

      for (int segmentNumber = 0; segmentNumber < ranges.size(); segmentNumber++) {
        final int partitionNumber = ranges.get(segmentNumber).lhs;
        final ShardSpec shardSpec;

        if (shardColumns.isEmpty()) {
          shardSpec = new NumberedShardSpec(segmentNumber, ranges.size());
        } else {
          final ClusterByPartition range = ranges.get(segmentNumber).rhs;
          final StringTuple start =
              segmentNumber == 0
              ? null
              : makeStringTuple(clusterBy, keyReader, range.getStart(), shardColumns.size());
          final StringTuple end =
              segmentNumber == ranges.size() - 1
              ? null
              : makeStringTuple(clusterBy, keyReader, range.getEnd(), shardColumns.size());

          shardSpec = new DimensionRangeShardSpec(shardColumns, start, end, segmentNumber, ranges.size());
        }

        retVal[partitionNumber] = new SegmentIdWithShardSpec(destination.getDataSource(), interval, version, shardSpec);
      }
    }

    return Arrays.asList(retVal);
  }

  @Override
  public List<String> getWorkerIds()
  {
    if (workerManager == null) {
      return Collections.emptyList();
    }

    return workerManager.getWorkerIds();
  }

  @Override
  public boolean hasWorker(String workerId)
  {
    if (workerManager == null) {
      return false;
    }

    return workerManager.getWorkerNumber(workerId) != WorkerManager.UNKNOWN_WORKER_NUMBER;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Nullable
  private Int2ObjectMap<Object> makeWorkerFactoryInfosForStage(
      final QueryDefinition queryDef,
      final int stageNumber,
      final WorkerInputs workerInputs,
      @Nullable final List<SegmentIdWithShardSpec> segmentsToGenerate
  )
  {
    if (MSQControllerTask.isIngestion(querySpec) &&
        stageNumber == queryDef.getFinalStageDefinition().getStageNumber()) {
      final DataSourceMSQDestination destination = (DataSourceMSQDestination) querySpec.getDestination();
      TerminalStageSpec terminalStageSpec = destination.getTerminalStageSpec();
      if (destination.getTerminalStageSpec() instanceof SegmentGenerationStageSpec) {
        return (Int2ObjectMap) ((SegmentGenerationStageSpec) terminalStageSpec).getWorkerInfo(
            workerInputs,
            segmentsToGenerate
        );
      }
    }
    return null;
  }

  /**
   * A blocking function used to contact multiple workers. Checks if all the workers are running before contacting them.
   *
   * @param queryKernel
   * @param contactFn
   * @param workers        set of workers to contact
   * @param successFn      After contacting all the tasks, a custom callback is invoked in the main thread for each successfully contacted task.
   * @param retryOnFailure If true, after contacting all the tasks, adds this worker to retry queue in the main thread.
   *                       If false, cancel all the futures and propagate the exception to the caller.
   */
  private void contactWorkersForStage(
      final ControllerQueryKernel queryKernel,
      final IntSet workers,
      final TaskContactFn contactFn,
      final TaskContactSuccess successFn,
      final boolean retryOnFailure
  )
  {
    // Sorted copy of target worker numbers to ensure consistent iteration order.
    final List<Integer> workersCopy = Ordering.natural().sortedCopy(workers);
    final List<String> workerIds = getWorkerIds();
    final List<ListenableFuture<Void>> workerFutures = new ArrayList<>(workersCopy.size());

    try {
      workerManager.waitForWorkers(workers);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    for (final int workerNumber : workersCopy) {
      workerFutures.add(contactFn.contactTask(netClient, workerIds.get(workerNumber), workerNumber));
    }

    final List<Either<Throwable, Void>> workerResults =
        FutureUtils.getUnchecked(FutureUtils.coalesce(workerFutures), true);

    for (int i = 0; i < workerResults.size(); i++) {
      final int workerNumber = workersCopy.get(i);
      final String workerId = workerIds.get(workerNumber);
      final Either<Throwable, Void> workerResult = workerResults.get(i);

      if (workerResult.isValue()) {
        successFn.onSuccess(workerId, workerNumber);
      } else if (retryOnFailure) {
        // Possibly retryable failure.
        log.info(
            workerResult.error(),
            "Detected failure while contacting task[%s]. Initiating relaunch of worker[%d] if applicable",
            workerId,
            workerNumber
        );

        addToRetryQueue(queryKernel, workerNumber, new WorkerRpcFailedFault(workerId, workerResult.error().toString()));
      } else {
        // Nonretryable failure.
        throw new RuntimeException(workerResult.error());
      }
    }
  }

  private void startWorkForStage(
      final QueryDefinition queryDef,
      final ControllerQueryKernel queryKernel,
      final int stageNumber,
      @Nullable final List<SegmentIdWithShardSpec> segmentsToGenerate
  )
  {
    final Int2ObjectMap<Object> extraInfos = makeWorkerFactoryInfosForStage(
        queryDef,
        stageNumber,
        queryKernel.getWorkerInputsForStage(queryKernel.getStageId(stageNumber)),
        segmentsToGenerate
    );

    final Int2ObjectMap<WorkOrder> workOrders = queryKernel.createWorkOrders(stageNumber, extraInfos);
    final StageId stageId = new StageId(queryDef.getQueryId(), stageNumber);

    queryKernel.startStage(stageId);
    contactWorkersForStage(
        queryKernel,
        workOrders.keySet(),
        (netClient, taskId, workerNumber) -> (
            netClient.postWorkOrder(taskId, workOrders.get(workerNumber))),
        (workerId, workerNumber) ->
            queryKernel.workOrdersSentForWorker(stageId, workerNumber),
        queryKernelConfig.isFaultTolerant()
    );
  }

  private void postResultPartitionBoundariesForStage(
      final ControllerQueryKernel queryKernel,
      final QueryDefinition queryDef,
      final int stageNumber,
      final ClusterByPartitions resultPartitionBoundaries,
      final IntSet workers
  )
  {
    final StageId stageId = new StageId(queryDef.getQueryId(), stageNumber);

    contactWorkersForStage(
        queryKernel,
        workers,
        (netClient, workerId, workerNumber) ->
            netClient.postResultPartitionBoundaries(workerId, stageId, resultPartitionBoundaries),
        (workerId, workerNumber) ->
            queryKernel.partitionBoundariesSentForWorker(stageId, workerNumber),
        queryKernelConfig.isFaultTolerant()
    );
  }

  /**
   * Publish the list of segments. Additionally, if {@link DataSourceMSQDestination#isReplaceTimeChunks()},
   * also drop all other segments within the replacement intervals.
   */
  private void publishAllSegments(
      final Set<DataSegment> segments,
      Function<Set<DataSegment>, Set<DataSegment>> compactionStateAnnotateFunction
  ) throws IOException
  {
    final DataSourceMSQDestination destination =
        (DataSourceMSQDestination) querySpec.getDestination();
    final Set<DataSegment> segmentsWithTombstones = new HashSet<>(segments);
    int numTombstones = 0;
    final TaskLockType taskLockType = context.taskLockType();

    if (destination.isReplaceTimeChunks()) {
      final List<Interval> intervalsToDrop = findIntervalsToDrop(Preconditions.checkNotNull(segments, "segments"));

      if (!intervalsToDrop.isEmpty()) {
        TombstoneHelper tombstoneHelper = new TombstoneHelper(context.taskActionClient());
        try {
          Set<DataSegment> tombstones = tombstoneHelper.computeTombstoneSegmentsForReplace(
              intervalsToDrop,
              destination.getReplaceTimeChunks(),
              destination.getDataSource(),
              destination.getSegmentGranularity(),
              Limits.MAX_PARTITION_BUCKETS
          );
          segmentsWithTombstones.addAll(tombstones);
          numTombstones = tombstones.size();
        }
        catch (IllegalStateException e) {
          throw new MSQException(e, InsertLockPreemptedFault.instance());
        }
        catch (TooManyBucketsException e) {
          throw new MSQException(e, new TooManyBucketsFault(Limits.MAX_PARTITION_BUCKETS));
        }
      }

      if (segmentsWithTombstones.isEmpty()) {
        // Nothing to publish, only drop. We already validated that the intervalsToDrop do not have any
        // partially-overlapping segments, so it's safe to drop them as intervals instead of as specific segments.
        // This should not need a segment load wait as segments are marked as unused immediately.
        for (final Interval interval : intervalsToDrop) {
          context.taskActionClient()
                 .submit(new MarkSegmentsAsUnusedAction(destination.getDataSource(), interval));
        }
      } else {
        if (MultiStageQueryContext.shouldWaitForSegmentLoad(querySpec.getContext())) {
          segmentLoadWaiter = new SegmentLoadStatusFetcher(
              context.injector().getInstance(BrokerClient.class),
              context.jsonMapper(),
              queryId,
              destination.getDataSource(),
              segmentsWithTombstones,
              true
          );
        }
        performSegmentPublish(
            context.taskActionClient(),
            createOverwriteAction(taskLockType, compactionStateAnnotateFunction.apply(segmentsWithTombstones))
        );
      }
    } else if (!segments.isEmpty()) {
      if (MultiStageQueryContext.shouldWaitForSegmentLoad(querySpec.getContext())) {
        segmentLoadWaiter = new SegmentLoadStatusFetcher(
            context.injector().getInstance(BrokerClient.class),
            context.jsonMapper(),
            queryId,
            destination.getDataSource(),
            segments,
            true
        );
      }
      // Append mode.
      performSegmentPublish(
          context.taskActionClient(),
          createAppendAction(segments, taskLockType)
      );
    }

    context.emitMetric("ingest/tombstones/count", numTombstones);
    // Include tombstones in the reported segments count
    context.emitMetric("ingest/segments/count", segmentsWithTombstones.size());
  }

  private static TaskAction<SegmentPublishResult> createAppendAction(
      Set<DataSegment> segments,
      TaskLockType taskLockType
  )
  {
    if (taskLockType.equals(TaskLockType.APPEND)) {
      return SegmentTransactionalAppendAction.forSegments(segments, null);
    } else if (taskLockType.equals(TaskLockType.SHARED)) {
      return SegmentTransactionalInsertAction.appendAction(segments, null, null, null, null, null);
    } else {
      throw DruidException.defensive("Invalid lock type [%s] received for append action", taskLockType);
    }
  }

  private TaskAction<SegmentPublishResult> createOverwriteAction(
      TaskLockType taskLockType,
      Set<DataSegment> segmentsWithTombstones
  )
  {
    if (taskLockType.equals(TaskLockType.REPLACE)) {
      return SegmentTransactionalReplaceAction.create(segmentsWithTombstones, null);
    } else if (taskLockType.equals(TaskLockType.EXCLUSIVE)) {
      return SegmentTransactionalInsertAction.overwriteAction(null, segmentsWithTombstones, null);
    } else {
      throw DruidException.defensive("Invalid lock type [%s] received for overwrite action", taskLockType);
    }
  }

  /**
   * When doing an ingestion with {@link DataSourceMSQDestination#isReplaceTimeChunks()}, finds intervals
   * containing data that should be dropped.
   */
  private List<Interval> findIntervalsToDrop(final Set<DataSegment> publishedSegments)
  {
    // Safe to cast because publishAllSegments is only called for dataSource destinations.
    final DataSourceMSQDestination destination =
        (DataSourceMSQDestination) querySpec.getDestination();
    final List<Interval> replaceIntervals =
        new ArrayList<>(JodaUtils.condenseIntervals(destination.getReplaceTimeChunks()));
    final List<Interval> publishIntervals =
        JodaUtils.condenseIntervals(Iterables.transform(publishedSegments, DataSegment::getInterval));
    return IntervalUtils.difference(replaceIntervals, publishIntervals);
  }

  private CounterSnapshotsTree fetchCountersFromWorkers(final IntSet workers)
  {
    final CounterSnapshotsTree retVal = new CounterSnapshotsTree();
    final List<String> taskList = getWorkerIds();

    final List<ListenableFuture<CounterSnapshotsTree>> futures = new ArrayList<>();

    for (int workerNumber : workers) {
      futures.add(netClient.getCounters(taskList.get(workerNumber)));
    }

    final List<CounterSnapshotsTree> snapshotsTrees =
        FutureUtils.getUnchecked(MSQFutureUtils.allAsList(futures, true), true);

    for (CounterSnapshotsTree snapshotsTree : snapshotsTrees) {
      retVal.putAll(snapshotsTree);
    }

    return retVal;
  }

  private void postFinishToWorkers(final IntSet workers)
  {
    final List<String> taskList = getWorkerIds();

    final List<ListenableFuture<Void>> futures = new ArrayList<>();

    for (int workerNumber : workers) {
      futures.add(netClient.postFinish(taskList.get(workerNumber)));
    }

    FutureUtils.getUnchecked(MSQFutureUtils.allAsList(futures, true), true);
  }

  private CounterSnapshotsTree makeCountersSnapshotForLiveReports()
  {
    // taskCountersForLiveReports is mutable: Copy so we get a point-in-time snapshot.
    return CounterSnapshotsTree.fromMap(taskCountersForLiveReports.copyMap());
  }

  private CounterSnapshotsTree getFinalCountersSnapshot(@Nullable final ControllerQueryKernel queryKernel)
  {
    if (queryKernel != null && queryKernel.isSuccess()) {
      return fetchCountersFromWorkers(queryKernel.getAllParticipatingWorkers());
    } else {
      return makeCountersSnapshotForLiveReports();
    }
  }

  private void handleQueryResults(
      final QueryDefinition queryDef,
      final ControllerQueryKernel queryKernel
  ) throws IOException
  {
    if (!queryKernel.isSuccess()) {
      return;
    }
    if (MSQControllerTask.isIngestion(querySpec)) {
      // Publish segments if needed.
      final StageId finalStageId = queryKernel.getStageId(queryDef.getFinalStageDefinition().getStageNumber());
      Function<Set<DataSegment>, Set<DataSegment>> compactionStateAnnotateFunction = Function.identity();

      @SuppressWarnings("unchecked")
      Set<DataSegment> segments = (Set<DataSegment>) queryKernel.getResultObjectForStage(finalStageId);

      boolean storeCompactionState = querySpec.getContext()
          .getBoolean(
              Tasks.STORE_COMPACTION_STATE_KEY,
              Tasks.DEFAULT_STORE_COMPACTION_STATE
          );

      if (storeCompactionState) {
        DataSourceMSQDestination destination = (DataSourceMSQDestination) querySpec.getDestination();
        if (!destination.isReplaceTimeChunks()) {
          // Store compaction state only for replace queries.
          log.warn(
              "Ignoring storeCompactionState flag since it is set for a non-REPLACE query[%s].",
              queryDef.getQueryId()
          );
        } else {
          DataSchema dataSchema = ((SegmentGeneratorStageProcessor) queryKernel
              .getStageDefinition(finalStageId).getProcessor()).getDataSchema();

          ShardSpec shardSpec = segments.isEmpty() ? null : segments.stream().findFirst().get().getShardSpec();
          ClusterBy clusterBy = queryKernel.getStageDefinition(finalStageId).getClusterBy();

          compactionStateAnnotateFunction = addCompactionStateToSegments(
              querySpec,
              context.jsonMapper(),
              dataSchema,
              shardSpec,
              clusterBy,
              queryDef.getQueryId()
          );
        }
      }
      log.info("Query [%s] publishing %d segments.", queryDef.getQueryId(), segments.size());
      publishAllSegments(segments, compactionStateAnnotateFunction);
    } else if (MSQControllerTask.isExport(querySpec.getDestination())) {
      // Write manifest file.
      ExportMSQDestination destination = (ExportMSQDestination) querySpec.getDestination();
      ExportMetadataManager exportMetadataManager = new ExportMetadataManager(
          destination.getExportStorageProvider(),
          context.taskTempDir()
      );

      final StageId finalStageId = queryKernel.getStageId(queryDef.getFinalStageDefinition().getStageNumber());
      //noinspection unchecked


      Object resultObjectForStage = queryKernel.getResultObjectForStage(finalStageId);
      if (!(resultObjectForStage instanceof List)) {
        // This might occur if all workers are running on an older version. We are not able to write a manifest file in this case.
        log.warn(
            "Unable to create export manifest file. Received result[%s] from worker instead of a list of file names.",
            resultObjectForStage
        );
        return;
      }
      @SuppressWarnings("unchecked")
      List<String> exportedFiles = (List<String>) queryKernel.getResultObjectForStage(finalStageId);
      log.info("Query [%s] exported %d files.", queryDef.getQueryId(), exportedFiles.size());
      exportMetadataManager.writeMetadata(exportedFiles);
    }
  }

  private static Function<Set<DataSegment>, Set<DataSegment>> addCompactionStateToSegments(
      MSQSpec querySpec,
      ObjectMapper jsonMapper,
      DataSchema dataSchema,
      @Nullable ShardSpec shardSpec,
      @Nullable ClusterBy clusterBy,
      String queryId
  )
  {
    final MSQTuningConfig tuningConfig = querySpec.getTuningConfig();
    PartitionsSpec partitionSpec;

    // shardSpec is absent in the absence of segments, which happens when only tombstones are generated by an
    // MSQControllerTask.
    if (shardSpec != null) {
      if (Objects.equals(shardSpec.getType(), ShardSpec.Type.RANGE)) {
        List<String> partitionDimensions = ((DimensionRangeShardSpec) shardSpec).getDimensions();
        // Effective maxRowsPerSegment is propagated as rowsPerSegment in MSQ
        partitionSpec = new DimensionRangePartitionsSpec(
            null,
            tuningConfig.getRowsPerSegment(),
            partitionDimensions,
            false
        );
      } else if (Objects.equals(shardSpec.getType(), ShardSpec.Type.NUMBERED)) {
        // MSQ tasks don't use maxTotalRows. Hence using LONG.MAX_VALUE.
        partitionSpec = new DynamicPartitionsSpec(tuningConfig.getRowsPerSegment(), Long.MAX_VALUE);
      } else {
        // SingleDimenionShardSpec and other shard specs are never created in MSQ.
        throw new MSQException(
            UnknownFault.forMessage(
                StringUtils.format(
                    "Query[%s] cannot store compaction state in segments as shard spec of unsupported type[%s].",
                    queryId,
                    shardSpec.getType()
                )));
      }
    } else if (clusterBy != null && !clusterBy.getColumns().isEmpty()) {
      // Effective maxRowsPerSegment is propagated as rowsPerSegment in MSQ
      partitionSpec = new DimensionRangePartitionsSpec(
          null,
          tuningConfig.getRowsPerSegment(),
          clusterBy.getColumns()
                   .stream()
                   .map(KeyColumn::columnName).collect(Collectors.toList()),
          false
      );
    } else {
      partitionSpec = new DynamicPartitionsSpec(tuningConfig.getRowsPerSegment(), Long.MAX_VALUE);
    }

    Granularity segmentGranularity = ((DataSourceMSQDestination) querySpec.getDestination())
        .getSegmentGranularity();

    GranularitySpec granularitySpec = new UniformGranularitySpec(
        segmentGranularity,
        querySpec.getContext()
                    .getGranularity(DruidSqlInsert.SQL_INSERT_QUERY_GRANULARITY, jsonMapper),
        dataSchema.getGranularitySpec().isRollup(),
        // Not using dataSchema.getGranularitySpec().inputIntervals() as that always has ETERNITY
        ((DataSourceMSQDestination) querySpec.getDestination()).getReplaceTimeChunks()
    );

    DimensionsSpec dimensionsSpec = dataSchema.getDimensionsSpec();
    CompactionTransformSpec transformSpec = TransformSpec.NONE.equals(dataSchema.getTransformSpec())
                                            ? null
                                            : CompactionTransformSpec.of(dataSchema.getTransformSpec());
    List<AggregatorFactory> metricsSpec = buildMSQCompactionMetrics(querySpec, dataSchema);

    IndexSpec indexSpec = tuningConfig.getIndexSpec();

    log.info("Query[%s] storing compaction state in segments.", queryId);

    return CompactionState.addCompactionStateToSegments(
        partitionSpec,
        dimensionsSpec,
        metricsSpec,
        transformSpec,
        indexSpec,
        granularitySpec,
        dataSchema.getProjections()
    );
  }

  public static List<AggregatorFactory> buildMSQCompactionMetrics(MSQSpec msqSpec, DataSchema dataSchema)
  {
    if (!(msqSpec instanceof LegacyMSQSpec)) {
      throw DruidException.defensive("Compaction is only supported for LegacyMSQSpec!");
    }
    LegacyMSQSpec legacyMSQSpec = (LegacyMSQSpec) msqSpec;
    Query<?> query = legacyMSQSpec.getQuery();

    List<AggregatorFactory> metricsSpec = Collections.emptyList();

    if (query instanceof GroupByQuery) {
      // For group-by queries, the aggregators are transformed to their combining factories in the dataschema, resulting
      // in a mismatch between schema in compaction spec and the one in compaction state. Sourcing the original
      // AggregatorFactory definition for aggregators in the dataSchema, therefore, directly from the querySpec.
      GroupByQuery groupByQuery = (GroupByQuery) query;
      // Collect all aggregators that are part of the current dataSchema, since a non-rollup query (isRollup() is false)
      // moves metrics columns to dimensions in the final schema.
      Set<String> aggregatorsInDataSchema = Arrays.stream(dataSchema.getAggregators())
                                                  .map(AggregatorFactory::getName)
                                                  .collect(Collectors.toSet());
      metricsSpec = groupByQuery.getAggregatorSpecs()
                                .stream()
                                .filter(aggregatorFactory -> aggregatorsInDataSchema.contains(aggregatorFactory.getName()))
                                .collect(Collectors.toList());
    }
    return metricsSpec;
  }

  /**
   * Clean up durable storage, if used for stage output.
   * <p>
   * Note that this is only called by the controller task itself. It isn't called automatically by anything in
   * particular if the controller fails early without being able to run its cleanup routines. This can cause files
   * to be left in durable storage beyond their useful life.
   */
  private void cleanUpDurableStorageIfNeeded()
  {
    if (queryKernelConfig != null && queryKernelConfig.isDurableStorage()) {
      final String controllerDirName = DurableStorageUtils.getControllerDirectory(queryId());
      try {
        // Delete all temporary files as a failsafe
        MSQTasks.makeStorageConnector(context.injector()).deleteRecursively(controllerDirName);
      }
      catch (Exception e) {
        // If an error is thrown while cleaning up a file, log it and try to continue with the cleanup
        log.warn(e, "Error while cleaning up temporary files at path[%s]. Skipping.", controllerDirName);
      }
    }
  }

  private static String getDataSourceForIngestion(final MSQSpec querySpec)
  {
    return ((DataSourceMSQDestination) querySpec.getDestination()).getDataSource();
  }

  /**
   * Compute shard columns for {@link DimensionRangeShardSpec}. Returns an empty list if range-based sharding
   * is not applicable.
   *
   * @return pair of shard columns and commentary
   */
  private static Pair<List<String>, String> computeShardColumns(
      final RowSignature signature,
      final ClusterBy clusterBy,
      final ColumnMappings columnMappings,
      boolean mayHaveMultiValuedClusterByFields
  )
  {
    if (mayHaveMultiValuedClusterByFields) {
      // DimensionRangeShardSpec cannot handle multivalued fields.
      return Pair.of(
          Collections.emptyList(),
          "Cannot use 'range' shard specs since CLUSTERED BY contains multi-valued fields."
      );
    }
    final List<KeyColumn> clusterByColumns = clusterBy.getColumns();
    final List<String> shardColumns = new ArrayList<>();
    final boolean boosted = isClusterByBoosted(clusterBy);
    final int numShardColumns = clusterByColumns.size() - clusterBy.getBucketByCount() - (boosted ? 1 : 0);

    if (numShardColumns == 0) {
      return Pair.of(Collections.emptyList(), "CLUSTERED BY clause is empty.");
    }

    for (int i = clusterBy.getBucketByCount(); i < clusterBy.getBucketByCount() + numShardColumns; i++) {
      final KeyColumn column = clusterByColumns.get(i);
      final IntList outputColumns = columnMappings.getOutputColumnsForQueryColumn(column.columnName());

      // DimensionRangeShardSpec only handles ascending order.
      if (column.order() != KeyOrder.ASCENDING) {
        return Pair.of(
            shardColumns,
            StringUtils.format(
                "Using[%d] CLUSTERED BY columns for 'range' shard specs, since the next column has order[%s].",
                shardColumns.size(),
                column.order()
            )
        );
      }

      ColumnType columnType = signature.getColumnType(column.columnName()).orElse(null);

      // DimensionRangeShardSpec only handles strings.
      if (!(ColumnType.STRING.equals(columnType))) {
        return Pair.of(
            shardColumns,
            StringUtils.format(
                "Using[%d] CLUSTERED BY columns for 'range' shard specs, since the next column is of type[%s]. "
                + "Only string columns are included in 'range' shard specs.",
                shardColumns.size(),
                columnType
            )
        );
      }

      // DimensionRangeShardSpec only handles columns that appear as-is in the output.
      if (outputColumns.isEmpty()) {
        return Pair.of(
            shardColumns,
            StringUtils.format(
                "Using only[%d] CLUSTERED BY columns for 'range' shard specs, since the next column was not mapped to "
                + "an output column.",
                shardColumns.size()
            )
        );
      }

      shardColumns.add(columnMappings.getOutputColumnName(outputColumns.getInt(0)));
    }

    return Pair.of(shardColumns, "Using 'range' shard specs with all CLUSTERED BY fields.");
  }

  /**
   * Checks if the {@link ClusterBy} has a {@link QueryKitUtils#PARTITION_BOOST_COLUMN}. See javadocs for that
   * constant for more details about what it does.
   */
  private static boolean isClusterByBoosted(final ClusterBy clusterBy)
  {
    return !clusterBy.getColumns().isEmpty()
           && clusterBy.getColumns()
                       .get(clusterBy.getColumns().size() - 1)
                       .columnName()
                       .equals(QueryKitUtils.PARTITION_BOOST_COLUMN);
  }

  private static StringTuple makeStringTuple(
      final ClusterBy clusterBy,
      final RowKeyReader keyReader,
      final RowKey key,
      final int shardFieldCount
  )
  {
    final String[] array = new String[clusterBy.getColumns().size() - clusterBy.getBucketByCount()];

    for (int i = 0; i < shardFieldCount; i++) {
      final Object val = keyReader.read(key, clusterBy.getBucketByCount() + i);
      array[i] = (String) val;
    }

    return new StringTuple(array);
  }

  private static DateTime getBucketDateTime(
      final ClusterByPartition partitionBoundary,
      final Granularity segmentGranularity,
      final RowKeyReader keyReader
  )
  {
    if (Granularities.ALL.equals(segmentGranularity)) {
      return DateTimes.utc(0);
    } else {
      final RowKey startKey = partitionBoundary.getStart();
      final DateTime timestamp =
          DateTimes.utc(MSQTasks.primaryTimestampFromObjectForInsert(keyReader.read(startKey, 0)));

      if (segmentGranularity.bucketStart(timestamp.getMillis()) != timestamp.getMillis()) {
        // It's a bug in... something? if this happens.
        throw new ISE(
            "Received boundary value [%s] misaligned with segmentGranularity [%s]",
            timestamp,
            segmentGranularity
        );
      }

      return timestamp;
    }
  }

  private static MSQStagesReport makeStageReport(
      final QueryDefinition queryDef,
      final Map<Integer, ControllerStagePhase> stagePhaseMap,
      final Map<Integer, Interval> stageRuntimeMap,
      final Map<Integer, Integer> stageWorkerCountMap,
      final Map<Integer, Integer> stagePartitionCountMap,
      final Map<Integer, OutputChannelMode> stageOutputChannelModeMap
  )
  {
    return MSQStagesReport.create(
        queryDef,
        ImmutableMap.copyOf(stagePhaseMap),
        copyOfStageRuntimesEndingAtCurrentTime(stageRuntimeMap),
        stageWorkerCountMap,
        stagePartitionCountMap,
        stageOutputChannelModeMap
    );
  }

  private static MSQStatusReport makeStatusReport(
      final TaskState taskState,
      @Nullable final MSQErrorReport errorReport,
      final Queue<MSQErrorReport> errorReports,
      @Nullable final DateTime queryStartTime,
      final long queryDuration,
      final WorkerManager taskLauncher,
      final SegmentLoadStatusFetcher segmentLoadWaiter,
      @Nullable MSQSegmentReport msqSegmentReport
  )
  {
    int pendingTasks = -1;
    int runningTasks = 1;
    Map<Integer, List<WorkerStats>> workerStatsMap = new HashMap<>();

    if (taskLauncher != null) {
      WorkerCount workerTaskCount = taskLauncher.getWorkerCount();
      pendingTasks = workerTaskCount.getPendingWorkerCount();
      runningTasks = workerTaskCount.getRunningWorkerCount() + 1; // To account for controller.
      workerStatsMap = taskLauncher.getWorkerStats();
    }

    SegmentLoadStatusFetcher.SegmentLoadWaiterStatus status = segmentLoadWaiter == null
                                                              ? null
                                                              : segmentLoadWaiter.status();

    return new MSQStatusReport(
        taskState,
        errorReport,
        errorReports,
        queryStartTime,
        queryDuration,
        workerStatsMap,
        pendingTasks,
        runningTasks,
        status,
        msqSegmentReport
    );
  }

  private static InputSpecSlicerFactory makeInputSpecSlicerFactory(final InputSpecSlicer tableInputSpecSlicer)
  {
    return (stagePartitionsMap, stageOutputChannelModeMap) -> new MapInputSpecSlicer(
        ImmutableMap.<Class<? extends InputSpec>, InputSpecSlicer>builder()
                    .put(StageInputSpec.class, new StageInputSpecSlicer(stagePartitionsMap, stageOutputChannelModeMap))
                    .put(ExternalInputSpec.class, new ExternalInputSpecSlicer())
                    .put(InlineInputSpec.class, new InlineInputSpecSlicer())
                    .put(LookupInputSpec.class, new LookupInputSpecSlicer())
                    .put(TableInputSpec.class, tableInputSpecSlicer)
                    .build()
    );
  }

  private static Map<Integer, Interval> copyOfStageRuntimesEndingAtCurrentTime(
      final Map<Integer, Interval> stageRuntimesMap
  )
  {
    final Int2ObjectMap<Interval> retVal = new Int2ObjectOpenHashMap<>(stageRuntimesMap.size());
    final DateTime now = DateTimes.nowUtc();

    for (Map.Entry<Integer, Interval> entry : stageRuntimesMap.entrySet()) {
      final int stageNumber = entry.getKey();
      final Interval interval = entry.getValue();

      retVal.put(
          stageNumber,
          interval.getEnd().equals(DateTimes.MAX) ? new Interval(interval.getStart(), now) : interval
      );
    }

    return retVal;
  }

  /**
   * Performs a particular {@link SegmentTransactionalInsertAction}, publishing segments.
   * <p>
   * Throws {@link MSQException} with {@link InsertLockPreemptedFault} if the action fails due to lock preemption.
   */
  static void performSegmentPublish(
      final TaskActionClient client,
      final TaskAction<SegmentPublishResult> action
  ) throws IOException
  {
    try {
      final SegmentPublishResult result = client.submit(action);

      if (!result.isSuccess()) {
        throw new MSQException(InsertLockPreemptedFault.instance());
      }
    }
    catch (Exception e) {
      if (isTaskLockPreemptedException(e)) {
        throw new MSQException(e, InsertLockPreemptedFault.instance());
      } else {
        throw e;
      }
    }
  }

  /**
   * Method that determines whether an exception was raised due to the task lock for the controller task being
   * preempted. Uses string comparison, because the relevant Overlord APIs do not have a more reliable way of
   * discerning the cause of errors.
   * <p>
   * Error strings are taken from {@link org.apache.druid.indexing.common.actions.TaskLocks}
   * and {@link SegmentAllocateAction}.
   */
  private static boolean isTaskLockPreemptedException(Exception e)
  {
    final String exceptionMsg = e.getMessage();
    if (exceptionMsg == null) {
      return false;
    }
    final List<String> validExceptionExcerpts = ImmutableList.of(
        "are not covered by locks" /* From TaskLocks */,
        "is preempted and no longer valid" /* From SegmentAllocateAction */
    );
    return validExceptionExcerpts.stream().anyMatch(exceptionMsg::contains);
  }

  private static void logKernelStatus(final String queryId, final ControllerQueryKernel queryKernel)
  {
    if (log.isDebugEnabled()) {
      log.debug(
          "Query [%s] kernel state: %s",
          queryId,
          queryKernel.getActiveStages()
                     .stream()
                     .sorted(Comparator.comparing(id -> queryKernel.getStageDefinition(id).getStageNumber()))
                     .map(id -> StringUtils.format(
                              "%d:%d[%s:%s]>%s",
                              queryKernel.getStageDefinition(id).getStageNumber(),
                              queryKernel.getWorkerInputsForStage(id).workerCount(),
                              queryKernel.getStageDefinition(id).doesShuffle() ? "SHUFFLE" : "RETAIN",
                              queryKernel.getStagePhase(id),
                              queryKernel.doesStageHaveResultPartitions(id)
                              ? Iterators.size(queryKernel.getResultPartitionsForStage(id).iterator())
                              : "?"
                          )
                     )
                     .collect(Collectors.joining("; "))
      );
    }
  }

  /**
   * Create a result-reader executor for {@link RunQueryUntilDone#readQueryResults()}.
   */
  private static FrameProcessorExecutor createResultReaderExec(final String queryId)
  {
    return new FrameProcessorExecutor(
        MoreExecutors.listeningDecorator(
            Execs.singleThreaded(StringUtils.encodeForFormat("msq-result-reader[" + queryId + "]")))
    );
  }

  /**
   * Cancel any currently-running work and shut down a result-reader executor, like one created by
   * {@link #createResultReaderExec(String)}.
   */
  private static void closeResultReaderExec(final FrameProcessorExecutor exec)
  {
    try {
      exec.cancel(RESULT_READER_CANCELLATION_ID);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    finally {
      exec.shutdownNow();
    }
  }

  private void stopExternalFetchers()
  {
    if (workerSketchFetcher != null) {
      workerSketchFetcher.close();
    }
    if (segmentLoadWaiter != null) {
      segmentLoadWaiter.close();
    }
  }

  /**
   * Main controller logic for running a multi-stage query.
   */
  private class RunQueryUntilDone
  {
    private final QueryDefinition queryDef;
    private final InputSpecSlicerFactory inputSpecSlicerFactory;
    private final QueryListener queryListener;
    private final Closer closer;
    private final ControllerQueryKernel queryKernel;

    /**
     * Return value of {@link WorkerManager#start()}. Set by {@link #startTaskLauncher()}.
     */
    private ListenableFuture<?> workerTaskLauncherFuture;

    /**
     * Segments to generate. Populated prior to launching the final stage of a query with destination
     * {@link DataSourceMSQDestination} (which originate from SQL INSERT or REPLACE). The final stage of such a query
     * uses {@link SegmentGeneratorStageProcessor}, which requires a list of segment IDs to generate.
     */
    private List<SegmentIdWithShardSpec> segmentsToGenerate;

    /**
     * Future that resolves when the reader from {@link #startQueryResultsReader()} finishes. Prior to that method
     * being called, this future is null.
     */
    @Nullable
    private ListenableFuture<Void> queryResultsReaderFuture;

    public RunQueryUntilDone(
        final QueryDefinition queryDef,
        final ControllerQueryKernelConfig queryKernelConfig,
        final InputSpecSlicerFactory inputSpecSlicerFactory,
        final QueryListener queryListener,
        final Closer closer
    )
    {
      this.queryDef = queryDef;
      this.inputSpecSlicerFactory = inputSpecSlicerFactory;
      this.queryListener = queryListener;
      this.closer = closer;
      this.queryKernel = new ControllerQueryKernel(queryDef, queryKernelConfig);
    }

    /**
     * Primary 'run' method.
     */
    private Pair<ControllerQueryKernel, ListenableFuture<?>> run() throws IOException, InterruptedException
    {
      startTaskLauncher();

      boolean runAgain;
      final DateTime queryFailDeadline = getQueryDeadline(querySpec.getContext());

      // The timeout could have already elapsed while waiting for the controller to start, check it now.
      checkTimeout(queryFailDeadline);

      while (!queryKernel.isDone()) {
        startStages();
        fetchStatsFromWorkers();
        sendPartitionBoundaries();
        updateLiveReportMaps();
        readQueryResults();
        runAgain = cleanUpEffectivelyFinishedStages();
        retryFailedTasks();
        checkForErrorsInSketchFetcher();

        if (!runAgain) {
          runKernelCommands(queryFailDeadline);
        }

        checkTimeout(queryFailDeadline);
      }

      if (!queryKernel.isSuccess()) {
        throwKernelExceptionIfNotUnknown();
      }

      updateLiveReportMaps();
      cleanUpEffectivelyFinishedStages();
      return Pair.of(queryKernel, workerTaskLauncherFuture);
    }

    /**
     * Retrieves the timeout and start time from the query context and calculates the timeout deadline.
     */
    private DateTime getQueryDeadline(QueryContext queryContext)
    {
      // Fetch the timeout, but don't use default server configured timeout if the user has not specified one.
      final long timeout = queryContext.getTimeout(QueryContexts.NO_TIMEOUT);
      // Not using QueryContexts.hasTimeout(), as this considers the default timeout as timeout being set.
      if (timeout == QueryContexts.NO_TIMEOUT) {
        return DateTimes.MAX;
      }
      return MultiStageQueryContext.getStartTime(queryContext).plus(timeout);
    }

    /**
     * Checks the queryFailDeadline and fails the query with a {@link CanceledFault} if it has passed.
     */
    private void checkTimeout(DateTime queryFailDeadline)
    {
      if (queryFailDeadline.isBeforeNow()) {
        throw new MSQException(CanceledFault.timeout());
      }
    }

    private void checkForErrorsInSketchFetcher()
    {
      Throwable throwable = workerSketchFetcher.getError();
      if (throwable != null) {
        throw new ISE(throwable, "worker sketch fetch failed");
      }
    }

    /**
     * Read query results, if appropriate and possible. Returns true if something was read.
     */
    private void readQueryResults()
    {
      // Open query results channel, if appropriate.
      if (queryListener.readResults() && queryKernel.canReadQueryResults() && queryResultsReaderFuture == null) {
        startQueryResultsReader();
      }
    }

    private void retryFailedTasks() throws InterruptedException
    {
      // if no work orders to rety skip
      if (workOrdersToRetry.isEmpty()) {
        return;
      }
      Set<Integer> workersNeedToBeFullyStarted = new HashSet<>();

      // transform work orders from map<Worker,Set<WorkOrders> to Map<StageId,Map<Worker,WorkOrder>>
      // since we would want workOrders of processed per stage
      Map<StageId, Map<Integer, WorkOrder>> stageWorkerOrders = new HashMap<>();

      for (Map.Entry<Integer, Set<WorkOrder>> workerStages : workOrdersToRetry.entrySet()) {
        workersNeedToBeFullyStarted.add(workerStages.getKey());
        for (WorkOrder workOrder : workerStages.getValue()) {
          stageWorkerOrders.compute(
              new StageId(queryDef.getQueryId(), workOrder.getStageNumber()),
              (stageId, workOrders) -> {
                if (workOrders == null) {
                  workOrders = new HashMap<>();
                }
                workOrders.put(workerStages.getKey(), workOrder);
                return workOrders;
              }
          );
        }
      }

      // wait till the workers identified above are fully ready
      workerManager.waitForWorkers(workersNeedToBeFullyStarted);

      for (Map.Entry<StageId, Map<Integer, WorkOrder>> stageWorkOrders : stageWorkerOrders.entrySet()) {
        contactWorkersForStage(
            queryKernel,
            new IntArraySet(stageWorkOrders.getValue().keySet()),
            (netClient, workerId, workerNumber) ->
                netClient.postWorkOrder(workerId, stageWorkOrders.getValue().get(workerNumber)),
            (workerId, workerNumber) -> {
              queryKernel.workOrdersSentForWorker(stageWorkOrders.getKey(), workerNumber);

              // remove successfully contacted workOrders from workOrdersToRetry
              workOrdersToRetry.compute(
                  workerNumber,
                  (task, workOrderSet) -> {
                    if (workOrderSet == null
                        || workOrderSet.size() == 0
                        || !workOrderSet.remove(stageWorkOrders.getValue().get(workerNumber))) {
                      throw new ISE("Worker[%s] with number[%d] orders not found", workerId, workerNumber);
                    }
                    if (workOrderSet.size() == 0) {
                      return null;
                    }
                    return workOrderSet;
                  }
              );
            },
            queryKernelConfig.isFaultTolerant()
        );
      }
    }

    /**
     * Run at least one command from {@link #kernelManipulationQueue}, waiting for it if necessary.
     */
    private void runKernelCommands(DateTime queryFailDeadline) throws InterruptedException
    {
      if (!queryKernel.isDone()) {
        // Run the next command, waiting till timeout for it if necessary.
        Consumer<ControllerQueryKernel> command = kernelManipulationQueue.poll(
            queryFailDeadline.getMillis() - DateTimes.nowUtc().getMillis(),
            TimeUnit.MILLISECONDS
        );
        if (command == null) {
          return;
        }
        command.accept(queryKernel);

        // Run all pending commands after that one. Helps avoid deep queues.
        // After draining the command queue, move on to the next iteration of the controller loop.
        while ((command = kernelManipulationQueue.poll()) != null) {
          command.accept(queryKernel);
        }
      }
    }

    /**
     * Start up the {@link WorkerManager}, such that later on it can be used to launch new tasks
     * via {@link WorkerManager#launchWorkersIfNeeded}.
     */
    private void startTaskLauncher()
    {
      // Start tasks.
      log.debug("Query [%s] starting task launcher.", queryDef.getQueryId());

      workerTaskLauncherFuture = workerManager.start();
      closer.register(() -> workerManager.stop(true));

      workerTaskLauncherFuture.addListener(
          () ->
              addToKernelManipulationQueue(queryKernel -> {
                // Throw an exception in the main loop, if anything went wrong.
                FutureUtils.getUncheckedImmediately(workerTaskLauncherFuture);
              }),
          Execs.directExecutor()
      );
    }

    /**
     * Enqueues the fetching {@link ClusterByStatisticsCollector}
     * from each worker via {@link WorkerSketchFetcher}
     */
    private void fetchStatsFromWorkers()
    {

      for (Map.Entry<StageId, Set<Integer>> stageToWorker : queryKernel.getStagesAndWorkersToFetchClusterStats()
                                                                       .entrySet()) {
        List<String> allTasks = workerManager.getWorkerIds();
        Set<String> tasks = stageToWorker.getValue().stream().map(allTasks::get).collect(Collectors.toSet());

        ClusterStatisticsMergeMode clusterStatisticsMergeMode = stageToStatsMergingMode.get(stageToWorker.getKey()
                                                                                                         .getStageNumber());
        switch (clusterStatisticsMergeMode) {
          case SEQUENTIAL:
            submitSequentialMergeFetchRequests(stageToWorker.getKey(), tasks);
            break;
          case PARALLEL:
            submitParallelMergeRequests(stageToWorker.getKey(), tasks);
            break;
          default:
            throw new IllegalStateException("No fetching strategy found for mode: " + clusterStatisticsMergeMode);
        }
      }
    }

    private void submitParallelMergeRequests(StageId stageId, Set<String> tasks)
    {

      // eagerly change state of workers whose state is being fetched so that we do not keep on queuing fetch requests.
      queryKernel.startFetchingStatsFromWorker(
          stageId,
          tasks.stream().map(workerManager::getWorkerNumber).collect(Collectors.toSet())
      );
      workerSketchFetcher.inMemoryFullSketchMerging(
          ControllerImpl.this::addToKernelManipulationQueue,
          stageId, tasks,
          ControllerImpl.this::addToRetryQueue
      );
    }

    private void submitSequentialMergeFetchRequests(StageId stageId, Set<String> tasks)
    {
      if (queryKernel.allPartialKeyInformationPresent(stageId)) {
        // eagerly change state of workers whose state is being fetched so that we do not keep on queuing fetch requests.
        queryKernel.startFetchingStatsFromWorker(
            stageId,
            tasks.stream()
                 .map(workerManager::getWorkerNumber)
                 .collect(Collectors.toSet())
        );
        workerSketchFetcher.sequentialTimeChunkMerging(
            ControllerImpl.this::addToKernelManipulationQueue,
            queryKernel.getCompleteKeyStatisticsInformation(stageId),
            stageId,
            tasks,
            ControllerImpl.this::addToRetryQueue
        );
      }
    }

    /**
     * Start up any stages that are ready to start.
     */
    private void startStages() throws IOException, InterruptedException
    {
      final long maxInputBytesPerWorker =
          MultiStageQueryContext.getMaxInputBytesPerWorker(querySpec.getContext());

      logKernelStatus(queryDef.getQueryId(), queryKernel);

      List<StageId> newStageIds;

      do {
        newStageIds = queryKernel.createAndGetNewStageIds(
            inputSpecSlicerFactory,
            querySpec.getAssignmentStrategy(),
            maxInputBytesPerWorker
        );

        for (final StageId stageId : newStageIds) {
          // Allocate segments, if this is the final stage of an ingestion.
          if (MSQControllerTask.isIngestion(querySpec)
              && stageId.getStageNumber() == queryDef.getFinalStageDefinition().getStageNumber()
              && (((DataSourceMSQDestination) querySpec.getDestination()).getTerminalStageSpec() instanceof SegmentGenerationStageSpec)) {
            populateSegmentsToGenerate();
          }

          final int workerCount = queryKernel.getWorkerInputsForStage(stageId).workerCount();
          final StageDefinition stageDef = queryKernel.getStageDefinition(stageId);
          log.info(
              "Query [%s] using workers[%d] for stage[%d], writing to[%s], shuffle[%s].",
              stageId.getQueryId(),
              workerCount,
              stageId.getStageNumber(),
              queryKernel.getStageOutputChannelMode(stageId),
              stageDef.doesShuffle() ? stageDef.getShuffleSpec().kind() : "none"
          );

          workerManager.launchWorkersIfNeeded(workerCount);
          stageRuntimesForLiveReports.put(stageId.getStageNumber(), new Interval(DateTimes.nowUtc(), DateTimes.MAX));
          startWorkForStage(queryDef, queryKernel, stageId.getStageNumber(), segmentsToGenerate);
        }
      } while (!newStageIds.isEmpty());
    }

    /**
     * Populate {@link #segmentsToGenerate} for ingestion.
     */
    private void populateSegmentsToGenerate() throws IOException
    {
      // We need to find the shuffle details (like partition ranges) to generate segments. Generally this is
      // going to correspond to the stage immediately prior to the final segment-generator stage.
      int shuffleStageNumber = Iterables.getOnlyElement(queryDef.getFinalStageDefinition().getInputStageNumbers());

      // The following logic assumes that output of all the stages without a shuffle retain the partition boundaries
      // of the input to that stage. This may not always be the case. For example: GROUP BY queries without an
      // ORDER BY clause. This works for QueryKit generated queries up until now, but it should be reworked as it
      // might not always be the case.
      while (!queryDef.getStageDefinition(shuffleStageNumber).doesShuffle()) {
        shuffleStageNumber =
            Iterables.getOnlyElement(queryDef.getStageDefinition(shuffleStageNumber).getInputStageNumbers());
      }

      final StageId shuffleStageId = new StageId(queryDef.getQueryId(), shuffleStageNumber);

      final boolean isFailOnEmptyInsertEnabled =
          MultiStageQueryContext.isFailOnEmptyInsertEnabled(querySpec.getContext());
      final Boolean isShuffleStageOutputEmpty = queryKernel.isStageOutputEmpty(shuffleStageId);
      if (isFailOnEmptyInsertEnabled && Boolean.TRUE.equals(isShuffleStageOutputEmpty)) {
        throw new MSQException(new InsertCannotBeEmptyFault(getDataSourceForIngestion(querySpec)));
      }

      final ClusterByPartitions partitionBoundaries =
          queryKernel.getResultPartitionBoundariesForStage(shuffleStageId);

      final boolean mayHaveMultiValuedClusterByFields =
          !queryKernel.getStageDefinition(shuffleStageId).mustGatherResultKeyStatistics()
          || queryKernel.hasStageCollectorEncounteredAnyMultiValueField(shuffleStageId);

      segmentsToGenerate = generateSegmentIdsWithShardSpecs(
          (DataSourceMSQDestination) querySpec.getDestination(),
          queryKernel.getStageDefinition(shuffleStageId).getSignature(),
          queryKernel.getStageDefinition(shuffleStageId).getClusterBy(),
          partitionBoundaries,
          mayHaveMultiValuedClusterByFields,
          isShuffleStageOutputEmpty
      );

      log.info("Query [%s] generating %d segments.", queryDef.getQueryId(), partitionBoundaries.size());
    }

    /**
     * Send partition boundaries to any stages that are ready to receive partition boundaries.
     */
    private void sendPartitionBoundaries()
    {
      logKernelStatus(queryDef.getQueryId(), queryKernel);
      for (final StageId stageId : queryKernel.getActiveStages()) {

        if (queryKernel.getStageDefinition(stageId).mustGatherResultKeyStatistics()
            && queryKernel.doesStageHaveResultPartitions(stageId)) {
          IntSet workersToSendPartitionBoundaries = queryKernel.getWorkersToSendPartitionBoundaries(stageId);
          if (workersToSendPartitionBoundaries.isEmpty()) {
            log.debug("No workers for stage[%s] ready to receive partition boundaries", stageId);
            continue;
          }
          final ClusterByPartitions partitions = queryKernel.getResultPartitionBoundariesForStage(stageId);

          if (log.isDebugEnabled()) {
            log.debug(
                "Query [%s] sending out partition boundaries for stage %d: %s for workers %s",
                stageId.getQueryId(),
                stageId.getStageNumber(),
                IntStream.range(0, partitions.size())
                         .mapToObj(i -> StringUtils.format("%s:%s", i, partitions.get(i)))
                         .collect(Collectors.joining(", ")),
                workersToSendPartitionBoundaries.toString()
            );
          } else {
            log.info(
                "Query [%s] sending out partition boundaries for stage %d for workers %s",
                stageId.getQueryId(),
                stageId.getStageNumber(),
                workersToSendPartitionBoundaries.toString()
            );
          }

          postResultPartitionBoundariesForStage(
              queryKernel,
              queryDef,
              stageId.getStageNumber(),
              partitions,
              workersToSendPartitionBoundaries
          );
        }
      }
    }

    /**
     * Update the various maps used for live reports.
     */
    private void updateLiveReportMaps()
    {
      logKernelStatus(queryDef.getQueryId(), queryKernel);

      // Live reports: update stage phases, worker counts, partition counts, output channel modes.
      for (StageId stageId : queryKernel.getActiveStages()) {
        final int stageNumber = stageId.getStageNumber();
        stagePhasesForLiveReports.put(stageNumber, queryKernel.getStagePhase(stageId));

        if (queryKernel.doesStageHaveResultPartitions(stageId)) {
          stagePartitionCountsForLiveReports.computeIfAbsent(
              stageNumber,
              k -> Iterators.size(queryKernel.getResultPartitionsForStage(stageId).iterator())
          );
        }

        stageWorkerCountsForLiveReports.computeIfAbsent(
            stageNumber,
            k -> queryKernel.getWorkerInputsForStage(stageId).workerCount()
        );

        stageOutputChannelModesForLiveReports.computeIfAbsent(
            stageNumber,
            k -> queryKernel.getStageOutputChannelMode(stageId)
        );
      }

      // Live reports: update stage end times for any stages that just ended.
      for (StageId stageId : queryKernel.getActiveStages()) {
        if (queryKernel.getStagePhase(stageId).isSuccess()) {
          stageRuntimesForLiveReports.compute(
              queryKernel.getStageDefinition(stageId).getStageNumber(),
              (k, currentValue) -> {
                if (currentValue.getEnd().equals(DateTimes.MAX)) {
                  return new Interval(currentValue.getStart(), DateTimes.nowUtc());
                } else {
                  return currentValue;
                }
              }
          );
        }
      }
    }

    /**
     * Issue cleanup commands to any stages that are effectivley finished, allowing them to delete their outputs.
     *
     * @return true if any stages were cleaned up
     */
    private boolean cleanUpEffectivelyFinishedStages()
    {
      final StageId finalStageId = queryDef.getFinalStageDefinition().getId();
      boolean didSomething = false;
      for (final StageId stageId : queryKernel.getEffectivelyFinishedStageIds()) {
        if (finalStageId.equals(stageId)
            && queryListener.readResults()
            && (queryResultsReaderFuture == null || !queryResultsReaderFuture.isDone())) {
          // Don't clean up final stage until results are done being read.
          continue;
        }

        log.info("Query [%s] issuing cleanup order for stage %d.", queryDef.getQueryId(), stageId.getStageNumber());
        contactWorkersForStage(
            queryKernel,
            queryKernel.getWorkerInputsForStage(stageId).workers(),
            (netClient, workerId, workerNumber) -> netClient.postCleanupStage(workerId, stageId),
            (workerId, workerNumber) -> {},
            false
        );
        queryKernel.finishStage(stageId, true);
        didSomething = true;
      }
      return didSomething;
    }

    /**
     * Start a {@link ControllerQueryResultsReader} that pushes results to our {@link QueryListener}.
     * <p>
     * The reader runs in a single-threaded executor that is created by this method, and shut down when results
     * are done being read.
     */
    private void startQueryResultsReader()
    {
      if (queryResultsReaderFuture != null) {
        throw new ISE("Already started");
      }

      final StageId finalStageId = queryKernel.getStageId(queryDef.getFinalStageDefinition().getStageNumber());
      final List<String> taskIds = getWorkerIds();

      final InputChannelFactory inputChannelFactory;

      if (queryKernelConfig.isDurableStorage() || MSQControllerTask.writeFinalStageResultsToDurableStorage(querySpec.getDestination())) {
        inputChannelFactory = DurableStorageInputChannelFactory.createStandardImplementation(
            queryId(),
            MSQTasks.makeStorageConnector(context.injector()),
            closer,
            MSQControllerTask.writeFinalStageResultsToDurableStorage(querySpec.getDestination())
        );
      } else {
        inputChannelFactory = new WorkerInputChannelFactory(netClient, () -> taskIds);
      }

      final FrameProcessorExecutor resultReaderExec = createResultReaderExec(queryId());
      resultReaderExec.registerCancellationId(RESULT_READER_CANCELLATION_ID);

      ReadableConcatFrameChannel resultsChannel = null;

      try {
        final InputChannels inputChannels = new InputChannelsImpl(
            queryDef,
            queryKernel.getResultPartitionsForStage(finalStageId),
            inputChannelFactory,
            () -> ArenaMemoryAllocator.createOnHeap(5_000_000),
            resultReaderExec,
            RESULT_READER_CANCELLATION_ID,
            null,
            MultiStageQueryContext.removeNullBytes(querySpec.getContext())
        );

        resultsChannel = ReadableConcatFrameChannel.open(
            StreamSupport.stream(queryKernel.getResultPartitionsForStage(finalStageId).spliterator(), false)
                         .map(
                             readablePartition -> {
                               try {
                                 return inputChannels.openChannel(
                                     new StagePartition(
                                         queryKernel.getStageDefinition(finalStageId).getId(),
                                         readablePartition.getPartitionNumber()
                                     )
                                 );
                               }
                               catch (IOException e) {
                                 throw new RuntimeException(e);
                               }
                             }
                         )
                         .iterator()
        );

        final ControllerQueryResultsReader resultsReader = new ControllerQueryResultsReader(
            resultsChannel,
            queryDef.getFinalStageDefinition().getFrameReader(),
            querySpec.getColumnMappings(),
            resultsContext,
            context.jsonMapper(),
            queryListener
        );

        queryResultsReaderFuture = resultReaderExec.runFully(resultsReader, RESULT_READER_CANCELLATION_ID);

        // When results are done being read, kick the main thread.
        // Important: don't use FutureUtils.futureWithBaggage, because we need queryResultsReaderFuture to resolve
        // *before* the main thread is kicked.
        queryResultsReaderFuture.addListener(
            () -> addToKernelManipulationQueue(holder -> {}),
            Execs.directExecutor()
        );
      }
      catch (Throwable e) {
        // There was some issue setting up the result reader. Shut down the results channel and stop the executor.
        final ReadableConcatFrameChannel finalResultsChannel = resultsChannel;
        throw CloseableUtils.closeAndWrapInCatch(
            e,
            () -> CloseableUtils.closeAll(
                finalResultsChannel,
                () -> closeResultReaderExec(resultReaderExec)
            )
        );
      }

      // Result reader is set up. Register with the query-wide closer.
      closer.register(() -> closeResultReaderExec(resultReaderExec));
    }

    /**
     * Throw {@link MSQException} if the kernel method {@link ControllerQueryKernel#getFailureReasonForStage}
     * has any failure reason other than {@link UnknownFault}.
     */
    private void throwKernelExceptionIfNotUnknown()
    {
      for (final StageId stageId : queryKernel.getActiveStages()) {
        if (queryKernel.getStagePhase(stageId) == ControllerStagePhase.FAILED) {
          final MSQFault fault = queryKernel.getFailureReasonForStage(stageId);

          // Fall through (without throwing an exception) in case of UnknownFault; we may be able to generate
          // a better exception later in query teardown.
          if (!UnknownFault.CODE.equals(fault.getErrorCode())) {
            throw new MSQException(fault);
          }
        }
      }
    }
  }

  static ClusterStatisticsMergeMode finalizeClusterStatisticsMergeMode(
      StageDefinition stageDef,
      ClusterStatisticsMergeMode initialMode
  )
  {
    ClusterStatisticsMergeMode mergeMode = initialMode;
    if (initialMode == ClusterStatisticsMergeMode.AUTO) {
      ClusterBy clusterBy = stageDef.getClusterBy();
      if (clusterBy.getBucketByCount() == 0) {
        // If there is no time clustering, there is no scope for sequential merge
        mergeMode = ClusterStatisticsMergeMode.PARALLEL;
      } else if (stageDef.getMaxWorkerCount() > Limits.MAX_WORKERS_FOR_PARALLEL_MERGE) {
        mergeMode = ClusterStatisticsMergeMode.SEQUENTIAL;
      } else {
        mergeMode = ClusterStatisticsMergeMode.PARALLEL;
      }
      log.info(
          "Stage [%d] AUTO mode: chose %s mode to merge key statistics",
          stageDef.getStageNumber(),
          mergeMode
      );
    }
    return mergeMode;
  }

  /**
   * Maps the query column names (used internally while generating the query plan) to output column names (the one used
   * by the user in the SQL query) for certain errors reported by workers (where they have limited knowledge of the
   * ColumnMappings). For remaining errors not relying on the query column names, it returns it as is.
   */
  @Nullable
  private MSQErrorReport mapQueryColumnNameToOutputColumnName(
      @Nullable final MSQErrorReport workerErrorReport
  )
  {

    if (workerErrorReport == null) {
      return null;
    } else if (workerErrorReport.getFault() instanceof InvalidNullByteFault) {
      InvalidNullByteFault inbf = (InvalidNullByteFault) workerErrorReport.getFault();
      return MSQErrorReport.fromException(
          workerErrorReport.getTaskId(),
          workerErrorReport.getHost(),
          workerErrorReport.getStageNumber(),
          InvalidNullByteException.builder()
                                  .source(inbf.getSource())
                                  .rowNumber(inbf.getRowNumber())
                                  .column(inbf.getColumn())
                                  .value(inbf.getValue())
                                  .position(inbf.getPosition())
                                  .build(),
          querySpec.getColumnMappings()
      );
    } else if (workerErrorReport.getFault() instanceof InvalidFieldFault) {
      InvalidFieldFault iff = (InvalidFieldFault) workerErrorReport.getFault();
      return MSQErrorReport.fromException(
          workerErrorReport.getTaskId(),
          workerErrorReport.getHost(),
          workerErrorReport.getStageNumber(),
          InvalidFieldException.builder()
                               .source(iff.getSource())
                               .rowNumber(iff.getRowNumber())
                               .column(iff.getColumn())
                               .errorMsg(iff.getErrorMsg())
                               .build(),
          querySpec.getColumnMappings()
      );
    } else {
      return workerErrorReport;
    }
  }


  /**
   * Interface used by {@link #contactWorkersForStage}.
   */
  private interface TaskContactFn
  {
    ListenableFuture<Void> contactTask(WorkerClient client, String workerId, int workerNumber);
  }

  /**
   * Interface used when {@link TaskContactFn#contactTask(WorkerClient, String, int)} returns a successful future.
   */
  private interface TaskContactSuccess
  {
    void onSuccess(String workerId, int workerNumber);
  }

  @Override
  public ControllerContext getControllerContext()
  {
    return context;
  }
}
