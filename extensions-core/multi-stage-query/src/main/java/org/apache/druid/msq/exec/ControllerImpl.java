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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.discovery.BrokerClient;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.FrameChannelSequence;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.ClusterByPartition;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.frame.key.RowKeyReader;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.frame.util.DurableStorageUtils;
import org.apache.druid.frame.write.InvalidNullByteException;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.LockReleaseAction;
import org.apache.druid.indexing.common.actions.MarkSegmentsAsUnusedAction;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalAppendAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalReplaceAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.batch.TooManyBucketsException;
import org.apache.druid.indexing.common.task.batch.parallel.TombstoneHelper;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.counters.CounterSnapshots;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.indexing.InputChannelFactory;
import org.apache.druid.msq.indexing.InputChannelsImpl;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.MSQWorkerTaskLauncher;
import org.apache.druid.msq.indexing.WorkerCount;
import org.apache.druid.msq.indexing.client.ControllerChatHandler;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.msq.indexing.destination.DurableStorageMSQDestination;
import org.apache.druid.msq.indexing.destination.ExportMSQDestination;
import org.apache.druid.msq.indexing.destination.MSQSelectDestination;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.indexing.error.CanceledFault;
import org.apache.druid.msq.indexing.error.CannotParseExternalDataFault;
import org.apache.druid.msq.indexing.error.FaultsExceededChecker;
import org.apache.druid.msq.indexing.error.InsertCannotAllocateSegmentFault;
import org.apache.druid.msq.indexing.error.InsertCannotBeEmptyFault;
import org.apache.druid.msq.indexing.error.InsertLockPreemptedFault;
import org.apache.druid.msq.indexing.error.InsertTimeOutOfBoundsFault;
import org.apache.druid.msq.indexing.error.InvalidNullByteFault;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.MSQFault;
import org.apache.druid.msq.indexing.error.MSQFaultUtils;
import org.apache.druid.msq.indexing.error.MSQWarningReportLimiterPublisher;
import org.apache.druid.msq.indexing.error.MSQWarnings;
import org.apache.druid.msq.indexing.error.QueryNotSupportedFault;
import org.apache.druid.msq.indexing.error.TooManyBucketsFault;
import org.apache.druid.msq.indexing.error.TooManyWarningsFault;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.indexing.error.WorkerRpcFailedFault;
import org.apache.druid.msq.indexing.processor.SegmentGeneratorFrameProcessorFactory;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.indexing.report.MSQStagesReport;
import org.apache.druid.msq.indexing.report.MSQStatusReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.input.InputSpecSlicerFactory;
import org.apache.druid.msq.input.InputSpecs;
import org.apache.druid.msq.input.MapInputSpecSlicer;
import org.apache.druid.msq.input.external.ExternalInputSpec;
import org.apache.druid.msq.input.external.ExternalInputSpecSlicer;
import org.apache.druid.msq.input.inline.InlineInputSpec;
import org.apache.druid.msq.input.inline.InlineInputSpecSlicer;
import org.apache.druid.msq.input.lookup.LookupInputSpec;
import org.apache.druid.msq.input.lookup.LookupInputSpecSlicer;
import org.apache.druid.msq.input.stage.InputChannels;
import org.apache.druid.msq.input.stage.ReadablePartition;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.input.stage.StageInputSpecSlicer;
import org.apache.druid.msq.input.table.DataSegmentWithLocation;
import org.apache.druid.msq.input.table.TableInputSpec;
import org.apache.druid.msq.input.table.TableInputSpecSlicer;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.StagePartition;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernel;
import org.apache.druid.msq.kernel.controller.ControllerStagePhase;
import org.apache.druid.msq.kernel.controller.WorkerInputs;
import org.apache.druid.msq.querykit.DataSegmentTimelineView;
import org.apache.druid.msq.querykit.MultiQueryKit;
import org.apache.druid.msq.querykit.QueryKit;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.msq.querykit.ShuffleSpecFactory;
import org.apache.druid.msq.querykit.groupby.GroupByQueryKit;
import org.apache.druid.msq.querykit.results.ExportResultsFrameProcessorFactory;
import org.apache.druid.msq.querykit.results.QueryResultFrameProcessorFactory;
import org.apache.druid.msq.querykit.scan.ScanQueryKit;
import org.apache.druid.msq.shuffle.input.DurableStorageInputChannelFactory;
import org.apache.druid.msq.shuffle.input.WorkerInputChannelFactory;
import org.apache.druid.msq.statistics.PartialKeyStatisticsInformation;
import org.apache.druid.msq.util.ArrayIngestMode;
import org.apache.druid.msq.util.DimensionSchemaUtils;
import org.apache.druid.msq.util.IntervalUtils;
import org.apache.druid.msq.util.MSQFutureUtils;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.msq.util.PassthroughAggregatorFactory;
import org.apache.druid.msq.util.SqlStatementResourceHelper;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.storage.ExportStorageProvider;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.apache.druid.timeline.partition.NumberedPartialShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class ControllerImpl implements Controller
{
  private static final Logger log = new Logger(ControllerImpl.class);

  private final MSQControllerTask task;
  private final ControllerContext context;

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

  private WorkerSketchFetcher workerSketchFetcher;
  // Time at which the query started.
  // For live reports. Written by the main controller thread, read by HTTP threads.

  // WorkerNumber -> WorkOrders which need to be retried and our determined by the controller.
  // Map is always populated in the main controller thread by addToRetryQueue, and pruned in retryFailedTasks.
  private final Map<Integer, Set<WorkOrder>> workOrdersToRetry = new HashMap<>();
  private volatile DateTime queryStartTime = null;

  private volatile DruidNode selfDruidNode;
  private volatile MSQWorkerTaskLauncher workerTaskLauncher;
  private volatile WorkerClient netClient;

  private volatile FaultsExceededChecker faultsExceededChecker = null;

  private Map<Integer, ClusterStatisticsMergeMode> stageToStatsMergingMode;
  private WorkerMemoryParameters workerMemoryParameters;
  private boolean isDurableStorageEnabled;
  private final boolean isFaultToleranceEnabled;
  private final boolean isFailOnEmptyInsertEnabled;
  private volatile SegmentLoadStatusFetcher segmentLoadWaiter;

  public ControllerImpl(
      final MSQControllerTask task,
      final ControllerContext context
  )
  {
    this.task = task;
    this.context = context;
    this.isDurableStorageEnabled = MultiStageQueryContext.isDurableStorageEnabled(
        task.getQuerySpec().getQuery().context()
    );
    this.isFaultToleranceEnabled = MultiStageQueryContext.isFaultToleranceEnabled(
        task.getQuerySpec().getQuery().context()
    );
    this.isFailOnEmptyInsertEnabled = MultiStageQueryContext.isFailOnEmptyInsertEnabled(
        task.getQuerySpec().getQuery().context()
    );
  }

  @Override
  public String id()
  {
    return task.getId();
  }

  @Override
  public MSQControllerTask task()
  {
    return task;
  }

  @Override
  public TaskStatus run() throws Exception
  {
    final Closer closer = Closer.create();

    try {
      return runTask(closer);
    }
    catch (Throwable e) {
      try {
        closer.close();
      }
      catch (Throwable e2) {
        e.addSuppressed(e2);
      }

      // We really don't expect this to error out. runTask should handle everything nicely. If it doesn't, something
      // strange happened, so log it.
      log.warn(e, "Encountered unhandled controller exception.");
      return TaskStatus.failure(id(), e.toString());
    }
    finally {
      closer.close();
    }
  }

  @Override
  public void stopGracefully()
  {
    final QueryDefinition queryDef = queryDefRef.get();

    // stopGracefully() is called when the containing process is terminated, or when the task is canceled.
    log.info("Query [%s] canceled.", queryDef != null ? queryDef.getQueryId() : "<no id yet>");

    stopExternalFetchers();
    addToKernelManipulationQueue(
        kernel -> {
          throw new MSQException(CanceledFault.INSTANCE);
        }
    );

    if (workerTaskLauncher != null) {
      workerTaskLauncher.stop(true);
    }
  }

  public TaskStatus runTask(final Closer closer)
  {
    QueryDefinition queryDef = null;
    ControllerQueryKernel queryKernel = null;
    ListenableFuture<?> workerTaskRunnerFuture = null;
    CounterSnapshotsTree countersSnapshot = null;
    Yielder<Object[]> resultsYielder = null;
    Throwable exceptionEncountered = null;

    final TaskState taskStateForReport;
    final MSQErrorReport errorForReport;

    try {
      // Planning-related: convert the native query from MSQSpec into a multi-stage QueryDefinition.
      this.queryStartTime = DateTimes.nowUtc();
      queryDef = initializeQueryDefAndState(closer);

      final InputSpecSlicerFactory inputSpecSlicerFactory = makeInputSpecSlicerFactory(makeDataSegmentTimelineView());

      // Execution-related: run the multi-stage QueryDefinition.
      final Pair<ControllerQueryKernel, ListenableFuture<?>> queryRunResult =
          new RunQueryUntilDone(queryDef, inputSpecSlicerFactory, closer).run();

      queryKernel = Preconditions.checkNotNull(queryRunResult.lhs);
      workerTaskRunnerFuture = Preconditions.checkNotNull(queryRunResult.rhs);
      resultsYielder = getFinalResultsYielder(queryDef, queryKernel);
      publishSegmentsIfNeeded(queryDef, queryKernel);
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
      final MSQErrorReport controllerError =
          exceptionEncountered != null
          ? MSQErrorReport.fromException(
              id(),
              selfHost,
              null,
              exceptionEncountered,
              task.getQuerySpec().getColumnMappings()
          )
          : null;
      MSQErrorReport workerError = workerErrorRef.get();

      taskStateForReport = TaskState.FAILED;
      errorForReport = MSQTasks.makeErrorReport(id(), selfHost, controllerError, workerError);

      // Log the errors we encountered.
      if (controllerError != null) {
        log.warn("Controller: %s", MSQTasks.errorReportToLogMessage(controllerError));
      }

      if (workerError != null) {
        log.warn("Worker: %s", MSQTasks.errorReportToLogMessage(workerError));
      }
    }

    if (queryKernel != null && queryKernel.isSuccess()) {
      // If successful, encourage the tasks to exit successfully.
      postFinishToAllTasks();
      workerTaskLauncher.stop(false);
    } else {
      // If not successful, cancel running tasks.
      if (workerTaskLauncher != null) {
        workerTaskLauncher.stop(true);
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

    boolean shouldWaitForSegmentLoad = MultiStageQueryContext.shouldWaitForSegmentLoad(task.getQuerySpec().getQuery().context());

    try {
      releaseTaskLocks();
      cleanUpDurableStorageIfNeeded();

      if (queryKernel != null && queryKernel.isSuccess()) {
        if (shouldWaitForSegmentLoad && segmentLoadWaiter != null) {
          // If successful, there are segments created and segment load is enabled, segmentLoadWaiter should wait
          // for them to become available.
          log.info("Controller will now wait for segments to be loaded. The query has already finished executing,"
                   + " and results will be included once the segments are loaded, even if this query is cancelled now.");
          segmentLoadWaiter.waitForSegmentsToLoad();
        }
      }
      stopExternalFetchers();
    }
    catch (Exception e) {
      log.warn(e, "Exception thrown during cleanup. Ignoring it and writing task report.");
    }

    try {
      // Write report even if something went wrong.
      final MSQStagesReport stagesReport;
      final MSQResultsReport resultsReport;

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
            stagePartitionCountsForLiveReports
        );
      } else {
        stagesReport = null;
      }

      if (resultsYielder != null) {
        resultsReport = makeResultsTaskReport(
            queryDef,
            resultsYielder,
            task.getQuerySpec().getColumnMappings(),
            task.getSqlTypeNames(),
            MultiStageQueryContext.getSelectDestination(task.getQuerySpec().getQuery().context())
        );
      } else {
        resultsReport = null;
      }

      final MSQTaskReportPayload taskReportPayload = new MSQTaskReportPayload(
          makeStatusReport(
              taskStateForReport,
              errorForReport,
              workerWarnings,
              queryStartTime,
              new Interval(queryStartTime, DateTimes.nowUtc()).toDurationMillis(),
              workerTaskLauncher,
              segmentLoadWaiter
          ),
          stagesReport,
          countersSnapshot,
          resultsReport
      );

      context.writeReports(
          id(),
          TaskReport.buildTaskReports(new MSQTaskReport(id(), taskReportPayload))
      );
    }
    catch (Throwable e) {
      log.warn(e, "Error encountered while writing task report. Skipping.");
    }

    if (taskStateForReport == TaskState.SUCCESS) {
      return TaskStatus.success(id());
    } else {
      // errorForReport is nonnull when taskStateForReport != SUCCESS. Use that message.
      return TaskStatus.failure(id(), MSQFaultUtils.generateMessageWithErrorCode(errorForReport.getFault()));
    }
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

  private QueryDefinition initializeQueryDefAndState(final Closer closer)
  {
    final QueryContext queryContext = task.getQuerySpec().getQuery().context();
    if (isFaultToleranceEnabled) {
      if (!queryContext.containsKey(MultiStageQueryContext.CTX_DURABLE_SHUFFLE_STORAGE)) {
        // if context key not set, enable durableStorage automatically.
        isDurableStorageEnabled = true;
      } else {
        // if context key is set, and durableStorage is turned on.
        if (MultiStageQueryContext.isDurableStorageEnabled(queryContext)) {
          isDurableStorageEnabled = true;
        } else {
          throw new MSQException(
              UnknownFault.forMessage(
                  StringUtils.format(
                      "Context param[%s] cannot be explicitly set to false when context param[%s] is"
                      + " set to true. Either remove the context param[%s] or explicitly set it to true.",
                      MultiStageQueryContext.CTX_DURABLE_SHUFFLE_STORAGE,
                      MultiStageQueryContext.CTX_FAULT_TOLERANCE,
                      MultiStageQueryContext.CTX_DURABLE_SHUFFLE_STORAGE
                  )));
        }
      }
    } else {
      isDurableStorageEnabled = MultiStageQueryContext.isDurableStorageEnabled(queryContext);
    }

    log.debug("Task [%s] durable storage mode is set to %s.", task.getId(), isDurableStorageEnabled);
    log.debug("Task [%s] fault tolerance mode is set to %s.", task.getId(), isFaultToleranceEnabled);

    this.selfDruidNode = context.selfNode();
    context.registerController(this, closer);

    this.netClient = new ExceptionWrappingWorkerClient(context.taskClientFor(this));
    closer.register(netClient::close);

    final QueryDefinition queryDef = makeQueryDefinition(
        id(),
        makeQueryControllerToolKit(),
        task.getQuerySpec(),
        context.jsonMapper()
    );

    QueryValidator.validateQueryDef(queryDef);
    queryDefRef.set(queryDef);

    final long maxParseExceptions = task.getQuerySpec().getQuery().context().getLong(
        MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED,
        MSQWarnings.DEFAULT_MAX_PARSE_EXCEPTIONS_ALLOWED
    );

    ImmutableMap.Builder<String, Object> taskContextOverridesBuilder = ImmutableMap.builder();
    taskContextOverridesBuilder
        .put(MultiStageQueryContext.CTX_DURABLE_SHUFFLE_STORAGE, isDurableStorageEnabled)
        .put(MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, maxParseExceptions);

    if (!MSQControllerTask.isIngestion(task.getQuerySpec())) {
      if (MSQControllerTask.writeResultsToDurableStorage(task.getQuerySpec())) {
        taskContextOverridesBuilder.put(
            MultiStageQueryContext.CTX_SELECT_DESTINATION,
            MSQSelectDestination.DURABLESTORAGE.getName()
        );
      } else {
        // we need not pass the value 'TaskReport' to the worker since the worker impl does not do anything in such a case.
        // but we are passing it anyway for completeness
        taskContextOverridesBuilder.put(
            MultiStageQueryContext.CTX_SELECT_DESTINATION,
            MSQSelectDestination.TASKREPORT.getName()
        );
      }
    }

    taskContextOverridesBuilder.put(
        MultiStageQueryContext.CTX_IS_REINDEX,
        MSQControllerTask.isReplaceInputDataSourceTask(task)
    );

    this.workerTaskLauncher = new MSQWorkerTaskLauncher(
        id(),
        task.getDataSource(),
        context,
        (failedTask, fault) -> {
          if (isFaultToleranceEnabled && ControllerQueryKernel.isRetriableFault(fault)) {
            addToKernelManipulationQueue((kernel) -> {
              addToRetryQueue(kernel, failedTask.getWorkerNumber(), fault);
            });
          } else {
            throw new MSQException(fault);
          }
        },
        taskContextOverridesBuilder.build(),
        // 10 minutes +- 2 minutes jitter
        TimeUnit.SECONDS.toMillis(600 + ThreadLocalRandom.current().nextInt(-4, 5) * 30L)
    );

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
    this.workerMemoryParameters = WorkerMemoryParameters.createProductionInstanceForController(context.injector());
    this.workerSketchFetcher = new WorkerSketchFetcher(
        netClient,
        workerTaskLauncher,
        isFaultToleranceEnabled
    );
    closer.register(workerSketchFetcher::close);

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
    List<WorkOrder> retriableWorkOrders = kernel.getWorkInCaseWorkerEligibleForRetryElseThrow(worker, fault);
    if (retriableWorkOrders.size() != 0) {
      log.info("Submitting worker[%s] for relaunch because of fault[%s]", worker, fault);
      workerTaskLauncher.submitForRelaunch(worker);
      workOrdersToRetry.compute(worker, (workerNumber, workOrders) -> {
        if (workOrders == null) {
          return new HashSet<>(retriableWorkOrders);
        } else {
          workOrders.addAll(retriableWorkOrders);
          return workOrders;
        }
      });
    } else {
      log.info(
          "Worker[%d] has no active workOrders that need relaunch therefore not relaunching",
          worker
      );
      workerTaskLauncher.reportFailedInactiveWorker(worker);
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

          // We need a specially-decorated ObjectMapper to deserialize key statistics.
          final StageDefinition stageDef = queryKernel.getStageDefinition(stageId);
          final ObjectMapper mapper = MSQTasks.decorateObjectMapperForKeyCollectorSnapshot(
              context.jsonMapper(),
              stageDef.getShuffleSpec().clusterBy(),
              stageDef.getShuffleSpec().doesAggregate()
          );

          final PartialKeyStatisticsInformation partialKeyStatisticsInformation;
          try {
            partialKeyStatisticsInformation = mapper.convertValue(
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
  public void workerError(MSQErrorReport errorReport)
  {
    if (workerTaskLauncher.isTaskCanceledByController(errorReport.getTaskId()) ||
        !workerTaskLauncher.isTaskLatest(errorReport.getTaskId())) {
      log.info("Ignoring task %s", errorReport.getTaskId());
    } else {
      workerErrorRef.compareAndSet(
          null,
          mapQueryColumnNameToOutputColumnName(errorReport)
      );
    }
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
          final Object convertedResultObject;
          try {
            convertedResultObject = context.jsonMapper().convertValue(
                resultObject,
                queryKernel.getStageDefinition(stageId).getProcessorFactory().getResultTypeReference()
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
  public Map<String, TaskReport> liveReports()
  {
    final QueryDefinition queryDef = queryDefRef.get();

    if (queryDef == null) {
      return null;
    }

    return TaskReport.buildTaskReports(
        new MSQTaskReport(
            id(),
            new MSQTaskReportPayload(
                makeStatusReport(
                    TaskState.RUNNING,
                    null,
                    workerWarnings,
                    queryStartTime,
                    queryStartTime == null ? -1L : new Interval(queryStartTime, DateTimes.nowUtc()).toDurationMillis(),
                    workerTaskLauncher,
                    segmentLoadWaiter
                ),
                makeStageReport(
                    queryDef,
                    stagePhasesForLiveReports,
                    stageRuntimesForLiveReports,
                    stageWorkerCountsForLiveReports,
                    stagePartitionCountsForLiveReports
                ),
                makeCountersSnapshotForLiveReports(),
                null
            )
        )
    );
  }

  /**
   * @param isStageOutputEmpty {@code true} if the stage output is empty, {@code false} if the stage output is non-empty,
   * {@code null} for stages where cluster key statistics are not gathered or is incomplete.
   *
   * @return  the segments that will be generated by this job. Delegates to
   * {@link #generateSegmentIdsWithShardSpecsForAppend} or {@link #generateSegmentIdsWithShardSpecsForReplace} as
   * appropriate. This is a potentially expensive call, since it requires calling Overlord APIs.
   *
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
          MultiStageQueryContext.validateAndGetTaskLockType(QueryContext.of(task.getQuerySpec().getQuery().getContext()), false),
          isStageOutputEmpty
      );
    }
  }

  /**
   * Used by {@link #generateSegmentIdsWithShardSpecs}.
   *
   * @param isStageOutputEmpty {@code true} if the stage output is empty, {@code false} if the stage output is non-empty,
   * {@code null} for stages where cluster key statistics are not gathered or is incomplete.
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

    String previousSegmentId = null;

    for (ClusterByPartition partitionBoundary : partitionBoundaries) {
      final DateTime timestamp = getBucketDateTime(partitionBoundary, segmentGranularity, keyReader);
      final SegmentIdWithShardSpec allocation;
      try {
        allocation = context.taskActionClient().submit(
            new SegmentAllocateAction(
                task.getDataSource(),
                timestamp,
                // Same granularity for queryGranularity, segmentGranularity because we don't have insight here
                // into what queryGranularity "actually" is. (It depends on what time floor function was used.)
                segmentGranularity,
                segmentGranularity,
                id(),
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
                task.getDataSource(),
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
                task.getDataSource(),
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
   * Used by {@link #generateSegmentIdsWithShardSpecs}.
   *
   * @param isStageOutputEmpty {@code true} if the stage output is empty, {@code false} if the stage output is non-empty,
   * {@code null} for stages where cluster key statistics are not gathered or is incomplete.
   *
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
    final List<String> shardColumns;

    if (mayHaveMultiValuedClusterByFields) {
      // DimensionRangeShardSpec cannot handle multi-valued fields.
      shardColumns = Collections.emptyList();
    } else {
      shardColumns = computeShardColumns(signature, clusterBy, task.getQuerySpec().getColumnMappings());
    }

    // Group partition ranges by bucket (time chunk), so we can generate shardSpecs for each bucket independently.
    final Map<DateTime, List<Pair<Integer, ClusterByPartition>>> partitionsByBucket = new HashMap<>();
    for (int i = 0; i < partitionBoundaries.ranges().size(); i++) {
      ClusterByPartition partitionBoundary = partitionBoundaries.ranges().get(i);
      final DateTime bucketDateTime = getBucketDateTime(partitionBoundary, segmentGranularity, keyReader);
      partitionsByBucket.computeIfAbsent(bucketDateTime, ignored -> new ArrayList<>())
                        .add(Pair.of(i, partitionBoundary));
    }

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
              segmentNumber == 0 ? null : makeStringTuple(clusterBy, keyReader, range.getStart());
          final StringTuple end =
              segmentNumber == ranges.size() - 1 ? null : makeStringTuple(clusterBy, keyReader, range.getEnd());

          shardSpec = new DimensionRangeShardSpec(shardColumns, start, end, segmentNumber, ranges.size());
        }

        retVal[partitionNumber] = new SegmentIdWithShardSpec(task.getDataSource(), interval, version, shardSpec);
      }
    }

    return Arrays.asList(retVal);
  }

  /**
   * Returns a complete list of task ids, ordered by worker number. The Nth task has worker number N.
   * <p>
   * If the currently-running set of tasks is incomplete, returns an absent Optional.
   */
  @Override
  public List<String> getTaskIds()
  {
    if (workerTaskLauncher == null) {
      return Collections.emptyList();
    }

    return workerTaskLauncher.getActiveTasks();
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
    if (MSQControllerTask.isIngestion(task.getQuerySpec()) &&
        stageNumber == queryDef.getFinalStageDefinition().getStageNumber()) {
      // noinspection unchecked,rawtypes
      return (Int2ObjectMap) makeSegmentGeneratorWorkerFactoryInfos(workerInputs, segmentsToGenerate);
    } else {
      return null;
    }
  }

  @SuppressWarnings("rawtypes")
  private QueryKit makeQueryControllerToolKit()
  {
    final Map<Class<? extends Query>, QueryKit> kitMap =
        ImmutableMap.<Class<? extends Query>, QueryKit>builder()
                    .put(ScanQuery.class, new ScanQueryKit(context.jsonMapper()))
                    .put(GroupByQuery.class, new GroupByQueryKit(context.jsonMapper()))
                    .build();

    return new MultiQueryKit(kitMap);
  }

  private DataSegmentTimelineView makeDataSegmentTimelineView()
  {
    final SegmentSource includeSegmentSource = MultiStageQueryContext.getSegmentSources(
        task.getQuerySpec()
            .getQuery()
            .context()
    );

    final boolean includeRealtime = SegmentSource.shouldQueryRealtimeServers(includeSegmentSource);

    return (dataSource, intervals) -> {
      final Iterable<ImmutableSegmentLoadInfo> realtimeAndHistoricalSegments;

      // Fetch the realtime segments and segments loaded on the historical. Do this first so that we don't miss any
      // segment if they get handed off between the two calls. Segments loaded on historicals are deduplicated below,
      // since we are only interested in realtime segments for now.
      if (includeRealtime) {
        realtimeAndHistoricalSegments = context.coordinatorClient().fetchServerViewSegments(dataSource, intervals);
      } else {
        realtimeAndHistoricalSegments = ImmutableList.of();
      }

      // Fetch all published, used segments (all non-realtime segments) from the metadata store.
      // If the task is operating with a REPLACE lock,
      // any segment created after the lock was acquired for its interval will not be considered.
      final Collection<DataSegment> publishedUsedSegments;
      try {
        // Additional check as the task action does not accept empty intervals
        if (intervals.isEmpty()) {
          publishedUsedSegments = Collections.emptySet();
        } else {
          publishedUsedSegments = context.taskActionClient().submit(new RetrieveUsedSegmentsAction(
              dataSource,
              intervals
          ));
        }
      }
      catch (IOException e) {
        throw new MSQException(e, UnknownFault.forException(e));
      }

      int realtimeCount = 0;

      // Deduplicate segments, giving preference to published used segments.
      // We do this so that if any segments have been handed off in between the two metadata calls above,
      // we directly fetch it from deep storage.
      Set<DataSegment> unifiedSegmentView = new HashSet<>(publishedUsedSegments);

      // Iterate over the realtime segments and segments loaded on the historical
      for (ImmutableSegmentLoadInfo segmentLoadInfo : realtimeAndHistoricalSegments) {
        ImmutableSet<DruidServerMetadata> servers = segmentLoadInfo.getServers();
        // Filter out only realtime servers. We don't want to query historicals for now, but we can in the future.
        // This check can be modified then.
        Set<DruidServerMetadata> realtimeServerMetadata
            = servers.stream()
                     .filter(druidServerMetadata -> includeSegmentSource.getUsedServerTypes()
                                                                        .contains(druidServerMetadata.getType())
                     )
                     .collect(Collectors.toSet());
        if (!realtimeServerMetadata.isEmpty()) {
          realtimeCount += 1;
          DataSegmentWithLocation dataSegmentWithLocation = new DataSegmentWithLocation(
              segmentLoadInfo.getSegment(),
              realtimeServerMetadata
          );
          unifiedSegmentView.add(dataSegmentWithLocation);
        } else {
          // We don't have any segments of the required segment source, ignore the segment
        }
      }

      if (includeRealtime) {
        log.info(
            "Fetched total [%d] segments from coordinator: [%d] from metadata stoure, [%d] from server view",
            unifiedSegmentView.size(),
            publishedUsedSegments.size(),
            realtimeCount
        );
      }

      if (unifiedSegmentView.isEmpty()) {
        return Optional.empty();
      } else {
        return Optional.of(SegmentTimeline.forSegments(unifiedSegmentView));
      }
    };
  }

  private Int2ObjectMap<List<SegmentIdWithShardSpec>> makeSegmentGeneratorWorkerFactoryInfos(
      final WorkerInputs workerInputs,
      final List<SegmentIdWithShardSpec> segmentsToGenerate
  )
  {
    final Int2ObjectMap<List<SegmentIdWithShardSpec>> retVal = new Int2ObjectAVLTreeMap<>();

    // Empty segments validation already happens when the stages are started -- so we cannot have both
    // isFailOnEmptyInsertEnabled and segmentsToGenerate.isEmpty() be true here.
    if (segmentsToGenerate.isEmpty()) {
      return retVal;
    }

    for (final int workerNumber : workerInputs.workers()) {
      // SegmentGenerator stage has a single input from another stage.
      final StageInputSlice stageInputSlice =
          (StageInputSlice) Iterables.getOnlyElement(workerInputs.inputsForWorker(workerNumber));

      final List<SegmentIdWithShardSpec> workerSegments = new ArrayList<>();
      retVal.put(workerNumber, workerSegments);

      for (final ReadablePartition partition : stageInputSlice.getPartitions()) {
        workerSegments.add(segmentsToGenerate.get(partition.getPartitionNumber()));
      }
    }

    return retVal;
  }

  /**
   * A blocking function used to contact multiple workers. Checks if all the workers are running before contacting them.
   *
   * @param queryKernel
   * @param contactFn
   * @param workers         set of workers to contact
   * @param successCallBack After contacting all the tasks, a custom callback is invoked in the main thread for each successfully contacted task.
   * @param retryOnFailure  If true, after contacting all the tasks, adds this worker to retry queue in the main thread.
   *                        If false, cancel all the futures and propagate the exception to the caller.
   */
  private void contactWorkersForStage(
      final ControllerQueryKernel queryKernel,
      final TaskContactFn contactFn,
      final IntSet workers,
      final TaskContactSuccess successCallBack,
      final boolean retryOnFailure
  )
  {
    final List<String> taskIds = getTaskIds();
    final List<ListenableFuture<Boolean>> taskFutures = new ArrayList<>(workers.size());

    try {
      workerTaskLauncher.waitUntilWorkersReady(workers);
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    Set<String> failedCalls = ConcurrentHashMap.newKeySet();
    Set<String> successfulCalls = ConcurrentHashMap.newKeySet();

    for (int workerNumber : workers) {
      final String taskId = taskIds.get(workerNumber);
      SettableFuture<Boolean> settableFuture = SettableFuture.create();
      ListenableFuture<Void> apiFuture = contactFn.contactTask(netClient, taskId, workerNumber);
      Futures.addCallback(apiFuture, new FutureCallback<Void>()
      {
        @Override
        public void onSuccess(@Nullable Void result)
        {
          successfulCalls.add(taskId);
          settableFuture.set(true);
        }

        @Override
        public void onFailure(Throwable t)
        {
          if (retryOnFailure) {
            log.info(
                t,
                "Detected failure while contacting task[%s]. Initiating relaunch of worker[%d] if applicable",
                taskId,
                MSQTasks.workerFromTaskId(taskId)
            );
            failedCalls.add(taskId);
            settableFuture.set(false);
          } else {
            settableFuture.setException(t);
          }
        }
      }, MoreExecutors.directExecutor());

      taskFutures.add(settableFuture);
    }

    FutureUtils.getUnchecked(MSQFutureUtils.allAsList(taskFutures, true), true);

    for (String taskId : successfulCalls) {
      successCallBack.onSuccess(taskId);
    }

    if (retryOnFailure) {
      for (String taskId : failedCalls) {
        addToRetryQueue(queryKernel, MSQTasks.workerFromTaskId(taskId), new WorkerRpcFailedFault(taskId));
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
        (netClient, taskId, workerNumber) -> (
            netClient.postWorkOrder(taskId, workOrders.get(workerNumber))), workOrders.keySet(),
        (taskId) -> queryKernel.workOrdersSentForWorker(stageId, MSQTasks.workerFromTaskId(taskId)),
        isFaultToleranceEnabled
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
        (netClient, taskId, workerNumber) -> netClient.postResultPartitionBoundaries(
            taskId,
            stageId,
            resultPartitionBoundaries
        ),
        workers,
        (taskId) -> queryKernel.partitionBoundariesSentForWorker(stageId, MSQTasks.workerFromTaskId(taskId)),
        isFaultToleranceEnabled
    );
  }

  /**
   * Publish the list of segments. Additionally, if {@link DataSourceMSQDestination#isReplaceTimeChunks()},
   * also drop all other segments within the replacement intervals.
   */
  private void publishAllSegments(final Set<DataSegment> segments) throws IOException
  {
    final DataSourceMSQDestination destination =
        (DataSourceMSQDestination) task.getQuerySpec().getDestination();
    final Set<DataSegment> segmentsWithTombstones = new HashSet<>(segments);
    int numTombstones = 0;
    final TaskLockType taskLockType = MultiStageQueryContext.validateAndGetTaskLockType(
        QueryContext.of(task.getQuerySpec().getQuery().getContext()),
        destination.isReplaceTimeChunks()
    );

    if (destination.isReplaceTimeChunks()) {
      final List<Interval> intervalsToDrop = findIntervalsToDrop(Preconditions.checkNotNull(segments, "segments"));

      if (!intervalsToDrop.isEmpty()) {
        TombstoneHelper tombstoneHelper = new TombstoneHelper(context.taskActionClient());
        try {
          Set<DataSegment> tombstones = tombstoneHelper.computeTombstoneSegmentsForReplace(
              intervalsToDrop,
              destination.getReplaceTimeChunks(),
              task.getDataSource(),
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
                 .submit(new MarkSegmentsAsUnusedAction(task.getDataSource(), interval));
        }
      } else {
        if (MultiStageQueryContext.shouldWaitForSegmentLoad(task.getQuerySpec().getQuery().context())) {
          segmentLoadWaiter = new SegmentLoadStatusFetcher(
              context.injector().getInstance(BrokerClient.class),
              context.jsonMapper(),
              task.getId(),
              task.getDataSource(),
              segmentsWithTombstones,
              true
          );
        }
        performSegmentPublish(
            context.taskActionClient(),
            createOverwriteAction(taskLockType, segmentsWithTombstones)
        );
      }
    } else if (!segments.isEmpty()) {
      if (MultiStageQueryContext.shouldWaitForSegmentLoad(task.getQuerySpec().getQuery().context())) {
        segmentLoadWaiter = new SegmentLoadStatusFetcher(
            context.injector().getInstance(BrokerClient.class),
            context.jsonMapper(),
            task.getId(),
            task.getDataSource(),
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

    task.emitMetric(context.emitter(), "ingest/tombstones/count", numTombstones);
    // Include tombstones in the reported segments count
    task.emitMetric(context.emitter(), "ingest/segments/count", segmentsWithTombstones.size());
  }

  private static TaskAction<SegmentPublishResult> createAppendAction(
      Set<DataSegment> segments,
      TaskLockType taskLockType
  )
  {
    if (taskLockType.equals(TaskLockType.APPEND)) {
      return SegmentTransactionalAppendAction.forSegments(segments);
    } else if (taskLockType.equals(TaskLockType.SHARED)) {
      return SegmentTransactionalInsertAction.appendAction(segments, null, null);
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
      return SegmentTransactionalReplaceAction.create(segmentsWithTombstones);
    } else if (taskLockType.equals(TaskLockType.EXCLUSIVE)) {
      return SegmentTransactionalInsertAction.overwriteAction(null, segmentsWithTombstones);
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
        (DataSourceMSQDestination) task.getQuerySpec().getDestination();
    final List<Interval> replaceIntervals =
        new ArrayList<>(JodaUtils.condenseIntervals(destination.getReplaceTimeChunks()));
    final List<Interval> publishIntervals =
        JodaUtils.condenseIntervals(Iterables.transform(publishedSegments, DataSegment::getInterval));
    return IntervalUtils.difference(replaceIntervals, publishIntervals);
  }

  private CounterSnapshotsTree getCountersFromAllTasks()
  {
    final CounterSnapshotsTree retVal = new CounterSnapshotsTree();
    final List<String> taskList = getTaskIds();

    final List<ListenableFuture<CounterSnapshotsTree>> futures = new ArrayList<>();

    for (String taskId : taskList) {
      futures.add(netClient.getCounters(taskId));
    }

    final List<CounterSnapshotsTree> snapshotsTrees =
        FutureUtils.getUnchecked(MSQFutureUtils.allAsList(futures, true), true);

    for (CounterSnapshotsTree snapshotsTree : snapshotsTrees) {
      retVal.putAll(snapshotsTree);
    }

    return retVal;
  }

  private void postFinishToAllTasks()
  {
    final List<String> taskList = getTaskIds();

    final List<ListenableFuture<Void>> futures = new ArrayList<>();

    for (String taskId : taskList) {
      futures.add(netClient.postFinish(taskId));
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
      return getCountersFromAllTasks();
    } else {
      return makeCountersSnapshotForLiveReports();
    }
  }

  @Nullable
  private Yielder<Object[]> getFinalResultsYielder(
      final QueryDefinition queryDef,
      final ControllerQueryKernel queryKernel
  )
  {
    if (queryKernel.isSuccess() && isInlineResults(task.getQuerySpec())) {
      final StageId finalStageId = queryKernel.getStageId(queryDef.getFinalStageDefinition().getStageNumber());
      final List<String> taskIds = getTaskIds();
      final Closer closer = Closer.create();

      final ListeningExecutorService resultReaderExec =
          MoreExecutors.listeningDecorator(Execs.singleThreaded("result-reader-%d"));
      closer.register(resultReaderExec::shutdownNow);

      final InputChannelFactory inputChannelFactory;

      if (isDurableStorageEnabled || MSQControllerTask.writeResultsToDurableStorage(task.getQuerySpec())) {
        inputChannelFactory = DurableStorageInputChannelFactory.createStandardImplementation(
            id(),
            MSQTasks.makeStorageConnector(
                context.injector()),
            closer,
            MSQControllerTask.writeResultsToDurableStorage(task.getQuerySpec())
        );
      } else {
        inputChannelFactory = new WorkerInputChannelFactory(netClient, () -> taskIds);
      }

      final InputChannels inputChannels = new InputChannelsImpl(
          queryDef,
          queryKernel.getResultPartitionsForStage(finalStageId),
          inputChannelFactory,
          () -> ArenaMemoryAllocator.createOnHeap(5_000_000),
          new FrameProcessorExecutor(resultReaderExec),
          null
      );

      return Yielders.each(
          Sequences.concat(
                       StreamSupport.stream(queryKernel.getResultPartitionsForStage(finalStageId).spliterator(), false)
                                    .map(
                                        readablePartition -> {
                                          try {
                                            return new FrameChannelSequence(
                                                inputChannels.openChannel(
                                                    new StagePartition(
                                                        queryKernel.getStageDefinition(finalStageId).getId(),
                                                        readablePartition.getPartitionNumber()
                                                    )
                                                )
                                            );
                                          }
                                          catch (IOException e) {
                                            throw new RuntimeException(e);
                                          }
                                        }
                                    ).collect(Collectors.toList())
                   ).flatMap(
                       frame ->
                           SqlStatementResourceHelper.getResultSequence(
                               task,
                               queryDef.getFinalStageDefinition(),
                               frame,
                               context.jsonMapper()
                           )
                   )
                   .withBaggage(resultReaderExec::shutdownNow)
      );
    } else {
      return null;
    }
  }

  private void publishSegmentsIfNeeded(
      final QueryDefinition queryDef,
      final ControllerQueryKernel queryKernel
  ) throws IOException
  {
    if (queryKernel.isSuccess() && MSQControllerTask.isIngestion(task.getQuerySpec())) {
      final StageId finalStageId = queryKernel.getStageId(queryDef.getFinalStageDefinition().getStageNumber());

      //noinspection unchecked
      @SuppressWarnings("unchecked")
      final Set<DataSegment> segments = (Set<DataSegment>) queryKernel.getResultObjectForStage(finalStageId);
      log.info("Query [%s] publishing %d segments.", queryDef.getQueryId(), segments.size());
      publishAllSegments(segments);
    }
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
    if (isDurableStorageEnabled) {
      final String controllerDirName = DurableStorageUtils.getControllerDirectory(task.getId());
      try {
        // Delete all temporary files as a failsafe
        MSQTasks.makeStorageConnector(context.injector()).deleteRecursively(controllerDirName);
      }
      catch (Exception e) {
        // If an error is thrown while cleaning up a file, log it and try to continue with the cleanup
        log.warn(e, "Error while cleaning up temporary files at path %s", controllerDirName);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static QueryDefinition makeQueryDefinition(
      final String queryId,
      @SuppressWarnings("rawtypes") final QueryKit toolKit,
      final MSQSpec querySpec,
      final ObjectMapper jsonMapper
  )
  {
    final MSQTuningConfig tuningConfig = querySpec.getTuningConfig();
    final ColumnMappings columnMappings = querySpec.getColumnMappings();
    final Query<?> queryToPlan;
    final ShuffleSpecFactory shuffleSpecFactory;

    if (MSQControllerTask.isIngestion(querySpec)) {
      shuffleSpecFactory = querySpec.getDestination()
                                    .getShuffleSpecFactory(tuningConfig.getRowsPerSegment());

      if (!columnMappings.hasUniqueOutputColumnNames()) {
        // We do not expect to hit this case in production, because the SQL validator checks that column names
        // are unique for INSERT and REPLACE statements (i.e. anything where MSQControllerTask.isIngestion would
        // be true). This check is here as defensive programming.
        throw new ISE("Column names are not unique: [%s]", columnMappings.getOutputColumnNames());
      }

      if (columnMappings.hasOutputColumn(ColumnHolder.TIME_COLUMN_NAME)) {
        // We know there's a single time column, because we've checked columnMappings.hasUniqueOutputColumnNames().
        final int timeColumn = columnMappings.getOutputColumnsByName(ColumnHolder.TIME_COLUMN_NAME).getInt(0);
        queryToPlan = querySpec.getQuery().withOverriddenContext(
            ImmutableMap.of(
                QueryKitUtils.CTX_TIME_COLUMN_NAME,
                columnMappings.getQueryColumnName(timeColumn)
            )
        );
      } else {
        queryToPlan = querySpec.getQuery();
      }
    } else {
      shuffleSpecFactory = querySpec.getDestination()
                                    .getShuffleSpecFactory(MultiStageQueryContext.getRowsPerPage(querySpec.getQuery().context()));
      queryToPlan = querySpec.getQuery();
    }

    final QueryDefinition queryDef;

    try {
      queryDef = toolKit.makeQueryDefinition(
          queryId,
          queryToPlan,
          toolKit,
          shuffleSpecFactory,
          tuningConfig.getMaxNumWorkers(),
          0
      );
    }
    catch (MSQException e) {
      // If the toolkit throws a MSQFault, don't wrap it in a more generic QueryNotSupportedFault
      throw e;
    }
    catch (Exception e) {
      throw new MSQException(e, QueryNotSupportedFault.INSTANCE);
    }

    if (MSQControllerTask.isIngestion(querySpec)) {
      final RowSignature querySignature = queryDef.getFinalStageDefinition().getSignature();
      final ClusterBy queryClusterBy = queryDef.getFinalStageDefinition().getClusterBy();

      // Find the stage that provides shuffled input to the final segment-generation stage.
      StageDefinition finalShuffleStageDef = queryDef.getFinalStageDefinition();

      while (!finalShuffleStageDef.doesShuffle()
             && InputSpecs.getStageNumbers(finalShuffleStageDef.getInputSpecs()).size() == 1) {
        finalShuffleStageDef = queryDef.getStageDefinition(
            Iterables.getOnlyElement(InputSpecs.getStageNumbers(finalShuffleStageDef.getInputSpecs()))
        );
      }

      if (!finalShuffleStageDef.doesShuffle()) {
        finalShuffleStageDef = null;
      }

      // Add all query stages.
      // Set shuffleCheckHasMultipleValues on the stage that serves as input to the final segment-generation stage.
      final QueryDefinitionBuilder builder = QueryDefinition.builder();

      for (final StageDefinition stageDef : queryDef.getStageDefinitions()) {
        if (stageDef.equals(finalShuffleStageDef)) {
          builder.add(StageDefinition.builder(stageDef).shuffleCheckHasMultipleValues(true));
        } else {
          builder.add(StageDefinition.builder(stageDef));
        }
      }

      // Then, add a segment-generation stage.
      final DataSchema dataSchema =
          generateDataSchema(querySpec, querySignature, queryClusterBy, columnMappings, jsonMapper);

      builder.add(
          StageDefinition.builder(queryDef.getNextStageNumber())
                         .inputs(new StageInputSpec(queryDef.getFinalStageDefinition().getStageNumber()))
                         .maxWorkerCount(tuningConfig.getMaxNumWorkers())
                         .processorFactory(
                             new SegmentGeneratorFrameProcessorFactory(
                                 dataSchema,
                                 columnMappings,
                                 tuningConfig
                             )
                         )
      );

      return builder.build();
    } else if (querySpec.getDestination() instanceof TaskReportMSQDestination) {
      return queryDef;
    } else if (querySpec.getDestination() instanceof DurableStorageMSQDestination) {

      // attaching new query results stage if the final stage does sort during shuffle so that results are ordered.
      StageDefinition finalShuffleStageDef = queryDef.getFinalStageDefinition();
      if (finalShuffleStageDef.doesSortDuringShuffle()) {
        final QueryDefinitionBuilder builder = QueryDefinition.builder();
        builder.addAll(queryDef);
        builder.add(StageDefinition.builder(queryDef.getNextStageNumber())
                                   .inputs(new StageInputSpec(queryDef.getFinalStageDefinition().getStageNumber()))
                                   .maxWorkerCount(tuningConfig.getMaxNumWorkers())
                                   .signature(finalShuffleStageDef.getSignature())
                                   .shuffleSpec(null)
                                   .processorFactory(new QueryResultFrameProcessorFactory())
        );
        return builder.build();
      } else {
        return queryDef;
      }
    } else if (querySpec.getDestination() instanceof ExportMSQDestination) {
      final ExportMSQDestination exportMSQDestination = (ExportMSQDestination) querySpec.getDestination();
      final ExportStorageProvider exportStorageProvider = exportMSQDestination.getExportStorageProvider();

      try {
        // Check that the export destination is empty as a sanity check. We want to avoid modifying any other files with export.
        Iterator<String> filesIterator = exportStorageProvider.get().listDir("");
        if (filesIterator.hasNext()) {
          throw DruidException.forPersona(DruidException.Persona.USER)
                              .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                              .build("Found files at provided export destination[%s]. Export is only allowed to "
                                     + "an empty path. Please provide an empty path/subdirectory or move the existing files.",
                                     exportStorageProvider.getBasePath());
        }
      }
      catch (IOException e) {
        throw DruidException.forPersona(DruidException.Persona.USER)
                            .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                            .build(e, "Exception occurred while connecting to export destination.");
      }


      final ResultFormat resultFormat = exportMSQDestination.getResultFormat();
      final QueryDefinitionBuilder builder = QueryDefinition.builder();
      builder.addAll(queryDef);
      builder.add(StageDefinition.builder(queryDef.getNextStageNumber())
                                 .inputs(new StageInputSpec(queryDef.getFinalStageDefinition().getStageNumber()))
                                 .maxWorkerCount(tuningConfig.getMaxNumWorkers())
                                 .signature(queryDef.getFinalStageDefinition().getSignature())
                                 .shuffleSpec(null)
                                 .processorFactory(new ExportResultsFrameProcessorFactory(
                                     queryId,
                                     exportStorageProvider,
                                     resultFormat
                                 ))
      );
      return builder.build();
    } else {
      throw new ISE("Unsupported destination [%s]", querySpec.getDestination());
    }
  }



  private static DataSchema generateDataSchema(
      MSQSpec querySpec,
      RowSignature querySignature,
      ClusterBy queryClusterBy,
      ColumnMappings columnMappings,
      ObjectMapper jsonMapper
  )
  {
    final DataSourceMSQDestination destination = (DataSourceMSQDestination) querySpec.getDestination();
    final boolean isRollupQuery = isRollupQuery(querySpec.getQuery());

    final Pair<List<DimensionSchema>, List<AggregatorFactory>> dimensionsAndAggregators =
        makeDimensionsAndAggregatorsForIngestion(
            querySignature,
            queryClusterBy,
            destination.getSegmentSortOrder(),
            columnMappings,
            isRollupQuery,
            querySpec.getQuery()
        );

    return new DataSchema(
        destination.getDataSource(),
        new TimestampSpec(ColumnHolder.TIME_COLUMN_NAME, "millis", null),
        new DimensionsSpec(dimensionsAndAggregators.lhs),
        dimensionsAndAggregators.rhs.toArray(new AggregatorFactory[0]),
        makeGranularitySpecForIngestion(querySpec.getQuery(), querySpec.getColumnMappings(), isRollupQuery, jsonMapper),
        new TransformSpec(null, Collections.emptyList())
    );
  }

  private static GranularitySpec makeGranularitySpecForIngestion(
      final Query<?> query,
      final ColumnMappings columnMappings,
      final boolean isRollupQuery,
      final ObjectMapper jsonMapper
  )
  {
    if (isRollupQuery) {
      final String queryGranularityString =
          query.context().getString(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD_GRANULARITY, "");

      if (timeIsGroupByDimension((GroupByQuery) query, columnMappings) && !queryGranularityString.isEmpty()) {
        final Granularity queryGranularity;

        try {
          queryGranularity = jsonMapper.readValue(queryGranularityString, Granularity.class);
        }
        catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }

        return new ArbitraryGranularitySpec(queryGranularity, true, Intervals.ONLY_ETERNITY);
      }
      return new ArbitraryGranularitySpec(Granularities.NONE, true, Intervals.ONLY_ETERNITY);
    } else {
      return new ArbitraryGranularitySpec(Granularities.NONE, false, Intervals.ONLY_ETERNITY);
    }
  }

  /**
   * Checks that a {@link GroupByQuery} is grouping on the primary time column.
   * <p>
   * The logic here is roundabout. First, we check which column in the {@link GroupByQuery} corresponds to the
   * output column {@link ColumnHolder#TIME_COLUMN_NAME}, using our {@link ColumnMappings}. Then, we check for the
   * presence of an optimization done in {@link DruidQuery#toGroupByQuery()}, where the context parameter
   * {@link GroupByQuery#CTX_TIMESTAMP_RESULT_FIELD} and various related parameters are set when one of the dimensions
   * is detected to be a time-floor. Finally, we check that the name of that dimension, and the name of our time field
   * from {@link ColumnMappings}, are the same.
   */
  private static boolean timeIsGroupByDimension(GroupByQuery groupByQuery, ColumnMappings columnMappings)
  {
    final IntList positions = columnMappings.getOutputColumnsByName(ColumnHolder.TIME_COLUMN_NAME);

    if (positions.size() == 1) {
      final String queryTimeColumn = columnMappings.getQueryColumnName(positions.getInt(0));
      return queryTimeColumn.equals(groupByQuery.context().getString(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD));
    } else {
      return false;
    }
  }

  /**
   * Whether a native query represents an ingestion with rollup.
   * <p>
   * Checks for three things:
   * <p>
   * - The query must be a {@link GroupByQuery}, because rollup requires columns to be split into dimensions and
   * aggregations.
   * - The query must not finalize aggregations, because rollup requires inserting the intermediate type of
   * complex aggregations, not the finalized type. (So further rollup is possible.)
   * - The query must explicitly disable {@link GroupByQueryConfig#CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING}, because
   * groupBy on multi-value dimensions implicitly unnests, which is not desired behavior for rollup at ingestion time
   * (rollup expects multi-value dimensions to be treated as arrays).
   */
  private static boolean isRollupQuery(Query<?> query)
  {
    return query instanceof GroupByQuery
           && !MultiStageQueryContext.isFinalizeAggregations(query.context())
           && !query.context().getBoolean(GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING, true);
  }

  private static boolean isInlineResults(final MSQSpec querySpec)
  {
    return querySpec.getDestination() instanceof TaskReportMSQDestination
           || querySpec.getDestination() instanceof DurableStorageMSQDestination;
  }

  private static boolean isTimeBucketedIngestion(final MSQSpec querySpec)
  {
    return MSQControllerTask.isIngestion(querySpec)
           && !((DataSourceMSQDestination) querySpec.getDestination()).getSegmentGranularity()
                                                                      .equals(Granularities.ALL);
  }

  /**
   * Compute shard columns for {@link DimensionRangeShardSpec}. Returns an empty list if range-based sharding
   * is not applicable.
   */
  private static List<String> computeShardColumns(
      final RowSignature signature,
      final ClusterBy clusterBy,
      final ColumnMappings columnMappings
  )
  {
    final List<KeyColumn> clusterByColumns = clusterBy.getColumns();
    final List<String> shardColumns = new ArrayList<>();
    final boolean boosted = isClusterByBoosted(clusterBy);
    final int numShardColumns = clusterByColumns.size() - clusterBy.getBucketByCount() - (boosted ? 1 : 0);

    if (numShardColumns == 0) {
      return Collections.emptyList();
    }

    for (int i = clusterBy.getBucketByCount(); i < clusterBy.getBucketByCount() + numShardColumns; i++) {
      final KeyColumn column = clusterByColumns.get(i);
      final IntList outputColumns = columnMappings.getOutputColumnsForQueryColumn(column.columnName());

      // DimensionRangeShardSpec only handles ascending order.
      if (column.order() != KeyOrder.ASCENDING) {
        return Collections.emptyList();
      }

      ColumnType columnType = signature.getColumnType(column.columnName()).orElse(null);

      // DimensionRangeShardSpec only handles strings.
      if (!(ColumnType.STRING.equals(columnType))) {
        return Collections.emptyList();
      }

      // DimensionRangeShardSpec only handles columns that appear as-is in the output.
      if (outputColumns.isEmpty()) {
        return Collections.emptyList();
      }

      shardColumns.add(columnMappings.getOutputColumnName(outputColumns.getInt(0)));
    }

    return shardColumns;
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
      final RowKey key
  )
  {
    final String[] array = new String[clusterBy.getColumns().size() - clusterBy.getBucketByCount()];
    final boolean boosted = isClusterByBoosted(clusterBy);

    for (int i = 0; i < array.length; i++) {
      final Object val = keyReader.read(key, clusterBy.getBucketByCount() + i);

      if (i == array.length - 1 && boosted) {
        // Boost column
        //noinspection RedundantCast: false alarm; the cast is necessary
        array[i] = StringUtils.format("%016d", (long) val);
      } else {
        array[i] = (String) val;
      }
    }

    return new StringTuple(array);
  }

  private static Pair<List<DimensionSchema>, List<AggregatorFactory>> makeDimensionsAndAggregatorsForIngestion(
      final RowSignature querySignature,
      final ClusterBy queryClusterBy,
      final List<String> segmentSortOrder,
      final ColumnMappings columnMappings,
      final boolean isRollupQuery,
      final Query<?> query
  )
  {
    // Log a warning unconditionally if arrayIngestMode is MVD, since the behaviour is incorrect, and is subject to
    // deprecation and removal in future
    if (MultiStageQueryContext.getArrayIngestMode(query.context()) == ArrayIngestMode.MVD) {
      log.warn(
          "'%s' is set to 'mvd' in the query's context. This ingests the string arrays as multi-value "
          + "strings instead of arrays, and is preserved for legacy reasons when MVDs were the only way to ingest string "
          + "arrays in Druid. It is incorrect behaviour and will likely be removed in the future releases of Druid",
          MultiStageQueryContext.CTX_ARRAY_INGEST_MODE
      );
    }

    final List<DimensionSchema> dimensions = new ArrayList<>();
    final List<AggregatorFactory> aggregators = new ArrayList<>();

    // During ingestion, segment sort order is determined by the order of fields in the DimensionsSchema. We want
    // this to match user intent as dictated by the declared segment sort order and CLUSTERED BY, so add things in
    // that order.

    // Start with segmentSortOrder.
    final Set<String> outputColumnsInOrder = new LinkedHashSet<>(segmentSortOrder);

    // Then the query-level CLUSTERED BY.
    // Note: this doesn't work when CLUSTERED BY specifies an expression that is not being selected.
    // Such fields in CLUSTERED BY still control partitioning as expected, but do not affect sort order of rows
    // within an individual segment.
    for (final KeyColumn clusterByColumn : queryClusterBy.getColumns()) {
      final IntList outputColumns = columnMappings.getOutputColumnsForQueryColumn(clusterByColumn.columnName());
      for (final int outputColumn : outputColumns) {
        outputColumnsInOrder.add(columnMappings.getOutputColumnName(outputColumn));
      }
    }

    // Then all other columns.
    outputColumnsInOrder.addAll(columnMappings.getOutputColumnNames());

    Map<String, AggregatorFactory> outputColumnAggregatorFactories = new HashMap<>();

    if (isRollupQuery) {
      // Populate aggregators from the native query when doing an ingest in rollup mode.
      for (AggregatorFactory aggregatorFactory : ((GroupByQuery) query).getAggregatorSpecs()) {
        for (final int outputColumn : columnMappings.getOutputColumnsForQueryColumn(aggregatorFactory.getName())) {
          final String outputColumnName = columnMappings.getOutputColumnName(outputColumn);
          if (outputColumnAggregatorFactories.containsKey(outputColumnName)) {
            throw new ISE("There can only be one aggregation for column [%s].", outputColumn);
          } else {
            outputColumnAggregatorFactories.put(
                outputColumnName,
                aggregatorFactory.withName(outputColumnName).getCombiningFactory()
            );
          }
        }
      }
    }

    // Each column can be of either time, dimension, aggregator. For this method. we can ignore the time column.
    // For non-complex columns, If the aggregator factory of the column is not available, we treat the column as
    // a dimension. For complex columns, certains hacks are in place.
    for (final String outputColumnName : outputColumnsInOrder) {
      // CollectionUtils.getOnlyElement because this method is only called during ingestion, where we require
      // that output names be unique.
      final int outputColumn = CollectionUtils.getOnlyElement(
          columnMappings.getOutputColumnsByName(outputColumnName),
          xs -> new ISE("Expected single output column for name [%s], but got [%s]", outputColumnName, xs)
      );
      final String queryColumn = columnMappings.getQueryColumnName(outputColumn);
      final ColumnType type =
          querySignature.getColumnType(queryColumn)
                        .orElseThrow(() -> new ISE("No type for column [%s]", outputColumnName));

      if (!outputColumnName.equals(ColumnHolder.TIME_COLUMN_NAME)) {

        if (!type.is(ValueType.COMPLEX)) {
          // non complex columns
          populateDimensionsAndAggregators(
              dimensions,
              aggregators,
              outputColumnAggregatorFactories,
              outputColumnName,
              type,
              query.context()
          );
        } else {
          // complex columns only
          if (DimensionHandlerUtils.DIMENSION_HANDLER_PROVIDERS.containsKey(type.getComplexTypeName())) {
            dimensions.add(
                DimensionSchemaUtils.createDimensionSchema(
                    outputColumnName,
                    type,
                    MultiStageQueryContext.useAutoColumnSchemas(query.context()),
                    MultiStageQueryContext.getArrayIngestMode(query.context())
                )
            );
          } else if (!isRollupQuery) {
            aggregators.add(new PassthroughAggregatorFactory(outputColumnName, type.getComplexTypeName()));
          } else {
            populateDimensionsAndAggregators(
                dimensions,
                aggregators,
                outputColumnAggregatorFactories,
                outputColumnName,
                type,
                query.context()
            );
          }
        }
      }
    }

    return Pair.of(dimensions, aggregators);
  }


  /**
   * If the output column is present in the outputColumnAggregatorFactories that means we already have the aggregator information for this column.
   * else treat this column as a dimension.
   *
   * @param dimensions                      list is poulated if the output col is deemed to be a dimension
   * @param aggregators                     list is populated with the aggregator if the output col is deemed to be a aggregation column.
   * @param outputColumnAggregatorFactories output col -> AggregatorFactory map
   * @param outputColumn                    column name
   * @param type                            columnType
   */
  private static void populateDimensionsAndAggregators(
      List<DimensionSchema> dimensions,
      List<AggregatorFactory> aggregators,
      Map<String, AggregatorFactory> outputColumnAggregatorFactories,
      String outputColumn,
      ColumnType type,
      QueryContext context
  )
  {
    if (outputColumnAggregatorFactories.containsKey(outputColumn)) {
      aggregators.add(outputColumnAggregatorFactories.get(outputColumn));
    } else {
      dimensions.add(
          DimensionSchemaUtils.createDimensionSchema(
              outputColumn,
              type,
              MultiStageQueryContext.useAutoColumnSchemas(context),
              MultiStageQueryContext.getArrayIngestMode(context)
          )
      );
    }
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
      final Map<Integer, Integer> stagePartitionCountMap
  )
  {
    return MSQStagesReport.create(
        queryDef,
        ImmutableMap.copyOf(stagePhaseMap),
        copyOfStageRuntimesEndingAtCurrentTime(stageRuntimeMap),
        stageWorkerCountMap,
        stagePartitionCountMap
    );
  }

  private static MSQResultsReport makeResultsTaskReport(
      final QueryDefinition queryDef,
      final Yielder<Object[]> resultsYielder,
      final ColumnMappings columnMappings,
      @Nullable final List<SqlTypeName> sqlTypeNames,
      final MSQSelectDestination selectDestination
  )
  {
    final RowSignature querySignature = queryDef.getFinalStageDefinition().getSignature();
    final ImmutableList.Builder<MSQResultsReport.ColumnAndType> mappedSignature = ImmutableList.builder();

    for (final ColumnMapping mapping : columnMappings.getMappings()) {
      mappedSignature.add(
          new MSQResultsReport.ColumnAndType(
              mapping.getOutputColumn(),
              querySignature.getColumnType(mapping.getQueryColumn()).orElse(null)
          )
      );
    }

    return MSQResultsReport.createReportAndLimitRowsIfNeeded(
        mappedSignature.build(),
        sqlTypeNames,
        resultsYielder,
        selectDestination
    );
  }

  private static MSQStatusReport makeStatusReport(
      final TaskState taskState,
      @Nullable final MSQErrorReport errorReport,
      final Queue<MSQErrorReport> errorReports,
      @Nullable final DateTime queryStartTime,
      final long queryDuration,
      MSQWorkerTaskLauncher taskLauncher,
      final SegmentLoadStatusFetcher segmentLoadWaiter
  )
  {
    int pendingTasks = -1;
    int runningTasks = 1;
    Map<Integer, List<MSQWorkerTaskLauncher.WorkerStats>> workerStatsMap = new HashMap<>();

    if (taskLauncher != null) {
      WorkerCount workerTaskCount = taskLauncher.getWorkerTaskCount();
      pendingTasks = workerTaskCount.getPendingWorkerCount();
      runningTasks = workerTaskCount.getRunningWorkerCount() + 1; // To account for controller.
      workerStatsMap = taskLauncher.getWorkerStats();
    }

    SegmentLoadStatusFetcher.SegmentLoadWaiterStatus status = segmentLoadWaiter == null ? null : segmentLoadWaiter.status();

    return new MSQStatusReport(
        taskState,
        errorReport,
        errorReports,
        queryStartTime,
        queryDuration,
        workerStatsMap,
        pendingTasks,
        runningTasks,
        status
    );
  }

  private static InputSpecSlicerFactory makeInputSpecSlicerFactory(final DataSegmentTimelineView timelineView)
  {
    return stagePartitionsMap -> new MapInputSpecSlicer(
        ImmutableMap.<Class<? extends InputSpec>, InputSpecSlicer>builder()
                    .put(StageInputSpec.class, new StageInputSpecSlicer(stagePartitionsMap))
                    .put(ExternalInputSpec.class, new ExternalInputSpecSlicer())
                    .put(InlineInputSpec.class, new InlineInputSpecSlicer())
                    .put(LookupInputSpec.class, new LookupInputSpecSlicer())
                    .put(TableInputSpec.class, new TableInputSpecSlicer(timelineView))
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
    private final Closer closer;
    private final ControllerQueryKernel queryKernel;

    /**
     * Return value of {@link MSQWorkerTaskLauncher#start()}. Set by {@link #startTaskLauncher()}.
     */
    private ListenableFuture<?> workerTaskLauncherFuture;

    /**
     * Segments to generate. Populated prior to launching the final stage of a query with destination
     * {@link DataSourceMSQDestination} (which originate from SQL INSERT or REPLACE). The final stage of such a query
     * uses {@link SegmentGeneratorFrameProcessorFactory}, which requires a list of segment IDs to generate.
     */
    private List<SegmentIdWithShardSpec> segmentsToGenerate;

    public RunQueryUntilDone(
        final QueryDefinition queryDef,
        final InputSpecSlicerFactory inputSpecSlicerFactory,
        final Closer closer
    )
    {
      this.queryDef = queryDef;
      this.inputSpecSlicerFactory = inputSpecSlicerFactory;
      this.closer = closer;
      this.queryKernel = new ControllerQueryKernel(
          queryDef,
          workerMemoryParameters.getPartitionStatisticsMaxRetainedBytes(),
          isFaultToleranceEnabled
      );
    }

    /**
     * Primary 'run' method.
     */
    private Pair<ControllerQueryKernel, ListenableFuture<?>> run() throws IOException, InterruptedException
    {
      startTaskLauncher();

      while (!queryKernel.isDone()) {
        startStages();
        fetchStatsFromWorkers();
        sendPartitionBoundaries();
        updateLiveReportMaps();
        cleanUpEffectivelyFinishedStages();
        retryFailedTasks();
        checkForErrorsInSketchFetcher();
        runKernelCommands();
      }

      if (!queryKernel.isSuccess()) {
        throwKernelExceptionIfNotUnknown();
      }

      updateLiveReportMaps();
      cleanUpEffectivelyFinishedStages();
      return Pair.of(queryKernel, workerTaskLauncherFuture);
    }

    private void checkForErrorsInSketchFetcher()
    {
      Throwable throwable = workerSketchFetcher.getError();
      if (throwable != null) {
        throw new ISE(throwable, "worker sketch fetch failed");
      }
    }


    private void retryFailedTasks() throws InterruptedException
    {
      // if no work orders to rety skip
      if (workOrdersToRetry.size() == 0) {
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
                  workOrders = new HashMap<Integer, WorkOrder>();
                }
                workOrders.put(workerStages.getKey(), workOrder);
                return workOrders;
              }
          );
        }
      }

      // wait till the workers identified above are fully ready
      workerTaskLauncher.waitUntilWorkersReady(workersNeedToBeFullyStarted);

      for (Map.Entry<StageId, Map<Integer, WorkOrder>> stageWorkOrders : stageWorkerOrders.entrySet()) {

        contactWorkersForStage(
            queryKernel,
            (netClient, taskId, workerNumber) -> netClient.postWorkOrder(
                taskId,
                stageWorkOrders.getValue().get(workerNumber)
            ),
            new IntArraySet(stageWorkOrders.getValue().keySet()),
            (taskId) -> {
              int workerNumber = MSQTasks.workerFromTaskId(taskId);
              queryKernel.workOrdersSentForWorker(stageWorkOrders.getKey(), workerNumber);

              // remove successfully contacted workOrders from workOrdersToRetry
              workOrdersToRetry.compute(workerNumber, (task, workOrderSet) -> {
                if (workOrderSet == null || workOrderSet.size() == 0 || !workOrderSet.remove(stageWorkOrders.getValue()
                                                                                                            .get(
                                                                                                                workerNumber))) {
                  throw new ISE("Worker[%d] orders not found", workerNumber);
                }
                if (workOrderSet.size() == 0) {
                  return null;
                }
                return workOrderSet;
              });
            },
            isFaultToleranceEnabled
        );
      }
    }

    /**
     * Run at least one command from {@link #kernelManipulationQueue}, waiting for it if necessary.
     */
    private void runKernelCommands() throws InterruptedException
    {
      if (!queryKernel.isDone()) {
        // Run the next command, waiting for it if necessary.
        Consumer<ControllerQueryKernel> command = kernelManipulationQueue.take();
        command.accept(queryKernel);

        // Run all pending commands after that one. Helps avoid deep queues.
        // After draining the command queue, move on to the next iteration of the controller loop.
        while ((command = kernelManipulationQueue.poll()) != null) {
          command.accept(queryKernel);
        }
      }
    }

    /**
     * Start up the {@link MSQWorkerTaskLauncher}, such that later on it can be used to launch new tasks
     * via {@link MSQWorkerTaskLauncher#launchTasksIfNeeded}.
     */
    private void startTaskLauncher()
    {
      // Start tasks.
      log.debug("Query [%s] starting task launcher.", queryDef.getQueryId());

      workerTaskLauncherFuture = workerTaskLauncher.start();
      closer.register(() -> workerTaskLauncher.stop(true));

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
     * Enqueues the fetching {@link org.apache.druid.msq.statistics.ClusterByStatisticsCollector}
     * from each worker via {@link WorkerSketchFetcher}
     */
    private void fetchStatsFromWorkers()
    {

      for (Map.Entry<StageId, Set<Integer>> stageToWorker : queryKernel.getStagesAndWorkersToFetchClusterStats()
                                                                       .entrySet()) {
        List<String> allTasks = workerTaskLauncher.getActiveTasks();
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
          tasks.stream().map(MSQTasks::workerFromTaskId).collect(Collectors.toSet())
      );
      workerSketchFetcher.inMemoryFullSketchMerging(ControllerImpl.this::addToKernelManipulationQueue,
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
                 .map(MSQTasks::workerFromTaskId)
                 .collect(Collectors.toSet())
        );
        workerSketchFetcher.sequentialTimeChunkMerging(
            ControllerImpl.this::addToKernelManipulationQueue,
            queryKernel.getCompleteKeyStatisticsInformation(stageId),
            stageId, tasks,
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
          MultiStageQueryContext.getMaxInputBytesPerWorker(task.getQuerySpec().getQuery().context());

      logKernelStatus(queryDef.getQueryId(), queryKernel);
      final List<StageId> newStageIds = queryKernel.createAndGetNewStageIds(
          inputSpecSlicerFactory,
          task.getQuerySpec().getAssignmentStrategy(),
          maxInputBytesPerWorker
      );

      for (final StageId stageId : newStageIds) {

        // Allocate segments, if this is the final stage of an ingestion.
        if (MSQControllerTask.isIngestion(task.getQuerySpec())
            && stageId.getStageNumber() == queryDef.getFinalStageDefinition().getStageNumber()) {
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
          final Boolean isShuffleStageOutputEmpty = queryKernel.isStageOutputEmpty(shuffleStageId);
          if (isFailOnEmptyInsertEnabled && Boolean.TRUE.equals(isShuffleStageOutputEmpty)) {
            throw new MSQException(new InsertCannotBeEmptyFault(task.getDataSource()));
          }

          final ClusterByPartitions partitionBoundaries =
              queryKernel.getResultPartitionBoundariesForStage(shuffleStageId);

          final boolean mayHaveMultiValuedClusterByFields =
              !queryKernel.getStageDefinition(shuffleStageId).mustGatherResultKeyStatistics()
              || queryKernel.hasStageCollectorEncounteredAnyMultiValueField(shuffleStageId);

          segmentsToGenerate = generateSegmentIdsWithShardSpecs(
              (DataSourceMSQDestination) task.getQuerySpec().getDestination(),
              queryKernel.getStageDefinition(shuffleStageId).getSignature(),
              queryKernel.getStageDefinition(shuffleStageId).getClusterBy(),
              partitionBoundaries,
              mayHaveMultiValuedClusterByFields,
              isShuffleStageOutputEmpty
          );

          log.info("Query[%s] generating %d segments.", queryDef.getQueryId(), segmentsToGenerate.size());
        }

        final int workerCount = queryKernel.getWorkerInputsForStage(stageId).workerCount();
        log.info(
            "Query [%s] starting %d workers for stage %d.",
            stageId.getQueryId(),
            workerCount,
            stageId.getStageNumber()
        );

        workerTaskLauncher.launchTasksIfNeeded(workerCount);
        stageRuntimesForLiveReports.put(stageId.getStageNumber(), new Interval(DateTimes.nowUtc(), DateTimes.MAX));
        startWorkForStage(queryDef, queryKernel, stageId.getStageNumber(), segmentsToGenerate);
      }
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

      // Live reports: update stage phases, worker counts, partition counts.
      for (StageId stageId : queryKernel.getActiveStages()) {
        final int stageNumber = stageId.getStageNumber();
        stagePhasesForLiveReports.put(stageNumber, queryKernel.getStagePhase(stageId));

        if (queryKernel.doesStageHaveResultPartitions(stageId)) {
          stagePartitionCountsForLiveReports.computeIfAbsent(
              stageNumber,
              k -> Iterators.size(queryKernel.getResultPartitionsForStage(stageId).iterator())
          );
        }

        stageWorkerCountsForLiveReports.putIfAbsent(
            stageNumber,
            queryKernel.getWorkerInputsForStage(stageId).workerCount()
        );
      }

      // Live reports: update stage end times for any stages that just ended.
      for (StageId stageId : queryKernel.getActiveStages()) {
        if (ControllerStagePhase.isSuccessfulTerminalPhase(queryKernel.getStagePhase(stageId))) {
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
     */
    private void cleanUpEffectivelyFinishedStages()
    {
      for (final StageId stageId : queryKernel.getEffectivelyFinishedStageIds()) {
        log.info("Query [%s] issuing cleanup order for stage %d.", queryDef.getQueryId(), stageId.getStageNumber());
        contactWorkersForStage(
            queryKernel,
            (netClient, taskId, workerNumber) -> netClient.postCleanupStage(taskId, stageId),
            queryKernel.getWorkerInputsForStage(stageId).workers(),
            (ignore1) -> {
            },
            false
        );
        queryKernel.finishStage(stageId, true);
      }
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
          task.getQuerySpec().getColumnMappings()
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
    ListenableFuture<Void> contactTask(WorkerClient client, String taskId, int workerNumber);
  }

  /**
   * Interface used when {@link TaskContactFn#contactTask(WorkerClient, String, int)} returns a successful future.
   */
  private interface TaskContactSuccess
  {
    void onSuccess(String taskId);

  }
}
