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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.FirehoseFactoryToInputSourceAdaptor;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.Rows;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.indexer.Checks;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.indexer.Property;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SecondaryPartitionType;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.TaskRealtimeMetricsMonitorBuilder;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.stats.TaskRealtimeMetricsMonitor;
import org.apache.druid.indexing.common.task.batch.parallel.PartialHashSegmentGenerateTask;
import org.apache.druid.indexing.common.task.batch.parallel.iterator.DefaultIndexTaskInputRowIteratorBuilder;
import org.apache.druid.indexing.common.task.batch.partition.CompletePartitionAnalysis;
import org.apache.druid.indexing.common.task.batch.partition.HashPartitionAnalysis;
import org.apache.druid.indexing.common.task.batch.partition.LinearPartitionAnalysis;
import org.apache.druid.indexing.common.task.batch.partition.PartitionAnalysis;
import org.apache.druid.indexing.overlord.sampler.InputSourceSampler;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.BatchIOConfig;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.IngestionSpec;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.apache.druid.segment.realtime.appenderator.BaseAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.BatchAppenderatorDriver;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;

public class IndexTask extends AbstractBatchIndexTask implements ChatHandler
{
  public static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();

  private static final Logger log = new Logger(IndexTask.class);
  private static final String TYPE = "index";

  private static String makeGroupId(IndexIngestionSpec ingestionSchema)
  {
    return makeGroupId(ingestionSchema.ioConfig.appendToExisting, ingestionSchema.dataSchema.getDataSource());
  }

  private static String makeGroupId(boolean isAppendToExisting, String dataSource)
  {
    if (isAppendToExisting) {
      // Shared locking group for all tasks that append, since they are OK to run concurrently.
      return StringUtils.format("%s_append_%s", TYPE, dataSource);
    } else {
      // Return null, one locking group per task.
      return null;
    }
  }

  private final IndexIngestionSpec ingestionSchema;

  private IngestionState ingestionState;

  @MonotonicNonNull
  private ParseExceptionHandler determinePartitionsParseExceptionHandler;

  @MonotonicNonNull
  private ParseExceptionHandler buildSegmentsParseExceptionHandler;

  @MonotonicNonNull
  private AuthorizerMapper authorizerMapper;

  @MonotonicNonNull
  private RowIngestionMeters determinePartitionsMeters;

  @MonotonicNonNull
  private RowIngestionMeters buildSegmentsMeters;

  @Nullable
  private String errorMsg;


  @JsonCreator
  public IndexTask(
      @JsonProperty("id") final String id,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("spec") final IndexIngestionSpec ingestionSchema,
      @JsonProperty("context") final Map<String, Object> context
  )
  {
    this(
        id,
        makeGroupId(ingestionSchema),
        taskResource,
        ingestionSchema.dataSchema.getDataSource(),
        ingestionSchema,
        context
    );
  }

  public IndexTask(
      String id,
      String groupId,
      TaskResource resource,
      String dataSource,
      IndexIngestionSpec ingestionSchema,
      Map<String, Object> context
  )
  {
    super(
        getOrMakeId(id, TYPE, dataSource),
        groupId,
        resource,
        dataSource,
        context
    );
    this.ingestionSchema = ingestionSchema;
    this.ingestionState = IngestionState.NOT_STARTED;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    final IndexTuningConfig tuningConfig = getIngestionSchema().getTuningConfig();
    if (tuningConfig != null && tuningConfig.getPartitionsSpec() != null) {
      if (tuningConfig.getPartitionsSpec().getType() != SecondaryPartitionType.LINEAR
          && tuningConfig.getPartitionsSpec().getType() != SecondaryPartitionType.HASH) {
        throw new UOE("partitionsSpec[%s] is not supported", tuningConfig.getPartitionsSpec().getClass().getName());
      }
    }
    return determineLockGranularityAndTryLock(taskActionClient, ingestionSchema.dataSchema.getGranularitySpec());
  }

  @Override
  public boolean requireLockExistingSegments()
  {
    return isGuaranteedRollup(ingestionSchema.ioConfig, ingestionSchema.tuningConfig)
           || !ingestionSchema.ioConfig.isAppendToExisting();
  }

  @Override
  public List<DataSegment> findSegmentsToLock(TaskActionClient taskActionClient, List<Interval> intervals)
      throws IOException
  {
    return findInputSegments(
        getDataSource(),
        taskActionClient,
        intervals,
        ingestionSchema.ioConfig.firehoseFactory
    );
  }

  @Override
  public boolean isPerfectRollup()
  {
    return isGuaranteedRollup(ingestionSchema.ioConfig, ingestionSchema.tuningConfig);
  }

  @Nullable
  @Override
  public Granularity getSegmentGranularity()
  {
    final GranularitySpec granularitySpec = ingestionSchema.getDataSchema().getGranularitySpec();
    if (granularitySpec instanceof ArbitraryGranularitySpec) {
      return null;
    } else {
      return granularitySpec.getSegmentGranularity();
    }
  }

  @GET
  @Path("/unparseableEvents")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getUnparseableEvents(
      @Context final HttpServletRequest req,
      @QueryParam("full") String full
  )
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    Map<String, List<String>> events = new HashMap<>();

    boolean needsDeterminePartitions = false;
    boolean needsBuildSegments = false;

    if (full != null) {
      needsDeterminePartitions = true;
      needsBuildSegments = true;
    } else {
      switch (ingestionState) {
        case DETERMINE_PARTITIONS:
          needsDeterminePartitions = true;
          break;
        case BUILD_SEGMENTS:
        case COMPLETED:
          needsBuildSegments = true;
          break;
        default:
          break;
      }
    }

    if (needsDeterminePartitions) {
      events.put(
          RowIngestionMeters.DETERMINE_PARTITIONS,
          IndexTaskUtils.getMessagesFromSavedParseExceptions(
              determinePartitionsParseExceptionHandler.getSavedParseExceptions()
          )
      );
    }

    if (needsBuildSegments) {
      events.put(
          RowIngestionMeters.BUILD_SEGMENTS,
          IndexTaskUtils.getMessagesFromSavedParseExceptions(
              buildSegmentsParseExceptionHandler.getSavedParseExceptions()
          )
      );
    }

    return Response.ok(events).build();
  }

  private Map<String, Object> doGetRowStats(String full)
  {
    Map<String, Object> returnMap = new HashMap<>();
    Map<String, Object> totalsMap = new HashMap<>();
    Map<String, Object> averagesMap = new HashMap<>();

    boolean needsDeterminePartitions = false;
    boolean needsBuildSegments = false;

    if (full != null) {
      needsDeterminePartitions = true;
      needsBuildSegments = true;
    } else {
      switch (ingestionState) {
        case DETERMINE_PARTITIONS:
          needsDeterminePartitions = true;
          break;
        case BUILD_SEGMENTS:
        case COMPLETED:
          needsBuildSegments = true;
          break;
        default:
          break;
      }
    }

    if (needsDeterminePartitions) {
      totalsMap.put(
          RowIngestionMeters.DETERMINE_PARTITIONS,
          determinePartitionsMeters.getTotals()
      );
      averagesMap.put(
          RowIngestionMeters.DETERMINE_PARTITIONS,
          determinePartitionsMeters.getMovingAverages()
      );
    }

    if (needsBuildSegments) {
      totalsMap.put(
          RowIngestionMeters.BUILD_SEGMENTS,
          buildSegmentsMeters.getTotals()
      );
      averagesMap.put(
          RowIngestionMeters.BUILD_SEGMENTS,
          buildSegmentsMeters.getMovingAverages()
      );
    }

    returnMap.put("totals", totalsMap);
    returnMap.put("movingAverages", averagesMap);
    return returnMap;
  }

  @GET
  @Path("/rowStats")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRowStats(
      @Context final HttpServletRequest req,
      @QueryParam("full") String full
  )
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    return Response.ok(doGetRowStats(full)).build();
  }

  @GET
  @Path("/liveReports")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLiveReports(
      @Context final HttpServletRequest req,
      @QueryParam("full") String full
  )
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    Map<String, Object> returnMap = new HashMap<>();
    Map<String, Object> ingestionStatsAndErrors = new HashMap<>();
    Map<String, Object> payload = new HashMap<>();
    Map<String, Object> events = getTaskCompletionUnparseableEvents();

    payload.put("ingestionState", ingestionState);
    payload.put("unparseableEvents", events);
    payload.put("rowStats", doGetRowStats(full));

    ingestionStatsAndErrors.put("taskId", getId());
    ingestionStatsAndErrors.put("payload", payload);
    ingestionStatsAndErrors.put("type", "ingestionStatsAndErrors");

    returnMap.put("ingestionStatsAndErrors", ingestionStatsAndErrors);
    return Response.ok(returnMap).build();
  }

  @JsonProperty("spec")
  public IndexIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @Override
  public TaskStatus runTask(final TaskToolbox toolbox)
  {
    try {
      log.debug("Found chat handler of class[%s]", toolbox.getChatHandlerProvider().getClass().getName());

      if (toolbox.getChatHandlerProvider().get(getId()).isPresent()) {
        // This is a workaround for ParallelIndexSupervisorTask to avoid double registering when it runs in the
        // sequential mode. See ParallelIndexSupervisorTask.runSequential().
        // Note that all HTTP endpoints are not available in this case. This works only for
        // ParallelIndexSupervisorTask because it doesn't support APIs for live ingestion reports.
        log.warn("Chat handler is already registered. Skipping chat handler registration.");
      } else {
        toolbox.getChatHandlerProvider().register(getId(), this, false);
      }

      this.authorizerMapper = toolbox.getAuthorizerMapper();
      this.determinePartitionsMeters = toolbox.getRowIngestionMetersFactory().createRowIngestionMeters();
      this.buildSegmentsMeters = toolbox.getRowIngestionMetersFactory().createRowIngestionMeters();
      this.determinePartitionsParseExceptionHandler = new ParseExceptionHandler(
          determinePartitionsMeters,
          ingestionSchema.getTuningConfig().isLogParseExceptions(),
          ingestionSchema.getTuningConfig().getMaxParseExceptions(),
          ingestionSchema.getTuningConfig().getMaxSavedParseExceptions()
      );
      this.buildSegmentsParseExceptionHandler = new ParseExceptionHandler(
          buildSegmentsMeters,
          ingestionSchema.getTuningConfig().isLogParseExceptions(),
          ingestionSchema.getTuningConfig().getMaxParseExceptions(),
          ingestionSchema.getTuningConfig().getMaxSavedParseExceptions()
      );

      final boolean determineIntervals = !ingestionSchema.getDataSchema()
                                                         .getGranularitySpec()
                                                         .bucketIntervals()
                                                         .isPresent();

      final InputSource inputSource = ingestionSchema.getIOConfig().getNonNullInputSource(
          ingestionSchema.getDataSchema().getParser()
      );

      final File tmpDir = toolbox.getIndexingTmpDir();

      ingestionState = IngestionState.DETERMINE_PARTITIONS;

      // Initialize maxRowsPerSegment and maxTotalRows lazily
      final IndexTuningConfig tuningConfig = ingestionSchema.tuningConfig;
      final PartitionsSpec partitionsSpec = tuningConfig.getGivenOrDefaultPartitionsSpec();
      final PartitionAnalysis partitionAnalysis = determineShardSpecs(
          toolbox,
          inputSource,
          tmpDir,
          partitionsSpec
      );
      final List<Interval> allocateIntervals = new ArrayList<>(partitionAnalysis.getAllIntervalsToIndex());
      final DataSchema dataSchema;
      if (determineIntervals) {
        if (!determineLockGranularityAndTryLock(toolbox.getTaskActionClient(), allocateIntervals)) {
          throw new ISE("Failed to get locks for intervals[%s]", allocateIntervals);
        }

        dataSchema = ingestionSchema.getDataSchema().withGranularitySpec(
            ingestionSchema.getDataSchema()
                           .getGranularitySpec()
                           .withIntervals(JodaUtils.condenseIntervals(allocateIntervals))
        );
      } else {
        dataSchema = ingestionSchema.getDataSchema();
      }

      ingestionState = IngestionState.BUILD_SEGMENTS;
      return generateAndPublishSegments(
          toolbox,
          dataSchema,
          inputSource,
          tmpDir,
          partitionAnalysis
      );
    }
    catch (Exception e) {
      log.error(e, "Encountered exception in %s.", ingestionState);
      errorMsg = Throwables.getStackTraceAsString(e);
      toolbox.getTaskReportFileWriter().write(getId(), getTaskCompletionReports());
      return TaskStatus.failure(
          getId(),
          errorMsg
      );
    }

    finally {
      toolbox.getChatHandlerProvider().unregister(getId());
    }
  }

  private Map<String, TaskReport> getTaskCompletionReports()
  {
    return TaskReport.buildTaskReports(
        new IngestionStatsAndErrorsTaskReport(
            getId(),
            new IngestionStatsAndErrorsTaskReportData(
                ingestionState,
                getTaskCompletionUnparseableEvents(),
                getTaskCompletionRowStats(),
                errorMsg
            )
        )
    );
  }

  private Map<String, Object> getTaskCompletionUnparseableEvents()
  {
    Map<String, Object> unparseableEventsMap = new HashMap<>();
    List<String> determinePartitionsParseExceptionMessages = IndexTaskUtils.getMessagesFromSavedParseExceptions(
        determinePartitionsParseExceptionHandler.getSavedParseExceptions()
    );
    List<String> buildSegmentsParseExceptionMessages = IndexTaskUtils.getMessagesFromSavedParseExceptions(
        buildSegmentsParseExceptionHandler.getSavedParseExceptions()
    );

    if (determinePartitionsParseExceptionMessages != null || buildSegmentsParseExceptionMessages != null) {
      unparseableEventsMap.put(RowIngestionMeters.DETERMINE_PARTITIONS, determinePartitionsParseExceptionMessages);
      unparseableEventsMap.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegmentsParseExceptionMessages);
    }

    return unparseableEventsMap;
  }

  private Map<String, Object> getTaskCompletionRowStats()
  {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put(
        RowIngestionMeters.DETERMINE_PARTITIONS,
        determinePartitionsMeters.getTotals()
    );

    metrics.put(
        RowIngestionMeters.BUILD_SEGMENTS,
        buildSegmentsMeters.getTotals()
    );

    return metrics;
  }

  /**
   * Determines intervals and shardSpecs for input data.  This method first checks that it must determine intervals and
   * shardSpecs by itself.  Intervals must be determined if they are not specified in {@link GranularitySpec}.
   * ShardSpecs must be determined if the perfect rollup must be guaranteed even though the number of shards is not
   * specified in {@link IndexTuningConfig}.
   * <p/>
   * If both intervals and shardSpecs don't have to be determined, this method simply returns {@link ShardSpecs} for the
   * given intervals.  Here, if {@link HashedPartitionsSpec#numShards} is not specified, {@link NumberedShardSpec} is
   * used.
   * <p/>
   * If one of intervals or shardSpecs need to be determined, this method reads the entire input for determining one of
   * them.  If the perfect rollup must be guaranteed, {@link HashBasedNumberedShardSpec} is used for hash partitioning
   * of input data.  In the future we may want to also support single-dimension partitioning.
   *
   * @return a map indicating how many shardSpecs need to be created per interval.
   */
  private PartitionAnalysis determineShardSpecs(
      final TaskToolbox toolbox,
      final InputSource inputSource,
      final File tmpDir,
      @Nonnull final PartitionsSpec partitionsSpec
  ) throws IOException
  {
    final ObjectMapper jsonMapper = toolbox.getJsonMapper();

    final GranularitySpec granularitySpec = ingestionSchema.getDataSchema().getGranularitySpec();

    // Must determine intervals if unknown, since we acquire all locks before processing any data.
    final boolean determineIntervals = !granularitySpec.bucketIntervals().isPresent();

    // Must determine partitions if rollup is guaranteed and the user didn't provide a specific value.
    final boolean determineNumPartitions = partitionsSpec.needsDeterminePartitions(false);

    // if we were given number of shards per interval and the intervals, we don't need to scan the data
    if (!determineNumPartitions && !determineIntervals) {
      log.info("Skipping determine partition scan");
      if (partitionsSpec.getType() == SecondaryPartitionType.HASH) {
        return PartialHashSegmentGenerateTask.createHashPartitionAnalysisFromPartitionsSpec(
            granularitySpec,
            (HashedPartitionsSpec) partitionsSpec,
            null // not overriding numShards
        );
      } else if (partitionsSpec.getType() == SecondaryPartitionType.LINEAR) {
        return createLinearPartitionAnalysis(granularitySpec, (DynamicPartitionsSpec) partitionsSpec);
      } else {
        throw new UOE("%s", partitionsSpec.getClass().getName());
      }
    } else {
      // determine intervals containing data and prime HLL collectors
      log.info("Determining intervals and shardSpecs");
      return createShardSpecsFromInput(
          jsonMapper,
          ingestionSchema,
          inputSource,
          tmpDir,
          granularitySpec,
          partitionsSpec,
          determineIntervals
      );
    }
  }

  private static LinearPartitionAnalysis createLinearPartitionAnalysis(
      GranularitySpec granularitySpec,
      @Nonnull DynamicPartitionsSpec partitionsSpec
  )
  {
    final SortedSet<Interval> intervals = granularitySpec.bucketIntervals().get();
    final int numBucketsPerInterval = 1;
    final LinearPartitionAnalysis partitionAnalysis = new LinearPartitionAnalysis(partitionsSpec);
    intervals.forEach(interval -> partitionAnalysis.updateBucket(interval, numBucketsPerInterval));
    return partitionAnalysis;
  }

  private PartitionAnalysis createShardSpecsFromInput(
      ObjectMapper jsonMapper,
      IndexIngestionSpec ingestionSchema,
      InputSource inputSource,
      File tmpDir,
      GranularitySpec granularitySpec,
      @Nonnull PartitionsSpec partitionsSpec,
      boolean determineIntervals
  ) throws IOException
  {
    assert partitionsSpec.getType() != SecondaryPartitionType.RANGE;
    long determineShardSpecsStartMillis = System.currentTimeMillis();

    final Map<Interval, Optional<HyperLogLogCollector>> hllCollectors = collectIntervalsAndShardSpecs(
        jsonMapper,
        ingestionSchema,
        inputSource,
        tmpDir,
        granularitySpec,
        partitionsSpec,
        determineIntervals
    );
    final PartitionAnalysis<Integer, ?> partitionAnalysis;
    if (partitionsSpec.getType() == SecondaryPartitionType.LINEAR) {
      partitionAnalysis = new LinearPartitionAnalysis((DynamicPartitionsSpec) partitionsSpec);
    } else if (partitionsSpec.getType() == SecondaryPartitionType.HASH) {
      partitionAnalysis = new HashPartitionAnalysis((HashedPartitionsSpec) partitionsSpec);
    } else {
      throw new UOE("%s", partitionsSpec.getClass().getName());
    }
    for (final Map.Entry<Interval, Optional<HyperLogLogCollector>> entry : hllCollectors.entrySet()) {
      final Interval interval = entry.getKey();
      final int numBucketsPerInterval;
      if (partitionsSpec.getType() == SecondaryPartitionType.HASH) {
        final HashedPartitionsSpec hashedPartitionsSpec = (HashedPartitionsSpec) partitionsSpec;
        final HyperLogLogCollector collector = entry.getValue().orNull();

        if (partitionsSpec.needsDeterminePartitions(false)) {
          final long numRows = Preconditions.checkNotNull(collector, "HLL collector").estimateCardinalityRound();
          final int nonNullMaxRowsPerSegment = partitionsSpec.getMaxRowsPerSegment() == null
                                               ? PartitionsSpec.DEFAULT_MAX_ROWS_PER_SEGMENT
                                               : partitionsSpec.getMaxRowsPerSegment();
          numBucketsPerInterval = (int) Math.ceil((double) numRows / nonNullMaxRowsPerSegment);
          log.info(
              "Estimated [%,d] rows of data for interval [%s], creating [%,d] shards",
              numRows,
              interval,
              numBucketsPerInterval
          );
        } else {
          numBucketsPerInterval = hashedPartitionsSpec.getNumShards() == null ? 1 : hashedPartitionsSpec.getNumShards();
          log.info("Creating [%,d] buckets for interval [%s]", numBucketsPerInterval, interval);
        }
      } else {
        numBucketsPerInterval = 1;
      }

      partitionAnalysis.updateBucket(interval, numBucketsPerInterval);
    }
    log.info("Found intervals and shardSpecs in %,dms", System.currentTimeMillis() - determineShardSpecsStartMillis);

    return partitionAnalysis;
  }

  private Map<Interval, Optional<HyperLogLogCollector>> collectIntervalsAndShardSpecs(
      ObjectMapper jsonMapper,
      IndexIngestionSpec ingestionSchema,
      InputSource inputSource,
      File tmpDir,
      GranularitySpec granularitySpec,
      @Nonnull PartitionsSpec partitionsSpec,
      boolean determineIntervals
  ) throws IOException
  {
    final Map<Interval, Optional<HyperLogLogCollector>> hllCollectors = new TreeMap<>(
        Comparators.intervalsByStartThenEnd()
    );
    final Granularity queryGranularity = granularitySpec.getQueryGranularity();
    final Predicate<InputRow> rowFilter = inputRow -> {
      if (inputRow == null) {
        return false;
      }
      if (determineIntervals) {
        return true;
      }
      final Optional<Interval> optInterval = granularitySpec.bucketInterval(inputRow.getTimestamp());
      return optInterval.isPresent();
    };

    try (final CloseableIterator<InputRow> inputRowIterator = AbstractBatchIndexTask.inputSourceReader(
        tmpDir,
        ingestionSchema.getDataSchema(),
        inputSource,
        inputSource.needsFormat() ? getInputFormat(ingestionSchema) : null,
        rowFilter,
        determinePartitionsMeters,
        determinePartitionsParseExceptionHandler
    )) {
      while (inputRowIterator.hasNext()) {
        final InputRow inputRow = inputRowIterator.next();

        final Interval interval;
        if (determineIntervals) {
          interval = granularitySpec.getSegmentGranularity().bucket(inputRow.getTimestamp());
        } else {
          final Optional<Interval> optInterval = granularitySpec.bucketInterval(inputRow.getTimestamp());
          // this interval must exist since it passed the rowFilter
          assert optInterval.isPresent();
          interval = optInterval.get();
        }

        if (partitionsSpec.needsDeterminePartitions(false)) {
          hllCollectors.computeIfAbsent(interval, intv -> Optional.of(HyperLogLogCollector.makeLatestCollector()));

          List<Object> groupKey = Rows.toGroupKey(
              queryGranularity.bucketStart(inputRow.getTimestamp()).getMillis(),
              inputRow
          );
          hllCollectors.get(interval).get()
                       .add(HASH_FUNCTION.hashBytes(jsonMapper.writeValueAsBytes(groupKey)).asBytes());
        } else {
          // we don't need to determine partitions but we still need to determine intervals, so add an Optional.absent()
          // for the interval and don't instantiate a HLL collector
          hllCollectors.putIfAbsent(interval, Optional.absent());
        }
        determinePartitionsMeters.incrementProcessed();
      }
    }

    // These metrics are reported in generateAndPublishSegments()
    if (determinePartitionsMeters.getThrownAway() > 0) {
      log.warn("Unable to find a matching interval for [%,d] events", determinePartitionsMeters.getThrownAway());
    }
    if (determinePartitionsMeters.getUnparseable() > 0) {
      log.warn("Unable to parse [%,d] events", determinePartitionsMeters.getUnparseable());
    }

    return hllCollectors;
  }

  /**
   * This method reads input data row by row and adds the read row to a proper segment using {@link BaseAppenderatorDriver}.
   * If there is no segment for the row, a new one is created.  Segments can be published in the middle of reading inputs
   * if {@link DynamicPartitionsSpec} is used and one of below conditions are satisfied.
   *
   * <ul>
   * <li>
   * If the number of rows in a segment exceeds {@link DynamicPartitionsSpec#maxRowsPerSegment}
   * </li>
   * <li>
   * If the number of rows added to {@link BaseAppenderatorDriver} so far exceeds {@link DynamicPartitionsSpec#maxTotalRows}
   * </li>
   * </ul>
   * <p>
   * At the end of this method, all the remaining segments are published.
   *
   * @return the last {@link TaskStatus}
   */
  private TaskStatus generateAndPublishSegments(
      final TaskToolbox toolbox,
      final DataSchema dataSchema,
      final InputSource inputSource,
      final File tmpDir,
      final PartitionAnalysis partitionAnalysis
  ) throws IOException, InterruptedException
  {
    final FireDepartment fireDepartmentForMetrics =
        new FireDepartment(dataSchema, new RealtimeIOConfig(null, null), null);
    FireDepartmentMetrics buildSegmentsFireDepartmentMetrics = fireDepartmentForMetrics.getMetrics();

    if (toolbox.getMonitorScheduler() != null) {
      final TaskRealtimeMetricsMonitor metricsMonitor = TaskRealtimeMetricsMonitorBuilder.build(
          this,
          fireDepartmentForMetrics,
          buildSegmentsMeters
      );
      toolbox.getMonitorScheduler().addMonitor(metricsMonitor);
    }

    final PartitionsSpec partitionsSpec = partitionAnalysis.getPartitionsSpec();
    final IndexTuningConfig tuningConfig = ingestionSchema.getTuningConfig();
    final long pushTimeout = tuningConfig.getPushTimeout();

    final SegmentAllocatorForBatch segmentAllocator;
    final SequenceNameFunction sequenceNameFunction;
    switch (partitionsSpec.getType()) {
      case HASH:
      case RANGE:
        final SegmentAllocatorForBatch localSegmentAllocator = SegmentAllocators.forNonLinearPartitioning(
            toolbox,
            getDataSource(),
            getId(),
            dataSchema.getGranularitySpec(),
            null,
            (CompletePartitionAnalysis) partitionAnalysis
        );
        sequenceNameFunction = localSegmentAllocator.getSequenceNameFunction();
        segmentAllocator = localSegmentAllocator;
        break;
      case LINEAR:
        segmentAllocator = SegmentAllocators.forLinearPartitioning(
            toolbox,
            getId(),
            null,
            dataSchema,
            getTaskLockHelper(),
            ingestionSchema.getIOConfig().isAppendToExisting(),
            partitionAnalysis.getPartitionsSpec()
        );
        sequenceNameFunction = segmentAllocator.getSequenceNameFunction();
        break;
      default:
        throw new UOE("[%s] secondary partition type is not supported", partitionsSpec.getType());
    }

    final TransactionalSegmentPublisher publisher = (segmentsToBeOverwritten, segmentsToPublish, commitMetadata) ->
        toolbox.getTaskActionClient()
               .submit(SegmentTransactionalInsertAction.overwriteAction(segmentsToBeOverwritten, segmentsToPublish));

    String effectiveId = getContextValue(CompactionTask.CTX_KEY_APPENDERATOR_TRACKING_TASK_ID, null);
    if (effectiveId == null) {
      effectiveId = getId();
    }

    final Appenderator appenderator = BatchAppenderators.newAppenderator(
        effectiveId,
        toolbox.getAppenderatorsManager(),
        buildSegmentsFireDepartmentMetrics,
        toolbox,
        dataSchema,
        tuningConfig,
        buildSegmentsMeters,
        buildSegmentsParseExceptionHandler
    );
    boolean exceptionOccurred = false;
    try (final BatchAppenderatorDriver driver = BatchAppenderators.newDriver(appenderator, toolbox, segmentAllocator)) {
      driver.startJob();

      InputSourceProcessor.process(
          dataSchema,
          driver,
          partitionsSpec,
          inputSource,
          inputSource.needsFormat() ? getInputFormat(ingestionSchema) : null,
          tmpDir,
          sequenceNameFunction,
          new DefaultIndexTaskInputRowIteratorBuilder(),
          buildSegmentsMeters,
          buildSegmentsParseExceptionHandler,
          pushTimeout
      );

      // If we use timeChunk lock, then we don't have to specify what segments will be overwritten because
      // it will just overwrite all segments overlapped with the new segments.
      final Set<DataSegment> inputSegments = getTaskLockHelper().isUseSegmentLock()
                                             ? getTaskLockHelper().getLockedExistingSegments()
                                             : null;
      final boolean storeCompactionState = getContextValue(
          Tasks.STORE_COMPACTION_STATE_KEY,
          Tasks.DEFAULT_STORE_COMPACTION_STATE
      );
      final Function<Set<DataSegment>, Set<DataSegment>> annotateFunction =
          compactionStateAnnotateFunction(
              storeCompactionState,
              toolbox,
              ingestionSchema.getTuningConfig()
          );

      // Probably we can publish atomicUpdateGroup along with segments.
      final SegmentsAndCommitMetadata published =
          awaitPublish(driver.publishAll(inputSegments, publisher, annotateFunction), pushTimeout);
      appenderator.close();

      ingestionState = IngestionState.COMPLETED;
      if (published == null) {
        log.error("Failed to publish segments, aborting!");
        errorMsg = "Failed to publish segments.";
        toolbox.getTaskReportFileWriter().write(getId(), getTaskCompletionReports());
        return TaskStatus.failure(
            getId(),
            errorMsg
        );
      } else {
        log.info(
            "Processed[%,d] events, unparseable[%,d], thrownAway[%,d].",
            buildSegmentsMeters.getProcessed(),
            buildSegmentsMeters.getUnparseable(),
            buildSegmentsMeters.getThrownAway()
        );
        log.info("Published [%s] segments", published.getSegments().size());
        log.debugSegments(published.getSegments(), "Published segments");

        toolbox.getTaskReportFileWriter().write(getId(), getTaskCompletionReports());
        return TaskStatus.success(getId());
      }
    }
    catch (TimeoutException | ExecutionException e) {
      exceptionOccurred = true;
      throw new RuntimeException(e);
    }
    catch (Exception e) {
      exceptionOccurred = true;
      throw e;
    }
    finally {
      if (exceptionOccurred) {
        appenderator.closeNow();
      } else {
        appenderator.close();
      }
    }
  }

  private static SegmentsAndCommitMetadata awaitPublish(
      ListenableFuture<SegmentsAndCommitMetadata> publishFuture,
      long publishTimeout
  ) throws ExecutionException, InterruptedException, TimeoutException
  {
    if (publishTimeout == 0) {
      return publishFuture.get();
    } else {
      return publishFuture.get(publishTimeout, TimeUnit.MILLISECONDS);
    }
  }

  private static InputFormat getInputFormat(IndexIngestionSpec ingestionSchema)
  {
    return ingestionSchema.getIOConfig().getNonNullInputFormat();
  }

  public static class IndexIngestionSpec extends IngestionSpec<IndexIOConfig, IndexTuningConfig>
  {
    private final DataSchema dataSchema;
    private final IndexIOConfig ioConfig;
    private final IndexTuningConfig tuningConfig;

    @JsonCreator
    public IndexIngestionSpec(
        @JsonProperty("dataSchema") DataSchema dataSchema,
        @JsonProperty("ioConfig") IndexIOConfig ioConfig,
        @JsonProperty("tuningConfig") IndexTuningConfig tuningConfig
    )
    {
      super(dataSchema, ioConfig, tuningConfig);

      if (dataSchema.getParserMap() != null && ioConfig.getInputSource() != null) {
        throw new IAE("Cannot use parser and inputSource together. Try using inputFormat instead of parser.");
      }
      if (ioConfig.getInputSource() != null && ioConfig.getInputSource().needsFormat()) {
        Checks.checkOneNotNullOrEmpty(
            ImmutableList.of(
                new Property<>("parser", dataSchema.getParserMap()),
                new Property<>("inputFormat", ioConfig.getInputFormat())
            )
        );
      }

      this.dataSchema = dataSchema;
      this.ioConfig = ioConfig;
      this.tuningConfig = tuningConfig == null ? new IndexTuningConfig() : tuningConfig;
    }

    @Override
    @JsonProperty("dataSchema")
    public DataSchema getDataSchema()
    {
      return dataSchema;
    }

    @Override
    @JsonProperty("ioConfig")
    public IndexIOConfig getIOConfig()
    {
      return ioConfig;
    }

    @Override
    @JsonProperty("tuningConfig")
    public IndexTuningConfig getTuningConfig()
    {
      return tuningConfig;
    }
  }

  @JsonTypeName("index")
  public static class IndexIOConfig implements BatchIOConfig
  {
    private static final boolean DEFAULT_APPEND_TO_EXISTING = false;

    private final FirehoseFactory firehoseFactory;
    private final InputSource inputSource;
    private final InputFormat inputFormat;
    private final boolean appendToExisting;

    @JsonCreator
    public IndexIOConfig(
        @Deprecated @JsonProperty("firehose") @Nullable FirehoseFactory firehoseFactory,
        @JsonProperty("inputSource") @Nullable InputSource inputSource,
        @JsonProperty("inputFormat") @Nullable InputFormat inputFormat,
        @JsonProperty("appendToExisting") @Nullable Boolean appendToExisting
    )
    {
      Checks.checkOneNotNullOrEmpty(
          ImmutableList.of(new Property<>("firehose", firehoseFactory), new Property<>("inputSource", inputSource))
      );
      if (firehoseFactory != null && inputFormat != null) {
        throw new IAE("Cannot use firehose and inputFormat together. Try using inputSource instead of firehose.");
      }
      this.firehoseFactory = firehoseFactory;
      this.inputSource = inputSource;
      this.inputFormat = inputFormat;
      this.appendToExisting = appendToExisting == null ? DEFAULT_APPEND_TO_EXISTING : appendToExisting;
    }

    // old constructor for backward compatibility
    @Deprecated
    public IndexIOConfig(FirehoseFactory firehoseFactory, @Nullable Boolean appendToExisting)
    {
      this(firehoseFactory, null, null, appendToExisting);
    }

    @Nullable
    @JsonProperty("firehose")
    @JsonInclude(Include.NON_NULL)
    @Deprecated
    public FirehoseFactory getFirehoseFactory()
    {
      return firehoseFactory;
    }

    @Nullable
    @Override
    @JsonProperty
    public InputSource getInputSource()
    {
      return inputSource;
    }

    /**
     * Returns {@link InputFormat}. Can be null if {@link DataSchema#parserMap} is specified.
     * Also can be null in {@link InputSourceSampler}.
     */
    @Nullable
    @Override
    @JsonProperty
    public InputFormat getInputFormat()
    {
      return inputFormat;
    }

    public InputSource getNonNullInputSource(@Nullable InputRowParser inputRowParser)
    {
      if (inputSource == null) {
        return new FirehoseFactoryToInputSourceAdaptor(
            (FiniteFirehoseFactory) firehoseFactory,
            inputRowParser
        );
      } else {
        return inputSource;
      }
    }

    public InputFormat getNonNullInputFormat()
    {
      return Preconditions.checkNotNull(inputFormat, "inputFormat");
    }

    @Override
    @JsonProperty
    public boolean isAppendToExisting()
    {
      return appendToExisting;
    }
  }

  public static class IndexTuningConfig implements AppenderatorConfig
  {
    private static final IndexSpec DEFAULT_INDEX_SPEC = new IndexSpec();
    private static final int DEFAULT_MAX_PENDING_PERSISTS = 0;
    private static final boolean DEFAULT_GUARANTEE_ROLLUP = false;
    private static final boolean DEFAULT_REPORT_PARSE_EXCEPTIONS = false;
    private static final long DEFAULT_PUSH_TIMEOUT = 0;

    private final AppendableIndexSpec appendableIndexSpec;
    private final int maxRowsInMemory;
    private final long maxBytesInMemory;
    private final int maxColumnsToMerge;

    // null if all partitionsSpec related params are null. see getDefaultPartitionsSpec() for details.
    @Nullable
    private final PartitionsSpec partitionsSpec;
    private final IndexSpec indexSpec;
    private final IndexSpec indexSpecForIntermediatePersists;
    private final File basePersistDirectory;
    private final int maxPendingPersists;

    /**
     * This flag is to force _perfect rollup mode_. {@link IndexTask} will scan the whole input data twice to 1) figure
     * out proper shard specs for each segment and 2) generate segments. Note that perfect rollup mode basically assumes
     * that no more data will be appended in the future. As a result, in perfect rollup mode,
     * {@link HashBasedNumberedShardSpec} is used for shards.
     */
    private final boolean forceGuaranteedRollup;
    private final boolean reportParseExceptions;
    private final long pushTimeout;
    private final boolean logParseExceptions;
    private final int maxParseExceptions;
    private final int maxSavedParseExceptions;

    @Nullable
    private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;

    @Nullable
    private static PartitionsSpec getPartitionsSpec(
        boolean forceGuaranteedRollup,
        @Nullable PartitionsSpec partitionsSpec,
        @Nullable Integer maxRowsPerSegment,
        @Nullable Long maxTotalRows,
        @Nullable Integer numShards,
        @Nullable List<String> partitionDimensions
    )
    {
      if (partitionsSpec == null) {
        if (forceGuaranteedRollup) {
          if (maxRowsPerSegment != null
              || numShards != null
              || (partitionDimensions != null && !partitionDimensions.isEmpty())) {
            return new HashedPartitionsSpec(maxRowsPerSegment, numShards, partitionDimensions);
          } else {
            return null;
          }
        } else {
          if (maxRowsPerSegment != null || maxTotalRows != null) {
            return new DynamicPartitionsSpec(maxRowsPerSegment, maxTotalRows);
          } else {
            return null;
          }
        }
      } else {
        if (forceGuaranteedRollup) {
          if (!partitionsSpec.isForceGuaranteedRollupCompatibleType()) {
            throw new IAE(partitionsSpec.getClass().getSimpleName() + " cannot be used for perfect rollup");
          }
        } else {
          if (!(partitionsSpec instanceof DynamicPartitionsSpec)) {
            throw new IAE("DynamicPartitionsSpec must be used for best-effort rollup");
          }
        }
        return partitionsSpec;
      }
    }

    @JsonCreator
    public IndexTuningConfig(
        @JsonProperty("targetPartitionSize") @Deprecated @Nullable Integer targetPartitionSize,
        @JsonProperty("maxRowsPerSegment") @Deprecated @Nullable Integer maxRowsPerSegment,
        @JsonProperty("appendableIndexSpec") @Nullable AppendableIndexSpec appendableIndexSpec,
        @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
        @JsonProperty("maxBytesInMemory") @Nullable Long maxBytesInMemory,
        @JsonProperty("maxTotalRows") @Deprecated @Nullable Long maxTotalRows,
        @JsonProperty("rowFlushBoundary") @Deprecated @Nullable Integer rowFlushBoundary_forBackCompatibility,
        @JsonProperty("numShards") @Deprecated @Nullable Integer numShards,
        @JsonProperty("partitionDimensions") @Deprecated @Nullable List<String> partitionDimensions,
        @JsonProperty("partitionsSpec") @Nullable PartitionsSpec partitionsSpec,
        @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
        @JsonProperty("indexSpecForIntermediatePersists") @Nullable IndexSpec indexSpecForIntermediatePersists,
        @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
        @JsonProperty("forceGuaranteedRollup") @Nullable Boolean forceGuaranteedRollup,
        @JsonProperty("reportParseExceptions") @Deprecated @Nullable Boolean reportParseExceptions,
        @JsonProperty("publishTimeout") @Deprecated @Nullable Long publishTimeout,
        @JsonProperty("pushTimeout") @Nullable Long pushTimeout,
        @JsonProperty("segmentWriteOutMediumFactory") @Nullable
            SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
        @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
        @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
        @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions,
        @JsonProperty("maxColumnsToMerge") @Nullable Integer maxColumnsToMerge
    )
    {
      this(
          appendableIndexSpec,
          maxRowsInMemory != null ? maxRowsInMemory : rowFlushBoundary_forBackCompatibility,
          maxBytesInMemory != null ? maxBytesInMemory : 0,
          getPartitionsSpec(
              forceGuaranteedRollup == null ? DEFAULT_GUARANTEE_ROLLUP : forceGuaranteedRollup,
              partitionsSpec,
              maxRowsPerSegment == null ? targetPartitionSize : maxRowsPerSegment,
              maxTotalRows,
              numShards,
              partitionDimensions
          ),
          indexSpec,
          indexSpecForIntermediatePersists,
          maxPendingPersists,
          forceGuaranteedRollup,
          reportParseExceptions,
          pushTimeout != null ? pushTimeout : publishTimeout,
          null,
          segmentWriteOutMediumFactory,
          logParseExceptions,
          maxParseExceptions,
          maxSavedParseExceptions,
          maxColumnsToMerge
      );

      Preconditions.checkArgument(
          targetPartitionSize == null || maxRowsPerSegment == null,
          "Can't use targetPartitionSize and maxRowsPerSegment together"
      );
    }

    private IndexTuningConfig()
    {
      this(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
    }

    private IndexTuningConfig(
        @Nullable AppendableIndexSpec appendableIndexSpec,
        @Nullable Integer maxRowsInMemory,
        @Nullable Long maxBytesInMemory,
        @Nullable PartitionsSpec partitionsSpec,
        @Nullable IndexSpec indexSpec,
        @Nullable IndexSpec indexSpecForIntermediatePersists,
        @Nullable Integer maxPendingPersists,
        @Nullable Boolean forceGuaranteedRollup,
        @Nullable Boolean reportParseExceptions,
        @Nullable Long pushTimeout,
        @Nullable File basePersistDirectory,
        @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
        @Nullable Boolean logParseExceptions,
        @Nullable Integer maxParseExceptions,
        @Nullable Integer maxSavedParseExceptions,
        @Nullable Integer maxColumnsToMerge
    )
    {
      this.appendableIndexSpec = appendableIndexSpec == null ? DEFAULT_APPENDABLE_INDEX : appendableIndexSpec;
      this.maxRowsInMemory = maxRowsInMemory == null ? TuningConfig.DEFAULT_MAX_ROWS_IN_MEMORY : maxRowsInMemory;
      // initializing this to 0, it will be lazily initialized to a value
      // @see #getMaxBytesInMemoryOrDefault()
      this.maxBytesInMemory = maxBytesInMemory == null ? 0 : maxBytesInMemory;
      this.maxColumnsToMerge = maxColumnsToMerge == null
                               ? IndexMerger.UNLIMITED_MAX_COLUMNS_TO_MERGE
                               : maxColumnsToMerge;
      this.partitionsSpec = partitionsSpec;
      this.indexSpec = indexSpec == null ? DEFAULT_INDEX_SPEC : indexSpec;
      this.indexSpecForIntermediatePersists = indexSpecForIntermediatePersists == null ?
                                              this.indexSpec : indexSpecForIntermediatePersists;
      this.maxPendingPersists = maxPendingPersists == null ? DEFAULT_MAX_PENDING_PERSISTS : maxPendingPersists;
      this.forceGuaranteedRollup = forceGuaranteedRollup == null ? DEFAULT_GUARANTEE_ROLLUP : forceGuaranteedRollup;
      this.reportParseExceptions = reportParseExceptions == null
                                   ? DEFAULT_REPORT_PARSE_EXCEPTIONS
                                   : reportParseExceptions;
      this.pushTimeout = pushTimeout == null ? DEFAULT_PUSH_TIMEOUT : pushTimeout;
      this.basePersistDirectory = basePersistDirectory;

      this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;

      if (this.reportParseExceptions) {
        this.maxParseExceptions = 0;
        this.maxSavedParseExceptions = maxSavedParseExceptions == null ? 0 : Math.min(1, maxSavedParseExceptions);
      } else {
        this.maxParseExceptions = maxParseExceptions == null
                                  ? TuningConfig.DEFAULT_MAX_PARSE_EXCEPTIONS
                                  : maxParseExceptions;
        this.maxSavedParseExceptions = maxSavedParseExceptions == null
                                       ? TuningConfig.DEFAULT_MAX_SAVED_PARSE_EXCEPTIONS
                                       : maxSavedParseExceptions;
      }
      this.logParseExceptions = logParseExceptions == null
                                ? TuningConfig.DEFAULT_LOG_PARSE_EXCEPTIONS
                                : logParseExceptions;
    }

    @Override
    public IndexTuningConfig withBasePersistDirectory(File dir)
    {
      return new IndexTuningConfig(
          appendableIndexSpec,
          maxRowsInMemory,
          maxBytesInMemory,
          partitionsSpec,
          indexSpec,
          indexSpecForIntermediatePersists,
          maxPendingPersists,
          forceGuaranteedRollup,
          reportParseExceptions,
          pushTimeout,
          dir,
          segmentWriteOutMediumFactory,
          logParseExceptions,
          maxParseExceptions,
          maxSavedParseExceptions,
          maxColumnsToMerge
      );
    }

    public IndexTuningConfig withPartitionsSpec(PartitionsSpec partitionsSpec)
    {
      return new IndexTuningConfig(
          appendableIndexSpec,
          maxRowsInMemory,
          maxBytesInMemory,
          partitionsSpec,
          indexSpec,
          indexSpecForIntermediatePersists,
          maxPendingPersists,
          forceGuaranteedRollup,
          reportParseExceptions,
          pushTimeout,
          basePersistDirectory,
          segmentWriteOutMediumFactory,
          logParseExceptions,
          maxParseExceptions,
          maxSavedParseExceptions,
          maxColumnsToMerge
      );
    }

    @JsonProperty
    @Override
    public AppendableIndexSpec getAppendableIndexSpec()
    {
      return appendableIndexSpec;
    }

    @JsonProperty
    @Override
    public int getMaxRowsInMemory()
    {
      return maxRowsInMemory;
    }

    @JsonProperty
    @Override
    public long getMaxBytesInMemory()
    {
      return maxBytesInMemory;
    }

    @JsonProperty
    @Nullable
    @Override
    public PartitionsSpec getPartitionsSpec()
    {
      return partitionsSpec;
    }

    public PartitionsSpec getGivenOrDefaultPartitionsSpec()
    {
      if (partitionsSpec != null) {
        return partitionsSpec;
      }
      return forceGuaranteedRollup
             ? new HashedPartitionsSpec(null, null, null)
             : new DynamicPartitionsSpec(null, null);
    }

    @JsonProperty
    @Override
    public IndexSpec getIndexSpec()
    {
      return indexSpec;
    }

    @JsonProperty
    @Override
    public IndexSpec getIndexSpecForIntermediatePersists()
    {
      return indexSpecForIntermediatePersists;
    }

    @JsonProperty
    @Override
    public int getMaxPendingPersists()
    {
      return maxPendingPersists;
    }

    /**
     * Always returns true, doesn't affect the version being built.
     */
    @Deprecated
    @JsonProperty
    public boolean isBuildV9Directly()
    {
      return true;
    }

    @JsonProperty
    public boolean isForceGuaranteedRollup()
    {
      return forceGuaranteedRollup;
    }

    @JsonProperty
    @Override
    public boolean isReportParseExceptions()
    {
      return reportParseExceptions;
    }

    @JsonProperty
    public long getPushTimeout()
    {
      return pushTimeout;
    }

    @Nullable
    @Override
    @JsonProperty
    public SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory()
    {
      return segmentWriteOutMediumFactory;
    }

    @Override
    @JsonProperty
    public int getMaxColumnsToMerge()
    {
      return maxColumnsToMerge;
    }

    @JsonProperty
    public boolean isLogParseExceptions()
    {
      return logParseExceptions;
    }

    @JsonProperty
    public int getMaxParseExceptions()
    {
      return maxParseExceptions;
    }

    @JsonProperty
    public int getMaxSavedParseExceptions()
    {
      return maxSavedParseExceptions;
    }

    /**
     * Return the max number of rows per segment. This returns null if it's not specified in tuningConfig.
     * Deprecated in favor of {@link #getGivenOrDefaultPartitionsSpec()}.
     */
    @Nullable
    @Override
    @Deprecated
    @JsonProperty
    public Integer getMaxRowsPerSegment()
    {
      return partitionsSpec == null ? null : partitionsSpec.getMaxRowsPerSegment();
    }

    /**
     * Return the max number of total rows in appenderator. This returns null if it's not specified in tuningConfig.
     * Deprecated in favor of {@link #getGivenOrDefaultPartitionsSpec()}.
     */
    @Override
    @Nullable
    @Deprecated
    @JsonProperty
    public Long getMaxTotalRows()
    {
      return partitionsSpec instanceof DynamicPartitionsSpec
             ? ((DynamicPartitionsSpec) partitionsSpec).getMaxTotalRows()
             : null;
    }

    @Deprecated
    @Nullable
    @JsonProperty
    public Integer getNumShards()
    {
      return partitionsSpec instanceof HashedPartitionsSpec
             ? ((HashedPartitionsSpec) partitionsSpec).getNumShards()
             : null;
    }

    @Deprecated
    @JsonProperty
    public List<String> getPartitionDimensions()
    {
      return partitionsSpec instanceof HashedPartitionsSpec
             ? ((HashedPartitionsSpec) partitionsSpec).getPartitionDimensions()
             : Collections.emptyList();
    }

    @Override
    public File getBasePersistDirectory()
    {
      return basePersistDirectory;
    }

    @Override
    public Period getIntermediatePersistPeriod()
    {
      return new Period(Integer.MAX_VALUE); // intermediate persist doesn't make much sense for batch jobs
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      IndexTuningConfig that = (IndexTuningConfig) o;
      return Objects.equals(appendableIndexSpec, that.appendableIndexSpec) &&
             maxRowsInMemory == that.maxRowsInMemory &&
             maxBytesInMemory == that.maxBytesInMemory &&
             maxColumnsToMerge == that.maxColumnsToMerge &&
             maxPendingPersists == that.maxPendingPersists &&
             forceGuaranteedRollup == that.forceGuaranteedRollup &&
             reportParseExceptions == that.reportParseExceptions &&
             pushTimeout == that.pushTimeout &&
             logParseExceptions == that.logParseExceptions &&
             maxParseExceptions == that.maxParseExceptions &&
             maxSavedParseExceptions == that.maxSavedParseExceptions &&
             Objects.equals(partitionsSpec, that.partitionsSpec) &&
             Objects.equals(indexSpec, that.indexSpec) &&
             Objects.equals(indexSpecForIntermediatePersists, that.indexSpecForIntermediatePersists) &&
             Objects.equals(basePersistDirectory, that.basePersistDirectory) &&
             Objects.equals(segmentWriteOutMediumFactory, that.segmentWriteOutMediumFactory);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(
          appendableIndexSpec,
          maxRowsInMemory,
          maxBytesInMemory,
          maxColumnsToMerge,
          partitionsSpec,
          indexSpec,
          indexSpecForIntermediatePersists,
          basePersistDirectory,
          maxPendingPersists,
          forceGuaranteedRollup,
          reportParseExceptions,
          pushTimeout,
          logParseExceptions,
          maxParseExceptions,
          maxSavedParseExceptions,
          segmentWriteOutMediumFactory
      );
    }

    @Override
    public String toString()
    {
      return "IndexTuningConfig{" +
             "maxRowsInMemory=" + maxRowsInMemory +
             ", maxBytesInMemory=" + maxBytesInMemory +
             ", maxColumnsToMerge=" + maxColumnsToMerge +
             ", partitionsSpec=" + partitionsSpec +
             ", indexSpec=" + indexSpec +
             ", indexSpecForIntermediatePersists=" + indexSpecForIntermediatePersists +
             ", basePersistDirectory=" + basePersistDirectory +
             ", maxPendingPersists=" + maxPendingPersists +
             ", forceGuaranteedRollup=" + forceGuaranteedRollup +
             ", reportParseExceptions=" + reportParseExceptions +
             ", pushTimeout=" + pushTimeout +
             ", logParseExceptions=" + logParseExceptions +
             ", maxParseExceptions=" + maxParseExceptions +
             ", maxSavedParseExceptions=" + maxSavedParseExceptions +
             ", segmentWriteOutMediumFactory=" + segmentWriteOutMediumFactory +
             '}';
    }
  }
}
