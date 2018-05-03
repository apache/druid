/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.Rows;
import io.druid.hll.HyperLogLogCollector;
import io.druid.indexer.IngestionState;
import io.druid.indexer.TaskMetricsGetter;
import io.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import io.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import io.druid.indexing.common.IngestionStatsAndErrorsTaskReport;
import io.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskReport;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.firehose.IngestSegmentFirehoseFactory;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.JodaUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.DruidMetrics;
import io.druid.segment.IndexSpec;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.IOConfig;
import io.druid.segment.indexing.IngestionSpec;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.TuningConfig;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.FireDepartmentMetricsTaskMetricsGetter;
import io.druid.segment.realtime.RealtimeMetricsMonitor;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.AppenderatorConfig;
import io.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import io.druid.segment.realtime.appenderator.Appenderators;
import io.druid.segment.realtime.appenderator.BaseAppenderatorDriver;
import io.druid.segment.realtime.appenderator.BatchAppenderatorDriver;
import io.druid.segment.realtime.appenderator.SegmentAllocator;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import io.druid.segment.realtime.firehose.ChatHandler;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.segment.writeout.SegmentWriteOutMediumFactory;
import io.druid.server.security.Action;
import io.druid.server.security.AuthorizerMapper;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.ShardSpec;
import io.druid.utils.CircularBuffer;
import org.codehaus.plexus.util.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IndexTask extends AbstractTask implements ChatHandler
{
  private static final Logger log = new Logger(IndexTask.class);
  private static final HashFunction hashFunction = Hashing.murmur3_128();
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

  @JsonIgnore
  private final IndexIngestionSpec ingestionSchema;

  @JsonIgnore
  private IngestionState ingestionState;

  @JsonIgnore
  private final AuthorizerMapper authorizerMapper;

  @JsonIgnore
  private final Optional<ChatHandlerProvider> chatHandlerProvider;

  @JsonIgnore
  private FireDepartmentMetrics buildSegmentsFireDepartmentMetrics;

  @JsonIgnore
  private TaskMetricsGetter buildSegmentsMetricsGetter;

  @JsonIgnore
  private CircularBuffer<Throwable> buildSegmentsSavedParseExceptions;

  @JsonIgnore
  private FireDepartmentMetrics determinePartitionsFireDepartmentMetrics;

  @JsonIgnore
  private TaskMetricsGetter determinePartitionsMetricsGetter;

  @JsonIgnore
  private CircularBuffer<Throwable> determinePartitionsSavedParseExceptions;

  @JsonIgnore
  private String errorMsg;

  @JsonCreator
  public IndexTask(
      @JsonProperty("id") final String id,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("spec") final IndexIngestionSpec ingestionSchema,
      @JsonProperty("context") final Map<String, Object> context,
      @JacksonInject AuthorizerMapper authorizerMapper,
      @JacksonInject ChatHandlerProvider chatHandlerProvider
  )
  {
    this(
        id,
        makeGroupId(ingestionSchema),
        taskResource,
        ingestionSchema.dataSchema.getDataSource(),
        ingestionSchema,
        context,
        authorizerMapper,
        chatHandlerProvider
    );
  }

  IndexTask(
      String id,
      String groupId,
      TaskResource resource,
      String dataSource,
      IndexIngestionSpec ingestionSchema,
      Map<String, Object> context,
      AuthorizerMapper authorizerMapper,
      ChatHandlerProvider chatHandlerProvider
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
    this.authorizerMapper = authorizerMapper;
    this.chatHandlerProvider = Optional.fromNullable(chatHandlerProvider);
    if (ingestionSchema.getTuningConfig().getMaxSavedParseExceptions() > 0) {
      determinePartitionsSavedParseExceptions = new CircularBuffer<Throwable>(
          ingestionSchema.getTuningConfig().getMaxSavedParseExceptions()
      );

      buildSegmentsSavedParseExceptions = new CircularBuffer<Throwable>(
          ingestionSchema.getTuningConfig().getMaxSavedParseExceptions()
      );
    }
    this.ingestionState = IngestionState.NOT_STARTED;
  }

  @Override
  public int getPriority()
  {
    return getContextValue(Tasks.PRIORITY_KEY, Tasks.DEFAULT_BATCH_INDEX_TASK_PRIORITY);
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    final Optional<SortedSet<Interval>> intervals = ingestionSchema.getDataSchema()
                                                                   .getGranularitySpec()
                                                                   .bucketIntervals();

    if (intervals.isPresent()) {
      return isReady(taskActionClient, intervals.get());
    } else {
      return true;
    }
  }

  static boolean isReady(TaskActionClient actionClient, SortedSet<Interval> intervals) throws IOException
  {
    final List<TaskLock> locks = getTaskLocks(actionClient);
    if (locks.size() == 0) {
      try {
        Tasks.tryAcquireExclusiveLocks(actionClient, intervals);
      }
      catch (Exception e) {
        return false;
      }
    }
    return true;
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
    Map<String, List<String>> events = Maps.newHashMap();

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
          "determinePartitions",
          IndexTaskUtils.getMessagesFromSavedParseExceptions(determinePartitionsSavedParseExceptions)
      );
    }

    if (needsBuildSegments) {
      events.put(
          "buildSegments",
          IndexTaskUtils.getMessagesFromSavedParseExceptions(buildSegmentsSavedParseExceptions)
      );
    }

    return Response.ok(events).build();
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
    Map<String, Object> returnMap = Maps.newHashMap();
    Map<String, Object> totalsMap = Maps.newHashMap();

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
      if (determinePartitionsMetricsGetter != null) {
        totalsMap.put(
            "determinePartitions",
            determinePartitionsMetricsGetter.getTotalMetrics()
        );
      }
    }

    if (needsBuildSegments) {
      if (buildSegmentsMetricsGetter != null) {
        totalsMap.put(
            "buildSegments",
            buildSegmentsMetricsGetter.getTotalMetrics()
        );
      }
    }

    returnMap.put("totals", totalsMap);
    return Response.ok(returnMap).build();
  }

  @JsonProperty("spec")
  public IndexIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    try {
      if (chatHandlerProvider.isPresent()) {
        log.info("Found chat handler of class[%s]", chatHandlerProvider.get().getClass().getName());
        chatHandlerProvider.get().register(getId(), this, false);
      } else {
        log.warn("No chat handler detected");
      }

      final boolean determineIntervals = !ingestionSchema.getDataSchema()
                                                         .getGranularitySpec()
                                                         .bucketIntervals()
                                                         .isPresent();

      final FirehoseFactory firehoseFactory = ingestionSchema.getIOConfig().getFirehoseFactory();

      if (firehoseFactory instanceof IngestSegmentFirehoseFactory) {
        // pass toolbox to Firehose
        ((IngestSegmentFirehoseFactory) firehoseFactory).setTaskToolbox(toolbox);
      }

      final File firehoseTempDir = toolbox.getFirehoseTemporaryDir();
      // Firehose temporary directory is automatically removed when this IndexTask completes.
      FileUtils.forceMkdir(firehoseTempDir);

      ingestionState = IngestionState.DETERMINE_PARTITIONS;
      final ShardSpecs shardSpecs = determineShardSpecs(toolbox, firehoseFactory, firehoseTempDir);
      final DataSchema dataSchema;
      final Map<Interval, String> versions;
      if (determineIntervals) {
        final SortedSet<Interval> intervals = new TreeSet<>(Comparators.intervalsByStartThenEnd());
        intervals.addAll(shardSpecs.getIntervals());
        final Map<Interval, TaskLock> locks = Tasks.tryAcquireExclusiveLocks(
            toolbox.getTaskActionClient(),
            intervals
        );
        versions = locks.entrySet().stream()
                        .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().getVersion()));

        dataSchema = ingestionSchema.getDataSchema().withGranularitySpec(
            ingestionSchema.getDataSchema()
                           .getGranularitySpec()
                           .withIntervals(
                               JodaUtils.condenseIntervals(
                                   shardSpecs.getIntervals()
                               )
                           )
        );
      } else {
        versions = getTaskLocks(toolbox.getTaskActionClient())
            .stream()
            .collect(Collectors.toMap(TaskLock::getInterval, TaskLock::getVersion));
        dataSchema = ingestionSchema.getDataSchema();
      }

      ingestionState = IngestionState.BUILD_SEGMENTS;
      return generateAndPublishSegments(toolbox, dataSchema, shardSpecs, versions, firehoseFactory, firehoseTempDir);
    }
    catch (Exception e) {
      log.error(e, "Encountered exception in %s.", ingestionState);
      errorMsg = Throwables.getStackTraceAsString(e);
      toolbox.getTaskReportFileWriter().write(getTaskCompletionReports());
      return TaskStatus.failure(
          getId(),
          errorMsg
      );
    }

    finally {
      if (chatHandlerProvider.isPresent()) {
        chatHandlerProvider.get().unregister(getId());
      }
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
    Map<String, Object> unparseableEventsMap = Maps.newHashMap();
    List<String> determinePartitionsParseExceptionMessages = IndexTaskUtils.getMessagesFromSavedParseExceptions(
        determinePartitionsSavedParseExceptions);
    List<String> buildSegmentsParseExceptionMessages = IndexTaskUtils.getMessagesFromSavedParseExceptions(
        buildSegmentsSavedParseExceptions);

    if (determinePartitionsParseExceptionMessages != null || buildSegmentsParseExceptionMessages != null) {
      unparseableEventsMap.put("determinePartitions", determinePartitionsParseExceptionMessages);
      unparseableEventsMap.put("buildSegments", buildSegmentsParseExceptionMessages);
    }

    return unparseableEventsMap;
  }

  private Map<String, Object> getTaskCompletionRowStats()
  {
    Map<String, Object> metrics = Maps.newHashMap();
    if (determinePartitionsMetricsGetter != null) {
      metrics.put(
          "determinePartitions",
          determinePartitionsMetricsGetter.getTotalMetrics()
      );
    }
    if (buildSegmentsMetricsGetter != null) {
      metrics.put(
          "buildSegments",
          buildSegmentsMetricsGetter.getTotalMetrics()
      );
    }
    return metrics;
  }

  private static String findVersion(Map<Interval, String> versions, Interval interval)
  {
    return versions.entrySet().stream()
                   .filter(entry -> entry.getKey().contains(interval))
                   .map(Entry::getValue)
                   .findFirst()
                   .orElseThrow(() -> new ISE("Cannot find a version for interval[%s]", interval));
  }

  private static boolean isGuaranteedRollup(IndexIOConfig ioConfig, IndexTuningConfig tuningConfig)
  {
    Preconditions.checkState(
        !tuningConfig.isForceGuaranteedRollup() || !ioConfig.isAppendToExisting(),
        "Perfect rollup cannot be guaranteed when appending to existing dataSources"
    );
    return tuningConfig.isForceGuaranteedRollup();
  }

  private static boolean isExtendableShardSpecs(IndexIOConfig ioConfig, IndexTuningConfig tuningConfig)
  {
    return tuningConfig.isForceExtendableShardSpecs() || ioConfig.isAppendToExisting();
  }

  /**
   * Determines intervals and shardSpecs for input data.  This method first checks that it must determine intervals and
   * shardSpecs by itself.  Intervals must be determined if they are not specified in {@link GranularitySpec}.
   * ShardSpecs must be determined if the perfect rollup must be guaranteed even though the number of shards is not
   * specified in {@link IndexTuningConfig}.
   * <P/>
   * If both intervals and shardSpecs don't have to be determined, this method simply returns {@link ShardSpecs} for the
   * given intervals.  Here, if {@link IndexTuningConfig#numShards} is not specified, {@link NumberedShardSpec} is used.
   * <p/>
   * If one of intervals or shardSpecs need to be determined, this method reads the entire input for determining one of
   * them.  If the perfect rollup must be guaranteed, {@link HashBasedNumberedShardSpec} is used for hash partitioning
   * of input data.  In the future we may want to also support single-dimension partitioning.
   *
   * @return generated {@link ShardSpecs} representing a map of intervals and corresponding shard specs
   */
  private ShardSpecs determineShardSpecs(
      final TaskToolbox toolbox,
      final FirehoseFactory firehoseFactory,
      final File firehoseTempDir
  ) throws IOException
  {
    final ObjectMapper jsonMapper = toolbox.getObjectMapper();
    final IndexTuningConfig tuningConfig = ingestionSchema.getTuningConfig();
    final IndexIOConfig ioConfig = ingestionSchema.getIOConfig();

    final GranularitySpec granularitySpec = ingestionSchema.getDataSchema().getGranularitySpec();

    // Must determine intervals if unknown, since we acquire all locks before processing any data.
    final boolean determineIntervals = !granularitySpec.bucketIntervals().isPresent();

    // Must determine partitions if rollup is guaranteed and the user didn't provide a specific value.
    final boolean determineNumPartitions = tuningConfig.getNumShards() == null
                                           && isGuaranteedRollup(ioConfig, tuningConfig);

    // if we were given number of shards per interval and the intervals, we don't need to scan the data
    if (!determineNumPartitions && !determineIntervals) {
      log.info("Skipping determine partition scan");
      return createShardSpecWithoutInputScan(
          jsonMapper,
          granularitySpec,
          ioConfig,
          tuningConfig
      );
    } else {
      // determine intervals containing data and prime HLL collectors
      return createShardSpecsFromInput(
          jsonMapper,
          ingestionSchema,
          firehoseFactory,
          firehoseTempDir,
          granularitySpec,
          tuningConfig,
          determineIntervals,
          determineNumPartitions
      );
    }
  }

  private static ShardSpecs createShardSpecWithoutInputScan(
      ObjectMapper jsonMapper,
      GranularitySpec granularitySpec,
      IndexIOConfig ioConfig,
      IndexTuningConfig tuningConfig
  )
  {
    final Map<Interval, List<ShardSpec>> shardSpecs = new HashMap<>();
    final SortedSet<Interval> intervals = granularitySpec.bucketIntervals().get();

    if (isGuaranteedRollup(ioConfig, tuningConfig)) {
      // Overwrite mode, guaranteed rollup: shardSpecs must be known in advance.
      final int numShards = tuningConfig.getNumShards() == null ? 1 : tuningConfig.getNumShards();
      final BiFunction<Integer, Integer, ShardSpec> shardSpecCreateFn = getShardSpecCreateFunction(
          numShards,
          jsonMapper
      );

      for (Interval interval : intervals) {
        final List<ShardSpec> intervalShardSpecs = IntStream.range(0, numShards)
                                                            .mapToObj(
                                                                shardId -> shardSpecCreateFn.apply(shardId, numShards)
                                                            )
                                                            .collect(Collectors.toList());
        shardSpecs.put(interval, intervalShardSpecs);
      }
    } else {
      for (Interval interval : intervals) {
        shardSpecs.put(interval, ImmutableList.of());
      }
    }

    return new ShardSpecs(shardSpecs);
  }

  private ShardSpecs createShardSpecsFromInput(
      ObjectMapper jsonMapper,
      IndexIngestionSpec ingestionSchema,
      FirehoseFactory firehoseFactory,
      File firehoseTempDir,
      GranularitySpec granularitySpec,
      IndexTuningConfig tuningConfig,
      boolean determineIntervals,
      boolean determineNumPartitions
  ) throws IOException
  {
    log.info("Determining intervals and shardSpecs");
    long determineShardSpecsStartMillis = System.currentTimeMillis();

    final Map<Interval, Optional<HyperLogLogCollector>> hllCollectors = collectIntervalsAndShardSpecs(
        jsonMapper,
        ingestionSchema,
        firehoseFactory,
        firehoseTempDir,
        granularitySpec,
        determineIntervals,
        determineNumPartitions
    );

    final Map<Interval, List<ShardSpec>> intervalToShardSpecs = new HashMap<>();
    final int defaultNumShards = tuningConfig.getNumShards() == null ? 1 : tuningConfig.getNumShards();
    for (final Map.Entry<Interval, Optional<HyperLogLogCollector>> entry : hllCollectors.entrySet()) {
      final Interval interval = entry.getKey();
      final HyperLogLogCollector collector = entry.getValue().orNull();

      final int numShards;
      if (determineNumPartitions) {
        final long numRows = collector.estimateCardinalityRound();
        numShards = (int) Math.ceil((double) numRows / tuningConfig.getTargetPartitionSize());
        log.info("Estimated [%,d] rows of data for interval [%s], creating [%,d] shards", numRows, interval, numShards);
      } else {
        numShards = defaultNumShards;
        log.info("Creating [%,d] shards for interval [%s]", numShards, interval);
      }

      if (isGuaranteedRollup(ingestionSchema.getIOConfig(), ingestionSchema.getTuningConfig())) {
        // Overwrite mode, guaranteed rollup: shardSpecs must be known in advance.
        final BiFunction<Integer, Integer, ShardSpec> shardSpecCreateFn = getShardSpecCreateFunction(
            numShards,
            jsonMapper
        );

        final List<ShardSpec> intervalShardSpecs = IntStream.range(0, numShards)
                                                            .mapToObj(
                                                                shardId -> shardSpecCreateFn.apply(shardId, numShards)
                                                            ).collect(Collectors.toList());

        intervalToShardSpecs.put(interval, intervalShardSpecs);
      } else {
        intervalToShardSpecs.put(interval, ImmutableList.of());
      }
    }
    log.info("Found intervals and shardSpecs in %,dms", System.currentTimeMillis() - determineShardSpecsStartMillis);

    return new ShardSpecs(intervalToShardSpecs);
  }

  private Map<Interval, Optional<HyperLogLogCollector>> collectIntervalsAndShardSpecs(
      ObjectMapper jsonMapper,
      IndexIngestionSpec ingestionSchema,
      FirehoseFactory firehoseFactory,
      File firehoseTempDir,
      GranularitySpec granularitySpec,
      boolean determineIntervals,
      boolean determineNumPartitions
  ) throws IOException
  {
    determinePartitionsFireDepartmentMetrics = new FireDepartmentMetrics();
    determinePartitionsMetricsGetter = new FireDepartmentMetricsTaskMetricsGetter(
        determinePartitionsFireDepartmentMetrics
    );

    final Map<Interval, Optional<HyperLogLogCollector>> hllCollectors = new TreeMap<>(
        Comparators.intervalsByStartThenEnd()
    );
    int thrownAway = 0;
    int unparseable = 0;
    final Granularity queryGranularity = granularitySpec.getQueryGranularity();

    try (
        final Firehose firehose = firehoseFactory.connect(ingestionSchema.getDataSchema().getParser(), firehoseTempDir)
    ) {

      while (firehose.hasMore()) {
        try {
          final InputRow inputRow = firehose.nextRow();

          // The null inputRow means the caller must skip this row.
          if (inputRow == null) {
            determinePartitionsFireDepartmentMetrics.incrementThrownAway();
            continue;
          }

          final Interval interval;
          if (determineIntervals) {
            interval = granularitySpec.getSegmentGranularity().bucket(inputRow.getTimestamp());
          } else {
            if (!Intervals.ETERNITY.contains(inputRow.getTimestamp())) {
              final String errorMsg = StringUtils.format(
                  "Encountered row with timestamp that cannot be represented as a long: [%s]",
                  inputRow
              );
              throw new ParseException(errorMsg);
            }

            final Optional<Interval> optInterval = granularitySpec.bucketInterval(inputRow.getTimestamp());
            if (!optInterval.isPresent()) {
              determinePartitionsFireDepartmentMetrics.incrementThrownAway();
              continue;
            }
            interval = optInterval.get();
          }

          if (determineNumPartitions) {
            if (!hllCollectors.containsKey(interval)) {
              hllCollectors.put(interval, Optional.of(HyperLogLogCollector.makeLatestCollector()));
            }

            List<Object> groupKey = Rows.toGroupKey(
                queryGranularity.bucketStart(inputRow.getTimestamp()).getMillis(),
                inputRow
            );
            hllCollectors.get(interval).get()
                         .add(hashFunction.hashBytes(jsonMapper.writeValueAsBytes(groupKey)).asBytes());
          } else {
            // we don't need to determine partitions but we still need to determine intervals, so add an Optional.absent()
            // for the interval and don't instantiate a HLL collector
            if (!hllCollectors.containsKey(interval)) {
              hllCollectors.put(interval, Optional.absent());
            }
          }
          determinePartitionsFireDepartmentMetrics.incrementProcessed();
        }
        catch (ParseException e) {
          if (ingestionSchema.getTuningConfig().isLogParseExceptions()) {
            log.error(e, "Encountered parse exception: ");
          }

          if (determinePartitionsSavedParseExceptions != null) {
            determinePartitionsSavedParseExceptions.add(e);
          }

          determinePartitionsFireDepartmentMetrics.incrementUnparseable();
          if (determinePartitionsFireDepartmentMetrics.unparseable() > ingestionSchema.getTuningConfig()
                                                                                       .getMaxParseExceptions()) {
            throw new RuntimeException("Max parse exceptions exceeded, terminating task...");
          }
        }
      }
    }

    // These metrics are reported in generateAndPublishSegments()
    if (thrownAway > 0) {
      log.warn("Unable to find a matching interval for [%,d] events", thrownAway);
    }
    if (unparseable > 0) {
      log.warn("Unable to parse [%,d] events", unparseable);
    }
    return hllCollectors;
  }

  private static BiFunction<Integer, Integer, ShardSpec> getShardSpecCreateFunction(
      Integer numShards,
      ObjectMapper jsonMapper
  )
  {
    Preconditions.checkNotNull(numShards, "numShards");

    if (numShards == 1) {
      return (shardId, totalNumShards) -> NoneShardSpec.instance();
    } else {
      return (shardId, totalNumShards) -> new HashBasedNumberedShardSpec(shardId, totalNumShards, null, jsonMapper);
    }
  }

  /**
   * This method reads input data row by row and adds the read row to a proper segment using {@link BaseAppenderatorDriver}.
   * If there is no segment for the row, a new one is created.  Segments can be published in the middle of reading inputs
   * if one of below conditions are satisfied.
   *
   * <ul>
   * <li>
   * If the number of rows in a segment exceeds {@link IndexTuningConfig#targetPartitionSize}
   * </li>
   * <li>
   * If the number of rows added to {@link BaseAppenderatorDriver} so far exceeds {@link IndexTuningConfig#maxTotalRows}
   * </li>
   * </ul>
   *
   * At the end of this method, all the remaining segments are published.
   *
   * @return true if generated segments are successfully published, otherwise false
   */
  private TaskStatus generateAndPublishSegments(
      final TaskToolbox toolbox,
      final DataSchema dataSchema,
      final ShardSpecs shardSpecs,
      Map<Interval, String> versions,
      final FirehoseFactory firehoseFactory,
      final File firehoseTempDir
  ) throws IOException, InterruptedException
  {
    final GranularitySpec granularitySpec = dataSchema.getGranularitySpec();
    final FireDepartment fireDepartmentForMetrics = new FireDepartment(
        dataSchema, new RealtimeIOConfig(null, null, null), null
    );
    buildSegmentsFireDepartmentMetrics = fireDepartmentForMetrics.getMetrics();
    buildSegmentsMetricsGetter = new FireDepartmentMetricsTaskMetricsGetter(buildSegmentsFireDepartmentMetrics);

    if (toolbox.getMonitorScheduler() != null) {
      toolbox.getMonitorScheduler().addMonitor(
          new RealtimeMetricsMonitor(
              ImmutableList.of(fireDepartmentForMetrics),
              ImmutableMap.of(DruidMetrics.TASK_ID, new String[]{getId()})
          )
      );
    }

    final IndexIOConfig ioConfig = ingestionSchema.getIOConfig();
    final IndexTuningConfig tuningConfig = ingestionSchema.tuningConfig;
    final long pushTimeout = tuningConfig.getPushTimeout();
    final boolean isGuaranteedRollup = isGuaranteedRollup(ioConfig, tuningConfig);

    final SegmentAllocator segmentAllocator;
    if (isGuaranteedRollup) {
      // Overwrite mode, guaranteed rollup: segments are all known in advance and there is one per sequenceName.
      final Map<String, SegmentIdentifier> lookup = new HashMap<>();

      for (Map.Entry<Interval, List<ShardSpec>> entry : shardSpecs.getMap().entrySet()) {
        for (ShardSpec shardSpec : entry.getValue()) {
          final ShardSpec shardSpecForPublishing;

          if (isExtendableShardSpecs(ioConfig, tuningConfig)) {
            shardSpecForPublishing = new NumberedShardSpec(
                shardSpec.getPartitionNum(),
                entry.getValue().size()
            );
          } else {
            shardSpecForPublishing = shardSpec;
          }

          final String version = findVersion(versions, entry.getKey());
          lookup.put(
              Appenderators.getSequenceName(entry.getKey(), version, shardSpec),
              new SegmentIdentifier(getDataSource(), entry.getKey(), version, shardSpecForPublishing)
          );
        }
      }

      segmentAllocator = (row, sequenceName, previousSegmentId, skipSegmentLineageCheck) -> lookup.get(sequenceName);
    } else if (ioConfig.isAppendToExisting()) {
      // Append mode: Allocate segments as needed using Overlord APIs.
      segmentAllocator = new ActionBasedSegmentAllocator(toolbox.getTaskActionClient(), dataSchema);
    } else {
      // Overwrite mode, non-guaranteed rollup: We can make up our own segment ids but we don't know them in advance.
      final Map<Interval, AtomicInteger> counters = new HashMap<>();

      segmentAllocator = (row, sequenceName, previousSegmentId, skipSegmentLineageCheck) -> {
        final DateTime timestamp = row.getTimestamp();
        Optional<Interval> maybeInterval = granularitySpec.bucketInterval(timestamp);
        if (!maybeInterval.isPresent()) {
          throw new ISE("Could not find interval for timestamp [%s]", timestamp);
        }

        final Interval interval = maybeInterval.get();
        if (!shardSpecs.getMap().containsKey(interval)) {
          throw new ISE("Could not find shardSpec for interval[%s]", interval);
        }

        final int partitionNum = counters.computeIfAbsent(interval, x -> new AtomicInteger()).getAndIncrement();
        return new SegmentIdentifier(
            getDataSource(),
            interval,
            findVersion(versions, interval),
            new NumberedShardSpec(partitionNum, 0)
        );
      };
    }

    final TransactionalSegmentPublisher publisher = (segments, commitMetadata) -> {
      final SegmentTransactionalInsertAction action = new SegmentTransactionalInsertAction(segments);
      return toolbox.getTaskActionClient().submit(action).isSuccess();
    };

    try (
        final Appenderator appenderator = newAppenderator(buildSegmentsFireDepartmentMetrics, toolbox, dataSchema, tuningConfig);
        final BatchAppenderatorDriver driver = newDriver(appenderator, toolbox, segmentAllocator);
        final Firehose firehose = firehoseFactory.connect(dataSchema.getParser(), firehoseTempDir)
    ) {
      driver.startJob();

      while (firehose.hasMore()) {
        try {
          final InputRow inputRow = firehose.nextRow();

          if (inputRow == null) {
            buildSegmentsFireDepartmentMetrics.incrementThrownAway();
            continue;
          }

          if (!Intervals.ETERNITY.contains(inputRow.getTimestamp())) {
            final String errorMsg = StringUtils.format(
                "Encountered row with timestamp that cannot be represented as a long: [%s]",
                inputRow
            );
            throw new ParseException(errorMsg);
          }

          final Optional<Interval> optInterval = granularitySpec.bucketInterval(inputRow.getTimestamp());
          if (!optInterval.isPresent()) {
            buildSegmentsFireDepartmentMetrics.incrementThrownAway();
            continue;
          }

          final String sequenceName;

          if (isGuaranteedRollup) {
            // Sequence name is based solely on the shardSpec, and there will only be one segment per sequence.
            final Interval interval = optInterval.get();
            final ShardSpec shardSpec = shardSpecs.getShardSpec(interval, inputRow);
            sequenceName = Appenderators.getSequenceName(interval, findVersion(versions, interval), shardSpec);
          } else {
            // Segments are created as needed, using a single sequence name. They may be allocated from the overlord
            // (in append mode) or may be created on our own authority (in overwrite mode).
            sequenceName = getId();
          }
          final AppenderatorDriverAddResult addResult = driver.add(inputRow, sequenceName);

          if (addResult.isOk()) {
            // incremental segment publishment is allowed only when rollup don't have to be perfect.
            if (!isGuaranteedRollup &&
                (exceedMaxRowsInSegment(addResult.getNumRowsInSegment(), tuningConfig) ||
                 exceedMaxRowsInAppenderator(addResult.getTotalNumRowsInAppenderator(), tuningConfig))) {
              // There can be some segments waiting for being published even though any rows won't be added to them.
              // If those segments are not published here, the available space in appenderator will be kept to be small
              // which makes the size of segments smaller.
              final SegmentsAndMetadata pushed = driver.pushAllAndClear(pushTimeout);
              log.info("Pushed segments[%s]", pushed.getSegments());
            }
          } else {
            throw new ISE("Failed to add a row with timestamp[%s]", inputRow.getTimestamp());
          }

          if (addResult.getParseException() != null) {
            handleParseException(addResult.getParseException());
          } else {
            buildSegmentsFireDepartmentMetrics.incrementProcessed();
          }
        }
        catch (ParseException e) {
          handleParseException(e);
        }
      }

      final SegmentsAndMetadata pushed = driver.pushAllAndClear(pushTimeout);
      log.info("Pushed segments[%s]", pushed.getSegments());

      final SegmentsAndMetadata published = awaitPublish(
          driver.publishAll(publisher),
          pushTimeout
      );

      ingestionState = IngestionState.COMPLETED;
      if (published == null) {
        log.error("Failed to publish segments, aborting!");
        errorMsg = "Failed to publish segments.";
        toolbox.getTaskReportFileWriter().write(getTaskCompletionReports());
        return TaskStatus.failure(
            getId(),
            errorMsg
        );
      } else {
        log.info(
            "Processed[%,d] events, unparseable[%,d], thrownAway[%,d].",
            buildSegmentsFireDepartmentMetrics.processed(),
            buildSegmentsFireDepartmentMetrics.unparseable(),
            buildSegmentsFireDepartmentMetrics.thrownAway()
        );
        log.info(
            "Published segments[%s]", Joiner.on(", ").join(
                Iterables.transform(
                    published.getSegments(),
                    DataSegment::getIdentifier
                )
            )
        );

        toolbox.getTaskReportFileWriter().write(getTaskCompletionReports());
        return TaskStatus.success(getId());
      }
    }
    catch (TimeoutException | ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  private void handleParseException(ParseException e)
  {
    if (e.isFromPartiallyValidRow()) {
      buildSegmentsFireDepartmentMetrics.incrementProcessedWithErrors();
    } else {
      buildSegmentsFireDepartmentMetrics.incrementUnparseable();
    }

    if (ingestionSchema.tuningConfig.isLogParseExceptions()) {
      log.error(e, "Encountered parse exception:");
    }

    if (buildSegmentsSavedParseExceptions != null) {
      buildSegmentsSavedParseExceptions.add(e);
    }

    if (buildSegmentsFireDepartmentMetrics.unparseable()
        + buildSegmentsFireDepartmentMetrics.processedWithErrors() > ingestionSchema.tuningConfig.getMaxParseExceptions()) {
      log.error("Max parse exceptions exceeded, terminating task...");
      throw new RuntimeException("Max parse exceptions exceeded, terminating task...", e);
    }
  }

  private static boolean exceedMaxRowsInSegment(int numRowsInSegment, IndexTuningConfig indexTuningConfig)
  {
    // maxRowsInSegment should be null if numShards is set in indexTuningConfig
    final Integer maxRowsInSegment = indexTuningConfig.getTargetPartitionSize();
    return maxRowsInSegment != null && maxRowsInSegment <= numRowsInSegment;
  }

  private static boolean exceedMaxRowsInAppenderator(long numRowsInAppenderator, IndexTuningConfig indexTuningConfig)
  {
    // maxRowsInAppenderator should be null if numShards is set in indexTuningConfig
    final Long maxRowsInAppenderator = indexTuningConfig.getMaxTotalRows();
    return maxRowsInAppenderator != null && maxRowsInAppenderator <= numRowsInAppenderator;
  }

  private static SegmentsAndMetadata awaitPublish(
      ListenableFuture<SegmentsAndMetadata> publishFuture,
      long publishTimeout
  ) throws ExecutionException, InterruptedException, TimeoutException
  {
    if (publishTimeout == 0) {
      return publishFuture.get();
    } else {
      return publishFuture.get(publishTimeout, TimeUnit.MILLISECONDS);
    }
  }

  private static Appenderator newAppenderator(
      FireDepartmentMetrics metrics,
      TaskToolbox toolbox,
      DataSchema dataSchema,
      IndexTuningConfig tuningConfig
  )
  {
    return Appenderators.createOffline(
        dataSchema,
        tuningConfig.withBasePersistDirectory(toolbox.getPersistDir()),
        metrics,
        toolbox.getSegmentPusher(),
        toolbox.getObjectMapper(),
        toolbox.getIndexIO(),
        toolbox.getIndexMergerV9()
    );
  }

  private static BatchAppenderatorDriver newDriver(
      final Appenderator appenderator,
      final TaskToolbox toolbox,
      final SegmentAllocator segmentAllocator
  )
  {
    return new BatchAppenderatorDriver(
        appenderator,
        segmentAllocator,
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
        toolbox.getDataSegmentKiller()
    );
  }

  /**
   * This class represents a map of (Interval, ShardSpec) and is used for easy shardSpec generation.
   */
  static class ShardSpecs
  {
    private final Map<Interval, List<ShardSpec>> map;

    ShardSpecs(final Map<Interval, List<ShardSpec>> map)
    {
      this.map = map;
    }

    /**
     * Return the underlying map.
     *
     * @return a map of intervals to shardSpecs
     */
    Map<Interval, List<ShardSpec>> getMap()
    {
      return map;
    }

    Set<Interval> getIntervals()
    {
      return map.keySet();
    }

    /**
     * Return a shardSpec for the given interval and input row.
     *
     * @param interval interval for shardSpec
     * @param row      input row
     *
     * @return a shardSpec
     */
    ShardSpec getShardSpec(Interval interval, InputRow row)
    {
      final List<ShardSpec> shardSpecs = map.get(interval);
      if (shardSpecs == null || shardSpecs.isEmpty()) {
        throw new ISE("Failed to get shardSpec for interval[%s]", interval);
      }
      return shardSpecs.get(0).getLookup(shardSpecs).getShardSpec(row.getTimestampFromEpoch(), row);
    }
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
  public static class IndexIOConfig implements IOConfig
  {
    private static final boolean DEFAULT_APPEND_TO_EXISTING = false;

    private final FirehoseFactory firehoseFactory;
    private final boolean appendToExisting;

    @JsonCreator
    public IndexIOConfig(
        @JsonProperty("firehose") FirehoseFactory firehoseFactory,
        @JsonProperty("appendToExisting") @Nullable Boolean appendToExisting
    )
    {
      this.firehoseFactory = firehoseFactory;
      this.appendToExisting = appendToExisting == null ? DEFAULT_APPEND_TO_EXISTING : appendToExisting;
    }

    @JsonProperty("firehose")
    public FirehoseFactory getFirehoseFactory()
    {
      return firehoseFactory;
    }

    @JsonProperty("appendToExisting")
    public boolean isAppendToExisting()
    {
      return appendToExisting;
    }
  }

  @JsonTypeName("index")
  public static class IndexTuningConfig implements TuningConfig, AppenderatorConfig
  {
    private static final int DEFAULT_MAX_TOTAL_ROWS = 20_000_000;
    private static final IndexSpec DEFAULT_INDEX_SPEC = new IndexSpec();
    private static final int DEFAULT_MAX_PENDING_PERSISTS = 0;
    private static final boolean DEFAULT_FORCE_EXTENDABLE_SHARD_SPECS = false;
    private static final boolean DEFAULT_GUARANTEE_ROLLUP = false;
    private static final boolean DEFAULT_REPORT_PARSE_EXCEPTIONS = false;
    private static final long DEFAULT_PUSH_TIMEOUT = 0;

    static final int DEFAULT_TARGET_PARTITION_SIZE = 5000000;

    private final Integer targetPartitionSize;
    private final int maxRowsInMemory;
    private final long maxBytesInMemory;
    private final Long maxTotalRows;
    private final Integer numShards;
    private final IndexSpec indexSpec;
    private final File basePersistDirectory;
    private final int maxPendingPersists;

    /**
     * This flag is to force to always use an extendableShardSpec (like {@link NumberedShardSpec} even if
     * {@link #forceGuaranteedRollup} is set.
     */
    private final boolean forceExtendableShardSpecs;

    /**
     * This flag is to force _perfect rollup mode_. {@link IndexTask} will scan the whole input data twice to 1) figure
     * out proper shard specs for each segment and 2) generate segments. Note that perfect rollup mode basically assumes
     * that no more data will be appended in the future. As a result, in perfect rollup mode, {@link NoneShardSpec} and
     * {@link HashBasedNumberedShardSpec} are used for a single shard and two or shards, respectively.
     */
    private final boolean forceGuaranteedRollup;
    private final boolean reportParseExceptions;
    private final long pushTimeout;
    private final boolean logParseExceptions;
    private final int maxParseExceptions;
    private final int maxSavedParseExceptions;

    @Nullable
    private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;

    @JsonCreator
    public IndexTuningConfig(
        @JsonProperty("targetPartitionSize") @Nullable Integer targetPartitionSize,
        @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
        @JsonProperty("maxBytesInMemory") @Nullable Long maxBytesInMemory,
        @JsonProperty("maxTotalRows") @Nullable Long maxTotalRows,
        @JsonProperty("rowFlushBoundary") @Nullable Integer rowFlushBoundary_forBackCompatibility, // DEPRECATED
        @JsonProperty("numShards") @Nullable Integer numShards,
        @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
        @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
        // This parameter is left for compatibility when reading existing JSONs, to be removed in Druid 0.12.
        @JsonProperty("buildV9Directly") @Nullable Boolean buildV9Directly,
        @JsonProperty("forceExtendableShardSpecs") @Nullable Boolean forceExtendableShardSpecs,
        @JsonProperty("forceGuaranteedRollup") @Nullable Boolean forceGuaranteedRollup,
        @Deprecated @JsonProperty("reportParseExceptions") @Nullable Boolean reportParseExceptions,
        @JsonProperty("publishTimeout") @Nullable Long publishTimeout, // deprecated
        @JsonProperty("pushTimeout") @Nullable Long pushTimeout,
        @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
        @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
        @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
        @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions
    )
    {
      this(
          targetPartitionSize,
          maxRowsInMemory != null ? maxRowsInMemory : rowFlushBoundary_forBackCompatibility,
          maxBytesInMemory != null ? maxBytesInMemory : 0,
          maxTotalRows,
          numShards,
          indexSpec,
          maxPendingPersists,
          forceExtendableShardSpecs,
          forceGuaranteedRollup,
          reportParseExceptions,
          pushTimeout != null ? pushTimeout : publishTimeout,
          null,
          segmentWriteOutMediumFactory,
          logParseExceptions,
          maxParseExceptions,
          maxSavedParseExceptions
      );
    }

    private IndexTuningConfig()
    {
      this(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
    }

    private IndexTuningConfig(
        @Nullable Integer targetPartitionSize,
        @Nullable Integer maxRowsInMemory,
        @Nullable Long maxBytesInMemory,
        @Nullable Long maxTotalRows,
        @Nullable Integer numShards,
        @Nullable IndexSpec indexSpec,
        @Nullable Integer maxPendingPersists,
        @Nullable Boolean forceExtendableShardSpecs,
        @Nullable Boolean forceGuaranteedRollup,
        @Nullable Boolean reportParseExceptions,
        @Nullable Long pushTimeout,
        @Nullable File basePersistDirectory,
        @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
        @Nullable Boolean logParseExceptions,
        @Nullable Integer maxParseExceptions,
        @Nullable Integer maxSavedParseExceptions
    )
    {
      Preconditions.checkArgument(
          targetPartitionSize == null || targetPartitionSize.equals(-1) || numShards == null || numShards.equals(-1),
          "targetPartitionSize and numShards cannot both be set"
      );

      this.targetPartitionSize = initializeTargetPartitionSize(numShards, targetPartitionSize);
      this.maxRowsInMemory = maxRowsInMemory == null ? TuningConfig.DEFAULT_MAX_ROWS_IN_MEMORY : maxRowsInMemory;
      // initializing this to 0, it will be lazily initialized to a value
      // @see server.src.main.java.io.druid.segment.indexing.TuningConfigs#getMaxBytesInMemoryOrDefault(long)
      this.maxBytesInMemory = maxBytesInMemory == null ? 0 : maxBytesInMemory;
      this.maxTotalRows = initializeMaxTotalRows(numShards, maxTotalRows);
      this.numShards = numShards == null || numShards.equals(-1) ? null : numShards;
      this.indexSpec = indexSpec == null ? DEFAULT_INDEX_SPEC : indexSpec;
      this.maxPendingPersists = maxPendingPersists == null ? DEFAULT_MAX_PENDING_PERSISTS : maxPendingPersists;
      this.forceExtendableShardSpecs = forceExtendableShardSpecs == null
                                       ? DEFAULT_FORCE_EXTENDABLE_SHARD_SPECS
                                       : forceExtendableShardSpecs;
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
        this.maxParseExceptions = maxParseExceptions == null ? TuningConfig.DEFAULT_MAX_PARSE_EXCEPTIONS : maxParseExceptions;
        this.maxSavedParseExceptions = maxSavedParseExceptions == null
                                        ? TuningConfig.DEFAULT_MAX_SAVED_PARSE_EXCEPTIONS
                                        : maxSavedParseExceptions;
      }
      this.logParseExceptions = logParseExceptions == null ? TuningConfig.DEFAULT_LOG_PARSE_EXCEPTIONS : logParseExceptions;
    }

    private static Integer initializeTargetPartitionSize(Integer numShards, Integer targetPartitionSize)
    {
      if (numShards == null || numShards == -1) {
        return targetPartitionSize == null || targetPartitionSize.equals(-1)
               ? DEFAULT_TARGET_PARTITION_SIZE
               : targetPartitionSize;
      } else {
        return null;
      }
    }

    private static Long initializeMaxTotalRows(Integer numShards, Long maxTotalRows)
    {
      if (numShards == null || numShards == -1) {
        return maxTotalRows == null ? DEFAULT_MAX_TOTAL_ROWS : maxTotalRows;
      } else {
        return null;
      }
    }

    public IndexTuningConfig withBasePersistDirectory(File dir)
    {
      return new IndexTuningConfig(
          targetPartitionSize,
          maxRowsInMemory,
          maxBytesInMemory,
          maxTotalRows,
          numShards,
          indexSpec,
          maxPendingPersists,
          forceExtendableShardSpecs,
          forceGuaranteedRollup,
          reportParseExceptions,
          pushTimeout,
          dir,
          segmentWriteOutMediumFactory,
          logParseExceptions,
          maxParseExceptions,
          maxSavedParseExceptions
      );
    }

    @JsonProperty
    public Integer getTargetPartitionSize()
    {
      return targetPartitionSize;
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
    public Long getMaxTotalRows()
    {
      return maxTotalRows;
    }

    @JsonProperty
    public Integer getNumShards()
    {
      return numShards;
    }

    @JsonProperty
    @Override
    public IndexSpec getIndexSpec()
    {
      return indexSpec;
    }

    @Override
    public File getBasePersistDirectory()
    {
      return basePersistDirectory;
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
    public boolean isForceExtendableShardSpecs()
    {
      return forceExtendableShardSpecs;
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

    @Override
    public Period getIntermediatePersistPeriod()
    {
      return new Period(Integer.MAX_VALUE); // intermediate persist doesn't make much sense for batch jobs
    }

    @Nullable
    @Override
    @JsonProperty
    public SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory()
    {
      return segmentWriteOutMediumFactory;
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
      return maxRowsInMemory == that.maxRowsInMemory &&
             Objects.equals(maxTotalRows, that.maxTotalRows) &&
             maxPendingPersists == that.maxPendingPersists &&
             forceExtendableShardSpecs == that.forceExtendableShardSpecs &&
             forceGuaranteedRollup == that.forceGuaranteedRollup &&
             reportParseExceptions == that.reportParseExceptions &&
             pushTimeout == that.pushTimeout &&
             Objects.equals(targetPartitionSize, that.targetPartitionSize) &&
             Objects.equals(numShards, that.numShards) &&
             Objects.equals(indexSpec, that.indexSpec) &&
             Objects.equals(basePersistDirectory, that.basePersistDirectory) &&
             Objects.equals(segmentWriteOutMediumFactory, that.segmentWriteOutMediumFactory) &&
             logParseExceptions == that.logParseExceptions &&
             maxParseExceptions == that.maxParseExceptions &&
             maxSavedParseExceptions == that.maxSavedParseExceptions;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(
          targetPartitionSize,
          maxRowsInMemory,
          maxTotalRows,
          numShards,
          indexSpec,
          basePersistDirectory,
          maxPendingPersists,
          forceExtendableShardSpecs,
          forceGuaranteedRollup,
          reportParseExceptions,
          pushTimeout,
          segmentWriteOutMediumFactory,
          logParseExceptions,
          maxParseExceptions,
          maxSavedParseExceptions
      );
    }
  }
}
