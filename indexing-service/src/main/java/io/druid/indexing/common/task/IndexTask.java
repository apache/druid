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
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.Committer;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.guice.annotations.Smile;
import io.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import io.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.LockAcquireAction;
import io.druid.indexing.common.actions.LockTryAcquireAction;
import io.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.firehose.IngestSegmentFirehoseFactory;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
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
import io.druid.segment.realtime.RealtimeMetricsMonitor;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.AppenderatorConfig;
import io.druid.segment.realtime.appenderator.Appenderators;
import io.druid.segment.realtime.appenderator.FiniteAppenderatorDriver;
import io.druid.segment.realtime.appenderator.SegmentAllocator;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import io.druid.segment.realtime.firehose.ReplayableFirehoseFactory;
import io.druid.segment.realtime.plumber.Committers;
import io.druid.segment.realtime.plumber.NoopSegmentHandoffNotifierFactory;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.LinearShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

public class IndexTask extends AbstractTask
{
  private static final Logger log = new Logger(IndexTask.class);
  private static final HashFunction hashFunction = Hashing.murmur3_128();

  private static String makeId(String id, IndexIngestionSpec ingestionSchema)
  {
    return id != null ? id : String.format("index_%s_%s", makeDataSource(ingestionSchema), new DateTime());
  }

  private static String makeDataSource(IndexIngestionSpec ingestionSchema)
  {
    return ingestionSchema.getDataSchema().getDataSource();
  }

  @JsonIgnore
  private final IndexIngestionSpec ingestionSchema;
  private final ObjectMapper smileMapper;

  @JsonCreator
  public IndexTask(
      @JsonProperty("id") final String id,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("spec") final IndexIngestionSpec ingestionSchema,
      @JsonProperty("context") final Map<String, Object> context,
      @Smile @JacksonInject final ObjectMapper smileMapper
  )
  {
    super(makeId(id, ingestionSchema), null, taskResource, makeDataSource(ingestionSchema), context);

    this.ingestionSchema = ingestionSchema;
    this.smileMapper = smileMapper;
  }

  @Override
  public String getType()
  {
    return "index";
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    Optional<SortedSet<Interval>> intervals = ingestionSchema.getDataSchema().getGranularitySpec().bucketIntervals();

    if (intervals.isPresent()) {
      Interval interval = JodaUtils.umbrellaInterval(intervals.get());
      return taskActionClient.submit(new LockTryAcquireAction(interval)) != null;
    } else {
      return true;
    }
  }

  @JsonProperty("spec")
  public IndexIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    final boolean determineIntervals = !ingestionSchema.getDataSchema()
                                                       .getGranularitySpec()
                                                       .bucketIntervals()
                                                       .isPresent();

    final FirehoseFactory delegateFirehoseFactory = ingestionSchema.getIOConfig().getFirehoseFactory();

    if (delegateFirehoseFactory instanceof IngestSegmentFirehoseFactory) {
      // pass toolbox to Firehose
      ((IngestSegmentFirehoseFactory) delegateFirehoseFactory).setTaskToolbox(toolbox);
    }

    final FirehoseFactory firehoseFactory;
    if (ingestionSchema.getIOConfig().isSkipFirehoseCaching()
        || delegateFirehoseFactory instanceof ReplayableFirehoseFactory) {
      firehoseFactory = delegateFirehoseFactory;
    } else {
      firehoseFactory = new ReplayableFirehoseFactory(
          delegateFirehoseFactory,
          ingestionSchema.getTuningConfig().isReportParseExceptions(),
          null,
          null,
          smileMapper
      );
    }

    final ShardSpecs shardSpecs = determineShardSpecs(toolbox, firehoseFactory);

    final String version;
    final DataSchema dataSchema;
    if (determineIntervals) {
      Interval interval = JodaUtils.umbrellaInterval(shardSpecs.getIntervals());
      TaskLock lock = toolbox.getTaskActionClient().submit(new LockAcquireAction(interval));
      version = lock.getVersion();
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
      version = Iterables.getOnlyElement(getTaskLocks(toolbox)).getVersion();
      dataSchema = ingestionSchema.getDataSchema();
    }

    if (generateAndPublishSegments(toolbox, dataSchema, shardSpecs, version, firehoseFactory)) {
      return TaskStatus.success(getId());
    } else {
      return TaskStatus.failure(getId());
    }
  }

  /**
   * Determines the number of shards for each interval using a hash of queryGranularity timestamp + all dimensions (i.e
   * hash-based partitioning). In the future we may want to also support single-dimension partitioning.
   */
  private ShardSpecs determineShardSpecs(
      final TaskToolbox toolbox,
      final FirehoseFactory firehoseFactory
  ) throws IOException
  {
    final ObjectMapper jsonMapper = toolbox.getObjectMapper();
    final GranularitySpec granularitySpec = ingestionSchema.getDataSchema().getGranularitySpec();
    final boolean fixedNumPartitions = ingestionSchema.getTuningConfig().getNumShards() != null;
    final boolean fixedIntervals = ingestionSchema.getDataSchema()
                                                  .getGranularitySpec()
                                                  .bucketIntervals()
                                                  .isPresent()
                                   && !ingestionSchema.getTuningConfig().isForceExtendableShardSpecs()
                                   && !ingestionSchema.getIOConfig().isAppendToExisting();

    final Set<Interval> intervals;
    if (fixedIntervals) {
      log.info("intervals provided, skipping determine partition scan");
      intervals = ingestionSchema.getDataSchema()
                                 .getGranularitySpec()
                                 .bucketIntervals()
                                 .get();
    } else {
      // determine intervals containing data
      log.info("Determining intervals");
      intervals = new HashSet<>();
      long determineIntervalsStartMillis = System.currentTimeMillis();

      try (final Firehose firehose = firehoseFactory.connect(ingestionSchema.getDataSchema().getParser())) {
        while (firehose.hasMore()) {
          final InputRow inputRow = firehose.nextRow();
          final Interval interval = granularitySpec.getSegmentGranularity().bucket(inputRow.getTimestamp());

          intervals.add(interval);
        }
      }

      log.info("Found intervals in %,dms", System.currentTimeMillis() - determineIntervalsStartMillis);
    }

    if (fixedNumPartitions) {
      final int numShards = ingestionSchema.getTuningConfig().getNumShards();
      final Map<Interval, List<ShardSpec>> intervalToShardSpecs = new HashMap<>();

      for (Interval interval : intervals) {
        final List<ShardSpec> intervalShardSpecs = Lists.newArrayListWithCapacity(numShards);
        if (numShards > 1) {
          for (int i = 0; i < numShards; i++) {
            intervalShardSpecs.add(new HashBasedNumberedShardSpec(i, numShards, null, jsonMapper));
          }
        } else {
          intervalShardSpecs.add(NoneShardSpec.instance());
        }
        intervalToShardSpecs.put(interval, intervalShardSpecs);
      }

      return new ShardSpecs()
      {

        @Override
        public Collection<Interval> getIntervals()
        {
          return intervalToShardSpecs.keySet();
        }

        @Override
        public boolean isExtendable()
        {
          return false;
        }

        @Override
        public ShardSpec getShardSpec(Interval interval, long timestamp, InputRow row)
        {
          final List<ShardSpec> shardSpecs = intervalToShardSpecs.get(interval);
          if (shardSpecs == null || shardSpecs.isEmpty()) {
            throw new ISE("Failed to get shardSpec for interval[%s]", interval);
          }
          return shardSpecs.get(0).getLookup(shardSpecs).getShardSpec(timestamp, row);
        }

        @Override
        public void updateShardSpec(Interval interval)
        {
          // do nothing
        }
      };
    } else {
      final Map<Interval, LinearShardSpec> shardSpecMap = intervals.stream()
          .collect(Collectors.toMap(interval -> interval, key -> new LinearShardSpec(0)));
      return new ShardSpecs()
      {
        @Override
        public Collection<Interval> getIntervals()
        {
          return shardSpecMap.keySet();
        }

        @Override
        public boolean isExtendable()
        {
          return true;
        }

        @Override
        public ShardSpec getShardSpec(Interval interval, long timestamp, InputRow row)
        {
          return shardSpecMap.get(interval);
        }

        @Override
        public void updateShardSpec(Interval interval)
        {
          final LinearShardSpec previous = shardSpecMap.get(interval);
          Preconditions.checkNotNull(previous, "previous shardSpec for interval[%s]", interval);
          shardSpecMap.put(interval, new LinearShardSpec(previous.getPartitionNum() + 1));
        }
      };
    }
  }

  private boolean generateAndPublishSegments(
      final TaskToolbox toolbox,
      final DataSchema dataSchema,
      final ShardSpecs shardSpecs,
      final String version,
      final FirehoseFactory firehoseFactory
  ) throws IOException, InterruptedException

  {
    final GranularitySpec granularitySpec = dataSchema.getGranularitySpec();
    final FireDepartment fireDepartmentForMetrics = new FireDepartment(
        dataSchema, new RealtimeIOConfig(null, null, null), null
    );
    final FireDepartmentMetrics fireDepartmentMetrics = fireDepartmentForMetrics.getMetrics();

    if (toolbox.getMonitorScheduler() != null) {
      toolbox.getMonitorScheduler().addMonitor(
          new RealtimeMetricsMonitor(
              ImmutableList.of(fireDepartmentForMetrics),
              ImmutableMap.of(DruidMetrics.TASK_ID, new String[]{getId()})
          )
      );
    }

    final SegmentAllocator segmentAllocator;
    if (ingestionSchema.getIOConfig().isAppendToExisting()) {
      segmentAllocator = new ActionBasedSegmentAllocator(toolbox.getTaskActionClient(), dataSchema);
    } else {
      segmentAllocator = (timestamp, row, sequenceName, previousSegmentId) -> {
        Optional<Interval> interval = granularitySpec.bucketInterval(timestamp);
        if (!interval.isPresent()) {
          throw new ISE("Could not find interval for timestamp [%s]", timestamp);
        }

        ShardSpec shardSpec = shardSpecs.getShardSpec(interval.get(), timestamp.getMillis(), row);
        if (shardSpec == null) {
          throw new ISE("Could not find ShardSpec for sequenceName [%s]", sequenceName);
        }

        return new SegmentIdentifier(getDataSource(), interval.get(), version, shardSpec);
      };
    }

    try (
        final Appenderator appenderator = newAppenderator(fireDepartmentMetrics, toolbox, dataSchema);
        final FiniteAppenderatorDriver driver = newDriver(
            appenderator,
            toolbox,
            segmentAllocator,
            fireDepartmentMetrics,
            ingestionSchema.getTuningConfig()
        );
        final Firehose firehose = firehoseFactory.connect(dataSchema.getParser())
    ) {
      final Supplier<Committer> committerSupplier = Committers.supplierFromFirehose(firehose);
      final TransactionalSegmentPublisher publisher = (segments, commitMetadata) -> {
        final SegmentTransactionalInsertAction action = new SegmentTransactionalInsertAction(segments, null, null);
        return toolbox.getTaskActionClient().submit(action).isSuccess();
      };

      if (driver.startJob() != null) {
        driver.clear();
      }

      try {
        while (firehose.hasMore()) {
          try {
            final InputRow inputRow = firehose.nextRow();

            final Optional<Interval> optInterval = granularitySpec.bucketInterval(inputRow.getTimestamp());
            if (!optInterval.isPresent()) {
              fireDepartmentMetrics.incrementThrownAway();
              continue;
            }

            final Interval interval = optInterval.get();
            final String sequenceName = Appenderators.getSequenceName(interval, version, shardSpecs.getShardSpec(interval, inputRow.getTimestampFromEpoch(), inputRow));
            final Pair<SegmentIdentifier, List<SegmentIdentifier>> pair = driver.add(inputRow, sequenceName, committerSupplier, publisher, shardSpecs.isExtendable());
            final SegmentIdentifier identifier = pair.lhs;
            final List<SegmentIdentifier> publishedSegments = pair.rhs;

            if (identifier == null) {
              throw new ISE("Could not allocate segment for row with timestamp[%s]", inputRow.getTimestamp());
            }
            publishedSegments.forEach(segmentId -> shardSpecs.updateShardSpec(segmentId.getInterval()));

            fireDepartmentMetrics.incrementProcessed();
          }
          catch (ParseException e) {
            if (ingestionSchema.getTuningConfig().isReportParseExceptions()) {
              throw e;
            } else {
              fireDepartmentMetrics.incrementUnparseable();
            }
          }
        }
      }
      finally {
        driver.persist(committerSupplier.get());
      }

      final SegmentsAndMetadata published = driver.publishAndWaitHandoff(publisher, committerSupplier.get());
      if (published == null) {
        log.error("Failed to publish segments, aborting!");
        return false;
      } else {
        log.info(
            "Published segments[%s]", Joiner.on(", ").join(
                Iterables.transform(
                    published.getSegments(),
                    new Function<DataSegment, String>()
                    {
                      @Override
                      public String apply(DataSegment input)
                      {
                        return input.getIdentifier();
                      }
                    }
                )
            )
        );
        return true;
      }
    }
  }

  private Appenderator newAppenderator(FireDepartmentMetrics metrics, TaskToolbox toolbox, DataSchema dataSchema)
  {
    return Appenderators.createOffline(
        dataSchema,
        ingestionSchema.getTuningConfig().withBasePersistDirectory(new File(toolbox.getTaskWorkDir(), "persist")),
        metrics,
        toolbox.getSegmentPusher(),
        toolbox.getObjectMapper(),
        toolbox.getIndexIO(),
        ingestionSchema.getTuningConfig().isBuildV9Directly() ? toolbox.getIndexMergerV9() : toolbox.getIndexMerger()
    );
  }

  private FiniteAppenderatorDriver newDriver(
      final Appenderator appenderator,
      final TaskToolbox toolbox,
      final SegmentAllocator segmentAllocator,
      final FireDepartmentMetrics metrics,
      final IndexTuningConfig tuningConfig
  )
  {
    return new FiniteAppenderatorDriver(
        appenderator,
        segmentAllocator,
        new NoopSegmentHandoffNotifierFactory(), // don't wait for handoff since we don't serve queries
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
        toolbox.getObjectMapper(),
        // If targetPartitionSize is null, numShards must be set which means intervals are already partitioned with
        // proper shardSpecs. See determineShardSpecs().
        tuningConfig.getTargetPartitionSize() == null ? Integer.MAX_VALUE : tuningConfig.getTargetPartitionSize(),
        tuningConfig.getMaxPersistedSegmentsBytes(),
        0,
        metrics
    );
  }

  /**
   * This interface represents a map of (Interval, ShardSpec) and is used for easy shardSpec generation.  The most
   * important method is {@link #updateShardSpec(Interval)} which updates the map according to the type of shardSpec.
   */
  private interface ShardSpecs
  {
    /**
     * Return the key set of the underlying map.
     *
     * @return a set of intervals
     */
    Collection<Interval> getIntervals();

    /**
     * Indicate that the type of shardSpecs is extendable like {@link LinearShardSpec}.
     *
     * @return true if the type of shardSpecs is extendable
     */
    boolean isExtendable();

    /**
     * Return a shardSpec for the given interval, timestamp and input row.
     *
     * @param interval  interval for shardSpec
     * @param timestamp timestamp of input row
     * @param row       input row
     * @return a shardSpec
     */
    ShardSpec getShardSpec(Interval interval, long timestamp, InputRow row);

    /**
     * Update the shardSpec of the given interval.  When the type of shardSpecs is extendable, this method must update
     * the shardSpec properly.  For example, if the {@link LinearShardSpec} is used, an implementation of this method
     * may replace the shardSpec of the given interval with a new one having a greater partitionNum.
     *
     * @param interval interval for shardSpec to be updated
     */
    void updateShardSpec(Interval interval);
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
      this.tuningConfig = tuningConfig == null
                          ?
                          new IndexTuningConfig(null, null, null, null, null, null, null, null, null, (File) null)
                          : tuningConfig;
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
    private static final boolean DEFAULT_SKIP_FIREHOSE_CACHING = false;

    private final FirehoseFactory firehoseFactory;
    private final boolean appendToExisting;
    private final boolean skipFirehoseCaching;

    @JsonCreator
    public IndexIOConfig(
        @JsonProperty("firehose") FirehoseFactory firehoseFactory,
        @JsonProperty("appendToExisting") @Nullable Boolean appendToExisting,
        @JsonProperty("skipFirehoseCaching") @Nullable Boolean skipFirehoseCaching
    )
    {
      this.firehoseFactory = firehoseFactory;
      this.appendToExisting = appendToExisting == null ? DEFAULT_APPEND_TO_EXISTING : appendToExisting;
      this.skipFirehoseCaching = skipFirehoseCaching == null ? DEFAULT_SKIP_FIREHOSE_CACHING : skipFirehoseCaching;
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

    @JsonProperty("skipFirehoseCaching")
    public boolean isSkipFirehoseCaching()
    {
      return skipFirehoseCaching;
    }
  }

  @JsonTypeName("index")
  public static class IndexTuningConfig implements TuningConfig, AppenderatorConfig
  {
    private static final int DEFAULT_MAX_ROWS_IN_MEMORY = 75000;
    private static final long DEFAULT_MAX_PERSISTED_SEGMENTS_BYTES = 1024 * 1024 * 1024;
    private static final IndexSpec DEFAULT_INDEX_SPEC = new IndexSpec();
    private static final int DEFAULT_MAX_PENDING_PERSISTS = 0;
    private static final boolean DEFAULT_BUILD_V9_DIRECTLY = true;
    private static final boolean DEFAULT_FORCE_EXTENDABLE_SHARD_SPECS = false;
    private static final boolean DEFAULT_REPORT_PARSE_EXCEPTIONS = false;

    static final int DEFAULT_TARGET_PARTITION_SIZE = 5000000;

    private final Integer targetPartitionSize;
    private final int maxRowsInMemory;
    private final long maxPersistedSegmentsBytes;
    private final Integer numShards;
    private final IndexSpec indexSpec;
    private final File basePersistDirectory;
    private final int maxPendingPersists;
    private final boolean buildV9Directly;
    private final boolean forceExtendableShardSpecs;
    private final boolean reportParseExceptions;

    @JsonCreator
    public IndexTuningConfig(
        @JsonProperty("targetPartitionSize") @Nullable Integer targetPartitionSize,
        @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
        @JsonProperty("maxPersistedSegmentsBytes") @Nullable Long maxPersistedSegmentsBytes,
        @JsonProperty("rowFlushBoundary") @Nullable Integer rowFlushBoundary_forBackCompatibility, // DEPRECATED
        @JsonProperty("numShards") @Nullable Integer numShards,
        @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
        @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
        @JsonProperty("buildV9Directly") @Nullable Boolean buildV9Directly,
        @JsonProperty("forceExtendableShardSpecs") @Nullable Boolean forceExtendableShardSpecs,
        @JsonProperty("reportParseExceptions") @Nullable Boolean reportParseExceptions
    )
    {
      this(
          targetPartitionSize,
          maxRowsInMemory != null ? maxRowsInMemory : rowFlushBoundary_forBackCompatibility,
          maxPersistedSegmentsBytes,
          numShards,
          indexSpec,
          maxPendingPersists,
          buildV9Directly,
          forceExtendableShardSpecs,
          reportParseExceptions,
          null
      );
    }

    private IndexTuningConfig(
        @Nullable Integer targetPartitionSize,
        @Nullable Integer maxRowsInMemory,
        @Nullable Long maxPersistedSegmentsBytes,
        @Nullable Integer numShards,
        @Nullable IndexSpec indexSpec,
        @Nullable Integer maxPendingPersists,
        @Nullable Boolean buildV9Directly,
        @Nullable Boolean forceExtendableShardSpecs,
        @Nullable Boolean reportParseExceptions,
        @Nullable File basePersistDirectory
    )
    {
      Preconditions.checkArgument(
          targetPartitionSize == null || targetPartitionSize.equals(-1) || numShards == null || numShards.equals(-1),
          "targetPartitionSize and numShards cannot both be set"
      );

      this.targetPartitionSize = numShards != null && !numShards.equals(-1)
                                 ? null
                                 : (targetPartitionSize == null || targetPartitionSize.equals(-1)
                                    ? DEFAULT_TARGET_PARTITION_SIZE
                                    : targetPartitionSize);
      this.maxRowsInMemory = maxRowsInMemory == null ? DEFAULT_MAX_ROWS_IN_MEMORY : maxRowsInMemory;
      this.maxPersistedSegmentsBytes = maxPersistedSegmentsBytes == null
                                       ? DEFAULT_MAX_PERSISTED_SEGMENTS_BYTES
                                       : maxPersistedSegmentsBytes;
      this.numShards = numShards == null || numShards.equals(-1) ? null : numShards;
      this.indexSpec = indexSpec == null ? DEFAULT_INDEX_SPEC : indexSpec;
      this.maxPendingPersists = maxPendingPersists == null ? DEFAULT_MAX_PENDING_PERSISTS : maxPendingPersists;
      this.buildV9Directly = buildV9Directly == null ? DEFAULT_BUILD_V9_DIRECTLY : buildV9Directly;
      this.forceExtendableShardSpecs = forceExtendableShardSpecs == null
                                       ? DEFAULT_FORCE_EXTENDABLE_SHARD_SPECS
                                       : forceExtendableShardSpecs;
      this.reportParseExceptions = reportParseExceptions == null
                                   ? DEFAULT_REPORT_PARSE_EXCEPTIONS
                                   : reportParseExceptions;
      this.basePersistDirectory = basePersistDirectory;
    }

    public IndexTuningConfig withBasePersistDirectory(File dir)
    {
      return new IndexTuningConfig(
          targetPartitionSize,
          maxRowsInMemory,
          maxPersistedSegmentsBytes,
          numShards,
          indexSpec,
          maxPendingPersists,
          buildV9Directly,
          forceExtendableShardSpecs,
          reportParseExceptions,
          dir
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
    public long getMaxPersistedSegmentsBytes()
    {
      return maxPersistedSegmentsBytes;
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

    @JsonProperty
    public boolean isBuildV9Directly()
    {
      return buildV9Directly;
    }

    @JsonProperty
    @Override
    public boolean isReportParseExceptions()
    {
      return reportParseExceptions;
    }

    @JsonProperty
    public boolean isForceExtendableShardSpecs()
    {
      return forceExtendableShardSpecs;
    }

    @Override
    public Period getIntermediatePersistPeriod()
    {
      return new Period(Integer.MAX_VALUE); // intermediate persist doesn't make much sense for batch jobs
    }
  }
}
