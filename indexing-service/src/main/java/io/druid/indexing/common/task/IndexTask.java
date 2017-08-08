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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.Committer;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.Rows;
import io.druid.hll.HyperLogLogCollector;
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
import io.druid.segment.realtime.RealtimeMetricsMonitor;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.AppenderatorConfig;
import io.druid.segment.realtime.appenderator.AppenderatorDriver;
import io.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import io.druid.segment.realtime.appenderator.Appenderators;
import io.druid.segment.realtime.appenderator.SegmentAllocator;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import io.druid.segment.realtime.plumber.Committers;
import io.druid.segment.realtime.plumber.NoopSegmentHandoffNotifierFactory;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.codehaus.plexus.util.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IndexTask extends AbstractTask
{
  private static final Logger log = new Logger(IndexTask.class);
  private static final HashFunction hashFunction = Hashing.murmur3_128();
  private static final String TYPE = "index";

  private static String makeId(String id, IndexIngestionSpec ingestionSchema)
  {
    return id != null ? id : StringUtils.format("index_%s_%s", makeDataSource(ingestionSchema), new DateTime());
  }

  private static String makeGroupId(IndexIngestionSpec ingestionSchema)
  {
    if (ingestionSchema.getIOConfig().isAppendToExisting()) {
      // Shared locking group for all tasks that append, since they are OK to run concurrently.
      return StringUtils.format("%s_append_%s", TYPE, ingestionSchema.getDataSchema().getDataSource());
    } else {
      // Return null, one locking group per task.
      return null;
    }
  }

  private static String makeDataSource(IndexIngestionSpec ingestionSchema)
  {
    return ingestionSchema.getDataSchema().getDataSource();
  }

  @JsonIgnore
  private final IndexIngestionSpec ingestionSchema;

  @JsonCreator
  public IndexTask(
      @JsonProperty("id") final String id,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("spec") final IndexIngestionSpec ingestionSchema,
      @JsonProperty("context") final Map<String, Object> context
  )
  {
    super(
        makeId(id, ingestionSchema),
        makeGroupId(ingestionSchema),
        taskResource,
        makeDataSource(ingestionSchema),
        context
    );

    this.ingestionSchema = ingestionSchema;
  }

  @Override
  public String getType()
  {
    return TYPE;
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

    final FirehoseFactory firehoseFactory = ingestionSchema.getIOConfig().getFirehoseFactory();

    if (firehoseFactory instanceof IngestSegmentFirehoseFactory) {
      // pass toolbox to Firehose
      ((IngestSegmentFirehoseFactory) firehoseFactory).setTaskToolbox(toolbox);
    }

    final File firehoseTempDir = toolbox.getFirehoseTemporaryDir();
    // Firehose temporary directory is automatically removed when this IndexTask completes.
    FileUtils.forceMkdir(firehoseTempDir);

    final ShardSpecs shardSpecs = determineShardSpecs(toolbox, firehoseFactory, firehoseTempDir);

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

    if (generateAndPublishSegments(toolbox, dataSchema, shardSpecs, version, firehoseFactory, firehoseTempDir)) {
      return TaskStatus.success(getId());
    } else {
      return TaskStatus.failure(getId());
    }
  }

  private static boolean isGuaranteedRollup(IndexIOConfig ioConfig, IndexTuningConfig tuningConfig)
  {
    Preconditions.checkState(
        !(tuningConfig.isForceGuaranteedRollup() &&
          (tuningConfig.isForceExtendableShardSpecs() || ioConfig.isAppendToExisting())),
        "Perfect rollup cannot be guaranteed with extendable shardSpecs"
    );
    return tuningConfig.isForceGuaranteedRollup();
  }

  private static boolean isExtendableShardSpecs(IndexIOConfig ioConfig, IndexTuningConfig tuningConfig)
  {
    return !isGuaranteedRollup(ioConfig, tuningConfig);
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

  private static ShardSpecs createShardSpecsFromInput(
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
        final long numRows = new Double(collector.estimateCardinality()).longValue();
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

  private static Map<Interval, Optional<HyperLogLogCollector>> collectIntervalsAndShardSpecs(
      ObjectMapper jsonMapper,
      IndexIngestionSpec ingestionSchema,
      FirehoseFactory firehoseFactory,
      File firehoseTempDir,
      GranularitySpec granularitySpec,
      boolean determineIntervals,
      boolean determineNumPartitions
  ) throws IOException
  {
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
            continue;
          }

          final Interval interval;
          if (determineIntervals) {
            interval = granularitySpec.getSegmentGranularity().bucket(inputRow.getTimestamp());
          } else {
            final Optional<Interval> optInterval = granularitySpec.bucketInterval(inputRow.getTimestamp());
            if (!optInterval.isPresent()) {
              thrownAway++;
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
        }
        catch (ParseException e) {
          if (ingestionSchema.getTuningConfig().isReportParseExceptions()) {
            throw e;
          } else {
            unparseable++;
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
   * This method reads input data row by row and adds the read row to a proper segment using {@link AppenderatorDriver}.
   * If there is no segment for the row, a new one is created.  Segments can be published in the middle of reading inputs
   * if one of below conditions are satisfied.
   *
   * <ul>
   * <li>
   * If the number of rows in a segment exceeds {@link IndexTuningConfig#targetPartitionSize}
   * </li>
   * <li>
   * If the number of rows added to {@link AppenderatorDriver} so far exceeds {@link IndexTuningConfig#maxTotalRows}
   * </li>
   * </ul>
   *
   * At the end of this method, all the remaining segments are published.
   *
   * @return true if generated segments are successfully published, otherwise false
   */
  private boolean generateAndPublishSegments(
      final TaskToolbox toolbox,
      final DataSchema dataSchema,
      final ShardSpecs shardSpecs,
      final String version,
      final FirehoseFactory firehoseFactory,
      final File firehoseTempDir
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

    final IndexIOConfig ioConfig = ingestionSchema.getIOConfig();
    final IndexTuningConfig tuningConfig = ingestionSchema.tuningConfig;
    final long publishTimeout = tuningConfig.getPublishTimeout();
    final long maxRowsInAppenderator = tuningConfig.getMaxTotalRows();
    final int maxRowsInSegment = tuningConfig.getTargetPartitionSize() == null
                                 ? Integer.MAX_VALUE
                                 : tuningConfig.getTargetPartitionSize();
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

          lookup.put(
              Appenderators.getSequenceName(entry.getKey(), version, shardSpec),
              new SegmentIdentifier(getDataSource(), entry.getKey(), version, shardSpecForPublishing)
          );
        }
      }

      segmentAllocator = (row, sequenceName, previousSegmentId) -> lookup.get(sequenceName);
    } else if (ioConfig.isAppendToExisting()) {
      // Append mode: Allocate segments as needed using Overlord APIs.
      segmentAllocator = new ActionBasedSegmentAllocator(toolbox.getTaskActionClient(), dataSchema);
    } else {
      // Overwrite mode, non-guaranteed rollup: We can make up our own segment ids but we don't know them in advance.
      final Map<Interval, AtomicInteger> counters = new HashMap<>();

      segmentAllocator = (row, sequenceName, previousSegmentId) -> {
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
        return new SegmentIdentifier(getDataSource(), interval, version, new NumberedShardSpec(partitionNum, 0));
      };
    }

    final TransactionalSegmentPublisher publisher = (segments, commitMetadata) -> {
      final SegmentTransactionalInsertAction action = new SegmentTransactionalInsertAction(segments, null, null);
      return toolbox.getTaskActionClient().submit(action).isSuccess();
    };

    try (
        final Appenderator appenderator = newAppenderator(fireDepartmentMetrics, toolbox, dataSchema, tuningConfig);
        final AppenderatorDriver driver = newDriver(
            appenderator,
            toolbox,
            segmentAllocator,
            fireDepartmentMetrics
        );
        final Firehose firehose = firehoseFactory.connect(dataSchema.getParser(), firehoseTempDir)
    ) {
      final Supplier<Committer> committerSupplier = Committers.supplierFromFirehose(firehose);

      if (driver.startJob() != null) {
        driver.clear();
      }

      try {
        while (firehose.hasMore()) {
          try {
            final InputRow inputRow = firehose.nextRow();

            if (inputRow == null) {
              continue;
            }

            final Optional<Interval> optInterval = granularitySpec.bucketInterval(inputRow.getTimestamp());
            if (!optInterval.isPresent()) {
              fireDepartmentMetrics.incrementThrownAway();
              continue;
            }


            final String sequenceName;

            if (isGuaranteedRollup) {
              // Sequence name is based solely on the shardSpec, and there will only be one segment per sequence.
              final Interval interval = optInterval.get();
              final ShardSpec shardSpec = shardSpecs.getShardSpec(interval, inputRow);
              sequenceName = Appenderators.getSequenceName(interval, version, shardSpec);
            } else {
              // Segments are created as needed, using a single sequence name. They may be allocated from the overlord
              // (in append mode) or may be created on our own authority (in overwrite mode).
              sequenceName = getId();
            }

            final AppenderatorDriverAddResult addResult = driver.add(inputRow, sequenceName, committerSupplier);

            if (addResult.isOk()) {
              // incremental segment publishment is allowed only when rollup don't have to be perfect.
              if (!isGuaranteedRollup &&
                  (addResult.getNumRowsInSegment() >= maxRowsInSegment ||
                   addResult.getTotalNumRowsInAppenderator() >= maxRowsInAppenderator)) {
                // There can be some segments waiting for being published even though any rows won't be added to them.
                // If those segments are not published here, the available space in appenderator will be kept to be small
                // which makes the size of segments smaller.
                final SegmentsAndMetadata published = awaitPublish(
                    driver.publishAll(
                        publisher,
                        committerSupplier.get()
                    ),
                    publishTimeout
                );
                // Even though IndexTask uses NoopHandoffNotifier which does nothing for segment handoff,
                // the below code is needed to update the total number of rows added to the appenderator so far.
                // See AppenderatorDriver.registerHandoff() and Appenderator.drop().
                // A hard-coded timeout is used here because the below get() is expected to return immediately.
                driver.registerHandoff(published).get(30, TimeUnit.SECONDS);
              }
            } else {
              throw new ISE("Failed to add a row with timestamp[%s]", inputRow.getTimestamp());
            }

            fireDepartmentMetrics.incrementProcessed();
          }
          catch (ParseException e) {
            if (tuningConfig.isReportParseExceptions()) {
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

      final SegmentsAndMetadata published = awaitPublish(
          driver.publishAll(
              publisher,
              committerSupplier.get()
          ),
          publishTimeout
      );

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
    catch (TimeoutException | ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  private static SegmentsAndMetadata awaitPublish(
      ListenableFuture<SegmentsAndMetadata> publishFuture,
      long publishTimeout
  )
      throws ExecutionException, InterruptedException, TimeoutException
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

  private static AppenderatorDriver newDriver(
      final Appenderator appenderator,
      final TaskToolbox toolbox,
      final SegmentAllocator segmentAllocator,
      final FireDepartmentMetrics metrics
  )
  {
    return new AppenderatorDriver(
        appenderator,
        segmentAllocator,
        new NoopSegmentHandoffNotifierFactory(), // don't wait for handoff since we don't serve queries
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
        toolbox.getObjectMapper(),
        metrics
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
    private static final int DEFAULT_MAX_ROWS_IN_MEMORY = 75_000;
    private static final int DEFAULT_MAX_TOTAL_ROWS = 20_000_000;
    private static final IndexSpec DEFAULT_INDEX_SPEC = new IndexSpec();
    private static final int DEFAULT_MAX_PENDING_PERSISTS = 0;
    private static final boolean DEFAULT_FORCE_EXTENDABLE_SHARD_SPECS = false;
    private static final boolean DEFAULT_GUARANTEE_ROLLUP = false;
    private static final boolean DEFAULT_REPORT_PARSE_EXCEPTIONS = false;
    private static final long DEFAULT_PUBLISH_TIMEOUT = 0;

    static final int DEFAULT_TARGET_PARTITION_SIZE = 5000000;

    private final Integer targetPartitionSize;
    private final int maxRowsInMemory;
    private final int maxTotalRows;
    private final Integer numShards;
    private final IndexSpec indexSpec;
    private final File basePersistDirectory;
    private final int maxPendingPersists;
    private final boolean forceExtendableShardSpecs;
    private final boolean forceGuaranteedRollup;
    private final boolean reportParseExceptions;
    private final long publishTimeout;

    @JsonCreator
    public IndexTuningConfig(
        @JsonProperty("targetPartitionSize") @Nullable Integer targetPartitionSize,
        @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
        @JsonProperty("maxTotalRows") @Nullable Integer maxTotalRows,
        @JsonProperty("rowFlushBoundary") @Nullable Integer rowFlushBoundary_forBackCompatibility, // DEPRECATED
        @JsonProperty("numShards") @Nullable Integer numShards,
        @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
        @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
        // This parameter is left for compatibility when reading existing JSONs, to be removed in Druid 0.12.
        @JsonProperty("buildV9Directly") @Nullable Boolean buildV9Directly,
        @JsonProperty("forceExtendableShardSpecs") @Nullable Boolean forceExtendableShardSpecs,
        @JsonProperty("forceGuaranteedRollup") @Nullable Boolean forceGuaranteedRollup,
        @JsonProperty("reportParseExceptions") @Nullable Boolean reportParseExceptions,
        @JsonProperty("publishTimeout") @Nullable Long publishTimeout
    )
    {
      this(
          targetPartitionSize,
          maxRowsInMemory != null ? maxRowsInMemory : rowFlushBoundary_forBackCompatibility,
          maxTotalRows,
          numShards,
          indexSpec,
          maxPendingPersists,
          forceExtendableShardSpecs,
          forceGuaranteedRollup,
          reportParseExceptions,
          publishTimeout,
          null
      );
    }

    private IndexTuningConfig()
    {
      this(null, null, null, null, null, null, null, null, null, null, null);
    }

    private IndexTuningConfig(
        @Nullable Integer targetPartitionSize,
        @Nullable Integer maxRowsInMemory,
        @Nullable Integer maxTotalRows,
        @Nullable Integer numShards,
        @Nullable IndexSpec indexSpec,
        @Nullable Integer maxPendingPersists,
        @Nullable Boolean forceExtendableShardSpecs,
        @Nullable Boolean forceGuaranteedRollup,
        @Nullable Boolean reportParseExceptions,
        @Nullable Long publishTimeout,
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
      this.maxTotalRows = maxTotalRows == null
                          ? DEFAULT_MAX_TOTAL_ROWS
                          : maxTotalRows;
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
      this.publishTimeout = publishTimeout == null ? DEFAULT_PUBLISH_TIMEOUT : publishTimeout;
      this.basePersistDirectory = basePersistDirectory;

      Preconditions.checkArgument(
          !(this.forceExtendableShardSpecs && this.forceGuaranteedRollup),
          "Perfect rollup cannot be guaranteed with extendable shardSpecs"
      );
    }

    public IndexTuningConfig withBasePersistDirectory(File dir)
    {
      return new IndexTuningConfig(
          targetPartitionSize,
          maxRowsInMemory,
          maxTotalRows,
          numShards,
          indexSpec,
          maxPendingPersists,
          forceExtendableShardSpecs,
          forceGuaranteedRollup,
          reportParseExceptions,
          publishTimeout,
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
    public int getMaxTotalRows()
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
    public long getPublishTimeout()
    {
      return publishTimeout;
    }

    @Override
    public Period getIntermediatePersistPeriod()
    {
      return new Period(Integer.MAX_VALUE); // intermediate persist doesn't make much sense for batch jobs
    }
  }
}
