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

package io.druid.segment.realtime.plumber;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.common.guava.ThreadRenamingCallable;
import io.druid.common.guava.ThreadRenamingRunnable;
import io.druid.common.utils.VMUtils;
import io.druid.concurrent.TaskThreadPriority;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.java.util.common.io.Closer;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.SegmentDescriptor;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexSpec;
import io.druid.segment.Metadata;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.incremental.IncrementalIndexAddResult;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.TuningConfigs;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.FireHydrant;
import io.druid.segment.realtime.SegmentPublisher;
import io.druid.segment.realtime.appenderator.SinkQuerySegmentWalker;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.SingleElementPartitionChunk;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 */
public class RealtimePlumber implements Plumber
{
  private static final EmittingLogger log = new EmittingLogger(RealtimePlumber.class);
  private static final int WARN_DELAY = 1000;

  private final DataSchema schema;
  private final RealtimeTuningConfig config;
  private final RejectionPolicy rejectionPolicy;
  private final FireDepartmentMetrics metrics;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final DataSegmentPusher dataSegmentPusher;
  private final SegmentPublisher segmentPublisher;
  private final SegmentHandoffNotifier handoffNotifier;
  private final Object handoffCondition = new Object();
  private final Map<Long, Sink> sinks = new ConcurrentHashMap<>();
  private final VersionedIntervalTimeline<String, Sink> sinkTimeline = new VersionedIntervalTimeline<String, Sink>(
      String.CASE_INSENSITIVE_ORDER
  );
  private final QuerySegmentWalker texasRanger;

  private final Cache cache;

  private volatile long nextFlush = 0;
  private volatile boolean shuttingDown = false;
  private volatile boolean stopped = false;
  private volatile boolean cleanShutdown = true;
  private volatile ExecutorService persistExecutor = null;
  private volatile ExecutorService mergeExecutor = null;
  private volatile ScheduledExecutorService scheduledExecutor = null;
  private volatile IndexMerger indexMerger;
  private volatile IndexIO indexIO;

  private static final String COMMIT_METADATA_KEY = "%commitMetadata%";
  private static final String COMMIT_METADATA_TIMESTAMP_KEY = "%commitMetadataTimestamp%";

  public RealtimePlumber(
      DataSchema schema,
      RealtimeTuningConfig config,
      FireDepartmentMetrics metrics,
      ServiceEmitter emitter,
      QueryRunnerFactoryConglomerate conglomerate,
      DataSegmentAnnouncer segmentAnnouncer,
      ExecutorService queryExecutorService,
      DataSegmentPusher dataSegmentPusher,
      SegmentPublisher segmentPublisher,
      SegmentHandoffNotifier handoffNotifier,
      IndexMerger indexMerger,
      IndexIO indexIO,
      Cache cache,
      CacheConfig cacheConfig,
      ObjectMapper objectMapper
  )
  {
    this.schema = schema;
    this.config = config;
    this.rejectionPolicy = config.getRejectionPolicyFactory().create(config.getWindowPeriod());
    this.metrics = metrics;
    this.segmentAnnouncer = segmentAnnouncer;
    this.dataSegmentPusher = dataSegmentPusher;
    this.segmentPublisher = segmentPublisher;
    this.handoffNotifier = handoffNotifier;
    this.indexMerger = Preconditions.checkNotNull(indexMerger, "Null IndexMerger");
    this.indexIO = Preconditions.checkNotNull(indexIO, "Null IndexIO");
    this.cache = cache;
    this.texasRanger = new SinkQuerySegmentWalker(
        schema.getDataSource(),
        sinkTimeline,
        objectMapper,
        emitter,
        conglomerate,
        queryExecutorService,
        cache,
        cacheConfig
    );

    log.info("Creating plumber using rejectionPolicy[%s]", getRejectionPolicy());
  }

  public DataSchema getSchema()
  {
    return schema;
  }

  public RealtimeTuningConfig getConfig()
  {
    return config;
  }

  public RejectionPolicy getRejectionPolicy()
  {
    return rejectionPolicy;
  }

  public Map<Long, Sink> getSinks()
  {
    return sinks;
  }

  @Override
  public Object startJob()
  {
    computeBaseDir(schema).mkdirs();
    initializeExecutors();
    handoffNotifier.start();
    Object retVal = bootstrapSinksFromDisk();
    startPersistThread();
    // Push pending sinks bootstrapped from previous run
    mergeAndPush();
    resetNextFlush();
    return retVal;
  }

  @Override
  public IncrementalIndexAddResult add(InputRow row, Supplier<Committer> committerSupplier) throws IndexSizeExceededException
  {
    long messageTimestamp = row.getTimestampFromEpoch();
    final Sink sink = getSink(messageTimestamp);
    metrics.reportMessageMaxTimestamp(messageTimestamp);
    if (sink == null) {
      return Plumber.THROWAWAY;
    }

    final IncrementalIndexAddResult addResult = sink.add(row, false);
    if (config.isReportParseExceptions() && addResult.getParseException() != null) {
      throw addResult.getParseException();
    }

    if (!sink.canAppendRow() || System.currentTimeMillis() > nextFlush) {
      persist(committerSupplier.get());
    }

    return addResult;
  }

  private Sink getSink(long timestamp)
  {
    if (!rejectionPolicy.accept(timestamp)) {
      return null;
    }

    final Granularity segmentGranularity = schema.getGranularitySpec().getSegmentGranularity();
    final VersioningPolicy versioningPolicy = config.getVersioningPolicy();

    DateTime truncatedDateTime = segmentGranularity.bucketStart(DateTimes.utc(timestamp));
    final long truncatedTime = truncatedDateTime.getMillis();

    Sink retVal = sinks.get(truncatedTime);

    if (retVal == null) {
      final Interval sinkInterval = new Interval(
          truncatedDateTime,
          segmentGranularity.increment(truncatedDateTime)
      );

      retVal = new Sink(
          sinkInterval,
          schema,
          config.getShardSpec(),
          versioningPolicy.getVersion(sinkInterval),
          config.getMaxRowsInMemory(),
          TuningConfigs.getMaxBytesInMemoryOrDefault(config.getMaxBytesInMemory()),
          config.isReportParseExceptions(),
          config.getDedupColumn()
      );
      addSink(retVal);

    }

    return retVal;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(final Query<T> query)
  {
    // Calling getQueryRunnerForIntervals here works because there's only one segment per interval for RealtimePlumber.
    return texasRanger.getQueryRunnerForIntervals(query, query.getIntervals());
  }

  @Override
  public void persist(final Committer committer)
  {
    final List<Pair<FireHydrant, Interval>> indexesToPersist = Lists.newArrayList();
    for (Sink sink : sinks.values()) {
      if (sink.swappable()) {
        indexesToPersist.add(Pair.of(sink.swap(), sink.getInterval()));
      }
    }

    log.info("Submitting persist runnable for dataSource[%s]", schema.getDataSource());

    final Stopwatch runExecStopwatch = Stopwatch.createStarted();
    final Stopwatch persistStopwatch = Stopwatch.createStarted();

    final Map<String, Object> metadataElems = committer.getMetadata() == null ? null :
                                              ImmutableMap.of(
                                                  COMMIT_METADATA_KEY,
                                                  committer.getMetadata(),
                                                  COMMIT_METADATA_TIMESTAMP_KEY,
                                                  System.currentTimeMillis()
                                              );

    persistExecutor.execute(
        new ThreadRenamingRunnable(StringUtils.format("%s-incremental-persist", schema.getDataSource()))
        {
          @Override
          public void doRun()
          {
            /* Note:
            If plumber crashes after storing a subset of all the hydrants then we will lose data and next
            time we will start with the commitMetadata stored in those hydrants.
            option#1:
            maybe it makes sense to store the metadata outside the segments in a separate file. This is because the
            commit metadata isn't really associated with an individual segment-- it's associated with a set of segments
            that are persisted at the same time or maybe whole datasource. So storing it in the segments is asking for problems.
            Sort of like this:

            {
              "metadata" : {"foo": "bar"},
              "segments": [
                {"id": "datasource_2000_2001_2000_1", "hydrant": 10},
                {"id": "datasource_2001_2002_2001_1", "hydrant": 12},
              ]
            }
            When a realtime node crashes and starts back up, it would delete any hydrants numbered higher than the
            ones in the commit file.

            option#2
            We could also just include the set of segments for the same chunk of metadata in more metadata on each
            of the segments. we might also have to think about the hand-off in terms of the full set of segments being
            handed off instead of individual segments being handed off (that is, if one of the set succeeds in handing
            off and the others fail, the real-time would believe that it needs to re-ingest the data).
             */
            long persistThreadCpuTime = VMUtils.safeGetThreadCpuTime();
            try {
              for (Pair<FireHydrant, Interval> pair : indexesToPersist) {
                metrics.incrementRowOutputCount(
                    persistHydrant(
                        pair.lhs, schema, pair.rhs, metadataElems
                    )
                );
              }
              committer.run();
            }
            catch (Exception e) {
              metrics.incrementFailedPersists();
              throw e;
            }
            finally {
              metrics.incrementPersistCpuTime(VMUtils.safeGetThreadCpuTime() - persistThreadCpuTime);
              metrics.incrementNumPersists();
              metrics.incrementPersistTimeMillis(persistStopwatch.elapsed(TimeUnit.MILLISECONDS));
              persistStopwatch.stop();
            }
          }
        }
    );

    final long startDelay = runExecStopwatch.elapsed(TimeUnit.MILLISECONDS);
    metrics.incrementPersistBackPressureMillis(startDelay);
    if (startDelay > WARN_DELAY) {
      log.warn("Ingestion was throttled for [%,d] millis because persists were pending.", startDelay);
    }
    runExecStopwatch.stop();
    resetNextFlush();
  }

  // Submits persist-n-merge task for a Sink to the mergeExecutor
  private void persistAndMerge(final long truncatedTime, final Sink sink)
  {
    final String threadName = StringUtils.format(
        "%s-%s-persist-n-merge", schema.getDataSource(), DateTimes.utc(truncatedTime)
    );
    mergeExecutor.execute(
        new ThreadRenamingRunnable(threadName)
        {
          final Interval interval = sink.getInterval();
          Stopwatch mergeStopwatch = null;

          @Override
          public void doRun()
          {
            try {
              // Bail out if this sink has been abandoned by a previously-executed task.
              if (sinks.get(truncatedTime) != sink) {
                log.info("Sink[%s] was abandoned, bailing out of persist-n-merge.", sink);
                return;
              }

              // Use a file to indicate that pushing has completed.
              final File persistDir = computePersistDir(schema, interval);
              final File mergedTarget = new File(persistDir, "merged");
              final File isPushedMarker = new File(persistDir, "isPushedMarker");

              if (!isPushedMarker.exists()) {
                removeSegment(sink, mergedTarget);
                if (mergedTarget.exists()) {
                  log.wtf("Merged target[%s] exists?!", mergedTarget);
                  return;
                }
              } else {
                log.info("Already pushed sink[%s]", sink);
                return;
              }

            /*
            Note: it the plumber crashes after persisting a subset of hydrants then might duplicate data as these
            hydrants will be read but older commitMetadata will be used. fixing this possibly needs structural
            changes to plumber.
             */
              for (FireHydrant hydrant : sink) {
                synchronized (hydrant) {
                  if (!hydrant.hasSwapped()) {
                    log.info("Hydrant[%s] hasn't swapped yet, swapping. Sink[%s]", hydrant, sink);
                    final int rowCount = persistHydrant(hydrant, schema, interval, null);
                    metrics.incrementRowOutputCount(rowCount);
                  }
                }
              }
              final long mergeThreadCpuTime = VMUtils.safeGetThreadCpuTime();
              mergeStopwatch = Stopwatch.createStarted();

              final File mergedFile;
              List<QueryableIndex> indexes = Lists.newArrayList();
              Closer closer = Closer.create();
              try {
                for (FireHydrant fireHydrant : sink) {
                  Pair<Segment, Closeable> segmentAndCloseable = fireHydrant.getAndIncrementSegment();
                  final QueryableIndex queryableIndex = segmentAndCloseable.lhs.asQueryableIndex();
                  log.info("Adding hydrant[%s]", fireHydrant);
                  indexes.add(queryableIndex);
                  closer.register(segmentAndCloseable.rhs);
                }

                mergedFile = indexMerger.mergeQueryableIndex(
                    indexes,
                    schema.getGranularitySpec().isRollup(),
                    schema.getAggregators(),
                    mergedTarget,
                    config.getIndexSpec(),
                    config.getSegmentWriteOutMediumFactory()
                );
              }
              catch (Throwable t) {
                throw closer.rethrow(t);
              }
              finally {
                closer.close();
              }

              // emit merge metrics before publishing segment
              metrics.incrementMergeCpuTime(VMUtils.safeGetThreadCpuTime() - mergeThreadCpuTime);
              metrics.incrementMergeTimeMillis(mergeStopwatch.elapsed(TimeUnit.MILLISECONDS));

              log.info("Pushing [%s] to deep storage", sink.getSegment().getIdentifier());

              DataSegment segment = dataSegmentPusher.push(
                  mergedFile,
                  sink.getSegment().withDimensions(IndexMerger.getMergedDimensionsFromQueryableIndexes(indexes)),
                  false
              );
              log.info("Inserting [%s] to the metadata store", sink.getSegment().getIdentifier());
              segmentPublisher.publishSegment(segment);

              if (!isPushedMarker.createNewFile()) {
                log.makeAlert("Failed to create marker file for [%s]", schema.getDataSource())
                   .addData("interval", sink.getInterval())
                   .addData("partitionNum", segment.getShardSpec().getPartitionNum())
                   .addData("marker", isPushedMarker)
                   .emit();
              }
            }
            catch (Exception e) {
              metrics.incrementFailedHandoffs();
              log.makeAlert(e, "Failed to persist merged index[%s]", schema.getDataSource())
                 .addData("interval", interval)
                 .emit();
              if (shuttingDown) {
                // We're trying to shut down, and this segment failed to push. Let's just get rid of it.
                // This call will also delete possibly-partially-written files, so we don't need to do it explicitly.
                cleanShutdown = false;
                abandonSegment(truncatedTime, sink);
              }
            }
            finally {
              if (mergeStopwatch != null) {
                mergeStopwatch.stop();
              }
            }
          }
        }
    );
    handoffNotifier.registerSegmentHandoffCallback(
        new SegmentDescriptor(sink.getInterval(), sink.getVersion(), config.getShardSpec().getPartitionNum()),
        mergeExecutor, new Runnable()
        {
          @Override
          public void run()
          {
            abandonSegment(sink.getInterval().getStartMillis(), sink);
            metrics.incrementHandOffCount();
          }
        }
    );
  }

  @Override
  public void finishJob()
  {
    log.info("Shutting down...");

    shuttingDown = true;

    for (final Map.Entry<Long, Sink> entry : sinks.entrySet()) {
      entry.getValue().clearDedupCache();
      persistAndMerge(entry.getKey(), entry.getValue());
    }

    final long forceEndWaitTime = System.currentTimeMillis() + config.getHandoffConditionTimeout();
    while (!sinks.isEmpty()) {
      try {
        log.info(
            "Cannot shut down yet! Sinks remaining: %s",
            Joiner.on(", ").join(
                Iterables.transform(
                    sinks.values(),
                    new Function<Sink, String>()
                    {
                      @Override
                      public String apply(Sink input)
                      {
                        return input.getSegment().getIdentifier();
                      }
                    }
                )
            )
        );

        synchronized (handoffCondition) {
          while (!sinks.isEmpty()) {
            if (config.getHandoffConditionTimeout() == 0) {
              handoffCondition.wait();
            } else {
              long curr = System.currentTimeMillis();
              if (forceEndWaitTime - curr > 0) {
                handoffCondition.wait(forceEndWaitTime - curr);
              } else {
                throw new ISE(
                    "Segment handoff wait timeout. [%s] segments might not have completed handoff.",
                    sinks.size()
                );
              }
            }
          }
        }
      }
      catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }

    handoffNotifier.close();
    shutdownExecutors();

    stopped = true;

    if (!cleanShutdown) {
      throw new ISE("Exception occurred during persist and merge.");
    }
  }

  private void resetNextFlush()
  {
    nextFlush = DateTimes.nowUtc().plus(config.getIntermediatePersistPeriod()).getMillis();
  }

  protected void initializeExecutors()
  {
    final int maxPendingPersists = config.getMaxPendingPersists();

    if (persistExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      persistExecutor = Execs.newBlockingSingleThreaded(
          "plumber_persist_%d",
          maxPendingPersists,
          TaskThreadPriority.getThreadPriorityFromTaskPriority(config.getPersistThreadPriority())
      );
    }
    if (mergeExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      mergeExecutor = Execs.newBlockingSingleThreaded(
          "plumber_merge_%d",
          1,
          TaskThreadPriority.getThreadPriorityFromTaskPriority(config.getMergeThreadPriority())
      );
    }

    if (scheduledExecutor == null) {
      scheduledExecutor = Execs.scheduledSingleThreaded("plumber_scheduled_%d");
    }
  }

  protected void shutdownExecutors()
  {
    // scheduledExecutor is shutdown here
    if (scheduledExecutor != null) {
      scheduledExecutor.shutdown();
      persistExecutor.shutdown();
      mergeExecutor.shutdown();
    }
  }

  protected Object bootstrapSinksFromDisk()
  {
    final VersioningPolicy versioningPolicy = config.getVersioningPolicy();

    File baseDir = computeBaseDir(schema);
    if (baseDir == null || !baseDir.exists()) {
      return null;
    }

    File[] files = baseDir.listFiles();
    if (files == null) {
      return null;
    }

    Object metadata = null;
    long latestCommitTime = 0;
    for (File sinkDir : files) {
      final Interval sinkInterval = Intervals.of(sinkDir.getName().replace("_", "/"));

      //final File[] sinkFiles = sinkDir.listFiles();
      // To avoid reading and listing of "merged" dir
      final File[] sinkFiles = sinkDir.listFiles(
          new FilenameFilter()
          {
            @Override
            public boolean accept(File dir, String fileName)
            {
              return !(Ints.tryParse(fileName) == null);
            }
          }
      );
      Arrays.sort(
          sinkFiles,
          new Comparator<File>()
          {
            @Override
            public int compare(File o1, File o2)
            {
              try {
                return Ints.compare(Integer.parseInt(o1.getName()), Integer.parseInt(o2.getName()));
              }
              catch (NumberFormatException e) {
                log.error(e, "Couldn't compare as numbers? [%s][%s]", o1, o2);
                return o1.compareTo(o2);
              }
            }
          }
      );
      boolean isCorrupted = false;
      List<FireHydrant> hydrants = Lists.newArrayList();
      for (File segmentDir : sinkFiles) {
        log.info("Loading previously persisted segment at [%s]", segmentDir);

        // Although this has been tackled at start of this method.
        // Just a doubly-check added to skip "merged" dir. from being added to hydrants
        // If 100% sure that this is not needed, this check can be removed.
        if (Ints.tryParse(segmentDir.getName()) == null) {
          continue;
        }
        QueryableIndex queryableIndex = null;
        try {
          queryableIndex = indexIO.loadIndex(segmentDir);
        }
        catch (IOException e) {
          log.error(e, "Problem loading segmentDir from disk.");
          isCorrupted = true;
        }
        if (isCorrupted) {
          try {
            File corruptSegmentDir = computeCorruptedFileDumpDir(segmentDir, schema);
            log.info("Renaming %s to %s", segmentDir.getAbsolutePath(), corruptSegmentDir.getAbsolutePath());
            FileUtils.copyDirectory(segmentDir, corruptSegmentDir);
            FileUtils.deleteDirectory(segmentDir);
          }
          catch (Exception e1) {
            log.error(e1, "Failed to rename %s", segmentDir.getAbsolutePath());
          }
          //Note: skipping corrupted segment might lead to dropping some data. This strategy should be changed
          //at some point.
          continue;
        }
        Metadata segmentMetadata = queryableIndex.getMetadata();
        if (segmentMetadata != null) {
          Object timestampObj = segmentMetadata.get(COMMIT_METADATA_TIMESTAMP_KEY);
          if (timestampObj != null) {
            long timestamp = ((Long) timestampObj).longValue();
            if (timestamp > latestCommitTime) {
              log.info(
                  "Found metaData [%s] with latestCommitTime [%s] greater than previous recorded [%s]",
                  queryableIndex.getMetadata(), timestamp, latestCommitTime
              );
              latestCommitTime = timestamp;
              metadata = queryableIndex.getMetadata().get(COMMIT_METADATA_KEY);
            }
          }
        }
        hydrants.add(
            new FireHydrant(
                new QueryableIndexSegment(
                    DataSegment.makeDataSegmentIdentifier(
                        schema.getDataSource(),
                        sinkInterval.getStart(),
                        sinkInterval.getEnd(),
                        versioningPolicy.getVersion(sinkInterval),
                        config.getShardSpec()
                    ),
                    queryableIndex
                ),
                Integer.parseInt(segmentDir.getName())
            )
        );
      }
      if (hydrants.isEmpty()) {
        // Probably encountered a corrupt sink directory
        log.warn(
            "Found persisted segment directory with no intermediate segments present at %s, skipping sink creation.",
            sinkDir.getAbsolutePath()
        );
        continue;
      }
      final Sink currSink = new Sink(
          sinkInterval,
          schema,
          config.getShardSpec(),
          versioningPolicy.getVersion(sinkInterval),
          config.getMaxRowsInMemory(),
          TuningConfigs.getMaxBytesInMemoryOrDefault(config.getMaxBytesInMemory()),
          config.isReportParseExceptions(),
          config.getDedupColumn(),
          hydrants
      );
      addSink(currSink);
    }
    return metadata;
  }

  private void addSink(final Sink sink)
  {
    sinks.put(sink.getInterval().getStartMillis(), sink);
    metrics.setSinkCount(sinks.size());
    sinkTimeline.add(
        sink.getInterval(),
        sink.getVersion(),
        new SingleElementPartitionChunk<Sink>(sink)
    );
    try {
      segmentAnnouncer.announceSegment(sink.getSegment());
    }
    catch (IOException e) {
      log.makeAlert(e, "Failed to announce new segment[%s]", schema.getDataSource())
         .addData("interval", sink.getInterval())
         .emit();
    }
    clearDedupCache();
  }

  protected void startPersistThread()
  {
    final Granularity segmentGranularity = schema.getGranularitySpec().getSegmentGranularity();
    final Period windowPeriod = config.getWindowPeriod();

    final DateTime truncatedNow = segmentGranularity.bucketStart(DateTimes.nowUtc());
    final long windowMillis = windowPeriod.toStandardDuration().getMillis();

    log.info(
        "Expect to run at [%s]",
        DateTimes.nowUtc().plus(
            new Duration(
                System.currentTimeMillis(),
                segmentGranularity.increment(truncatedNow).getMillis() + windowMillis
            )
        )
    );

    String threadName = StringUtils.format(
        "%s-overseer-%d",
        schema.getDataSource(),
        config.getShardSpec().getPartitionNum()
    );
    ThreadRenamingCallable<ScheduledExecutors.Signal> threadRenamingCallable =
        new ThreadRenamingCallable<ScheduledExecutors.Signal>(threadName)
        {
          @Override
          public ScheduledExecutors.Signal doCall()
          {
            if (stopped) {
              log.info("Stopping merge-n-push overseer thread");
              return ScheduledExecutors.Signal.STOP;
            }

            mergeAndPush();

            if (stopped) {
              log.info("Stopping merge-n-push overseer thread");
              return ScheduledExecutors.Signal.STOP;
            } else {
              return ScheduledExecutors.Signal.REPEAT;
            }
          }
        };
    Duration initialDelay = new Duration(
        System.currentTimeMillis(),
        segmentGranularity.increment(truncatedNow).getMillis() + windowMillis
    );
    Duration rate = new Duration(truncatedNow, segmentGranularity.increment(truncatedNow));
    ScheduledExecutors.scheduleAtFixedRate(scheduledExecutor, initialDelay, rate, threadRenamingCallable);
  }

  private void clearDedupCache()
  {
    long minTimestamp = getAllowedMinTime().getMillis();

    for (Map.Entry<Long, Sink> entry : sinks.entrySet()) {
      final Long intervalStart = entry.getKey();
      if (intervalStart < minTimestamp) {
        entry.getValue().clearDedupCache();
      }
    }
  }

  private DateTime getAllowedMinTime()
  {
    final Granularity segmentGranularity = schema.getGranularitySpec().getSegmentGranularity();
    final Period windowPeriod = config.getWindowPeriod();

    final long windowMillis = windowPeriod.toStandardDuration().getMillis();
    return segmentGranularity.bucketStart(
        DateTimes.utc(Math.max(windowMillis, rejectionPolicy.getCurrMaxTime().getMillis()) - windowMillis)
    );
  }

  private void mergeAndPush()
  {
    log.info("Starting merge and push.");
    DateTime minTimestampAsDate = getAllowedMinTime();
    long minTimestamp = minTimestampAsDate.getMillis();

    log.info(
        "Found [%,d] segments. Attempting to hand off segments that start before [%s].",
        sinks.size(),
        minTimestampAsDate
    );

    List<Map.Entry<Long, Sink>> sinksToPush = Lists.newArrayList();
    for (Map.Entry<Long, Sink> entry : sinks.entrySet()) {
      final Long intervalStart = entry.getKey();
      if (intervalStart < minTimestamp) {
        log.info("Adding entry [%s] for merge and push.", entry);
        sinksToPush.add(entry);
        entry.getValue().clearDedupCache();
      } else {
        log.info(
            "Skipping persist and merge for entry [%s] : Start time [%s] >= [%s] min timestamp required in this run. Segment will be picked up in a future run.",
            entry,
            DateTimes.utc(intervalStart),
            minTimestampAsDate
        );
      }
    }

    log.info("Found [%,d] sinks to persist and merge", sinksToPush.size());

    for (final Map.Entry<Long, Sink> entry : sinksToPush) {
      persistAndMerge(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Unannounces a given sink and removes all local references to it. It is important that this is only called
   * from the single-threaded mergeExecutor, since otherwise chaos may ensue if merged segments are deleted while
   * being created.
   *
   * @param truncatedTime sink key
   * @param sink          sink to unannounce
   */
  protected void abandonSegment(final long truncatedTime, final Sink sink)
  {
    if (sinks.containsKey(truncatedTime)) {
      try {
        segmentAnnouncer.unannounceSegment(sink.getSegment());
        removeSegment(sink, computePersistDir(schema, sink.getInterval()));
        log.info("Removing sinkKey %d for segment %s", truncatedTime, sink.getSegment().getIdentifier());
        sinks.remove(truncatedTime);
        metrics.setSinkCount(sinks.size());
        sinkTimeline.remove(
            sink.getInterval(),
            sink.getVersion(),
            new SingleElementPartitionChunk<>(sink)
        );
        for (FireHydrant hydrant : sink) {
          cache.close(SinkQuerySegmentWalker.makeHydrantCacheIdentifier(hydrant));
          hydrant.swapSegment(null);
        }
        synchronized (handoffCondition) {
          handoffCondition.notifyAll();
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Unable to abandon old segment for dataSource[%s]", schema.getDataSource())
           .addData("interval", sink.getInterval())
           .emit();
      }
    }
  }

  protected File computeBaseDir(DataSchema schema)
  {
    return new File(config.getBasePersistDirectory(), schema.getDataSource());
  }

  protected File computeCorruptedFileDumpDir(File persistDir, DataSchema schema)
  {
    return new File(
        persistDir.getAbsolutePath()
                  .replace(schema.getDataSource(), "corrupted" + File.pathSeparator + schema.getDataSource())
    );
  }

  protected File computePersistDir(DataSchema schema, Interval interval)
  {
    return new File(computeBaseDir(schema), interval.toString().replace("/", "_"));
  }

  /**
   * Persists the given hydrant and returns the number of rows persisted
   *
   * @param indexToPersist hydrant to persist
   * @param schema         datasource schema
   * @param interval       interval to persist
   *
   * @return the number of rows persisted
   */
  protected int persistHydrant(
      FireHydrant indexToPersist,
      DataSchema schema,
      Interval interval,
      Map<String, Object> metadataElems
  )
  {
    synchronized (indexToPersist) {
      if (indexToPersist.hasSwapped()) {
        log.info(
            "DataSource[%s], Interval[%s], Hydrant[%s] already swapped. Ignoring request to persist.",
            schema.getDataSource(), interval, indexToPersist
        );
        return 0;
      }

      log.info(
          "DataSource[%s], Interval[%s], Metadata [%s] persisting Hydrant[%s]",
          schema.getDataSource(),
          interval,
          metadataElems,
          indexToPersist
      );
      try {
        int numRows = indexToPersist.getIndex().size();

        final IndexSpec indexSpec = config.getIndexSpec();

        indexToPersist.getIndex().getMetadata().putAll(metadataElems);
        final File persistedFile = indexMerger.persist(
            indexToPersist.getIndex(),
            interval,
            new File(computePersistDir(schema, interval), String.valueOf(indexToPersist.getCount())),
            indexSpec,
            config.getSegmentWriteOutMediumFactory()
        );

        indexToPersist.swapSegment(
            new QueryableIndexSegment(
                indexToPersist.getSegmentIdentifier(),
                indexIO.loadIndex(persistedFile)
            )
        );
        return numRows;
      }
      catch (IOException e) {
        log.makeAlert("dataSource[%s] -- incremental persist failed", schema.getDataSource())
           .addData("interval", interval)
           .addData("count", indexToPersist.getCount())
           .emit();

        throw Throwables.propagate(e);
      }
    }
  }

  private void removeSegment(final Sink sink, final File target)
  {
    if (target.exists()) {
      try {
        log.info("Deleting Index File[%s]", target);
        FileUtils.deleteDirectory(target);
      }
      catch (Exception e) {
        log.makeAlert(e, "Unable to remove file for dataSource[%s]", schema.getDataSource())
           .addData("file", target)
           .addData("interval", sink.getInterval())
           .emit();
      }
    }
  }
}
