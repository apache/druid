/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.client.CachingQueryRunner;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.common.guava.ThreadRenamingCallable;
import io.druid.concurrent.Execs;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.query.BySegmentQueryRunner;
import io.druid.query.MetricsEmittingQueryRunner;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.QueryToolChest;
import io.druid.query.ReportTimelineMissingSegmentQueryRunner;
import io.druid.query.SegmentDescriptor;
import io.druid.query.spec.SpecificSegmentQueryRunner;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.FireHydrant;
import io.druid.segment.realtime.plumber.Sink;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 */
public class AppenderatorImpl implements Appenderator
{
  private static final EmittingLogger log = new EmittingLogger(AppenderatorImpl.class);
  private static final int WARN_DELAY = 1000;
  private static final String IDENTIFIER_FILE_NAME = "identifier.json";
  private static final String CONTEXT_SKIP_INCREMENTAL_SEGMENT = "skipIncrementalSegment";

  private final DataSchema schema;
  private final AppenderatorConfig tuningConfig;
  private final FireDepartmentMetrics metrics;
  private final DataSegmentPusher dataSegmentPusher;
  private final ObjectMapper objectMapper;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final ServiceEmitter emitter;
  private final ExecutorService queryExecutorService;
  private final IndexIO indexIO;
  private final IndexMerger indexMerger;
  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final Map<SegmentIdentifier, Sink> sinks = Maps.newConcurrentMap();
  private final Set<SegmentIdentifier> droppingSinks = Sets.newConcurrentHashSet();
  private final VersionedIntervalTimeline<String, Sink> sinkTimeline = new VersionedIntervalTimeline<>(
      String.CASE_INSENSITIVE_ORDER
  );

  private volatile ListeningExecutorService persistExecutor = null;
  private volatile ListeningExecutorService mergeExecutor = null;
  private volatile long nextFlush;
  private volatile FileLock basePersistDirLock = null;
  private volatile FileChannel basePersistDirLockChannel = null;

  public AppenderatorImpl(
      DataSchema schema,
      AppenderatorConfig tuningConfig,
      FireDepartmentMetrics metrics,
      DataSegmentPusher dataSegmentPusher,
      ObjectMapper objectMapper,
      QueryRunnerFactoryConglomerate conglomerate,
      DataSegmentAnnouncer segmentAnnouncer,
      ServiceEmitter emitter,
      ExecutorService queryExecutorService,
      IndexIO indexIO,
      IndexMerger indexMerger,
      Cache cache,
      CacheConfig cacheConfig
  )
  {
    this.schema = Preconditions.checkNotNull(schema, "schema");
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
    this.metrics = Preconditions.checkNotNull(metrics, "metrics");
    this.dataSegmentPusher = Preconditions.checkNotNull(dataSegmentPusher, "dataSegmentPusher");
    this.objectMapper = Preconditions.checkNotNull(objectMapper, "objectMapper");
    this.conglomerate = conglomerate;
    this.segmentAnnouncer = segmentAnnouncer;
    this.emitter = emitter;
    this.queryExecutorService = queryExecutorService;
    this.indexIO = indexIO;
    this.indexMerger = indexMerger;
    this.cache = cache;
    this.cacheConfig = cacheConfig;

    if (conglomerate != null) {
      // If we're not querying (no conglomerate) then it's ok for the other query stuff to be null.
      // But otherwise, we need them all.
      Preconditions.checkNotNull(segmentAnnouncer, "segmentAnnouncer");
      Preconditions.checkNotNull(emitter, "emitter");
      Preconditions.checkNotNull(queryExecutorService, "queryExecutorService");
      Preconditions.checkNotNull(cache, "cache");
      Preconditions.checkNotNull(cacheConfig, "cacheConfig");

      if (!cache.isLocal()) {
        log.error("Configured cache is not local, caching will not be enabled");
      }
    }

    log.info("Creating appenderator for dataSource[%s]", schema.getDataSource());
  }

  @Override
  public String getDataSource()
  {
    return schema.getDataSource();
  }

  @Override
  public Object startJob()
  {
    tuningConfig.getBasePersistDirectory().mkdirs();
    lockBasePersistDirectory();
    final Object retVal = bootstrapSinksFromDisk();
    initializeExecutors();
    resetNextFlush();
    return retVal;
  }

  @Override
  public int add(
      final SegmentIdentifier identifier,
      final InputRow row,
      final Supplier<Committer> committerSupplier
  ) throws IndexSizeExceededException, SegmentNotWritableException
  {
    if (!identifier.getDataSource().equals(schema.getDataSource())) {
      throw new IAE(
          "Expected dataSource[%s] but was asked to insert row for dataSource[%s]?!",
          schema.getDataSource(),
          identifier.getDataSource()
      );
    }

    final Sink sink = getOrCreateSink(identifier);
    int sinkRetVal;

    try {
      sinkRetVal = sink.add(row);
    }
    catch (IndexSizeExceededException e) {
      // Try one more time after swapping, then throw the exception out if it happens again.
      persistAll(committerSupplier.get());
      sinkRetVal = sink.add(row);
    }

    if (!sink.canAppendRow() || System.currentTimeMillis() > nextFlush) {
      persistAll(committerSupplier.get());
    }

    if (sinkRetVal < 0) {
      throw new SegmentNotWritableException("Attempt to add row to swapped-out sink for segment[%s].", identifier);
    } else {
      return sink.getNumRows();
    }
  }

  @Override
  public List<SegmentIdentifier> getSegments()
  {
    return ImmutableList.copyOf(sinks.keySet());
  }

  @Override
  public int getRowCount(final SegmentIdentifier identifier)
  {
    final Sink sink = sinks.get(identifier);

    if (sink == null) {
      throw new ISE("No such sink: %s", identifier);
    } else {
      return sink.getNumRows();
    }
  }

  private Sink getOrCreateSink(final SegmentIdentifier identifier)
  {
    Sink retVal = sinks.get(identifier);

    if (retVal == null) {
      retVal = new Sink(
          identifier.getInterval(),
          schema,
          identifier.getShardSpec(),
          identifier.getVersion(),
          tuningConfig.getMaxRowsInMemory(),
          tuningConfig.isReportParseExceptions()
      );

      try {
        segmentAnnouncer.announceSegment(retVal.getSegment());
      }
      catch (IOException e) {
        log.makeAlert(e, "Failed to announce new segment[%s]", schema.getDataSource())
           .addData("interval", retVal.getInterval())
           .emit();
      }

      sinks.put(identifier, retVal);
      sinkTimeline.add(retVal.getInterval(), retVal.getVersion(), identifier.getShardSpec().createChunk(retVal));
    }

    return retVal;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, final Iterable<Interval> intervals)
  {
    if (conglomerate == null) {
      throw new IllegalStateException("Don't query me, bro.");
    }

    final Iterable<SegmentDescriptor> specs = FunctionalIterable
        .create(intervals)
        .transformCat(
            new Function<Interval, Iterable<TimelineObjectHolder<String, Sink>>>()
            {
              @Override
              public Iterable<TimelineObjectHolder<String, Sink>> apply(final Interval interval)
              {
                return sinkTimeline.lookup(interval);
              }
            }
        )
        .transformCat(
            new Function<TimelineObjectHolder<String, Sink>, Iterable<SegmentDescriptor>>()
            {
              @Override
              public Iterable<SegmentDescriptor> apply(final TimelineObjectHolder<String, Sink> holder)
              {
                return FunctionalIterable
                    .create(holder.getObject())
                    .transform(
                        new Function<PartitionChunk<Sink>, SegmentDescriptor>()
                        {
                          @Override
                          public SegmentDescriptor apply(final PartitionChunk<Sink> chunk)
                          {
                            return new SegmentDescriptor(
                                holder.getInterval(),
                                holder.getVersion(),
                                chunk.getChunkNumber()
                            );
                          }
                        }
                    );
              }
            }
        );

    return getQueryRunnerForSegments(query, specs);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, final Iterable<SegmentDescriptor> specs)
  {
    if (conglomerate == null) {
      throw new IllegalStateException("Don't query me, bro.");
    }

    // We only handle one dataSource. Make sure it's in the list of names, then ignore from here on out.
    if (!query.getDataSource().getNames().contains(getDataSource())) {
      log.makeAlert("Received query for unknown dataSource")
         .addData("dataSource", query.getDataSource())
         .emit();
      return new NoopQueryRunner<>();
    }

    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
    if (factory == null) {
      throw new ISE("Unknown query type[%s].", query.getClass());
    }

    final QueryToolChest<T, Query<T>> toolchest = factory.getToolchest();
    final Function<Query<T>, ServiceMetricEvent.Builder> builderFn =
        new Function<Query<T>, ServiceMetricEvent.Builder>()
        {

          @Override
          public ServiceMetricEvent.Builder apply(@Nullable Query<T> input)
          {
            return toolchest.makeMetricBuilder(query);
          }
        };
    final boolean skipIncrementalSegment = query.getContextValue(CONTEXT_SKIP_INCREMENTAL_SEGMENT, false);

    return toolchest.mergeResults(
        factory.mergeRunners(
            queryExecutorService,
            FunctionalIterable
                .create(specs)
                .transform(
                    new Function<SegmentDescriptor, QueryRunner<T>>()
                    {
                      @Override
                      public QueryRunner<T> apply(final SegmentDescriptor descriptor)
                      {
                        final PartitionHolder<Sink> holder = sinkTimeline.findEntry(
                            descriptor.getInterval(),
                            descriptor.getVersion()
                        );
                        if (holder == null) {
                          return new ReportTimelineMissingSegmentQueryRunner<>(descriptor);
                        }

                        final PartitionChunk<Sink> chunk = holder.getChunk(descriptor.getPartitionNumber());
                        if (chunk == null) {
                          return new ReportTimelineMissingSegmentQueryRunner<>(descriptor);
                        }

                        final Sink theSink = chunk.getObject();

                        return new SpecificSegmentQueryRunner<>(
                            new MetricsEmittingQueryRunner<>(
                                emitter,
                                builderFn,
                                new BySegmentQueryRunner<T>(
                                    theSink.getSegment().getIdentifier(),
                                    descriptor.getInterval().getStart(),
                                    factory.mergeRunners(
                                        MoreExecutors.sameThreadExecutor(),
                                        Iterables.transform(
                                            theSink,
                                            new Function<FireHydrant, QueryRunner<T>>()
                                            {
                                              @Override
                                              public QueryRunner<T> apply(final FireHydrant hydrant)
                                              {
                                                // Hydrant might swap at any point, but if it's swapped at the start
                                                // then we know it's *definitely* swapped.
                                                final boolean hydrantDefinitelySwapped = hydrant.hasSwapped();

                                                if (skipIncrementalSegment && !hydrantDefinitelySwapped) {
                                                  return new NoopQueryRunner<>();
                                                }

                                                // Prevent the underlying segment from swapping when its being iterated
                                                final Pair<Segment, Closeable> segment = hydrant.getAndIncrementSegment();
                                                try {
                                                  QueryRunner<T> baseRunner = QueryRunnerHelper.makeClosingQueryRunner(
                                                      factory.createRunner(segment.lhs),
                                                      segment.rhs
                                                  );

                                                  if (hydrantDefinitelySwapped // only use caching if data is immutable
                                                      && cache.isLocal() // hydrants may not be in sync between replicas, make sure cache is local
                                                      ) {
                                                    return new CachingQueryRunner<>(
                                                        makeHydrantCacheIdentifier(hydrant, segment.lhs),
                                                        descriptor,
                                                        objectMapper,
                                                        cache,
                                                        toolchest,
                                                        baseRunner,
                                                        MoreExecutors.sameThreadExecutor(),
                                                        cacheConfig
                                                    );
                                                  } else {
                                                    return baseRunner;
                                                  }
                                                }
                                                catch (RuntimeException e) {
                                                  CloseQuietly.close(segment.rhs);
                                                  throw e;
                                                }
                                              }
                                            }
                                        )
                                    )
                                )
                            ).withWaitMeasuredFromNow(),
                            new SpecificSegmentSpec(descriptor)
                        );
                      }
                    }
                )
        )
    );
  }

  @Override
  public void clear() throws InterruptedException
  {
    // Drop commit metadata, then abandon all segments.

    try {
      final ListenableFuture<?> uncommitFuture = persistExecutor.submit(
          new Callable<Object>()
          {
            @Override
            public Object call() throws Exception
            {
              objectMapper.writeValue(computeCommitFile(), Committed.nil());
              return null;
            }
          }
      );

      // Await uncommit.
      uncommitFuture.get();

      // Drop everything.
      final List<ListenableFuture<?>> futures = Lists.newArrayList();
      for (Map.Entry<SegmentIdentifier, Sink> entry : sinks.entrySet()) {
        futures.add(abandonSegment(entry.getKey(), entry.getValue(), true));
      }

      // Await dropping.
      Futures.allAsList(futures).get();
    }
    catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ListenableFuture<?> drop(final SegmentIdentifier identifier)
  {
    final Sink sink = sinks.get(identifier);
    if (sink != null) {
      return abandonSegment(identifier, sink, true);
    } else {
      return Futures.immediateFuture(null);
    }
  }

  @Override
  public ListenableFuture<Object> persistAll(final Committer committer)
  {
    // Submit persistAll task to the persistExecutor

    final Map<SegmentIdentifier, Integer> commitHydrants = Maps.newHashMap();
    final List<Pair<FireHydrant, SegmentIdentifier>> indexesToPersist = Lists.newArrayList();
    final Set<SegmentIdentifier> identifiers = sinks.keySet();
    for (SegmentIdentifier identifier : identifiers) {
      final Sink sink = sinks.get(identifier);
      final List<FireHydrant> hydrants = Lists.newArrayList(sink);
      commitHydrants.put(identifier, hydrants.size());

      final int limit = sink.isWritable() ? hydrants.size() - 1 : hydrants.size();

      for (FireHydrant hydrant : hydrants.subList(0, limit)) {
        if (!hydrant.hasSwapped()) {
          log.info("Hydrant[%s] hasn't persisted yet, persisting. Segment[%s]", hydrant, identifier);
          indexesToPersist.add(Pair.of(hydrant, identifier));
        }
      }

      if (sink.swappable()) {
        indexesToPersist.add(Pair.of(sink.swap(), identifier));
      }
    }

    log.info("Submitting persist runnable for dataSource[%s]", schema.getDataSource());

    final String threadName = String.format("%s-incremental-persist", schema.getDataSource());
    final Object commitMetadata = committer.getMetadata();
    final Stopwatch runExecStopwatch = Stopwatch.createStarted();
    final Stopwatch persistStopwatch = Stopwatch.createStarted();
    final ListenableFuture<Object> future = persistExecutor.submit(
        new ThreadRenamingCallable<Object>(threadName)
        {
          @Override
          public Object doCall()
          {
            try {
              for (Pair<FireHydrant, SegmentIdentifier> pair : indexesToPersist) {
                metrics.incrementRowOutputCount(persistHydrant(pair.lhs, pair.rhs));
              }

              log.info(
                  "Committing metadata[%s] for sinks[%s].", commitMetadata, Joiner.on(", ").join(
                      Iterables.transform(
                          commitHydrants.entrySet(),
                          new Function<Map.Entry<SegmentIdentifier, Integer>, String>()
                          {
                            @Override
                            public String apply(Map.Entry<SegmentIdentifier, Integer> entry)
                            {
                              return String.format("%s:%d", entry.getKey().getIdentifierAsString(), entry.getValue());
                            }
                          }
                      )
                  )
              );

              committer.run();
              objectMapper.writeValue(computeCommitFile(), Committed.create(commitHydrants, commitMetadata));

              return commitMetadata;
            }
            catch (Exception e) {
              metrics.incrementFailedPersists();
              throw Throwables.propagate(e);
            }
            finally {
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

    return future;
  }

  @Override
  public ListenableFuture<SegmentsAndMetadata> push(
      final List<SegmentIdentifier> identifiers,
      final Committer committer
  )
  {
    final Map<SegmentIdentifier, Sink> theSinks = Maps.newHashMap();
    for (final SegmentIdentifier identifier : identifiers) {
      final Sink sink = sinks.get(identifier);
      if (sink == null) {
        throw new NullPointerException("No sink for identifier: " + identifier);
      }
      theSinks.put(identifier, sink);
      sink.finishWriting();
    }

    return Futures.transform(
        persistAll(committer),
        new Function<Object, SegmentsAndMetadata>()
        {
          @Override
          public SegmentsAndMetadata apply(Object commitMetadata)
          {
            final List<DataSegment> dataSegments = Lists.newArrayList();

            for (Map.Entry<SegmentIdentifier, Sink> entry : theSinks.entrySet()) {
              if (droppingSinks.contains(entry.getKey())) {
                log.info("Skipping push of currently-dropping sink[%s]", entry.getKey());
                continue;
              }

              final DataSegment dataSegment = mergeAndPush(entry.getKey(), entry.getValue());
              if (dataSegment != null) {
                dataSegments.add(dataSegment);
              } else {
                log.warn("mergeAndPush[%s] returned null, skipping.", entry.getKey());
              }
            }

            return new SegmentsAndMetadata(dataSegments, commitMetadata);
          }
        },
        mergeExecutor
    );
  }

  /**
   * Insert a barrier into the merge-and-push queue. When this future resolves, all pending pushes will have finished.
   * This is useful if we're going to do something that would otherwise potentially break currently in-progress
   * pushes.
   */
  private ListenableFuture<?> mergeBarrier()
  {
    return mergeExecutor.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            // Do nothing
          }
        }
    );
  }

  /**
   * Merge segment, push to deep storage. Should only be used on segments that have been fully persisted. Must only
   * be run in the single-threaded mergeExecutor.
   *
   * @param identifier sink identifier
   * @param sink       sink to push
   *
   * @return segment descriptor, or null if the sink is no longer valid
   */

  private DataSegment mergeAndPush(final SegmentIdentifier identifier, final Sink sink)
  {
    // Bail out if this sink is null or otherwise not what we expect.
    if (sinks.get(identifier) != sink) {
      log.warn("Sink for segment[%s] no longer valid, bailing out of mergeAndPush.", identifier);
      return null;
    }

    // Use a descriptor file to indicate that pushing has completed.
    final File persistDir = computePersistDir(identifier);
    final File mergedTarget = new File(persistDir, "merged");
    final File descriptorFile = computeDescriptorFile(identifier);

    // Sanity checks
    for (FireHydrant hydrant : sink) {
      if (sink.isWritable()) {
        throw new ISE("WTF?! Expected sink to be no longer writable before mergeAndPush. Segment[%s].", identifier);
      }

      synchronized (hydrant) {
        if (!hydrant.hasSwapped()) {
          throw new ISE("WTF?! Expected sink to be fully persisted before mergeAndPush. Segment[%s].", identifier);
        }
      }
    }

    try {
      if (descriptorFile.exists()) {
        // Already pushed.
        log.info("Segment[%s] already pushed.", identifier);
        return objectMapper.readValue(descriptorFile, DataSegment.class);
      }

      log.info("Pushing merged index for segment[%s].", identifier);

      removeDirectory(mergedTarget);

      if (mergedTarget.exists()) {
        throw new ISE("Merged target[%s] exists after removing?!", mergedTarget);
      }

      List<QueryableIndex> indexes = Lists.newArrayList();
      for (FireHydrant fireHydrant : sink) {
        Segment segment = fireHydrant.getSegment();
        final QueryableIndex queryableIndex = segment.asQueryableIndex();
        log.info("Adding hydrant[%s]", fireHydrant);
        indexes.add(queryableIndex);
      }

      final File mergedFile;
      mergedFile = indexMerger.mergeQueryableIndex(
          indexes,
          schema.getAggregators(),
          mergedTarget,
          tuningConfig.getIndexSpec()
      );

      QueryableIndex index = indexIO.loadIndex(mergedFile);

      DataSegment segment = dataSegmentPusher.push(
          mergedFile,
          sink.getSegment().withDimensions(Lists.newArrayList(index.getAvailableDimensions()))
      );

      objectMapper.writeValue(descriptorFile, segment);

      log.info("Pushed merged index for segment[%s], descriptor is: %s", identifier, segment);

      return segment;
    }
    catch (Exception e) {
      metrics.incrementFailedHandoffs();
      log.warn(e, "Failed to push merged index for segment[%s].", identifier);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close()
  {
    log.info("Shutting down...");

    final List<ListenableFuture<?>> futures = Lists.newArrayList();
    for (Map.Entry<SegmentIdentifier, Sink> entry : sinks.entrySet()) {
      futures.add(abandonSegment(entry.getKey(), entry.getValue(), false));
    }

    try {
      Futures.allAsList(futures).get();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn(e, "Interrupted during close()");
    }
    catch (ExecutionException e) {
      log.warn(e, "Unable to abandon existing segments during close()");
    }

    try {
      shutdownExecutors();
      Preconditions.checkState(persistExecutor.awaitTermination(365, TimeUnit.DAYS), "persistExecutor not terminated");
      Preconditions.checkState(mergeExecutor.awaitTermination(365, TimeUnit.DAYS), "mergeExecutor not terminated");
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ISE("Failed to shutdown executors during close()");
    }

    // Only unlock if executors actually shut down.
    unlockBasePersistDirectory();
  }

  private void lockBasePersistDirectory()
  {
    if (basePersistDirLock == null) {
      try {
        basePersistDirLockChannel = FileChannel.open(
            computeLockFile().toPath(),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE
        );

        basePersistDirLock = basePersistDirLockChannel.tryLock();
        if (basePersistDirLock == null) {
          throw new ISE("Cannot acquire lock on basePersistDir: %s", computeLockFile());
        }
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private void unlockBasePersistDirectory()
  {
    try {
      if (basePersistDirLock != null) {
        basePersistDirLock.release();
        basePersistDirLockChannel.close();
        basePersistDirLock = null;
      }
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private void initializeExecutors()
  {
    final int maxPendingPersists = tuningConfig.getMaxPendingPersists();

    if (persistExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      persistExecutor = MoreExecutors.listeningDecorator(
          Execs.newBlockingSingleThreaded(
              "appenderator_persist_%d", maxPendingPersists
          )
      );
    }
    if (mergeExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      mergeExecutor = MoreExecutors.listeningDecorator(
          Execs.newBlockingSingleThreaded(
              "appenderator_merge_%d", 1
          )
      );
    }
  }

  private void shutdownExecutors()
  {
    persistExecutor.shutdownNow();
    mergeExecutor.shutdownNow();
  }

  private void resetNextFlush()
  {
    nextFlush = new DateTime().plus(tuningConfig.getIntermediatePersistPeriod()).getMillis();
  }

  /**
   * Populate "sinks" and "sinkTimeline" with committed segments, and announce them with the segmentAnnouncer.
   *
   * @return persisted commit metadata
   */
  private Object bootstrapSinksFromDisk()
  {
    Preconditions.checkState(sinks.isEmpty(), "Already bootstrapped?!");

    final File baseDir = tuningConfig.getBasePersistDirectory();
    if (!baseDir.exists()) {
      return null;
    }

    final File[] files = baseDir.listFiles();
    if (files == null) {
      return null;
    }

    final File commitFile = computeCommitFile();
    final Committed committed;
    try {
      if (commitFile.exists()) {
        committed = objectMapper.readValue(commitFile, Committed.class);
      } else {
        committed = Committed.nil();
      }
    }
    catch (Exception e) {
      throw new ISE(e, "Failed to read commitFile: %s", commitFile);
    }

    log.info("Loading sinks from[%s]: %s", baseDir, committed.getHydrants().keySet());

    for (File sinkDir : files) {
      final File identifierFile = new File(sinkDir, IDENTIFIER_FILE_NAME);
      if (!identifierFile.isFile()) {
        // No identifier in this sinkDir; it must not actually be a sink directory. Skip it.
        continue;
      }

      try {
        final SegmentIdentifier identifier = objectMapper.readValue(
            new File(sinkDir, "identifier.json"),
            SegmentIdentifier.class
        );

        final int committedHydrants = committed.getCommittedHydrants(identifier.getIdentifierAsString());

        if (committedHydrants <= 0) {
          log.info("Removing uncommitted sink at [%s]", sinkDir);
          FileUtils.deleteDirectory(sinkDir);
          continue;
        }

        // To avoid reading and listing of "merged" dir and other special files
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
                return Ints.compare(Integer.parseInt(o1.getName()), Integer.parseInt(o2.getName()));
              }
            }
        );

        List<FireHydrant> hydrants = Lists.newArrayList();
        for (File hydrantDir : sinkFiles) {
          final int hydrantNumber = Integer.parseInt(hydrantDir.getName());

          if (hydrantNumber >= committedHydrants) {
            log.info("Removing uncommitted segment at [%s]", hydrantDir);
            FileUtils.deleteDirectory(hydrantDir);
          } else {
            log.info("Loading previously persisted segment at [%s]", hydrantDir);
            if (hydrantNumber != hydrants.size()) {
              throw new ISE("Missing hydrant [%,d] in sinkDir [%s].", hydrants.size(), sinkDir);
            }

            hydrants.add(
                new FireHydrant(
                    new QueryableIndexSegment(
                        identifier.getIdentifierAsString(),
                        indexIO.loadIndex(hydrantDir)
                    ),
                    hydrantNumber
                )
            );
          }
        }

        // Make sure we loaded enough hydrants.
        if (committedHydrants != hydrants.size()) {
          throw new ISE("Missing hydrant [%,d] in sinkDir [%s].", hydrants.size(), sinkDir);
        }

        Sink currSink = new Sink(
            identifier.getInterval(),
            schema,
            identifier.getShardSpec(),
            identifier.getVersion(),
            tuningConfig.getMaxRowsInMemory(),
            tuningConfig.isReportParseExceptions(),
            hydrants
        );
        sinks.put(identifier, currSink);
        sinkTimeline.add(
            currSink.getInterval(),
            currSink.getVersion(),
            identifier.getShardSpec().createChunk(currSink)
        );

        segmentAnnouncer.announceSegment(currSink.getSegment());
      }
      catch (IOException e) {
        log.makeAlert(e, "Problem loading sink[%s] from disk.", schema.getDataSource())
           .addData("sinkDir", sinkDir)
           .emit();
      }
    }

    // Make sure we loaded all committed sinks.
    final Set<String> loadedSinks = Sets.newHashSet(
        Iterables.transform(
            sinks.keySet(),
            new Function<SegmentIdentifier, String>()
            {
              @Override
              public String apply(SegmentIdentifier input)
              {
                return input.getIdentifierAsString();
              }
            }
        )
    );
    final Set<String> missingSinks = Sets.difference(committed.getHydrants().keySet(), loadedSinks);
    if (!missingSinks.isEmpty()) {
      throw new ISE("Missing committed sinks [%s]", Joiner.on(", ").join(missingSinks));
    }

    return committed.getMetadata();
  }

  private ListenableFuture<?> abandonSegment(
      final SegmentIdentifier identifier,
      final Sink sink,
      final boolean removeOnDiskData
  )
  {
    // Mark this identifier as dropping, so no future merge tasks will pick it up.
    droppingSinks.add(identifier);

    // Wait for any outstanding merges to finish, then abandon the segment inside the persist thread.
    return Futures.transform(
        mergeBarrier(),
        new Function<Object, Object>()
        {
          @Nullable
          @Override
          public Object apply(@Nullable Object input)
          {
            if (sinks.get(identifier) != sink) {
              // Only abandon sink if it is the same one originally requested to be abandoned.
              log.warn("Sink for segment[%s] no longer valid, not abandoning.");
              return null;
            }

            if (removeOnDiskData) {
              // Remove this segment from the committed list. This must be done from the persist thread.
              log.info("Removing commit metadata for segment[%s].", identifier);
              try {
                final File commitFile = computeCommitFile();
                if (commitFile.exists()) {
                  final Committed oldCommitted = objectMapper.readValue(commitFile, Committed.class);
                  objectMapper.writeValue(commitFile, oldCommitted.without(identifier.getIdentifierAsString()));
                }
              }
              catch (Exception e) {
                log.makeAlert(e, "Failed to update committed segments[%s]", schema.getDataSource())
                   .addData("identifier", identifier.getIdentifierAsString())
                   .emit();
                throw Throwables.propagate(e);
              }
            }

            // Unannounce the segment.
            try {
              segmentAnnouncer.unannounceSegment(sink.getSegment());
            }
            catch (Exception e) {
              log.makeAlert(e, "Failed to unannounce segment[%s]", schema.getDataSource())
                 .addData("identifier", identifier.getIdentifierAsString())
                 .emit();
            }

            log.info("Removing sink for segment[%s].", identifier);
            sinks.remove(identifier);
            droppingSinks.remove(identifier);
            sinkTimeline.remove(
                sink.getInterval(),
                sink.getVersion(),
                identifier.getShardSpec().createChunk(sink)
            );

            if (removeOnDiskData) {
              removeDirectory(computePersistDir(identifier));
            }

            return null;
          }
        },
        persistExecutor
    );
  }

  private File computeCommitFile()
  {
    return new File(tuningConfig.getBasePersistDirectory(), "commit.json");
  }

  private File computeLockFile()
  {
    return new File(tuningConfig.getBasePersistDirectory(), ".lock");
  }

  private File computePersistDir(SegmentIdentifier identifier)
  {
    return new File(tuningConfig.getBasePersistDirectory(), identifier.getIdentifierAsString());
  }

  private File computeIdentifierFile(SegmentIdentifier identifier)
  {
    return new File(computePersistDir(identifier), IDENTIFIER_FILE_NAME);
  }

  private File computeDescriptorFile(SegmentIdentifier identifier)
  {
    return new File(computePersistDir(identifier), "descriptor.json");
  }

  private File createPersistDirIfNeeded(SegmentIdentifier identifier) throws IOException
  {
    final File persistDir = computePersistDir(identifier);
    if (!persistDir.mkdir() && !persistDir.exists()) {
      throw new IOException(String.format("Could not create directory: %s", persistDir));
    }

    objectMapper.writeValue(computeIdentifierFile(identifier), identifier);

    return persistDir;
  }

  /**
   * Persists the given hydrant and returns the number of rows persisted. Must only be called in the single-threaded
   * persistExecutor.
   *
   * @param indexToPersist hydrant to persist
   * @param identifier     the segment this hydrant is going to be part of
   *
   * @return the number of rows persisted
   */
  private int persistHydrant(FireHydrant indexToPersist, SegmentIdentifier identifier)
  {
    synchronized (indexToPersist) {
      if (indexToPersist.hasSwapped()) {
        log.info(
            "Segment[%s], Hydrant[%s] already swapped. Ignoring request to persist.",
            identifier, indexToPersist
        );
        return 0;
      }

      log.info("Segment[%s], persisting Hydrant[%s]", identifier, indexToPersist);

      try {
        int numRows = indexToPersist.getIndex().size();

        final File persistedFile;
        final File persistDir = createPersistDirIfNeeded(identifier);
        final IndexSpec indexSpec = tuningConfig.getIndexSpec();
        persistedFile = indexMerger.persist(
            indexToPersist.getIndex(),
            identifier.getInterval(),
            new File(persistDir, String.valueOf(indexToPersist.getCount())),
            indexSpec
        );

        indexToPersist.swapSegment(
            new QueryableIndexSegment(
                indexToPersist.getSegment().getIdentifier(),
                indexIO.loadIndex(persistedFile)
            )
        );
        return numRows;
      }
      catch (IOException e) {
        log.makeAlert("dataSource[%s] -- incremental persist failed", schema.getDataSource())
           .addData("segment", identifier.getIdentifierAsString())
           .addData("count", indexToPersist.getCount())
           .emit();

        throw Throwables.propagate(e);
      }
    }
  }

  private void removeDirectory(final File target)
  {
    if (target.exists()) {
      try {
        log.info("Deleting Index File[%s]", target);
        FileUtils.deleteDirectory(target);
      }
      catch (Exception e) {
        log.makeAlert(e, "Failed to remove directory[%s]", schema.getDataSource())
           .addData("file", target)
           .emit();
      }
    }
  }

  private static String makeHydrantCacheIdentifier(FireHydrant input, Segment segment)
  {
    return segment.getIdentifier() + "_" + input.getCount();
  }
}
