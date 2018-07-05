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

package io.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.common.guava.ThreadRenamingCallable;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.RetryUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
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
import io.druid.timeline.VersionedIntervalTimeline;
import org.apache.commons.io.FileUtils;
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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 */
public class AppenderatorImpl implements Appenderator
{
  private static final EmittingLogger log = new EmittingLogger(AppenderatorImpl.class);
  private static final int WARN_DELAY = 1000;
  private static final String IDENTIFIER_FILE_NAME = "identifier.json";

  private final DataSchema schema;
  private final AppenderatorConfig tuningConfig;
  private final FireDepartmentMetrics metrics;
  private final DataSegmentPusher dataSegmentPusher;
  private final ObjectMapper objectMapper;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final IndexIO indexIO;
  private final IndexMerger indexMerger;
  private final Cache cache;
  private final Map<SegmentIdentifier, Sink> sinks = new ConcurrentHashMap<>();
  private final Set<SegmentIdentifier> droppingSinks = Sets.newConcurrentHashSet();
  private final VersionedIntervalTimeline<String, Sink> sinkTimeline = new VersionedIntervalTimeline<>(
      String.CASE_INSENSITIVE_ORDER
  );

  private final QuerySegmentWalker texasRanger;
  // This variable updated in add(), persist(), and drop()
  private final AtomicInteger rowsCurrentlyInMemory = new AtomicInteger();
  private final AtomicInteger totalRows = new AtomicInteger();
  // Synchronize persisting commitMetadata so that multiple persist threads (if present)
  // and abandon threads do not step over each other
  private final Lock commitLock = new ReentrantLock();

  private final AtomicBoolean closed = new AtomicBoolean(false);

  private volatile ListeningExecutorService persistExecutor = null;
  private volatile ListeningExecutorService pushExecutor = null;
  // use intermediate executor so that deadlock conditions can be prevented
  // where persist and push Executor try to put tasks in each other queues
  // thus creating circular dependency
  private volatile ListeningExecutorService intermediateTempExecutor = null;
  private volatile long nextFlush;
  private volatile FileLock basePersistDirLock = null;
  private volatile FileChannel basePersistDirLockChannel = null;

  private volatile Throwable persistError;

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
    this.segmentAnnouncer = Preconditions.checkNotNull(segmentAnnouncer, "segmentAnnouncer");
    this.indexIO = Preconditions.checkNotNull(indexIO, "indexIO");
    this.indexMerger = Preconditions.checkNotNull(indexMerger, "indexMerger");
    this.cache = cache;
    this.texasRanger = conglomerate == null ? null : new SinkQuerySegmentWalker(
        schema.getDataSource(),
        sinkTimeline,
        objectMapper,
        emitter,
        conglomerate,
        queryExecutorService,
        Preconditions.checkNotNull(cache, "cache"),
        cacheConfig
    );

    log.info("Created Appenderator for dataSource[%s].", schema.getDataSource());
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

  private void throwPersistErrorIfExists()
  {
    if (persistError != null) {
      throw new RE(persistError, "Error while persisting");
    }
  }

  @Override
  public AppenderatorAddResult add(
      final SegmentIdentifier identifier,
      final InputRow row,
      @Nullable final Supplier<Committer> committerSupplier,
      final boolean allowIncrementalPersists
  ) throws IndexSizeExceededException, SegmentNotWritableException
  {
    throwPersistErrorIfExists();

    if (!identifier.getDataSource().equals(schema.getDataSource())) {
      throw new IAE(
          "Expected dataSource[%s] but was asked to insert row for dataSource[%s]?!",
          schema.getDataSource(),
          identifier.getDataSource()
      );
    }

    final Sink sink = getOrCreateSink(identifier);
    metrics.reportMessageMaxTimestamp(row.getTimestampFromEpoch());
    final int sinkRowsInMemoryBeforeAdd = sink.getNumRowsInMemory();
    final int sinkRowsInMemoryAfterAdd;

    try {
      sinkRowsInMemoryAfterAdd = sink.add(row, !allowIncrementalPersists);
    }
    catch (IndexSizeExceededException e) {
      // Uh oh, we can't do anything about this! We can't persist (commit metadata would be out of sync) and we
      // can't add the row (it just failed). This should never actually happen, though, because we check
      // sink.canAddRow after returning from add.
      log.error(e, "Sink for segment[%s] was unexpectedly full!", identifier);
      throw e;
    }

    if (sinkRowsInMemoryAfterAdd < 0) {
      throw new SegmentNotWritableException("Attempt to add row to swapped-out sink for segment[%s].", identifier);
    }

    final int numAddedRows = sinkRowsInMemoryAfterAdd - sinkRowsInMemoryBeforeAdd;
    rowsCurrentlyInMemory.addAndGet(numAddedRows);
    totalRows.addAndGet(numAddedRows);

    boolean isPersistRequired = false;
    if (!sink.canAppendRow()
        || System.currentTimeMillis() > nextFlush
        || rowsCurrentlyInMemory.get() >= tuningConfig.getMaxRowsInMemory()) {
      if (allowIncrementalPersists) {
        // persistAll clears rowsCurrentlyInMemory, no need to update it.
        Futures.addCallback(
            persistAll(committerSupplier == null ? null : committerSupplier.get()),
            new FutureCallback<Object>()
            {
              @Override
              public void onSuccess(@Nullable Object result)
              {
                // do nothing
              }

              @Override
              public void onFailure(Throwable t)
              {
                persistError = t;
              }
            }
        );
      } else {
        isPersistRequired = true;
      }
    }

    return new AppenderatorAddResult(identifier, sink.getNumRows(), isPersistRequired);
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

  @Override
  public int getTotalRowCount()
  {
    return totalRows.get();
  }

  @VisibleForTesting
  int getRowsInMemory()
  {
    return rowsCurrentlyInMemory.get();
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
      metrics.setSinkCount(sinks.size());
      sinkTimeline.add(retVal.getInterval(), retVal.getVersion(), identifier.getShardSpec().createChunk(retVal));
    }

    return retVal;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, final Iterable<Interval> intervals)
  {
    if (texasRanger == null) {
      throw new IllegalStateException("Don't query me, bro.");
    }

    return texasRanger.getQueryRunnerForIntervals(query, intervals);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, final Iterable<SegmentDescriptor> specs)
  {
    if (texasRanger == null) {
      throw new IllegalStateException("Don't query me, bro.");
    }

    return texasRanger.getQueryRunnerForSegments(query, specs);
  }

  @Override
  public void clear() throws InterruptedException
  {
    // Drop commit metadata, then abandon all segments.

    try {
      throwPersistErrorIfExists();

      if (persistExecutor != null) {
        final ListenableFuture<?> uncommitFuture = persistExecutor.submit(
            new Callable<Object>()
            {
              @Override
              public Object call() throws Exception
              {
                try {
                  commitLock.lock();
                  objectMapper.writeValue(computeCommitFile(), Committed.nil());
                }
                finally {
                  commitLock.unlock();
                }
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
    }
    catch (ExecutionException e) {
      throw new RuntimeException(e);
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
  public ListenableFuture<Object> persistAll(@Nullable final Committer committer)
  {
    throwPersistErrorIfExists();

    final Map<String, Integer> currentHydrants = Maps.newHashMap();
    final List<Pair<FireHydrant, SegmentIdentifier>> indexesToPersist = Lists.newArrayList();
    int numPersistedRows = 0;
    for (SegmentIdentifier identifier : sinks.keySet()) {
      final Sink sink = sinks.get(identifier);
      if (sink == null) {
        throw new ISE("No sink for identifier: %s", identifier);
      }
      final List<FireHydrant> hydrants = Lists.newArrayList(sink);
      currentHydrants.put(identifier.getIdentifierAsString(), hydrants.size());
      numPersistedRows += sink.getNumRowsInMemory();

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

    final String threadName = StringUtils.format("%s-incremental-persist", schema.getDataSource());
    final Object commitMetadata = committer == null ? null : committer.getMetadata();
    final Stopwatch runExecStopwatch = Stopwatch.createStarted();
    final Stopwatch persistStopwatch = Stopwatch.createStarted();
    final ListenableFuture<Object> future = persistExecutor.submit(
        new ThreadRenamingCallable<Object>(threadName)
        {
          @Override
          public Object doCall() throws IOException
          {
            try {
              for (Pair<FireHydrant, SegmentIdentifier> pair : indexesToPersist) {
                metrics.incrementRowOutputCount(persistHydrant(pair.lhs, pair.rhs));
              }

              if (committer != null) {
                log.info(
                    "Committing metadata[%s] for sinks[%s].", commitMetadata, Joiner.on(", ").join(
                        currentHydrants.entrySet()
                                       .stream()
                                       .map(entry -> StringUtils.format(
                                           "%s:%d",
                                           entry.getKey(),
                                           entry.getValue()
                                       ))
                                       .collect(Collectors.toList())
                    )
                );

                committer.run();

                try {
                  commitLock.lock();
                  final Map<String, Integer> commitHydrants = Maps.newHashMap();
                  final Committed oldCommit = readCommit();
                  if (oldCommit != null) {
                    // merge current hydrants with existing hydrants
                    commitHydrants.putAll(oldCommit.getHydrants());
                  }
                  commitHydrants.putAll(currentHydrants);
                  writeCommit(new Committed(commitHydrants, commitMetadata));
                }
                finally {
                  commitLock.unlock();
                }
              }

              // return null if committer is null
              return commitMetadata;
            }
            catch (IOException e) {
              metrics.incrementFailedPersists();
              throw e;
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

    // NB: The rows are still in memory until they're done persisting, but we only count rows in active indexes.
    rowsCurrentlyInMemory.addAndGet(-numPersistedRows);

    return future;
  }

  @Override
  public ListenableFuture<SegmentsAndMetadata> push(
      final Collection<SegmentIdentifier> identifiers,
      @Nullable final Committer committer,
      final boolean useUniquePath
  )
  {
    final Map<SegmentIdentifier, Sink> theSinks = Maps.newHashMap();
    for (final SegmentIdentifier identifier : identifiers) {
      final Sink sink = sinks.get(identifier);
      if (sink == null) {
        throw new ISE("No sink for identifier: %s", identifier);
      }
      theSinks.put(identifier, sink);
      sink.finishWriting();
    }

    return Futures.transform(
        // We should always persist all segments regardless of the input because metadata should be committed for all
        // segments.
        persistAll(committer),
        (Function<Object, SegmentsAndMetadata>) commitMetadata -> {
          final List<DataSegment> dataSegments = Lists.newArrayList();

          for (Map.Entry<SegmentIdentifier, Sink> entry : theSinks.entrySet()) {
            if (droppingSinks.contains(entry.getKey())) {
              log.info("Skipping push of currently-dropping sink[%s]", entry.getKey());
              continue;
            }

            final DataSegment dataSegment = mergeAndPush(entry.getKey(), entry.getValue(), useUniquePath);
            if (dataSegment != null) {
              dataSegments.add(dataSegment);
            } else {
              log.warn("mergeAndPush[%s] returned null, skipping.", entry.getKey());
            }
          }

          return new SegmentsAndMetadata(dataSegments, commitMetadata);
        },
        pushExecutor
    );
  }

  /**
   * Insert a barrier into the merge-and-push queue. When this future resolves, all pending pushes will have finished.
   * This is useful if we're going to do something that would otherwise potentially break currently in-progress
   * pushes.
   */
  private ListenableFuture<?> pushBarrier()
  {
    return intermediateTempExecutor.submit(
        (Runnable) () -> pushExecutor.submit(() -> {})
    );
  }

  /**
   * Merge segment, push to deep storage. Should only be used on segments that have been fully persisted. Must only
   * be run in the single-threaded pushExecutor.
   *
   * @param identifier sink identifier
   * @param sink       sink to push
   * @param useUniquePath true if the segment should be written to a path with a unique identifier
   *
   * @return segment descriptor, or null if the sink is no longer valid
   */
  private DataSegment mergeAndPush(final SegmentIdentifier identifier, final Sink sink, final boolean useUniquePath)
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
            tuningConfig.getIndexSpec(),
            tuningConfig.getSegmentWriteOutMediumFactory()
        );
      }
      catch (Throwable t) {
        throw closer.rethrow(t);
      }
      finally {
        closer.close();
      }

      // Retry pushing segments because uploading to deep storage might fail especially for cloud storage types
      final DataSegment segment = RetryUtils.retry(
          // The appenderator is currently being used for the local indexing task and the Kafka indexing task. For the
          // Kafka indexing task, pushers must use unique file paths in deep storage in order to maintain exactly-once
          // semantics.
          () -> dataSegmentPusher.push(
              mergedFile,
              sink.getSegment().withDimensions(IndexMerger.getMergedDimensionsFromQueryableIndexes(indexes)),
              useUniquePath
          ),
          exception -> exception instanceof Exception,
          5
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
    if (!closed.compareAndSet(false, true)) {
      log.info("Appenderator already closed");
      return;
    }

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
      Preconditions.checkState(
          persistExecutor == null || persistExecutor.awaitTermination(365, TimeUnit.DAYS),
          "persistExecutor not terminated"
      );
      Preconditions.checkState(
          pushExecutor == null || pushExecutor.awaitTermination(365, TimeUnit.DAYS),
          "pushExecutor not terminated"
      );
      Preconditions.checkState(
          intermediateTempExecutor == null || intermediateTempExecutor.awaitTermination(365, TimeUnit.DAYS),
          "intermediateTempExecutor not terminated"
      );
      persistExecutor = null;
      pushExecutor = null;
      intermediateTempExecutor = null;
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ISE("Failed to shutdown executors during close()");
    }

    // Only unlock if executors actually shut down.
    unlockBasePersistDirectory();
  }

  /**
   * Unannounce the segments and wait for outstanding persists to finish.
   * Do not unlock base persist dir as we are not waiting for push executor to shut down
   * relying on current JVM to shutdown to not cause any locking problem if the task is restored.
   * In case when task is restored and current task is still active because of push executor (which it shouldn't be
   * since push executor starts daemon threads) then the locking should fail and new task should fail to start.
   * This also means that this method should only be called when task is shutting down.
   */
  @Override
  public void closeNow()
  {
    if (!closed.compareAndSet(false, true)) {
      log.info("Appenderator already closed");
      return;
    }

    log.info("Shutting down immediately...");
    for (Map.Entry<SegmentIdentifier, Sink> entry : sinks.entrySet()) {
      try {
        segmentAnnouncer.unannounceSegment(entry.getValue().getSegment());
      }
      catch (Exception e) {
        log.makeAlert(e, "Failed to unannounce segment[%s]", schema.getDataSource())
           .addData("identifier", entry.getKey().getIdentifierAsString())
           .emit();
      }
    }
    try {
      shutdownExecutors();
      // We don't wait for pushExecutor to be terminated. See Javadoc for more details.
      Preconditions.checkState(
          persistExecutor == null || persistExecutor.awaitTermination(365, TimeUnit.DAYS),
          "persistExecutor not terminated"
      );
      Preconditions.checkState(
          intermediateTempExecutor == null || intermediateTempExecutor.awaitTermination(365, TimeUnit.DAYS),
          "intermediateTempExecutor not terminated"
      );
      persistExecutor = null;
      intermediateTempExecutor = null;
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ISE("Failed to shutdown executors during close()");
    }
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
    if (pushExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      pushExecutor = MoreExecutors.listeningDecorator(
          Execs.newBlockingSingleThreaded(
              "appenderator_merge_%d", 1
          )
      );
    }
    if (intermediateTempExecutor == null) {
      // use single threaded executor with SynchronousQueue so that all abandon operations occur sequentially
      intermediateTempExecutor = MoreExecutors.listeningDecorator(
          Execs.newBlockingSingleThreaded(
              "appenderator_abandon_%d", 0
          )
      );
    }
  }

  private void shutdownExecutors()
  {
    if (persistExecutor != null) {
      persistExecutor.shutdownNow();
    }
    if (pushExecutor != null) {
      pushExecutor.shutdownNow();
    }
    if (intermediateTempExecutor != null) {
      intermediateTempExecutor.shutdownNow();
    }
  }

  private void resetNextFlush()
  {
    nextFlush = DateTimes.nowUtc().plus(tuningConfig.getIntermediatePersistPeriod()).getMillis();
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


    final Committed committed;
    File commitFile = null;
    try {
      commitLock.lock();
      commitFile = computeCommitFile();
      if (commitFile.exists()) {
        committed = objectMapper.readValue(commitFile, Committed.class);
      } else {
        committed = Committed.nil();
      }
    }
    catch (Exception e) {
      throw new ISE(e, "Failed to read commitFile: %s", commitFile);
    }
    finally {
      commitLock.unlock();
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
    // Ensure no future writes will be made to this sink.
    sink.finishWriting();

    // Mark this identifier as dropping, so no future push tasks will pick it up.
    droppingSinks.add(identifier);

    // Decrement this sink's rows from rowsCurrentlyInMemory (we only count active sinks).
    rowsCurrentlyInMemory.addAndGet(-sink.getNumRowsInMemory());
    totalRows.addAndGet(-sink.getNumRows());

    // Wait for any outstanding pushes to finish, then abandon the segment inside the persist thread.
    return Futures.transform(
        pushBarrier(),
        new Function<Object, Object>()
        {
          @Nullable
          @Override
          public Object apply(@Nullable Object input)
          {
            if (sinks.get(identifier) != sink) {
              // Only abandon sink if it is the same one originally requested to be abandoned.
              log.warn("Sink for segment[%s] no longer valid, not abandoning.", identifier);
              return null;
            }

            if (removeOnDiskData) {
              // Remove this segment from the committed list. This must be done from the persist thread.
              log.info("Removing commit metadata for segment[%s].", identifier);
              try {
                commitLock.lock();
                final Committed oldCommit = readCommit();
                if (oldCommit != null) {
                  writeCommit(oldCommit.without(identifier.getIdentifierAsString()));
                }
              }
              catch (Exception e) {
                log.makeAlert(e, "Failed to update committed segments[%s]", schema.getDataSource())
                   .addData("identifier", identifier.getIdentifierAsString())
                   .emit();
                throw Throwables.propagate(e);
              }
              finally {
                commitLock.unlock();
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
            metrics.setSinkCount(sinks.size());
            droppingSinks.remove(identifier);
            sinkTimeline.remove(
                sink.getInterval(),
                sink.getVersion(),
                identifier.getShardSpec().createChunk(sink)
            );
            for (FireHydrant hydrant : sink) {
              if (cache != null) {
                cache.close(SinkQuerySegmentWalker.makeHydrantCacheIdentifier(hydrant));
              }
              hydrant.swapSegment(null);
            }

            if (removeOnDiskData) {
              removeDirectory(computePersistDir(identifier));
            }

            return null;
          }
        },
        // use persistExecutor to make sure that all the pending persists completes before
        // starting to abandon segments
        persistExecutor
    );
  }

  private Committed readCommit() throws IOException
  {
    final File commitFile = computeCommitFile();
    if (commitFile.exists()) {
      // merge current hydrants with existing hydrants
      return objectMapper.readValue(commitFile, Committed.class);
    } else {
      return null;
    }
  }

  private void writeCommit(Committed newCommit) throws IOException
  {
    final File commitFile = computeCommitFile();
    objectMapper.writeValue(commitFile, newCommit);
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
    FileUtils.forceMkdir(persistDir);

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
            indexSpec,
            tuningConfig.getSegmentWriteOutMediumFactory()
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
}
