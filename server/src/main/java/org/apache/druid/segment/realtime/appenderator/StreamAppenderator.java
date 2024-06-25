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

package org.apache.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.BaseProgressIndicator;
import org.apache.druid.segment.DataSegmentWithMetadata;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.SchemaPayloadPlus;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexAddResult;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.FingerprintGenerator;
import org.apache.druid.segment.realtime.AppendableSegment;
import org.apache.druid.segment.realtime.PartialSegment;
import org.apache.druid.segment.realtime.SegmentGenerationMetrics;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@SuppressWarnings("CheckReturnValue")
public class StreamAppenderator implements Appenderator
{
  // Rough estimate of memory footprint of a ColumnHolder based on actual heap dumps
  public static final int ROUGH_OVERHEAD_PER_DIMENSION_COLUMN_HOLDER = 1000;
  public static final int ROUGH_OVERHEAD_PER_METRIC_COLUMN_HOLDER = 700;
  public static final int ROUGH_OVERHEAD_PER_TIME_COLUMN_HOLDER = 600;
  // Rough estimate of memory footprint of empty AppendableSegment based on actual heap dumps
  public static final int ROUGH_OVERHEAD_PER_APPENDABLE_SEGMENT = 5000;
  // Rough estimate of memory footprint of empty PartialSegment based on actual heap dumps
  public static final int ROUGH_OVERHEAD_PER_PARTIAL_SEGMENT = 1000;

  private static final EmittingLogger log = new EmittingLogger(StreamAppenderator.class);
  private static final int WARN_DELAY = 1000;
  private static final String IDENTIFIER_FILE_NAME = "identifier.json";

  private final String myId;
  private final DataSchema schema;
  private final AppenderatorConfig tuningConfig;
  private final SegmentGenerationMetrics metrics;
  private final DataSegmentPusher dataSegmentPusher;
  private final ObjectMapper objectMapper;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final IndexIO indexIO;
  private final IndexMerger indexMerger;
  private final Cache cache;
  /**
   * This map needs to be concurrent because it's accessed and mutated from multiple threads: both the thread from where
   * this Appenderator is used (and methods like {@link #add(SegmentIdWithShardSpec, InputRow, Supplier, boolean)} are
   * called) and from {@link #persistExecutor}. It could also be accessed (but not mutated) potentially in the context
   * of any thread from {@link #drop}.
   */
  private final ConcurrentMap<SegmentIdWithShardSpec, AppendableSegment> appendableSegments = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, SegmentIdWithShardSpec> idToPendingSegment = new ConcurrentHashMap<>();
  private final Set<SegmentIdWithShardSpec> droppingAppendableSegments = Sets.newConcurrentHashSet();
  private final VersionedIntervalTimeline<String, AppendableSegment> appendableSegmentTimeline;
  private final long maxBytesTuningConfig;
  private final boolean skipBytesInMemoryOverheadCheck;
  private final boolean useMaxMemoryEstimates;

  private final QuerySegmentWalker texasRanger;
  // This variable updated in add(), persist(), and drop()
  private final AtomicInteger rowsCurrentlyInMemory = new AtomicInteger();
  private final AtomicInteger totalRows = new AtomicInteger();
  private final AtomicLong bytesCurrentlyInMemory = new AtomicLong();
  private final RowIngestionMeters rowIngestionMeters;
  private final ParseExceptionHandler parseExceptionHandler;
  // Synchronize persisting commitMetadata so that multiple persist threads (if present)
  // and abandon threads do not step over each other
  private final Lock commitLock = new ReentrantLock();

  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Map from base segment identifier of a appendable segment to the set of all the segment ids associated with it.
   * The set contains the base segment itself and its upgraded versions announced as a result of a concurrent replace.
   * The map contains all the available appendable segment' identifiers in its keyset.
   */
  private final ConcurrentMap<SegmentIdWithShardSpec, Set<SegmentIdWithShardSpec>> baseSegmentToUpgradedSegments
      = new ConcurrentHashMap<>();
  /**
   * Map from the id of an upgraded pending segment to the segment corresponding to its upgradedFromSegmentId.
   */
  private final ConcurrentMap<SegmentIdWithShardSpec, SegmentIdWithShardSpec> upgradedSegmentToBaseSegment
      = new ConcurrentHashMap<>();
  /**
   * Set of all segment identifiers that have been marked to be abandoned.
   * This is used to determine if all the segments corresponding to an appendable segment have been abandoned and it
   * can be dropped.
   */
  private final ConcurrentHashMap.KeySetView<SegmentIdWithShardSpec, Boolean> abandonedSegments
      = ConcurrentHashMap.newKeySet();


  private final AppendableSegmentSchemaAnnouncer appendableSegmentSchemaAnnouncer;
  private final CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig;

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

  private final SegmentLoaderConfig segmentLoaderConfig;
  private ScheduledExecutorService exec;
  private final FingerprintGenerator fingerprintGenerator;

  /**
   * This constructor allows the caller to provide its own {@link AppendableSegmentQuerySegmentWalker}.
   *
   * The {@link #appendableSegmentTimeline} is set to the appendable segment timeline of the provided
   * {@link AppendableSegmentQuerySegmentWalker}. If the {@link AppendableSegmentQuerySegmentWalker} is null, a
   * new timeline is initialized.
   *
   * It is used by {@link UnifiedIndexerAppenderatorsManager} which allows queries on data associated with multiple
   * Appenderators.
   */
  StreamAppenderator(
      SegmentLoaderConfig segmentLoaderConfig,
      String id,
      DataSchema schema,
      AppenderatorConfig tuningConfig,
      SegmentGenerationMetrics metrics,
      DataSegmentPusher dataSegmentPusher,
      ObjectMapper objectMapper,
      DataSegmentAnnouncer segmentAnnouncer,
      @Nullable AppendableSegmentQuerySegmentWalker appendableSegmentQuerySegmentWalker,
      IndexIO indexIO,
      IndexMerger indexMerger,
      Cache cache,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler,
      boolean useMaxMemoryEstimates,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
  )
  {
    this.segmentLoaderConfig = segmentLoaderConfig;
    this.myId = id;
    this.schema = Preconditions.checkNotNull(schema, "schema");
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
    this.metrics = Preconditions.checkNotNull(metrics, "metrics");
    this.dataSegmentPusher = Preconditions.checkNotNull(dataSegmentPusher, "dataSegmentPusher");
    this.objectMapper = Preconditions.checkNotNull(objectMapper, "objectMapper");
    this.segmentAnnouncer = Preconditions.checkNotNull(segmentAnnouncer, "segmentAnnouncer");
    this.indexIO = Preconditions.checkNotNull(indexIO, "indexIO");
    this.indexMerger = Preconditions.checkNotNull(indexMerger, "indexMerger");
    this.cache = cache;
    this.texasRanger = appendableSegmentQuerySegmentWalker;
    this.rowIngestionMeters = Preconditions.checkNotNull(rowIngestionMeters, "rowIngestionMeters");
    this.parseExceptionHandler = Preconditions.checkNotNull(parseExceptionHandler, "parseExceptionHandler");

    if (appendableSegmentQuerySegmentWalker == null) {
      this.appendableSegmentTimeline = new VersionedIntervalTimeline<>(
          String.CASE_INSENSITIVE_ORDER
      );
    } else {
      this.appendableSegmentTimeline = appendableSegmentQuerySegmentWalker.getAppendableSegmentTimeline();
    }

    maxBytesTuningConfig = tuningConfig.getMaxBytesInMemoryOrDefault();
    skipBytesInMemoryOverheadCheck = tuningConfig.isSkipBytesInMemoryOverheadCheck();
    this.useMaxMemoryEstimates = useMaxMemoryEstimates;
    this.centralizedDatasourceSchemaConfig = centralizedDatasourceSchemaConfig;
    this.appendableSegmentSchemaAnnouncer = new AppendableSegmentSchemaAnnouncer();

    this.exec = Executors.newScheduledThreadPool(
        1,
        Execs.makeThreadFactory("StreamAppenderSegmentRemoval-%s")
    );
    this.fingerprintGenerator = new FingerprintGenerator(objectMapper);
  }

  @VisibleForTesting
  void setExec(ScheduledExecutorService testExec)
  {
    if (exec != null) {
      exec.shutdown();
    }
    exec = testExec;
  }

  @Override
  public String getId()
  {
    return myId;
  }

  @Override
  public String getDataSource()
  {
    return schema.getDataSource();
  }

  @Override
  public Object startJob()
  {
    lockBasePersistDirectory();
    final Object retVal = bootstrapAppendableSegmentsFromDisk();
    initializeExecutors();
    resetNextFlush();
    appendableSegmentSchemaAnnouncer.start();
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
      final SegmentIdWithShardSpec identifier,
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

    final AppendableSegment appendableSegment = getOrCreateAppendableSegment(identifier);
    metrics.reportMessageMaxTimestamp(row.getTimestampFromEpoch());
    final int appendableSegmentRowsInMemoryBeforeAdd = appendableSegment.getNumRowsInMemory();
    final int appendableSegmentRowsInMemoryAfterAdd;
    final long bytesInMemoryBeforeAdd = appendableSegment.getBytesInMemory();
    final long bytesInMemoryAfterAdd;
    final IncrementalIndexAddResult addResult;

    try {
      addResult = appendableSegment.add(row, !allowIncrementalPersists);
      appendableSegmentRowsInMemoryAfterAdd = addResult.getRowCount();
      bytesInMemoryAfterAdd = addResult.getBytesInMemory();
    }
    catch (IndexSizeExceededException e) {
      // Uh oh, we can't do anything about this! We can't persist (commit metadata would be out of sync) and we
      // can't add the row (it just failed). This should never actually happen, though, because we check
      // appendableSegment.canAddRow after returning from add.
      log.error(e, "Appendable segment[%s] was unexpectedly full!", identifier);
      throw e;
    }

    if (appendableSegmentRowsInMemoryAfterAdd < 0) {
      throw new SegmentNotWritableException("Attempt to add row to swapped-out appendable segment[%s].", identifier);
    }

    if (addResult.isRowAdded()) {
      rowIngestionMeters.incrementProcessed();
    } else if (addResult.hasParseException()) {
      parseExceptionHandler.handle(addResult.getParseException());
    }

    final int numAddedRows = appendableSegmentRowsInMemoryAfterAdd - appendableSegmentRowsInMemoryBeforeAdd;
    rowsCurrentlyInMemory.addAndGet(numAddedRows);
    bytesCurrentlyInMemory.addAndGet(bytesInMemoryAfterAdd - bytesInMemoryBeforeAdd);
    totalRows.addAndGet(numAddedRows);

    boolean isPersistRequired = false;
    boolean persist = false;
    List<String> persistReasons = new ArrayList<>();

    if (!appendableSegment.canAppendRow()) {
      persist = true;
      persistReasons.add("No more rows can be appended to appendable segment");
    }
    if (System.currentTimeMillis() > nextFlush) {
      persist = true;
      persistReasons.add(StringUtils.format(
          "current time[%d] is greater than nextFlush[%d]",
          System.currentTimeMillis(),
          nextFlush
      ));
    }
    if (rowsCurrentlyInMemory.get() >= tuningConfig.getMaxRowsInMemory()) {
      persist = true;
      persistReasons.add(StringUtils.format(
          "rowsCurrentlyInMemory[%d] is greater than maxRowsInMemory[%d]",
          rowsCurrentlyInMemory.get(),
          tuningConfig.getMaxRowsInMemory()
      ));
    }
    if (bytesCurrentlyInMemory.get() >= maxBytesTuningConfig) {
      persist = true;
      persistReasons.add(StringUtils.format(
          "(estimated) bytesCurrentlyInMemory[%d] is greater than maxBytesInMemory[%d]",
          bytesCurrentlyInMemory.get(),
          maxBytesTuningConfig
      ));
    }
    if (persist) {
      if (allowIncrementalPersists) {
        // persistAll clears rowsCurrentlyInMemory, no need to update it.
        log.info("Flushing in-memory data to disk because %s.", String.join(",", persistReasons));

        long bytesToBePersisted = 0L;
        for (Map.Entry<SegmentIdWithShardSpec, AppendableSegment> entry : appendableSegments.entrySet()) {
          final AppendableSegment appendableSegmentEntry = entry.getValue();
          if (appendableSegmentEntry != null) {
            bytesToBePersisted += appendableSegmentEntry.getBytesInMemory();
            if (appendableSegmentEntry.swappable()) {
              // After swapping the appendable segment, we use memory mapped segment instead (but only for real time
              // appenderators!). However, the memory mapped segment still consumes memory. These memory mapped
              // segments are held in memory throughout the ingestion phase and permanently add to the
              // bytesCurrentlyInMemory
              int memoryStillInUse = calculateMMappedPartialSegmentsMemoryInUsed(appendableSegment.getCurrentPartialSegment());
              bytesCurrentlyInMemory.addAndGet(memoryStillInUse);
            }
          }
        }

        if (!skipBytesInMemoryOverheadCheck && bytesCurrentlyInMemory.get() - bytesToBePersisted > maxBytesTuningConfig) {
          // We are still over maxBytesTuningConfig even after persisting.
          // This means that we ran out of all available memory to ingest (due to overheads created as part of ingestion)
          final String alertMessage = StringUtils.format(
              "Task has exceeded safe estimated heap usage limits, failing "
              + "(numAppendableSegments: [%d] numPartialSegmentsAcrossAllAppendableSegments: [%d] totalRows: [%d])"
              + "(bytesCurrentlyInMemory: [%d] - bytesToBePersisted: [%d] > maxBytesTuningConfig: [%d])",
              appendableSegments.size(),
              appendableSegments.values().stream().mapToInt(Iterables::size).sum(),
              getTotalRowCount(),
              bytesCurrentlyInMemory.get(),
              bytesToBePersisted,
              maxBytesTuningConfig
          );
          final String errorMessage = StringUtils.format(
              "%s.\nThis can occur when the overhead from too many intermediary segment persists becomes to "
              + "great to have enough space to process additional input rows. This check, along with metering the overhead "
              + "of these objects to factor into the 'maxBytesInMemory' computation, can be disabled by setting "
              + "'skipBytesInMemoryOverheadCheck' to 'true' (note that doing so might allow the task to naturally encounter "
              + "a 'java.lang.OutOfMemoryError'). Alternatively, 'maxBytesInMemory' can be increased which will cause an "
              + "increase in heap footprint, but will allow for more intermediary segment persists to occur before "
              + "reaching this condition.",
              alertMessage
          );
          log.makeAlert(alertMessage)
             .addData("dataSource", schema.getDataSource())
             .emit();
          throw new RuntimeException(errorMessage);
        }

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
            },
            MoreExecutors.directExecutor()
        );
      } else {
        log.info("Marking ready for non-incremental async persist due to reasons[%s].", persistReasons);
        isPersistRequired = true;
      }
    }
    return new AppenderatorAddResult(identifier, appendableSegment.getNumRows(), isPersistRequired);
  }

  @Override
  public List<SegmentIdWithShardSpec> getSegments()
  {
    return ImmutableList.copyOf(appendableSegments.keySet());
  }

  @Override
  public int getRowCount(final SegmentIdWithShardSpec identifier)
  {
    final AppendableSegment appendableSegment = appendableSegments.get(identifier);

    if (appendableSegment == null) {
      throw new ISE("No such appendable segment: %s", identifier);
    } else {
      return appendableSegment.getNumRows();
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

  @VisibleForTesting
  long getBytesCurrentlyInMemory()
  {
    return bytesCurrentlyInMemory.get();
  }

  @VisibleForTesting
  long getBytesInMemory(SegmentIdWithShardSpec identifier)
  {
    final AppendableSegment appendableSegment = appendableSegments.get(identifier);

    if (appendableSegment == null) {
      throw new ISE("No such appendable segment: %s", identifier);
    } else {
      return appendableSegment.getBytesInMemory();
    }
  }

  private AppendableSegment getOrCreateAppendableSegment(final SegmentIdWithShardSpec identifier)
  {
    AppendableSegment retVal = appendableSegments.get(identifier);

    if (retVal == null) {
      retVal = new AppendableSegment(
          identifier.getInterval(),
          schema,
          identifier.getShardSpec(),
          identifier.getVersion(),
          tuningConfig.getAppendableIndexSpec(),
          tuningConfig.getMaxRowsInMemory(),
          maxBytesTuningConfig,
          useMaxMemoryEstimates
      );
      bytesCurrentlyInMemory.addAndGet(calculateAppendableSegmentMemoryInUsed(retVal));

      try {
        segmentAnnouncer.announceSegment(retVal.getSegment());
      }
      catch (IOException e) {
        log.makeAlert(e, "Failed to announce new segment[%s]", schema.getDataSource())
           .addData("interval", retVal.getInterval())
           .emit();
      }

      addAppendableSegment(identifier, retVal);
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
            () -> {
              try {
                commitLock.lock();
                objectMapper.writeValue(computeCommitFile(), Committed.nil());
              }
              finally {
                commitLock.unlock();
              }
              return null;
            }
        );

        // Await uncommit.
        uncommitFuture.get();

        // Drop everything.
        final List<ListenableFuture<?>> futures = new ArrayList<>();
        for (Map.Entry<SegmentIdWithShardSpec, AppendableSegment> entry : appendableSegments.entrySet()) {
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
  public ListenableFuture<?> drop(final SegmentIdWithShardSpec identifier)
  {
    final AppendableSegment appendableSegment = appendableSegments.get(identifier);
    if (appendableSegment != null) {
      return abandonSegment(identifier, appendableSegment, true);
    } else {
      return Futures.immediateFuture(null);
    }
  }

  @Override
  public ListenableFuture<Object> persistAll(@Nullable final Committer committer)
  {
    throwPersistErrorIfExists();
    final Map<String, Integer> currentPartialSegments = new HashMap<>();
    final List<Pair<PartialSegment, SegmentIdWithShardSpec>> indexesToPersist = new ArrayList<>();
    int numPersistedRows = 0;
    long bytesPersisted = 0L;
    MutableLong totalPartialSegmentsCount = new MutableLong();
    MutableLong totalPartialSegmentsPersisted = new MutableLong();
    final long totalAppendableSegments = appendableSegments.size();
    for (Map.Entry<SegmentIdWithShardSpec, AppendableSegment> entry : appendableSegments.entrySet()) {
      final SegmentIdWithShardSpec identifier = entry.getKey();
      final AppendableSegment appendableSegment = entry.getValue();
      if (appendableSegment == null) {
        throw new ISE("No appendable segment for identifier: %s", identifier);
      }
      final List<PartialSegment> partialSegments = Lists.newArrayList(appendableSegment);
      totalPartialSegmentsCount.add(partialSegments.size());
      currentPartialSegments.put(identifier.toString(), partialSegments.size());
      numPersistedRows += appendableSegment.getNumRowsInMemory();
      bytesPersisted += appendableSegment.getBytesInMemory();

      final int limit = appendableSegment.isWritable() ? partialSegments.size() - 1 : partialSegments.size();

      // gather partialSegments that have not been persisted:
      for (PartialSegment partialSegment : partialSegments.subList(0, limit)) {
        if (!partialSegment.hasSwapped()) {
          log.debug("Partial segment[%s] hasn't persisted yet, persisting. Segment[%s]", partialSegment, identifier);
          indexesToPersist.add(Pair.of(partialSegment, identifier));
          totalPartialSegmentsPersisted.add(1);
        }
      }

      if (appendableSegment.swappable()) {
        // It is swappable. Get the old one to persist it and create a new one:
        indexesToPersist.add(Pair.of(appendableSegment.swap(), identifier));
        totalPartialSegmentsPersisted.add(1);
      }
    }
    log.debug("Submitting persist runnable for dataSource[%s]", schema.getDataSource());

    final Object commitMetadata = committer == null ? null : committer.getMetadata();
    final Stopwatch runExecStopwatch = Stopwatch.createStarted();
    final Stopwatch persistStopwatch = Stopwatch.createStarted();
    AtomicLong totalPersistedRows = new AtomicLong(numPersistedRows);
    final ListenableFuture<Object> future = persistExecutor.submit(
        new Callable<Object>()
        {
          @Override
          public Object call() throws IOException
          {
            try {
              for (Pair<PartialSegment, SegmentIdWithShardSpec> pair : indexesToPersist) {
                metrics.incrementRowOutputCount(persistPartialSegment(pair.lhs, pair.rhs));
              }

              if (committer != null) {
                log.debug(
                    "Committing metadata[%s] for appendable segment[%s].",
                    commitMetadata,
                    Joiner.on(", ").join(
                        currentPartialSegments.entrySet()
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
                  final Map<String, Integer> commitPartialSegments = new HashMap<>();
                  final Committed oldCommit = readCommit();
                  if (oldCommit != null) {
                    // merge current partial segments with existing partial segments
                    commitPartialSegments.putAll(oldCommit.getPartialSegments());
                  }
                  commitPartialSegments.putAll(currentPartialSegments);
                  writeCommit(new Committed(commitPartialSegments, commitMetadata));
                }
                finally {
                  commitLock.unlock();
                }
              }

              log.info(
                  "Flushed in-memory data with commit metadata [%s] for segments: %s",
                  commitMetadata,
                  indexesToPersist.stream()
                                  .map(itp -> itp.rhs.asSegmentId().toString())
                                  .distinct()
                                  .collect(Collectors.joining(", "))
              );
              log.info(
                  "Persisted stats: processed rows: [%d], persisted rows[%d], appendable segment: [%d], total partial segment (across appendable segment): [%d], persisted partial segment (across appendable segment): [%d]",
                  rowIngestionMeters.getProcessed(),
                  totalPersistedRows.get(),
                  totalAppendableSegments,
                  totalPartialSegmentsCount.longValue(),
                  totalPartialSegmentsPersisted.longValue()
              );

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
    bytesCurrentlyInMemory.addAndGet(-bytesPersisted);

    log.info("Persisted rows[%,d] and (estimated) bytes[%,d]", numPersistedRows, bytesPersisted);

    return future;
  }

  @Override
  public ListenableFuture<SegmentsAndCommitMetadata> push(
      final Collection<SegmentIdWithShardSpec> identifiers,
      @Nullable final Committer committer,
      final boolean useUniquePath
  )
  {
    final Map<SegmentIdWithShardSpec, AppendableSegment> theAppendableSegments = new HashMap<>();
    AtomicLong pushedPartialSegmentsCount = new AtomicLong();
    for (final SegmentIdWithShardSpec identifier : identifiers) {
      final AppendableSegment appendableSegment = appendableSegments.get(identifier);
      if (appendableSegment == null) {
        throw new ISE("No appendable segment for identifier: %s", identifier);
      }
      theAppendableSegments.put(identifier, appendableSegment);
      if (appendableSegment.finishWriting()) {
        totalRows.addAndGet(-appendableSegment.getNumRows());
      }
      // count partial segments for stats:
      pushedPartialSegmentsCount.addAndGet(Iterables.size(appendableSegment));
    }

    return Futures.transform(
        // We should always persist all segments regardless of the input because metadata should be committed for all
        // segments.
        persistAll(committer),
        (Function<Object, SegmentsAndCommitMetadata>) commitMetadata -> {
          final List<DataSegment> dataSegments = new ArrayList<>();
          final SegmentSchemaMapping segmentSchemaMapping = new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION);

          log.info("Preparing to push (stats): processed rows: [%d], appendable segments: [%d], partial segment (across appendable segments): [%d]",
                   rowIngestionMeters.getProcessed(), theAppendableSegments.size(), pushedPartialSegmentsCount.get()
          );

          log.debug(
              "Building and pushing segments: %s",
              theAppendableSegments.keySet().stream().map(SegmentIdWithShardSpec::toString).collect(Collectors.joining(", "))
          );

          for (Map.Entry<SegmentIdWithShardSpec, AppendableSegment> entry : theAppendableSegments.entrySet()) {
            if (droppingAppendableSegments.contains(entry.getKey())) {
              log.warn("Skipping push of currently-dropping appendable segment[%s]", entry.getKey());
              continue;
            }

            final DataSegmentWithMetadata dataSegmentWithMetadata = mergeAndPush(
                entry.getKey(),
                entry.getValue(),
                useUniquePath
            );
            if (dataSegmentWithMetadata != null) {
              DataSegment segment = dataSegmentWithMetadata.getDataSegment();
              dataSegments.add(segment);
              SchemaPayloadPlus schemaPayloadPlus = dataSegmentWithMetadata.getSegmentSchemaMetadata();
              if (schemaPayloadPlus != null) {
                SchemaPayload schemaPayload = schemaPayloadPlus.getSchemaPayload();
                segmentSchemaMapping.addSchema(
                    segment.getId(),
                    schemaPayloadPlus,
                    fingerprintGenerator.generateFingerprint(
                        schemaPayload,
                        segment.getDataSource(),
                        CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
                    )
                );
              }
            } else {
              log.warn("mergeAndPush[%s] returned null, skipping.", entry.getKey());
            }
          }

          log.info("Push complete...");

          return new SegmentsAndCommitMetadata(dataSegments, commitMetadata, segmentSchemaMapping);
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
   * @param identifier          appendable segment identifier
   * @param appendableSegment   appendable segment to push
   * @param useUniquePath       true if the segment should be written to a path with a unique identifier
   *
   * @return segment descriptor, or null if the appendable segment is no longer valid
   */
  @Nullable
  private DataSegmentWithMetadata mergeAndPush(
      final SegmentIdWithShardSpec identifier,
      final AppendableSegment appendableSegment,
      final boolean useUniquePath
  )
  {
    // Bail out if this appendable segment is null or otherwise not what we expect.
    //noinspection ObjectEquality
    if (appendableSegments.get(identifier) != appendableSegment) {
      log.warn("Appendable segment[%s] no longer valid, bailing out of mergeAndPush.", identifier);
      return null;
    }

    // Use a descriptor file to indicate that pushing has completed.
    final File persistDir = computePersistDir(identifier);
    final File mergedTarget = new File(persistDir, "merged");
    final File descriptorFile = computeDescriptorFile(identifier);

    // Sanity checks
    for (PartialSegment partialSegment : appendableSegment) {
      if (appendableSegment.isWritable()) {
        throw new ISE("Expected appendable segment to be no longer writable before mergeAndPush for segment[%s].", identifier);
      }

      synchronized (partialSegment) {
        if (!partialSegment.hasSwapped()) {
          throw new ISE("Expected appendable segment to be fully persisted before mergeAndPush for segment[%s].", identifier);
        }
      }
    }

    try {
      if (descriptorFile.exists()) {
        // Already pushed.

        if (useUniquePath) {
          // Don't reuse the descriptor, because the caller asked for a unique path. Leave the old one as-is, since
          // it might serve some unknown purpose.
          log.debug(
              "Segment[%s] already pushed, but we want a unique path, so will push again with a new path.",
              identifier
          );
        } else {
          log.info("Segment[%s] already pushed, skipping.", identifier);
          return new DataSegmentWithMetadata(
              objectMapper.readValue(descriptorFile, DataSegment.class),
              centralizedDatasourceSchemaConfig.isEnabled() ? TaskSegmentSchemaUtil.getSegmentSchema(
                  mergedTarget,
                  indexIO
              ) : null
          );
        }
      }

      removeDirectory(mergedTarget);

      if (mergedTarget.exists()) {
        throw new ISE("Merged target[%s] exists after removing?!", mergedTarget);
      }

      final File mergedFile;
      final long mergeFinishTime;
      final long startTime = System.nanoTime();
      List<QueryableIndex> indexes = new ArrayList<>();
      Closer closer = Closer.create();
      try {
        for (PartialSegment partialSegment : appendableSegment) {
          Pair<ReferenceCountingSegment, Closeable> segmentAndCloseable = partialSegment.getAndIncrementSegment();
          final QueryableIndex queryableIndex = segmentAndCloseable.lhs.asQueryableIndex();
          log.debug("Segment[%s] adding partial segment[%s]", identifier, partialSegment);
          indexes.add(queryableIndex);
          closer.register(segmentAndCloseable.rhs);
        }

        mergedFile = indexMerger.mergeQueryableIndex(
            indexes,
            schema.getGranularitySpec().isRollup(),
            schema.getAggregators(),
            schema.getDimensionsSpec(),
            mergedTarget,
            tuningConfig.getIndexSpec(),
            tuningConfig.getIndexSpecForIntermediatePersists(),
            new BaseProgressIndicator(),
            tuningConfig.getSegmentWriteOutMediumFactory(),
            tuningConfig.getMaxColumnsToMerge()
        );

        mergeFinishTime = System.nanoTime();

        log.debug("Segment[%s] built in %,dms.", identifier, (mergeFinishTime - startTime) / 1000000);
      }
      catch (Throwable t) {
        throw closer.rethrow(t);
      }
      finally {
        closer.close();
      }

      final DataSegment segmentToPush = appendableSegment.getSegment().withDimensions(
          IndexMerger.getMergedDimensionsFromQueryableIndexes(indexes, schema.getDimensionsSpec())
      );

      // dataSegmentPusher retries internally when appropriate; no need for retries here.
      final DataSegment segment = dataSegmentPusher.push(mergedFile, segmentToPush, useUniquePath);

      final long pushFinishTime = System.nanoTime();

      objectMapper.writeValue(descriptorFile, segment);

      log.info(
          "Segment[%s] of %,d bytes "
          + "built from %d incremental persist(s) in %,dms; "
          + "pushed to deep storage in %,dms. "
          + "Load spec is: %s",
          identifier,
          segment.getSize(),
          indexes.size(),
          (mergeFinishTime - startTime) / 1000000,
          (pushFinishTime - mergeFinishTime) / 1000000,
          objectMapper.writeValueAsString(segment.getLoadSpec())
      );

      return new DataSegmentWithMetadata(
          segment,
          centralizedDatasourceSchemaConfig.isEnabled()
          ? TaskSegmentSchemaUtil.getSegmentSchema(mergedTarget, indexIO)
          : null
      );
    }
    catch (Exception e) {
      metrics.incrementFailedHandoffs();
      log.warn(e, "Failed to push merged index for segment[%s].", identifier);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close()
  {
    if (!closed.compareAndSet(false, true)) {
      log.debug("Appenderator already closed, skipping close() call.");
      return;
    }

    log.debug("Shutting down...");

    final List<ListenableFuture<?>> futures = new ArrayList<>();
    for (Map.Entry<SegmentIdWithShardSpec, AppendableSegment> entry : appendableSegments.entrySet()) {
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

    appendableSegmentSchemaAnnouncer.stop();
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
      log.debug("Appenderator already closed, skipping closeNow() call.");
      return;
    }

    log.debug("Shutting down immediately...");
    for (Map.Entry<SegmentIdWithShardSpec, AppendableSegment> entry : appendableSegments.entrySet()) {
      unannounceAllVersionsOfSegment(entry.getValue().getSegment(), entry.getValue());
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

    appendableSegmentSchemaAnnouncer.stop();
  }

  @VisibleForTesting
  AppendableSegmentSchemaAnnouncer getAppendableSegmentSchemaAnnouncer()
  {
    return appendableSegmentSchemaAnnouncer;
  }

  /**
   * Unannounces the given base segment and all its upgraded versions.
   */
  private void unannounceAllVersionsOfSegment(DataSegment baseSegment, AppendableSegment appendableSegment)
  {
    synchronized (appendableSegment) {
      final SegmentIdWithShardSpec baseId = SegmentIdWithShardSpec.fromDataSegment(baseSegment);
      if (!baseSegmentToUpgradedSegments.containsKey(baseId)) {
        return;
      }

      final Set<SegmentIdWithShardSpec> upgradedVersionsOfSegment = baseSegmentToUpgradedSegments.remove(baseId);
      for (SegmentIdWithShardSpec newId : upgradedVersionsOfSegment) {
        final DataSegment newSegment = new DataSegment(
            newId.getDataSource(),
            newId.getInterval(),
            newId.getVersion(),
            baseSegment.getLoadSpec(),
            baseSegment.getDimensions(),
            baseSegment.getMetrics(),
            newId.getShardSpec(),
            baseSegment.getBinaryVersion(),
            baseSegment.getSize()
        );
        unannounceSegment(newSegment);
        upgradedSegmentToBaseSegment.remove(newId);
      }
    }
  }

  private void unannounceSegment(DataSegment segment)
  {
    try {
      segmentAnnouncer.unannounceSegment(segment);
    }
    catch (Exception e) {
      log.makeAlert(e, "Failed to unannounce segment[%s]", schema.getDataSource())
         .addData("identifier", segment.getId().toString())
         .emit();
    }
  }

  public void registerUpgradedPendingSegment(PendingSegmentRecord pendingSegmentRecord) throws IOException
  {
    SegmentIdWithShardSpec basePendingSegment = idToPendingSegment.get(pendingSegmentRecord.getUpgradedFromSegmentId());
    SegmentIdWithShardSpec upgradedPendingSegment = pendingSegmentRecord.getId();
    if (!appendableSegments.containsKey(basePendingSegment) || droppingAppendableSegments.contains(basePendingSegment)) {
      return;
    }

    // Update query mapping with AppendableSegmentQuerySegmentWalker
    ((AppendableSegmentQuerySegmentWalker) texasRanger).registerUpgradedPendingSegment(basePendingSegment, upgradedPendingSegment);

    // Announce segments
    final DataSegment baseSegment = appendableSegments.get(basePendingSegment).getSegment();
    final DataSegment newSegment = getUpgradedSegment(baseSegment, upgradedPendingSegment);

    segmentAnnouncer.announceSegment(newSegment);
    baseSegmentToUpgradedSegments.get(basePendingSegment).add(upgradedPendingSegment);
    upgradedSegmentToBaseSegment.put(upgradedPendingSegment, basePendingSegment);
  }

  private DataSegment getUpgradedSegment(DataSegment baseSegment, SegmentIdWithShardSpec upgradedVersion)
  {
    return new DataSegment(
        upgradedVersion.getDataSource(),
        upgradedVersion.getInterval(),
        upgradedVersion.getVersion(),
        baseSegment.getLoadSpec(),
        baseSegment.getDimensions(),
        baseSegment.getMetrics(),
        upgradedVersion.getShardSpec(),
        baseSegment.getBinaryVersion(),
        baseSegment.getSize()
    );
  }

  private void lockBasePersistDirectory()
  {
    if (basePersistDirLock == null) {
      try {
        FileUtils.mkdirp(tuningConfig.getBasePersistDirectory());

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
        throw new RuntimeException(e);
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
      throw new RuntimeException(e);
    }
  }

  private void initializeExecutors()
  {
    final int maxPendingPersists = tuningConfig.getMaxPendingPersists();

    if (persistExecutor == null) {
      log.info("Number of persist threads [%d]", tuningConfig.getNumPersistThreads());
      persistExecutor = MoreExecutors.listeningDecorator(
          Execs.newBlockingThreaded(
              "[" + StringUtils.encodeForFormat(myId) + "]-appenderator-persist",
              tuningConfig.getNumPersistThreads(), maxPendingPersists
          )
      );
    }

    if (pushExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      pushExecutor = MoreExecutors.listeningDecorator(
          Execs.newBlockingSingleThreaded("[" + StringUtils.encodeForFormat(myId) + "]-appenderator-merge", 1)
      );
    }

    if (intermediateTempExecutor == null) {
      // use single threaded executor with SynchronousQueue so that all abandon operations occur sequentially
      intermediateTempExecutor = MoreExecutors.listeningDecorator(
          Execs.newBlockingSingleThreaded("[" + StringUtils.encodeForFormat(myId) + "]-appenderator-abandon", 0)
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

    if (exec != null) {
      exec.shutdownNow();
    }
  }

  private void resetNextFlush()
  {
    nextFlush = DateTimes.nowUtc().plus(tuningConfig.getIntermediatePersistPeriod()).getMillis();
  }

  /**
   * Populate {@link #appendableSegments} and {@link #appendableSegmentTimeline} with committed segments, and announce
   * them with the segmentAnnouncer.
   *
   * @return persisted commit metadata
   */
  private Object bootstrapAppendableSegmentsFromDisk()
  {
    Preconditions.checkState(appendableSegments.isEmpty(), "Already bootstrapped?!");

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

    int rowsSoFar = 0;

    if (committed.equals(Committed.nil())) {
      log.debug("No previously committed metadata.");
    } else {
      log.info(
          "Loading partially-persisted segments[%s] from[%s] with commit metadata: %s",
          String.join(", ", committed.getPartialSegments().keySet()),
          baseDir,
          committed.getMetadata()
      );
    }

    for (File appendableSegmentDir : files) {
      final File identifierFile = new File(appendableSegmentDir, IDENTIFIER_FILE_NAME);
      if (!identifierFile.isFile()) {
        // No identifier in this appendableSegmentDir; it must not actually be a appendable segment directory. Skip it.
        continue;
      }

      try {
        final SegmentIdWithShardSpec identifier = objectMapper.readValue(
            new File(appendableSegmentDir, "identifier.json"),
            SegmentIdWithShardSpec.class
        );

        final int committedPartialSegments = committed.getCommittedPartialSegments(identifier.toString());

        if (committedPartialSegments <= 0) {
          log.info("Removing uncommitted segment at [%s].", appendableSegmentDir);
          FileUtils.deleteDirectory(appendableSegmentDir);
          continue;
        }

        // To avoid reading and listing of "merged" dir and other special files
        final File[] appendableSegmentFiles = appendableSegmentDir.listFiles(
            (dir, fileName) -> !(Ints.tryParse(fileName) == null)
        );

        Arrays.sort(
            appendableSegmentFiles,
            (o1, o2) -> Ints.compare(Integer.parseInt(o1.getName()), Integer.parseInt(o2.getName()))
        );

        List<PartialSegment> partialSegments = new ArrayList<>();
        for (File partialSegmentDir : appendableSegmentFiles) {
          final int partialSegmentNumber = Integer.parseInt(partialSegmentDir.getName());

          if (partialSegmentNumber >= committedPartialSegments) {
            log.info("Removing uncommitted partial segment at [%s]", partialSegmentDir);
            FileUtils.deleteDirectory(partialSegmentDir);
          } else {
            log.debug("Loading previously persisted partial segment at [%s]", partialSegmentDir);
            if (partialSegmentNumber != partialSegments.size()) {
              throw new ISE("Missing partial segment [%,d] in appendableSegmentDir [%s].", partialSegments.size(), appendableSegmentDir);
            }

            partialSegments.add(
                new PartialSegment(
                    new QueryableIndexSegment(indexIO.loadIndex(partialSegmentDir), identifier.asSegmentId()),
                    partialSegmentNumber
                )
            );
          }
        }

        // Make sure we loaded enough partialSegments.
        if (committedPartialSegments != partialSegments.size()) {
          throw new ISE("Missing partial segment [%,d] in appendableSegmentDir [%s].", partialSegments.size(), appendableSegmentDir);
        }

        AppendableSegment currAppendableSegment = new AppendableSegment(
            identifier.getInterval(),
            schema,
            identifier.getShardSpec(),
            identifier.getVersion(),
            tuningConfig.getAppendableIndexSpec(),
            tuningConfig.getMaxRowsInMemory(),
            maxBytesTuningConfig,
            useMaxMemoryEstimates,
            partialSegments
        );
        rowsSoFar += currAppendableSegment.getNumRows();

        addAppendableSegment(identifier, currAppendableSegment);
        segmentAnnouncer.announceSegment(currAppendableSegment.getSegment());
      }
      catch (IOException e) {
        log.makeAlert(e, "Problem loading appendable segment[%s] from disk.", schema.getDataSource())
           .addData("appendableSegmentDir", appendableSegmentDir)
           .emit();
      }
    }

    // Make sure we loaded all committed appendable segments.
    final Set<String> loadedAppendableSegments = Sets.newHashSet(
        Iterables.transform(appendableSegments.keySet(), SegmentIdWithShardSpec::toString)
    );
    final Set<String> missingAppendableSegments = Sets.difference(committed.getPartialSegments().keySet(), loadedAppendableSegments);
    if (!missingAppendableSegments.isEmpty()) {
      throw new ISE("Missing committed appendable segments [%s]", Joiner.on(", ").join(missingAppendableSegments));
    }

    totalRows.set(rowsSoFar);
    return committed.getMetadata();
  }

  /**
   * Update the state of the appenderator when adding a appendable segment.
   *
   * @param identifier        appendable segment identifier
   * @param appendableSegment appendable segment to be added
   */
  private void addAppendableSegment(SegmentIdWithShardSpec identifier, AppendableSegment appendableSegment)
  {
    appendableSegments.put(identifier, appendableSegment);
    // Asoociate the base segment of a appendable segment with its string identifier
    // Needed to get the base segment using upgradedFromSegmentId of a pending segment
    idToPendingSegment.put(identifier.asSegmentId().toString(), identifier);

    // The base segment is associated with itself in the maps to maintain all the upgraded ids of a appendable segment.
    baseSegmentToUpgradedSegments.put(identifier, new HashSet<>());
    baseSegmentToUpgradedSegments.get(identifier).add(identifier);

    appendableSegmentTimeline.add(
        appendableSegment.getInterval(),
        appendableSegment.getVersion(),
        identifier.getShardSpec().createChunk(appendableSegment)
    );
  }

  private ListenableFuture<?> abandonSegment(
      final SegmentIdWithShardSpec identifier,
      final AppendableSegment appendableSegment,
      final boolean removeOnDiskData
  )
  {
    abandonedSegments.add(identifier);
    final SegmentIdWithShardSpec baseIdentifier = upgradedSegmentToBaseSegment.getOrDefault(identifier, identifier);
    synchronized (appendableSegment) {
      if (baseSegmentToUpgradedSegments.containsKey(baseIdentifier)) {
        Set<SegmentIdWithShardSpec> relevantSegments = new HashSet<>(baseSegmentToUpgradedSegments.get(baseIdentifier));
        relevantSegments.removeAll(abandonedSegments);
        // If there are unabandoned segments associated with the appendable segment, return early
        // This may be the case if segments have been upgraded as the result of a concurrent replace
        if (!relevantSegments.isEmpty()) {
          return Futures.immediateFuture(null);
        }
      }
    }
    // Ensure no future writes will be made to this appendable segment.
    if (appendableSegment.finishWriting()) {
      // Decrement this appendable segment's rows from the counters. we only count active appendable segments so that
      // we don't double decrement, i.e. those that haven't been persisted for *InMemory counters, or pushed to deep
      // storage for the total counter.
      rowsCurrentlyInMemory.addAndGet(-appendableSegment.getNumRowsInMemory());
      bytesCurrentlyInMemory.addAndGet(-appendableSegment.getBytesInMemory());
      bytesCurrentlyInMemory.addAndGet(-calculateAppendableSegmentMemoryInUsed(appendableSegment));
      for (PartialSegment partialSegment : appendableSegment) {
        // Decrement memory used by all Memory Mapped partial segment
        if (!partialSegment.equals(appendableSegment.getCurrentPartialSegment())) {
          bytesCurrentlyInMemory.addAndGet(-calculateMMappedPartialSegmentsMemoryInUsed(partialSegment));
        }
      }
      totalRows.addAndGet(-appendableSegment.getNumRows());
    }

    // Mark this identifier as dropping, so no future push tasks will pick it up.
    droppingAppendableSegments.add(baseIdentifier);

    // Wait for any outstanding pushes to finish, then abandon the segment inside the persist thread.
    return Futures.transform(
        pushBarrier(),
        new Function<Object, Void>()
        {
          @Nullable
          @Override
          public Void apply(@Nullable Object input)
          {
            if (!appendableSegments.remove(baseIdentifier, appendableSegment)) {
              log.error("Appendable segment[%s] no longer valid, not abandoning.", baseIdentifier);
              return null;
            }

            metrics.setAppendableSegmentCount(appendableSegments.size());

            if (removeOnDiskData) {
              // Remove this segment from the committed list. This must be done from the persist thread.
              log.debug("Removing commit metadata for segment[%s].", baseIdentifier);
              try {
                commitLock.lock();
                final Committed oldCommit = readCommit();
                if (oldCommit != null) {
                  writeCommit(oldCommit.without(baseIdentifier.toString()));
                }
              }
              catch (Exception e) {
                log.makeAlert(e, "Failed to update committed segments[%s]", schema.getDataSource())
                   .addData("identifier", baseIdentifier.toString())
                   .emit();
                throw new RuntimeException(e);
              }
              finally {
                commitLock.unlock();
              }
            }

            unannounceAllVersionsOfSegment(appendableSegment.getSegment(), appendableSegment);

            Runnable removeRunnable = () -> {
              droppingAppendableSegments.remove(baseIdentifier);
              appendableSegmentTimeline.remove(
                  appendableSegment.getInterval(),
                  appendableSegment.getVersion(),
                  baseIdentifier.getShardSpec().createChunk(appendableSegment)
              );
              for (PartialSegment partialSegment : appendableSegment) {
                if (cache != null) {
                  cache.close(AppendableSegmentQuerySegmentWalker.makePartialSegmentCacheIdentifier(partialSegment));
                }
                partialSegment.swapSegment(null);
              }

              if (removeOnDiskData) {
                removeDirectory(computePersistDir(baseIdentifier));
              }

              log.info("Dropped segment[%s].", identifier);
            };

            if (segmentLoaderConfig == null) {
              log.info(
                  "Unannounced segment[%s]",
                  identifier
              );
              removeRunnable.run();
            } else {
              log.info(
                  "Unannounced segment[%s], scheduling drop in [%d] millisecs",
                  identifier,
                  segmentLoaderConfig.getDropSegmentDelayMillis()
              );
              // Keep the segments in the cache and appendableSegmentTimeline for dropSegmentDelay after unannouncing
              // the segments. This way, in transit queries which still see the segments in this peon would be able to
              // query the segments and not throw NullPtr exceptions.
              exec.schedule(
                  removeRunnable,
                  segmentLoaderConfig.getDropSegmentDelayMillis(),
                  TimeUnit.MILLISECONDS
              );
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
      // merge current partial segments with existing partial segments
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

  private File computePersistDir(SegmentIdWithShardSpec identifier)
  {
    return new File(tuningConfig.getBasePersistDirectory(), identifier.toString());
  }

  private File computeIdentifierFile(SegmentIdWithShardSpec identifier)
  {
    return new File(computePersistDir(identifier), IDENTIFIER_FILE_NAME);
  }

  private File computeDescriptorFile(SegmentIdWithShardSpec identifier)
  {
    return new File(computePersistDir(identifier), "descriptor.json");
  }

  private File createPersistDirIfNeeded(SegmentIdWithShardSpec identifier) throws IOException
  {
    final File persistDir = computePersistDir(identifier);
    FileUtils.mkdirp(persistDir);

    objectMapper.writeValue(computeIdentifierFile(identifier), identifier);

    return persistDir;
  }

  /**
   * Persists the given {@link PartialSegment} and returns the number of rows persisted. Must only be called in the
   * single-threaded persistExecutor.
   *
   * @param indexToPersist partial segment to persist
   * @param identifier     the segment this partial segment is going to be part of
   *
   * @return the number of rows persisted
   */
  private int persistPartialSegment(PartialSegment indexToPersist, SegmentIdWithShardSpec identifier)
  {
    synchronized (indexToPersist) {
      if (indexToPersist.hasSwapped()) {
        log.info(
            "Segment[%s] partial segment[%s] already swapped. Ignoring request to persist.",
            identifier,
            indexToPersist
        );
        return 0;
      }

      log.debug("Segment[%s], persisting partial segment[%s]", identifier, indexToPersist);

      try {
        final long startTime = System.nanoTime();
        int numRows = indexToPersist.getIndex().size();

        final File persistedFile;
        final File persistDir = createPersistDirIfNeeded(identifier);
        persistedFile = indexMerger.persist(
            indexToPersist.getIndex(),
            identifier.getInterval(),
            new File(persistDir, String.valueOf(indexToPersist.getCount())),
            tuningConfig.getIndexSpecForIntermediatePersists(),
            tuningConfig.getSegmentWriteOutMediumFactory()
        );

        log.info(
            "Flushed in-memory data for segment[%s] spill[%s] to disk in [%,d] ms (%,d rows).",
            indexToPersist.getSegmentId(),
            indexToPersist.getCount(),
            (System.nanoTime() - startTime) / 1000000,
            numRows
        );

        indexToPersist.swapSegment(new QueryableIndexSegment(
            indexIO.loadIndex(persistedFile),
            indexToPersist.getSegmentId()
        ));

        return numRows;
      }
      catch (IOException e) {
        log.makeAlert("Incremental persist failed")
           .addData("segment", identifier.toString())
           .addData("dataSource", schema.getDataSource())
           .addData("count", indexToPersist.getCount())
           .emit();

        throw new RuntimeException(e);
      }
    }
  }

  private void removeDirectory(final File target)
  {
    if (target.exists()) {
      try {
        FileUtils.deleteDirectory(target);
      }
      catch (Exception e) {
        log.makeAlert(e, "Failed to remove directory[%s]", schema.getDataSource())
           .addData("file", target)
           .emit();
      }
    }
  }

  private int calculateMMappedPartialSegmentsMemoryInUsed(PartialSegment partialSegment)
  {
    if (skipBytesInMemoryOverheadCheck) {
      return 0;
    }
    // These calculations are approximated from actual heap dumps.
    // Memory footprint includes count integer in partial segment, shorts in ReferenceCountingSegment,
    // Objects in SimpleQueryableIndex (such as SmooshedFileMapper, each ColumnHolder in column map, etc.)
    int total = Integer.BYTES + (4 * Short.BYTES) + ROUGH_OVERHEAD_PER_PARTIAL_SEGMENT +
                (partialSegment.getSegmentNumDimensionColumns() * ROUGH_OVERHEAD_PER_DIMENSION_COLUMN_HOLDER) +
                (partialSegment.getSegmentNumMetricColumns() * ROUGH_OVERHEAD_PER_METRIC_COLUMN_HOLDER) +
                ROUGH_OVERHEAD_PER_TIME_COLUMN_HOLDER;
    return total;
  }

  private int calculateAppendableSegmentMemoryInUsed(AppendableSegment appendableSegment)
  {
    if (skipBytesInMemoryOverheadCheck) {
      return 0;
    }
    // Rough estimate of memory footprint of empty appendable segment based on actual heap dumps
    return ROUGH_OVERHEAD_PER_APPENDABLE_SEGMENT;
  }

  /**
   * This inner class periodically computes absolute and delta schema for all the {@link StreamAppenderator#appendableSegments}
   * and announces them.
   */
  @VisibleForTesting
  class AppendableSegmentSchemaAnnouncer
  {
    private static final long SCHEMA_PUBLISH_DELAY_MILLIS = 0;
    private static final long SCHEMA_PUBLISH_PERIOD_MILLIS = 60_000;

    private final DataSegmentAnnouncer announcer;
    private final ScheduledExecutorService scheduledExecutorService;
    private final String taskId;
    // This structure is accessed only by a single thread (Appendable-Segment-Schema-Announcer-0), hence it is not thread safe.
    private Map<SegmentId, Pair<RowSignature, Integer>> previousAppendableSegmentsSignatureMap = new HashMap<>();

    AppendableSegmentSchemaAnnouncer()
    {
      this.announcer = StreamAppenderator.this.segmentAnnouncer;
      this.taskId = StreamAppenderator.this.myId;
      boolean enabled = centralizedDatasourceSchemaConfig.isEnabled();
      this.scheduledExecutorService = enabled ? ScheduledExecutors.fixed(1, "Appendable-Segment-Schema-Announcer-%d") : null;
    }

    private void start()
    {
      if (scheduledExecutorService != null) {
        scheduledExecutorService.scheduleAtFixedRate(
            this::computeAndAnnounce,
            SCHEMA_PUBLISH_DELAY_MILLIS,
            SCHEMA_PUBLISH_PERIOD_MILLIS,
            TimeUnit.MILLISECONDS
        );
      }
    }

    private void stop()
    {
      if (scheduledExecutorService != null) {
        announcer.removeSegmentSchemasForTask(taskId);
        scheduledExecutorService.shutdown();
      }
    }

    @VisibleForTesting
    void computeAndAnnounce()
    {
      Map<SegmentId, Pair<RowSignature, Integer>> currentAppendableSegmentSignatureMap = new HashMap<>();
      for (Map.Entry<SegmentIdWithShardSpec, AppendableSegment> appendableSegmentEntry : StreamAppenderator.this.appendableSegments.entrySet()) {
        SegmentIdWithShardSpec segmentIdWithShardSpec = appendableSegmentEntry.getKey();
        AppendableSegment appendableSegment = appendableSegmentEntry.getValue();
        currentAppendableSegmentSignatureMap.put(segmentIdWithShardSpec.asSegmentId(), Pair.of(appendableSegment.getSignature(), appendableSegment.getNumRows()));
      }

      Optional<SegmentSchemas> appendableSegmentsSchema = AppendableSegmentSchemaUtil.computeAbsoluteSchema(currentAppendableSegmentSignatureMap);
      Optional<SegmentSchemas> appendableSegmentsSchemaChange = AppendableSegmentSchemaUtil.computeSchemaChange(
          previousAppendableSegmentsSignatureMap,
          currentAppendableSegmentSignatureMap
      );

      // update previous reference
      previousAppendableSegmentsSignatureMap = currentAppendableSegmentSignatureMap;

      // announce schema
      appendableSegmentsSchema.ifPresent(
          segmentsSchema -> announcer.announceSegmentSchemas(
              taskId,
              segmentsSchema,
              appendableSegmentsSchemaChange.orElse(null)
          ));
    }
  }
}
