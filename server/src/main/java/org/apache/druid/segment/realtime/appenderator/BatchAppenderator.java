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
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.data.input.Committer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
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
import org.apache.druid.segment.incremental.IncrementalIndexAddResult;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.FingerprintGenerator;
import org.apache.druid.segment.realtime.AppendableSegment;
import org.apache.druid.segment.realtime.PartialSegment;
import org.apache.druid.segment.realtime.SegmentGenerationMetrics;
import org.apache.druid.timeline.DataSegment;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * This is a new class produced when the old {@code AppenderatorImpl} was split. For historical
 * reasons, the code for creating segments was all handled by the same code path in that class. The code
 * was correct but inefficient for batch ingestion from a memory perspective. If the input file being processed
 * by batch ingestion had enough {@link AppendableSegment} & {@link PartialSegment} produced then it may run
 * out of memory either in the {@link PartialSegment} creation phase (append) of this class or in the merge
 * {@link PartialSegment} phase. Therefore, a new class, {@code BatchAppenderator}, this class, was created to
 * specialize in batch ingestion and the old class for stream ingestion was renamed to {@link StreamAppenderator}.
 */
public class BatchAppenderator implements Appenderator
{
  public static final int ROUGH_OVERHEAD_PER_APPENDABLE_SEGMENT = 5000;
  // Rough estimate of memory footprint of empty PartialSegment based on actual heap dumps
  public static final int ROUGH_OVERHEAD_PER_PARTIAL_SEGMENT = 1000;

  private static final EmittingLogger log = new EmittingLogger(BatchAppenderator.class);
  private static final String IDENTIFIER_FILE_NAME = "identifier.json";

  private final String myId;
  private final DataSchema schema;
  private final AppenderatorConfig tuningConfig;
  private final SegmentGenerationMetrics metrics;
  private final DataSegmentPusher dataSegmentPusher;
  private final ObjectMapper objectMapper;
  private final IndexIO indexIO;
  private final IndexMerger indexMerger;
  private final long maxBytesTuningConfig;
  private final boolean skipBytesInMemoryOverheadCheck;
  private final boolean useMaxMemoryEstimates;

  private volatile ListeningExecutorService persistExecutor = null;
  private volatile ListeningExecutorService pushExecutor = null;
  private final int maxPendingPersists;
  private static final int PERSIST_WARN_DELAY = 1000;
  private volatile Throwable persistError;


  /**
   * The following map used to be accessed concurrently but not anymore since it is fully copied
   * then initalized just before scheduling the persit callable in the {@link #persistAll(Committer)}
   * method, so no longer need to use a syncronized map.
   */
  private final Map<SegmentIdWithShardSpec, AppendableSegment> appendableSegments = new HashMap<>();
  /**
   * The following appendable segments metadata map and associated class are the way to retain metadata now that
   * appendable segments are being completely removed from memory after each incremental persist. This map needs to
   * be concurrent since it is mutated in various methods potentially in different threads.
   * For example mutated in {@link #add}  when adding rows
   * and accessed/mutated in {@link #persistPartialSegment} during persists.
   */
  private final ConcurrentHashMap<SegmentIdWithShardSpec, AppendableSegmentMetadata> appendableSegmentsMetadata =
      new ConcurrentHashMap<>();

  // This variable updated in add(), persist(), and drop()
  private int rowsCurrentlyInMemory = 0;
  private int totalRows = 0;
  private long bytesCurrentlyInMemory = 0;
  private final RowIngestionMeters rowIngestionMeters;
  private final ParseExceptionHandler parseExceptionHandler;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  private volatile FileLock basePersistDirLock = null;
  private volatile FileChannel basePersistDirLockChannel = null;

  private final CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig;

  private final FingerprintGenerator fingerprintGenerator;

  BatchAppenderator(
      String id,
      DataSchema schema,
      AppenderatorConfig tuningConfig,
      SegmentGenerationMetrics metrics,
      DataSegmentPusher dataSegmentPusher,
      ObjectMapper objectMapper,
      IndexIO indexIO,
      IndexMerger indexMerger,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler,
      boolean useMaxMemoryEstimates,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
  )
  {
    this.myId = id;
    this.schema = Preconditions.checkNotNull(schema, "schema");
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
    this.metrics = Preconditions.checkNotNull(metrics, "metrics");
    this.dataSegmentPusher = Preconditions.checkNotNull(dataSegmentPusher, "dataSegmentPusher");
    this.objectMapper = Preconditions.checkNotNull(objectMapper, "objectMapper");
    this.indexIO = Preconditions.checkNotNull(indexIO, "indexIO");
    this.indexMerger = Preconditions.checkNotNull(indexMerger, "indexMerger");
    this.rowIngestionMeters = Preconditions.checkNotNull(rowIngestionMeters, "rowIngestionMeters");
    this.parseExceptionHandler = Preconditions.checkNotNull(parseExceptionHandler, "parseExceptionHandler");

    maxBytesTuningConfig = tuningConfig.getMaxBytesInMemoryOrDefault();
    skipBytesInMemoryOverheadCheck = tuningConfig.isSkipBytesInMemoryOverheadCheck();
    maxPendingPersists = tuningConfig.getMaxPendingPersists();
    this.useMaxMemoryEstimates = useMaxMemoryEstimates;
    this.centralizedDatasourceSchemaConfig = centralizedDatasourceSchemaConfig;
    this.fingerprintGenerator = new FingerprintGenerator(objectMapper);
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
    initializeExecutors();
    return null;
  }

  private void throwPersistErrorIfExists()
  {
    if (persistError != null) {
      throw new RE(persistError, "Error while persisting");
    }
  }

  private void initializeExecutors()
  {
    log.debug("There will be up to[%d] pending persists", maxPendingPersists);

    if (persistExecutor == null) {
      log.info("Number of persist threads [%d]", tuningConfig.getNumPersistThreads());
      persistExecutor = MoreExecutors.listeningDecorator(
          Execs.newBlockingThreaded(
              "[" + StringUtils.encodeForFormat(myId) + "]-batch-appenderator-persist",
              tuningConfig.getNumPersistThreads(), maxPendingPersists
          )
      );
    }

    if (pushExecutor == null) {
      // use a blocking single threaded executor to throttle the firehose when write to disk is slow
      pushExecutor = MoreExecutors.listeningDecorator(
          Execs.newBlockingSingleThreaded(
              "[" + StringUtils.encodeForFormat(myId) + "]-batch-appenderator-push",
              1
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

    Preconditions.checkArgument(
        committerSupplier == null,
        "Batch appenderator does not need a committer!"
    );

    Preconditions.checkArgument(
        allowIncrementalPersists,
        "Batch appenderator should always allow incremental persists!"
    );

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
      addResult = appendableSegment.add(row, false); // allow incrememtal persis is always true for batch
      appendableSegmentRowsInMemoryAfterAdd = addResult.getRowCount();
      bytesInMemoryAfterAdd = addResult.getBytesInMemory();
    }
    catch (IndexSizeExceededException e) {
      // Uh oh, we can't do anything about this! We can't persist (commit metadata would be out of sync) and we
      // can't add the row (it just failed). This should never actually happen, though, because we check
      // appendableSEgment.canAddRow after returning from add.
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
    rowsCurrentlyInMemory += numAddedRows;
    bytesCurrentlyInMemory += (bytesInMemoryAfterAdd - bytesInMemoryBeforeAdd);
    totalRows += numAddedRows;
    appendableSegmentsMetadata.computeIfAbsent(identifier, unused -> new AppendableSegmentMetadata()).addRows(numAddedRows);

    boolean persist = false;
    List<String> persistReasons = new ArrayList<>();

    if (!appendableSegment.canAppendRow()) {
      persist = true;
      persistReasons.add("No more rows can be appended to appendable segment");
    }
    if (rowsCurrentlyInMemory >= tuningConfig.getMaxRowsInMemory()) {
      persist = true;
      persistReasons.add(StringUtils.format(
          "rowsCurrentlyInMemory[%d] is greater than maxRowsInMemory[%d]",
          rowsCurrentlyInMemory,
          tuningConfig.getMaxRowsInMemory()
      ));
    }
    if (bytesCurrentlyInMemory >= maxBytesTuningConfig) {
      persist = true;
      persistReasons.add(StringUtils.format(
          "bytesCurrentlyInMemory[%d] is greater than maxBytesInMemory[%d]",
          bytesCurrentlyInMemory,
          maxBytesTuningConfig
      ));
    }

    if (persist) {
      // persistAll clears rowsCurrentlyInMemory, no need to update it.
      log.info("Incremental persist to disk because %s.", String.join(",", persistReasons));

      long bytesToBePersisted = 0L;
      for (Map.Entry<SegmentIdWithShardSpec, AppendableSegment> entry : appendableSegments.entrySet()) {
        final AppendableSegment appendableSegmentEntry = entry.getValue();
        if (appendableSegmentEntry != null) {
          bytesToBePersisted += appendableSegmentEntry.getBytesInMemory();
          if (appendableSegmentEntry.swappable()) {
            // Code for batch no longer memory maps partial segments, but they still take memory...
            int memoryStillInUse = calculateMemoryUsedByPartialSegments();
            bytesCurrentlyInMemory += memoryStillInUse;
          }
        }
      }

      if (!skipBytesInMemoryOverheadCheck
          && bytesCurrentlyInMemory - bytesToBePersisted > maxBytesTuningConfig) {
        // We are still over maxBytesTuningConfig even after persisting.
        // This means that we ran out of all available memory to ingest (due to overheads created as part of ingestion)
        final String alertMessage = StringUtils.format(
            "Task has exceeded safe estimated heap usage limits, failing "
            + "(numAppendableSegments: [%d] numPartialSegmentsAcrossAllAppendableSegments: [%d] totalRows: [%d])"
            + "(bytesCurrentlyInMemory: [%d] - bytesToBePersisted: [%d] > maxBytesTuningConfig: [%d])",
            appendableSegments.size(),
            appendableSegments.values().stream().mapToInt(Iterables::size).sum(),
            getTotalRowCount(),
            bytesCurrentlyInMemory,
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
          persistAll(null),
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
    }
    return new AppenderatorAddResult(identifier, appendableSegmentsMetadata.get(identifier).numRowsInSegment, false);
  }

  /**
   * Returns all active segments regardless whether they are in memory or persisted
   */
  @Override
  public List<SegmentIdWithShardSpec> getSegments()
  {
    return ImmutableList.copyOf(appendableSegmentsMetadata.keySet());
  }

  @VisibleForTesting
  public List<SegmentIdWithShardSpec> getInMemorySegments()
  {
    return ImmutableList.copyOf(appendableSegments.keySet());
  }

  @Override
  public int getRowCount(final SegmentIdWithShardSpec identifier)
  {
    return appendableSegmentsMetadata.get(identifier).getNumRowsInSegment();
  }

  @Override
  public int getTotalRowCount()
  {
    return totalRows;
  }

  @VisibleForTesting
  public int getRowsInMemory()
  {
    return rowsCurrentlyInMemory;
  }

  @VisibleForTesting
  public long getBytesCurrentlyInMemory()
  {
    return bytesCurrentlyInMemory;
  }

  @VisibleForTesting
  public long getBytesInMemory(SegmentIdWithShardSpec identifier)
  {
    final AppendableSegment appendableSegment = appendableSegments.get(identifier);

    if (appendableSegment == null) {
      return 0L; // appendable segments are removed after a persist
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
      bytesCurrentlyInMemory += calculateAppendableSegmentMemoryInUsed();
      appendableSegments.put(identifier, retVal);
      metrics.setAppendableSegmentCount(appendableSegments.size());
    }

    return retVal;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, final Iterable<Interval> intervals)
  {
    throw new UnsupportedOperationException("No query runner for batch appenderator");
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, final Iterable<SegmentDescriptor> specs)
  {
    throw new UnsupportedOperationException("No query runner for batch appenderator");
  }

  @Override
  public void clear()
  {
    throwPersistErrorIfExists();
    clear(appendableSegments, true);
  }

  private void clear(Map<SegmentIdWithShardSpec, AppendableSegment> appendableSegmentsToClear, boolean removeOnDiskData)
  {
    // Drop commit metadata, then abandon all segments.
    log.info(
        "Clearing all[%d] appendable segments & their partial segments, removing data on disk: [%s]",
        appendableSegmentsToClear.size(),
        removeOnDiskData
    );
    // Drop everything.
    Iterator<Map.Entry<SegmentIdWithShardSpec, AppendableSegment>> appendableSegmentsIterator =
        appendableSegmentsToClear.entrySet().iterator();
    appendableSegmentsIterator.forEachRemaining(entry -> {
      clearAppendableSegmentsMemoryCountersAndDiskStoredData(entry.getKey(), entry.getValue(), removeOnDiskData);
      appendableSegmentsIterator.remove();
    });
    metrics.setAppendableSegmentCount(appendableSegmentsToClear.size());
  }

  @Override
  public ListenableFuture<?> drop(final SegmentIdWithShardSpec identifier)
  {
    final AppendableSegment appendableSegment = appendableSegments.get(identifier);
    AppendableSegmentMetadata sm = appendableSegmentsMetadata.remove(identifier);
    if (sm != null) {
      int originalTotalRows = getTotalRowCount();
      int rowsToDrop = sm.getNumRowsInSegment();
      int totalRowsAfter = originalTotalRows - rowsToDrop;
      if (totalRowsAfter < 0) {
        log.warn("Total rows[%d] after dropping segment[%s] rows [%d]", totalRowsAfter, identifier, rowsToDrop);
      }
      totalRows = Math.max(totalRowsAfter, 0);
    }
    if (appendableSegment != null) {
      clearAppendableSegmentsMemoryCountersAndDiskStoredData(identifier, appendableSegment, true);
      if (appendableSegments.remove(identifier) == null) {
        log.warn("Appendable segment[%s] not found, skipping", identifier);
      }
    }
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<Object> persistAll(@Nullable final Committer committer)
  {

    throwPersistErrorIfExists();

    if (committer != null) {
      throw new ISE("committer must be null for BatchAppenderator");
    }
    // Get ready to persist all appendable segments:
    final Map<SegmentIdWithShardSpec, AppendableSegment> appendableSegmentsToPersist = swapAppendableSegments();

    final Stopwatch runExecStopwatch = Stopwatch.createStarted();
    ListenableFuture<Object> future = persistExecutor.submit(
        () -> {
          log.info("Spawning intermediate persist");

          // figure out partial segments to persist:
          final List<Pair<PartialSegment, SegmentIdWithShardSpec>> indexesToPersist = new ArrayList<>();
          int numPersistedRows = 0;
          long bytesPersisted = 0;
          int totalPartialSegmentsCount = 0;
          final long totalAppendableSegments = appendableSegmentsToPersist.size();
          for (Map.Entry<SegmentIdWithShardSpec, AppendableSegment> entry : appendableSegmentsToPersist.entrySet()) {
            final SegmentIdWithShardSpec identifier = entry.getKey();
            final AppendableSegment appendableSegment = entry.getValue();
            if (appendableSegment == null) {
              throw new ISE("No appendable segment: %s", identifier);
            }

            final List<PartialSegment> partialSegments = Lists.newArrayList(appendableSegment);
            // Since everytime we persist we also get rid of the in-memory references to appendable segment & partial
            // segment the invariant of exactly one, always swappable, appendable segment with exactly one
            // unpersisted partial segment must hold
            int totalPartialSegmentsForAppendableSegment = partialSegments.size();
            if (totalPartialSegmentsForAppendableSegment != 1) {
              throw new ISE(
                  "There should be only one partial segment for identifier[%s] but there are[%s]",
                  identifier,
                  totalPartialSegmentsForAppendableSegment
              );
            }
            totalPartialSegmentsCount++;
            numPersistedRows += appendableSegment.getNumRowsInMemory();
            bytesPersisted += appendableSegment.getBytesInMemory();

            if (!appendableSegment.swappable()) {
              throw new ISE("Appendable segment is not swappable![%s]", identifier);
            }
            indexesToPersist.add(Pair.of(appendableSegment.swap(), identifier));

          }

          if (indexesToPersist.isEmpty()) {
            log.info("No indexes will be persisted");
          }
          final Stopwatch persistStopwatch = Stopwatch.createStarted();
          try {
            for (Pair<PartialSegment, SegmentIdWithShardSpec> pair : indexesToPersist) {
              metrics.incrementRowOutputCount(persistPartialSegment(pair.lhs, pair.rhs));
            }

            log.info(
                "Persisted in-memory data for segments: %s",
                indexesToPersist.stream()
                                .filter(itp -> itp.rhs != null)
                                .map(itp -> itp.rhs.asSegmentId().toString())
                                .distinct()
                                .collect(Collectors.joining(", "))
            );
            log.info(
                "Persisted stats: processed rows: [%d], persisted rows[%d], persisted appendable segments: [%d], persisted partial segment (across appendable segments): [%d]",
                rowIngestionMeters.getProcessed(),
                numPersistedRows,
                totalAppendableSegments,
                totalPartialSegmentsCount
            );

            // note that we do not need to reset appendable segments metadata since we did it at the start...

          }
          catch (Exception e) {
            metrics.incrementFailedPersists();
            throw e;
          }
          finally {
            metrics.incrementNumPersists();
            long persistMillis = persistStopwatch.elapsed(TimeUnit.MILLISECONDS);
            metrics.incrementPersistTimeMillis(persistMillis);
            persistStopwatch.stop();
            // make sure no push can start while persisting:
            log.info(
                "Persisted rows[%,d] and bytes[%,d] and removed all appendable segments & partial segments from memory in[%d] millis",
                numPersistedRows,
                bytesPersisted,
                persistMillis
            );
            log.info("Persist is done.");
          }
          return null;
        }
    );

    final long startDelay = runExecStopwatch.elapsed(TimeUnit.MILLISECONDS);
    metrics.incrementPersistBackPressureMillis(startDelay);
    if (startDelay > PERSIST_WARN_DELAY) {
      log.warn("Ingestion was throttled for [%,d] millis because persists were pending.", startDelay);
    }
    runExecStopwatch.stop();
    return future;
  }

  /**
   * All appendable segments will be persisted so do a shallow copy of the {@link #appendableSegments} map, reset
   * the map and metadata (i.e. memory consumption counters) so that ingestion can go on
   * @return The map of appendable segments to persist, this map will be garbage collected after
   * persist is complete since we will not be keeping a reference to it...
   */
  Map<SegmentIdWithShardSpec, AppendableSegment> swapAppendableSegments()
  {
    Map<SegmentIdWithShardSpec, AppendableSegment> retVal = ImmutableMap.copyOf(appendableSegments);
    appendableSegments.clear();
    resetAppendableSegmentMetadata();
    return retVal;
  }


  @Override
  public ListenableFuture<SegmentsAndCommitMetadata> push(
      final Collection<SegmentIdWithShardSpec> identifiers,
      @Nullable final Committer committer,
      final boolean useUniquePath
  )
  {
    if (committer != null) {
      throw new ISE("There should be no committer for batch ingestion");
    }

    if (useUniquePath) {
      throw new ISE("Batch ingestion does not require uniquePath");
    }

    final List<DataSegment> dataSegments = new ArrayList<>();
    final SegmentSchemaMapping segmentSchemaMapping = new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION);

    return Futures.transform(
        persistAll(null), // make sure persists is done before push...
        (Function<Object, SegmentsAndCommitMetadata>) commitMetadata -> {

          log.info("Push started, processsing[%d] appendable segments", identifiers.size());

          int totalPartialSegmentsMerged = 0;
          for (SegmentIdWithShardSpec identifier : identifiers) {
            AppendableSegmentMetadata sm = appendableSegmentsMetadata.get(identifier);
            if (sm == null) {
              throw new ISE("No appendable segment has been processed for identifier[%s]", identifier);
            }
            File persistedDir = sm.getPersistedFileDir();
            if (persistedDir == null) {
              throw new ISE("Persisted directory for identifier[%s] is null in appendable segment metadata", identifier);
            }
            totalPartialSegmentsMerged += sm.getNumPartialSegments();

            // retrieve appendable segment from disk:
            AppendableSegment appendableSegmentForIdentifier;
            try {
              appendableSegmentForIdentifier = getAppendableSegmentForIdentifierPath(identifier, persistedDir);
            }
            catch (IOException e) {
              throw new ISE(e, "Failed to retrieve appendable segment for identifier[%s]", identifier);
            }

            // push it:
            final DataSegmentWithMetadata dataSegmentWithMetadata = mergeAndPush(
                identifier,
                appendableSegmentForIdentifier
            );

            // record it:
            if (dataSegmentWithMetadata.getDataSegment() != null) {
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
              log.warn("mergeAndPush[%s] returned null, skipping.", identifier);
            }
          }
          log.info("Push done: total appendable segments merged[%d], total partial segments merged[%d]",
                   identifiers.size(), totalPartialSegmentsMerged
          );
          return new SegmentsAndCommitMetadata(dataSegments, commitMetadata, segmentSchemaMapping);
        },
        pushExecutor // push it in the background, pushAndClear in BaseAppenderatorDriver guarantees
        // that segments are dropped before next add row
    );
  }

  /**
   * Merge segment, push to deep storage. Should only be used on segments that have been fully persisted.
   *
   * @param identifier          appendable segment identifier
   * @param appendableSegment   appendable segment to push
   * @return segment descriptor along with schema, or null if the appendable segment is no longer valid
   */
  private DataSegmentWithMetadata mergeAndPush(
      final SegmentIdWithShardSpec identifier,
      final AppendableSegment appendableSegment
  )
  {
    // Use a descriptor file to indicate that pushing has completed.
    final File persistDir = computePersistDir(identifier);
    final File mergedTarget = new File(persistDir, "merged");
    final File descriptorFile = computeDescriptorFile(identifier);

    // Sanity checks
    if (appendableSegment.isWritable()) {
      throw new ISE("Expected appendable segment to be no longer writable before mergeAndPush for segment[%s].", identifier);
    }

    int numPartialSegments = 0;
    for (PartialSegment partialSegment : appendableSegment) {
      if (!partialSegment.hasSwapped()) {
        throw new ISE("Expected appendable segment to be fully persisted before mergeAndPush for segment[%s].", identifier);
      }
      numPartialSegments++;
    }

    AppendableSegmentMetadata sm = appendableSegmentsMetadata.get(identifier);
    if (sm == null) {
      log.warn("Appendable segment metadata not found just before merge for identifier [%s]", identifier);
    } else if (numPartialSegments != sm.getNumPartialSegments()) {
      throw new ISE(
          "Number of restored partial segment[%d] for identifier[%s] does not match expected value[%d]",
          numPartialSegments,
          identifier,
          sm.getNumPartialSegments()
      );
    }

    try {
      if (descriptorFile.exists()) {
        // Already pushed.
        log.info("Segment[%s] already pushed, skipping.", identifier);
        return new DataSegmentWithMetadata(
            objectMapper.readValue(descriptorFile, DataSegment.class),
            centralizedDatasourceSchemaConfig.isEnabled() ? TaskSegmentSchemaUtil.getSegmentSchema(
                mergedTarget,
                indexIO
            ) : null
        );
      }

      removeDirectory(mergedTarget);

      if (mergedTarget.exists()) {
        throw new ISE("Merged target[%s] exists after removing?!", mergedTarget);
      }

      final File mergedFile;
      final long mergeFinishTime;
      final long startTime = System.nanoTime();
      List<QueryableIndex> indexes = new ArrayList<>();
      long rowsinMergedSegment = 0L;
      Closer closer = Closer.create();
      try {
        for (PartialSegment partialSegment : appendableSegment) {
          Pair<ReferenceCountingSegment, Closeable> segmentAndCloseable = partialSegment.getAndIncrementSegment();
          final QueryableIndex queryableIndex = segmentAndCloseable.lhs.asQueryableIndex();
          if (queryableIndex != null) {
            rowsinMergedSegment += queryableIndex.getNumRows();
          }
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
        metrics.incrementMergedRows(rowsinMergedSegment);

        log.debug("Segment[%s] built in %,dms.", identifier, (mergeFinishTime - startTime) / 1000000);
      }
      catch (Throwable t) {
        throw closer.rethrow(t);
      }
      finally {
        closer.close();
      }

      // dataSegmentPusher retries internally when appropriate; no need for retries here.
      final DataSegment segment = dataSegmentPusher.push(
          mergedFile,
          appendableSegment.getSegment()
                           .withDimensions(
                  IndexMerger.getMergedDimensionsFromQueryableIndexes(
                      indexes,
                      schema.getDimensionsSpec()
                  )
              ),
          false
      );

      // Drop the queryable indexes behind the partial segment... they are not needed anymore and their
      // mapped file references
      // can generate OOMs during merge if enough of them are held back...
      for (PartialSegment partialSegment : appendableSegment) {
        partialSegment.swapSegment(null);
      }

      SchemaPayloadPlus schemaMetadata =
          centralizedDatasourceSchemaConfig.isEnabled()
          ? TaskSegmentSchemaUtil.getSegmentSchema(mergedTarget, indexIO)
          : null;

      // cleanup, appendable segment no longer needed
      removeDirectory(computePersistDir(identifier));

      final long pushFinishTime = System.nanoTime();
      metrics.incrementPushedRows(rowsinMergedSegment);

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

      return new DataSegmentWithMetadata(segment, schemaMetadata);
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

    try {

      log.debug("Shutdown & wait for persistExecutor");
      if (persistExecutor != null) {
        persistExecutor.shutdown();
        if (!persistExecutor.awaitTermination(365, TimeUnit.DAYS)) {
          log.warn("persistExecutor not terminated");
        }
        persistExecutor = null;
      }

      log.debug("Shutdown & wait for pushExecutor");
      if (pushExecutor != null) {
        pushExecutor.shutdown();
        if (!pushExecutor.awaitTermination(365, TimeUnit.DAYS)) {
          log.warn("pushExecutor not terminated");
        }
        pushExecutor = null;
      }

    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ISE("Failed to wait & shutdown executors during close()");
    }

    log.debug("Waited for and shutdown executors...");

    // cleanup:
    clear(appendableSegments, false);
    unlockBasePersistDirectory();
    List<File> persistedIdentifiers = getPersistedidentifierPaths();
    if (persistedIdentifiers != null) {
      for (File identifier : persistedIdentifiers) {
        removeDirectory(identifier);
      }
    }

    totalRows = 0;
    appendableSegmentsMetadata.clear();
  }

  @Override
  public void closeNow()
  {
    if (!closed.compareAndSet(false, true)) {
      log.debug("Appenderator already closed, skipping closeNow() call.");
      return;
    }
    log.debug("Shutting down immediately...");
    shutdownExecutors();
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

  @VisibleForTesting
  @Nullable
  public List<File> getPersistedidentifierPaths()
  {

    ArrayList<File> retVal = new ArrayList<>();

    final File baseDir = tuningConfig.getBasePersistDirectory();
    if (!baseDir.exists()) {
      return null;
    }

    final File[] files = baseDir.listFiles();
    if (files == null) {
      return null;
    }

    for (File appendableSegmentDir : files) {
      final File identifierFile = new File(appendableSegmentDir, IDENTIFIER_FILE_NAME);
      if (!identifierFile.isFile()) {
        // No identifier in this appendableSegmentDir; it must not actually be a appendable segment directory. Skip it.
        continue;
      }
      retVal.add(appendableSegmentDir);
    }

    return retVal;
  }

  private AppendableSegment getAppendableSegmentForIdentifierPath(SegmentIdWithShardSpec identifier, File identifierPath)
      throws IOException
  {
    // To avoid reading and listing of "merged" dir and other special files
    final File[] appendableSegmentFiles = identifierPath.listFiles(
        (dir, fileName) -> !(Ints.tryParse(fileName) == null)
    );
    if (appendableSegmentFiles == null) {
      throw new ISE("Problem reading persisted appendable segments in path[%s]", identifierPath);
    }

    Arrays.sort(
        appendableSegmentFiles,
        (o1, o2) -> Ints.compare(Integer.parseInt(o1.getName()), Integer.parseInt(o2.getName()))
    );

    List<PartialSegment> partialSegments = new ArrayList<>();
    for (File partialSegmentDir : appendableSegmentFiles) {
      final int partialSegmentNumber = Integer.parseInt(partialSegmentDir.getName());

      log.debug("Loading previously persisted partial segment at [%s]", partialSegmentDir);
      if (partialSegmentNumber != partialSegments.size()) {
        throw new ISE("Missing partial segment [%,d] in identifier [%s].", partialSegments.size(), identifier);
      }

      partialSegments.add(
          new PartialSegment(
              new QueryableIndexSegment(indexIO.loadIndex(partialSegmentDir), identifier.asSegmentId()),
              partialSegmentNumber
          )
      );
    }

    AppendableSegment retVal = new AppendableSegment(
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
    retVal.finishWriting(); // this appendable segment is not writable
    return retVal;
  }

  private void resetAppendableSegmentMetadata()
  {
    rowsCurrentlyInMemory = 0;
    bytesCurrentlyInMemory = 0;
    metrics.setAppendableSegmentCount(0);
  }

  // This function does not remove the appendable segment from its tracking Map (appendableSegments), the caller is
  // responsible for that this is because the Map is not synchronized and removing elements from a map while
  // traversing it throws concurrent access exception
  private void clearAppendableSegmentsMemoryCountersAndDiskStoredData(
      final SegmentIdWithShardSpec identifier,
      final AppendableSegment appendableSegment,
      final boolean removeOnDiskData
  )
  {
    // Ensure no future writes will be made to this appendable segment.
    if (appendableSegment.finishWriting()) {
      // Decrement this appendable segment's rows from the counters. we only count active appendable segments so that
      // we don't double decrement, i.e. those that haven't been persisted for *InMemory counters, or pushed to deep
      // storage for the total counter.
      rowsCurrentlyInMemory -= appendableSegment.getNumRowsInMemory();
      bytesCurrentlyInMemory -= appendableSegment.getBytesInMemory();
      bytesCurrentlyInMemory -= calculateAppendableSegmentMemoryInUsed();
      for (PartialSegment partialSegment : appendableSegment) {
        // Decrement memory used by all Memory Mapped partial segment
        if (!partialSegment.equals(appendableSegment.getCurrentPartialSegment())) {
          bytesCurrentlyInMemory -= calculateMemoryUsedByPartialSegments();
        }
      }
      // totalRows are not decremented when removing the appendable segment from memory, appendable segment was just
      // persisted, and it still "lives" but it is in hibernation. It will be revived later just before push.
    }

    if (removeOnDiskData) {
      removeDirectory(computePersistDir(identifier));
    }

    log.info("Removed appendable segment[%s].", identifier);

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
   * Persists the given partial segment and returns the number of rows persisted.
   *
   * @param indexToPersist partial segment to persist
   * @param identifier     the segment this partial segment is going to be part of
   * @return the number of rows persisted
   */
  private int persistPartialSegment(PartialSegment indexToPersist, SegmentIdWithShardSpec identifier)
  {
    if (indexToPersist.hasSwapped()) {
      throw new ISE(
          "Segment[%s] partial segment[%s] already swapped. This cannot happen.",
          identifier,
          indexToPersist
      );
    }

    log.debug("Segment[%s], persisting partial segment[%s]", identifier, indexToPersist);

    try {
      final long startTime = System.nanoTime();
      int numRows = indexToPersist.getIndex().size();

      // since the appendable segment may have been persisted before it may have lost its
      // partial segment count, we remember that value in the appendable segments' metadata, so we have
      // to pull it from there....
      AppendableSegmentMetadata sm = appendableSegmentsMetadata.get(identifier);
      if (sm == null) {
        throw new ISE("Appendable segment must not be null for identifier when persisting partial segment[%s]", identifier);
      }
      final File persistDir = createPersistDirIfNeeded(identifier);
      indexMerger.persist(
          indexToPersist.getIndex(),
          identifier.getInterval(),
          new File(persistDir, String.valueOf(sm.getNumPartialSegments())),
          tuningConfig.getIndexSpecForIntermediatePersists(),
          tuningConfig.getSegmentWriteOutMediumFactory()
      );
      sm.setPersistedFileDir(persistDir);

      log.info(
          "Persisted in-memory data for segment[%s] spill[%s] to disk in [%,d] ms (%,d rows).",
          indexToPersist.getSegmentId(),
          sm.getNumPartialSegments(),
          (System.nanoTime() - startTime) / 1000000,
          numRows
      );

      indexToPersist.swapSegment(null);
      // remember partial segment count:
      sm.addPartialSegments(1);

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

  private void removeDirectory(final File target)
  {
    if (target.exists()) {
      try {
        FileUtils.deleteDirectory(target);
        log.info("Removed directory [%s]", target);
      }
      catch (Exception e) {
        log.makeAlert(e, "Failed to remove directory[%s]", schema.getDataSource())
           .addData("file", target)
           .emit();
      }
    }
  }

  private int calculateMemoryUsedByPartialSegments()
  {
    if (skipBytesInMemoryOverheadCheck) {
      return 0;
    }
    // These calculations are approximated from actual heap dumps.
    int total;
    total = Integer.BYTES + (4 * Short.BYTES) + ROUGH_OVERHEAD_PER_PARTIAL_SEGMENT;
    return total;
  }

  private int calculateAppendableSegmentMemoryInUsed()
  {
    if (skipBytesInMemoryOverheadCheck) {
      return 0;
    }
    // Rough estimate of memory footprint of empty appendable segment based on actual heap dumps
    return ROUGH_OVERHEAD_PER_APPENDABLE_SEGMENT;
  }

  /**
   * This class is used for information that needs to be kept related to {@link AppendableSegment} as
   * they are persisted and removed from memory at every incremental persist.
   * The information is used for sanity checks and as information required
   * for functionality, depending on the field that is used. More info about the
   * fields is annotated as comments in the class
   */
  private static class AppendableSegmentMetadata
  {
    /** This is used to maintain the rows in the appendable segment accross persists of the partial segments
     * used for functionality (i.e. to detect whether an incremental push
     * is needed {@link AppenderatorDriverAddResult#isPushRequired(Integer, Long)}
     **/
    private int numRowsInSegment;
    /** For sanity check as well as functionality: to make sure that all partial segments for an appendable segment are
     *  restored from disk at push time and also to remember the partial segment "count" when persisting it.
     */
    private int numPartialSegments;
    /* Reference to directory that holds the persisted data */
    File persistedFileDir;

    public AppendableSegmentMetadata()
    {
      this(0, 0);
    }

    public AppendableSegmentMetadata(int numRowsInSegment, int numPartialSegments)
    {
      this.numRowsInSegment = numRowsInSegment;
      this.numPartialSegments = numPartialSegments;
    }

    public void addRows(int num)
    {
      numRowsInSegment += num;
    }

    public void addPartialSegments(int num)
    {
      numPartialSegments += num;
    }

    public int getNumRowsInSegment()
    {
      return numRowsInSegment;
    }

    public int getNumPartialSegments()
    {
      return numPartialSegments;
    }

    public void setPersistedFileDir(File persistedFileDir)
    {
      this.persistedFileDir = persistedFileDir;
    }

    public File getPersistedFileDir()
    {
      return persistedFileDir;
    }

  }

}
