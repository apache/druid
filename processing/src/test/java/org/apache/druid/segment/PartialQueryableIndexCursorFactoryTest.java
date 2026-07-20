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

package org.apache.druid.segment;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.common.asyncresource.AsyncResources;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.ListBasedInputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Order;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.file.CountingRangeReader;
import org.apache.druid.segment.file.PartialSegmentDownloadListener;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.segment.file.SegmentFileMetadata;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

class PartialQueryableIndexCursorFactoryTest extends PartialQueryableIndexCursorFactoryTestBase
{
  private static final DateTime TIME = DateTimes.of("2025-01-01");
  private static final String PROJECTION_NAME = "dim1_metric1_sum";

  private static final RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                                .add("dim1", ColumnType.STRING)
                                                                .add("metric1", ColumnType.LONG)
                                                                .build();

  private static final List<AggregateProjectionSpec> PROJECTIONS = Collections.singletonList(
      AggregateProjectionSpec.builder(PROJECTION_NAME)
                             .groupingColumns(new StringDimensionSchema("dim1"))
                             .aggregators(
                                 new LongSumAggregatorFactory("_metric1_sum", "metric1"),
                                 new CountAggregatorFactory("_count")
                             )
                             .build()
  );

  private static final List<InputRow> ROWS = Arrays.asList(
      new ListBasedInputRow(ROW_SIGNATURE, TIME, ROW_SIGNATURE.getColumnNames(), Arrays.asList("a", 1L)),
      new ListBasedInputRow(ROW_SIGNATURE, TIME.plusMinutes(1), ROW_SIGNATURE.getColumnNames(), Arrays.asList("a", 2L)),
      new ListBasedInputRow(ROW_SIGNATURE, TIME.plusMinutes(2), ROW_SIGNATURE.getColumnNames(), Arrays.asList("b", 3L)),
      new ListBasedInputRow(ROW_SIGNATURE, TIME.plusMinutes(3), ROW_SIGNATURE.getColumnNames(), Arrays.asList("b", 4L))
  );

  @TempDir
  static File sharedTempDir;

  private static File segmentDir;
  private static ListeningExecutorService realExec;

  @BeforeAll
  static void buildSegment()
  {
    final File tmpDir = new File(sharedTempDir, "build_" + ThreadLocalRandom.current().nextInt());
    segmentDir = IndexBuilder.create()
                             .useV10()
                             .tmpDir(tmpDir)
                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                             .schema(
                                 IncrementalIndexSchema.builder()
                                                       .withDimensionsSpec(
                                                           DimensionsSpec.builder()
                                                                         .setDimensions(
                                                                             List.of(
                                                                                 new StringDimensionSchema("dim1"),
                                                                                 new LongDimensionSchema("metric1")
                                                                             )
                                                                         )
                                                                         .build()
                                                       )
                                                       .withRollup(false)
                                                       .withMinTimestamp(TIME.getMillis())
                                                       .withProjections(PROJECTIONS)
                                                       .build()
                             )
                             .indexSpec(IndexSpec.builder().withMetadataCompression(CompressionStrategy.NONE).build())
                             .rows(ROWS)
                             .buildMMappedIndexFile();

    realExec = MoreExecutors.listeningDecorator(Execs.singleThreaded("partial-cursor-test-%d"));
  }

  @AfterAll
  static void teardownExec()
  {
    realExec.shutdownNow();
  }


  @Test
  void testSyncMakeCursorHolderThrowsWhenSegmentNotFullyDownloaded() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "sync_throws")) {
      final PartialQueryableIndex index = opened.index();
      Assertions.assertFalse(index.isFullyDownloaded());
      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          index,
          QueryableIndexTimeBoundaryInspector.create(index),
          noOpAcquirer(directExec())
      );

      // Fresh mapper has only the header on disk; no internal files have been downloaded. The sync path must
      // refuse rather than trigger downloads on the calling thread.
      Assertions.assertThrows(
          DruidException.class,
          () -> factory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)
      );
    }
  }

  @Test
  void testSyncMakeCursorHolderSucceedsAfterFullDownload() throws Exception
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "sync_after_async")) {
      final PartialQueryableIndex index = opened.index();
      final PartialSegmentFileMapperV10 mapper = opened.mapper();
      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          index,
          QueryableIndexTimeBoundaryInspector.create(index),
          noOpAcquirer(directExec())
      );

      // Eagerly materialize the entire segment via the file mapper to simulate the eager-acquire path.
      mapper.fetchFiles(mapper.getSegmentFileMetadata().getFiles().keySet());
      Assertions.assertTrue(mapper.isFullyDownloaded(), "test precondition: every internal file should be on disk");

      try (CursorHolder holder = factory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
        Assertions.assertNotNull(holder);
      }
    }
  }

  @Test
  void testAsyncMakeCursorHolderDefersDownloadUntilExecutorRuns() throws Exception
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final CountDownLatch gate = new CountDownLatch(1);
    final ExecutorService rawExec = Execs.singleThreaded("partial-cursor-defer-%d");
    final ListeningExecutorService gatedExec = MoreExecutors.listeningDecorator(rawExec);
    try (IndexAndMapper opened = openIndex(rangeReader, "async_defer")) {
      final PartialQueryableIndex index = opened.index();
      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          index,
          QueryableIndexTimeBoundaryInspector.create(index),
          noOpAcquirer(gatedExec)
      );
      rangeReader.resetCount();

      // Hold the executor with a pre-queued blocker so the actual download task can't start.
      // assign the blocker future to an (intentionally unused) local so errorprone's CheckReturnValue is satisfied
      @SuppressWarnings("unused")
      ListenableFuture<?> unused = gatedExec.submit(() -> {
        try {
          gate.await();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      final AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN);
      Assertions.assertFalse(
          asyncHolder.isReady(),
          "async holder must not be ready while download executor is still blocked"
      );
      Assertions.assertEquals(0, rangeReader.getReadCount(), "no download should have started yet");

      final CountDownLatch ready = new CountDownLatch(1);
      asyncHolder.addReadyCallback(ready::countDown);

      // Release the gate; download should now run.
      gate.countDown();
      Assertions.assertTrue(ready.await(5, TimeUnit.SECONDS), "ready callback must fire after executor processes task");
      Assertions.assertTrue(asyncHolder.isReady());
      Assertions.assertTrue(rangeReader.getReadCount() > 0, "download must have happened by the time holder is ready");

      try (CursorHolder holder = asyncHolder.release()) {
        Assertions.assertNotNull(holder);
      }
    }
    finally {
      gatedExec.shutdownNow();
      rawExec.shutdownNow();
    }
  }

  @Test
  void testAlreadyResidentSegmentStillBuildsHolderOnDownloadExecutor() throws Exception
  {
    // Everything is already resident, so no download tasks get planned, the empty collect completes in its
    // constructor, and its ready callback fires inline on the thread calling makeCursorHolderAsync. The callback must
    // therefore do no real work itself: the materialization + holder build runs as its own continuation task on the
    // download executor (it deserializes columns and can even hit deep storage), never on the calling thread.
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final CountDownLatch gate = new CountDownLatch(1);
    final ExecutorService rawExec = Execs.singleThreaded("partial-cursor-resident-%d");
    final ListeningExecutorService gatedExec = MoreExecutors.listeningDecorator(rawExec);
    try (IndexAndMapper opened = openIndex(rangeReader, "resident_hop")) {
      opened.mapper().ensureAllDownloaded();
      final PartialQueryableIndex index = opened.index();
      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          index,
          QueryableIndexTimeBoundaryInspector.create(index),
          noOpAcquirer(gatedExec)
      );

      // Hold the executor with a pre-queued blocker so the continuation task can't run yet.
      @SuppressWarnings("unused")
      ListenableFuture<?> unused = gatedExec.submit(() -> {
        try {
          gate.await();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      final AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN);
      Assertions.assertFalse(
          asyncHolder.isReady(),
          "holder must not become ready inline on the calling thread even with nothing to download"
      );

      final CountDownLatch ready = new CountDownLatch(1);
      final AtomicReference<String> readyThread = new AtomicReference<>();
      asyncHolder.addReadyCallback(() -> {
        readyThread.set(Thread.currentThread().getName());
        ready.countDown();
      });
      gate.countDown();
      Assertions.assertTrue(ready.await(5, TimeUnit.SECONDS), "ready callback must fire once the executor drains");
      Assertions.assertTrue(
          readyThread.get().startsWith("partial-cursor-resident"),
          "holder must be produced on the download executor, not thread[" + readyThread.get() + "]"
      );
      try (CursorHolder holder = asyncHolder.release()) {
        Assertions.assertNotNull(holder);
      }
    }
    finally {
      gatedExec.shutdownNow();
      rawExec.shutdownNow();
    }
  }

  @Test
  void testMatchedProjectionAcquiresAndPrefetchesParentBundle() throws IOException
  {
    // Materializing a projection column also materializes its same-named base parent column when one exists, so the
    // plan must include a __base bundle: its hold keeps the parent mmaps eviction-excluded for the holder's lifetime
    // (they were previously only covered by the projection bundle's hold) and its planned fetches cover the parent
    // files up front.
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "proj_parent")) {
      final PartialQueryableIndex index = opened.index();
      final ListeningExecutorService exec = directExec();
      final Set<String> acquired = new TreeSet<>();
      final PartialBundleAcquirer acquirer = new PartialBundleAcquirer()
      {
        @Override
        public Closeable acquire(String bundleName)
        {
          acquired.add(bundleName);
          return () -> {};
        }

        @Override
        public <T> AsyncResource<T> submitDownload(Callable<T> task)
        {
          return AsyncResources.fromFutureUnmanaged(exec.submit(task));
        }
      };
      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          index,
          QueryableIndexTimeBoundaryInspector.create(index),
          acquirer
      );

      final CursorBuildSpec aggSpec = CursorBuildSpec.builder()
                                                     .setGroupingColumns(List.of("dim1"))
                                                     .setAggregators(List.of(
                                                         new LongSumAggregatorFactory("_metric1_sum", "metric1")
                                                     ))
                                                     .setPhysicalColumns(Set.of("dim1", "metric1"))
                                                     .build();

      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(aggSpec);
           CursorHolder holder = asyncHolder.release()) {
        Assertions.assertNotNull(holder);
        Assertions.assertEquals(
            new TreeSet<>(Set.of(PROJECTION_NAME, Projections.BASE_TABLE_PROJECTION_NAME)),
            acquired,
            "both the projection bundle and the parent __base bundle must be held"
        );
        final Set<String> downloaded = opened.mapper().getDownloadedFiles();
        final String basePrefix = Projections.BASE_TABLE_PROJECTION_NAME + "/";
        Assertions.assertTrue(
            downloaded.contains(basePrefix + "dim1"),
            "the parent base column's file must be prefetched; got: " + downloaded
        );
        // only dim1 reads through a parent: the aggregate columns have no same-named base column
        Assertions.assertFalse(downloaded.contains(basePrefix + "metric1"), "got: " + downloaded);
        Assertions.assertFalse(downloaded.contains(basePrefix + ColumnHolder.TIME_COLUMN_NAME), "got: " + downloaded);
      }
    }
  }

  @Test
  void testUnusedCursorHolderClosesWithoutTouchingColumns() throws IOException
  {
    // QueryableIndexCursorHolder builds its cursor resources lazily, and building them reads column data (the
    // FilterBundle computes filter bitmaps, the interval fold reads __time). Closing a holder that was never used
    // must not force that construction: over a partial segment it would mean deep-storage downloads on the closing
    // thread (after the async factory path has already released its bundle holds) just to throw the result away.
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "close_unused")) {
      final PartialQueryableIndex index = opened.index();
      final CursorBuildSpec spec = CursorBuildSpec.builder()
                                                  .setFilter(new EqualityFilter("dim1", ColumnType.STRING, "a", null))
                                                  .setPhysicalColumns(Set.of("dim1"))
                                                  .build();
      rangeReader.resetCount();
      new QueryableIndexCursorHolder(index, spec, QueryableIndexTimeBoundaryInspector.create(index)).close();
      Assertions.assertEquals(0, rangeReader.getReadCount(), "closing an unused cursor holder must not read columns");
      Assertions.assertTrue(
          opened.mapper().getDownloadedFiles().isEmpty(),
          "closing an unused cursor holder must not download anything"
      );
    }
  }

  @Test
  void testResidentBundleEvictedBeforeHoldAcquisitionIsRefetched() throws IOException
  {
    // A resident-but-unheld bundle can be evicted between makeCursorHolderAsync's plan resolution and the bundle
    // hold acquisition; evictContainer clears the residency bitmap, so a residency snapshot taken before the hold
    // would omit the formerly-resident files and never restore them, making materialization throw on their
    // non-resident mapFile. Fetch planning is deferred until the bundle's hold is acquired (PrefetchBundle
    // .planFetches), so the plan sees post-eviction residency and re-fetches. Model the worst-case timing by
    // evicting inside acquire() itself, immediately before the hold exists.
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "evict_window")) {
      final PartialQueryableIndex index = opened.index();
      final PartialSegmentFileMapperV10 mapper = opened.mapper();

      // the query's columns are resident before the query arrives
      mapper.fetchFiles(Set.of("__base/__time", "__base/dim1"));
      Assertions.assertTrue(mapper.getDownloadedFiles().containsAll(Set.of("__base/__time", "__base/dim1")));

      final ListeningExecutorService exec = directExec();
      final PartialBundleAcquirer evictingAcquirer = new PartialBundleAcquirer()
      {
        @Override
        public Closeable acquire(String bundleName)
        {
          // the eviction lands in the unheld window: every container's residency is wiped just before the hold
          for (int i = 0; i < mapper.getSegmentFileMetadata().getContainers().size(); i++) {
            mapper.evictContainer(i);
          }
          return () -> {};
        }

        @Override
        public <T> AsyncResource<T> submitDownload(Callable<T> task)
        {
          return AsyncResources.fromFutureUnmanaged(exec.submit(task));
        }
      };
      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          index,
          QueryableIndexTimeBoundaryInspector.create(index),
          evictingAcquirer
      );

      final CursorBuildSpec spec = CursorBuildSpec.builder().setPhysicalColumns(Set.of("dim1")).build();
      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(spec);
           CursorHolder holder = asyncHolder.release()) {
        Assertions.assertNotNull(holder);
        Assertions.assertTrue(
            mapper.getDownloadedFiles().containsAll(Set.of("__base/__time", "__base/dim1")),
            "evicted files must be re-fetched by the post-hold plan; got: " + mapper.getDownloadedFiles()
        );
      }
    }
  }

  @Test
  void testMemoizedColumnsSurviveBundleEvictionBetweenQueries() throws IOException
  {
    // Query 1 materializes (and memoizes) its columns; with all holds released, the bundle evicts; query 2
    // re-acquires and re-fetches. The holders memoized by query 1 wrap the unmapped old container mmap — serving
    // them to query 2 would read freed memory without ever consulting mapFile (so the non-resident throw can't
    // catch it). The eviction-aware suppliers detect the bundle-generation change and rebuild against the fresh
    // container, so query 2 reads correct values.
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "evict_between_queries")) {
      final PartialQueryableIndex index = opened.index();
      final PartialSegmentFileMapperV10 mapper = opened.mapper();
      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          index,
          QueryableIndexTimeBoundaryInspector.create(index),
          noOpAcquirer(directExec())
      );
      final CursorBuildSpec spec = CursorBuildSpec.builder().setPhysicalColumns(Set.of("dim1")).build();

      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(spec);
           CursorHolder holder = asyncHolder.release()) {
        Assertions.assertNotNull(holder.asCursor());
      }

      // every hold is released; the bundle evicts
      for (int i = 0; i < mapper.getSegmentFileMetadata().getContainers().size(); i++) {
        mapper.evictContainer(i);
      }

      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(spec);
           CursorHolder holder = asyncHolder.release()) {
        final Cursor cursor = holder.asCursor();
        final ColumnValueSelector<?> dim1 = cursor.getColumnSelectorFactory().makeColumnValueSelector("dim1");
        final List<Object> values = new ArrayList<>();
        while (!cursor.isDone()) {
          values.add(dim1.getObject());
          cursor.advance();
        }
        Assertions.assertEquals(List.of("a", "a", "b", "b"), values);
      }
    }
  }

  @Test
  void testMatchedProjectionDownloadsOnlyRequestedColumns() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "proj_columns")) {
      final PartialQueryableIndex index = opened.index();
      final PartialSegmentFileMapperV10 mapper = opened.mapper();
      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          index,
          QueryableIndexTimeBoundaryInspector.create(index),
          noOpAcquirer(directExec())
      );

      // Query asks for group-by dim1 + sum(metric1), but NOT count. The projection has dim1 / _metric1_sum / _count;
      // the projection match rewrites this to physical columns {dim1, _metric1_sum}. We should see those download,
      // but NOT the _count file; proves column-level pruning, not just projection-level.
      final CursorBuildSpec aggSpec = CursorBuildSpec.builder()
                                                     .setGroupingColumns(List.of("dim1"))
                                                     .setAggregators(List.of(
                                                         new LongSumAggregatorFactory("_metric1_sum", "metric1")
                                                     ))
                                                     .setPhysicalColumns(Set.of("dim1", "metric1"))
                                                     .build();

      // Async path triggers the column-level pre-download. directExec makes the future complete synchronously.
      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(aggSpec);
           CursorHolder holder = asyncHolder.release()) {
        Assertions.assertNotNull(holder);
        final String projPrefix = PROJECTION_NAME + "/";
        final String basePrefix = Projections.BASE_TABLE_PROJECTION_NAME + "/";
        final Set<String> downloaded = mapper.getDownloadedFiles();

        // Requested projection columns materialized.
        Assertions.assertTrue(downloaded.contains(projPrefix + "dim1"), "expected projection dim1; got: " + downloaded);
        Assertions.assertTrue(
            downloaded.contains(projPrefix + "_metric1_sum"),
            "expected projection _metric1_sum; got: " + downloaded
        );
        // The projection's _count file was NOT in the query; must not be downloaded
        Assertions.assertFalse(
            downloaded.contains(projPrefix + "_count"),
            "expected projection _count NOT to be downloaded; got: " + downloaded
        );
        // Base __time and base metric1 are NOT touched: projection dim1 may pull base dim1 as its parent column
        // (legitimate dependency), but unrelated base columns must stay untouched.
        Assertions.assertFalse(
            downloaded.contains(basePrefix + ColumnHolder.TIME_COLUMN_NAME),
            "expected base __time NOT to be downloaded; got: " + downloaded
        );
        Assertions.assertFalse(
            downloaded.contains(basePrefix + "metric1"),
            "expected base metric1 NOT to be downloaded; got: " + downloaded
        );
      }
    }
  }

  @Test
  void testOpeningTimeOrderedProjectionCursorTriggersNoDownload() throws IOException
  {
    // A projection grouped by [__gran(HOUR), dim1] is time-ordered, so QueryableIndexCursorHolder reads the
    // projection's (downloadable) time column when it builds the cursor (interval-checking offset + the projection's
    // own time-boundary inspector). A GROUP BY dim1 query matches the projection by re-aggregating over __gran and
    // does NOT list __time in physicalColumns. requiredColumns must nonetheless pre-fetch __time so that read happens
    // during the async pre-fetch rather than as a lazy download when the cursor is opened on a processing thread.
    final String projectionName = "hourly_dim1_metric1_sum";
    final File timeOrderedSegmentDir = buildTimeOrderedProjectionSegment(projectionName);

    final CountingRangeReader rangeReader = new CountingRangeReader(timeOrderedSegmentDir);
    final File cacheDir = new File(perTestTempDir, "time_ordered_proj");
    FileUtils.mkdirp(cacheDir);
    final PartialSegmentFileMapperV10 mapper = PartialSegmentFileMapperV10.create(
        rangeReader,
        TestHelper.makeJsonMapper(),
        cacheDir,
        IndexIO.V10_FILE_NAME,
        Collections.emptyList(),
        PartialSegmentDownloadListener.NOOP,
        PartialSegmentFileMapperV10.DEFAULT_COALESCE_GAP_BYTES,
        PartialSegmentFileMapperV10.DEFAULT_MAX_FETCH_RUN_BYTES
    );
    try {
      final PartialQueryableIndex index =
          new PartialQueryableIndex(mapper.getSegmentFileMetadata(), mapper, COLUMN_CONFIG);

      // Precondition: the matched projection's row selector really is time-ordered, so cursor build WOULD read its
      // time column. Without this, the assertion below would be vacuously satisfied.
      final QueryableIndex projectionIndex = index.getProjectionQueryableIndex(projectionName);
      Assertions.assertNotNull(projectionIndex, "test setup: projection should be present");
      Assertions.assertEquals(
          Order.ASCENDING,
          Cursors.getTimeOrdering(projectionIndex.getOrdering()),
          "test setup: projection must be time-ordered for this test to guard anything"
      );

      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          index,
          V10TimeBoundaryInspector.forBaseProjection(index.getBaseProjectionMetadata(), index.getDataInterval()),
          noOpAcquirer(directExec())
      );

      // ETERNITY interval so the offset is not clipped to a residual interval (a genuine reason to read __time);
      // here the only __time read is the time-ordered offset/inspector during cursor build, which the pre-fetch
      // must have already covered. physicalColumns deliberately omits __time.
      final CursorBuildSpec aggSpec = CursorBuildSpec.builder()
                                                     .setInterval(Intervals.ETERNITY)
                                                     .setGroupingColumns(List.of("dim1"))
                                                     .setAggregators(List.of(
                                                         new LongSumAggregatorFactory("_metric1_sum", "metric1")
                                                     ))
                                                     .setPhysicalColumns(Set.of("dim1", "metric1"))
                                                     .build();

      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(aggSpec);
           CursorHolder holder = asyncHolder.release()) {
        Assertions.assertNotNull(holder);

        // After the async pre-fetch resolves, the projection's required columns AND __time are on disk, even though
        // __time was not declared in physicalColumns.
        final String projPrefix = projectionName + "/";
        final Set<String> beforeOpen = mapper.getDownloadedFiles();
        Assertions.assertTrue(
            beforeOpen.contains(projPrefix + "_metric1_sum"),
            "test setup: projection path should have run and pre-fetched _metric1_sum; got: " + beforeOpen
        );
        // The projection's time column is stored under its granularity grouping name (__gran); it is exposed as
        // __time but the on-disk smoosh file (and thus the downloaded-files entry) is __gran.
        Assertions.assertTrue(
            beforeOpen.contains(projPrefix + "__gran"),
            "requiredColumns must pre-fetch the projection's time column for a time-ordered cursor; got: " + beforeOpen
        );

        // Opening the cursor reads __time (interval-checking offset + the projection's time-boundary inspector). It
        // was pre-fetched above, so this must not trigger any further (lazy) download on the processing thread.
        Assertions.assertNotNull(holder.asCursor());

        final Set<String> afterOpen = mapper.getDownloadedFiles();
        Assertions.assertEquals(
            beforeOpen,
            afterOpen,
            "opening a projection-matched cursor must not trigger a lazy download; newly downloaded: "
            + Sets.difference(afterOpen, beforeOpen)
        );
      }
    }
    finally {
      mapper.close();
    }
  }

  @Test
  void testOpeningTimeOrderedBaseCursorTriggersNoDownload() throws IOException
  {
    // The base table is time-ordered, so QueryableIndexCursorHolder reads __time when building the cursor (for its
    // interval-checking offset) even though a raw scan does not list __time in physicalColumns. requiredColumns must
    // pre-fetch __time so that read does not become a lazy download on the processing thread at cursor-open time.
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "base_open_no_download")) {
      final PartialQueryableIndex index = opened.index();
      final PartialSegmentFileMapperV10 mapper = opened.mapper();
      Assertions.assertEquals(
          Order.ASCENDING,
          Cursors.getTimeOrdering(index.getOrdering()),
          "test setup: base table must be time-ordered"
      );

      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          index,
          V10TimeBoundaryInspector.forBaseProjection(index.getBaseProjectionMetadata(), index.getDataInterval()),
          noOpAcquirer(directExec())
      );

      // Raw scan (no grouping/aggregators) so the aggregate projection is not matched and we exercise the base path.
      // __time is deliberately omitted from physicalColumns.
      final CursorBuildSpec scanSpec = CursorBuildSpec.builder()
                                                      .setInterval(Intervals.ETERNITY)
                                                      .setPhysicalColumns(Set.of("dim1"))
                                                      .build();

      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(scanSpec);
           CursorHolder holder = asyncHolder.release()) {
        Assertions.assertNotNull(holder);

        final String basePrefix = Projections.BASE_TABLE_PROJECTION_NAME + "/";
        final Set<String> beforeOpen = mapper.getDownloadedFiles();
        // base path ran (dim1 pre-fetched) and __time was pre-fetched despite not being in physicalColumns
        Assertions.assertTrue(
            beforeOpen.contains(basePrefix + "dim1"),
            "test setup: base path should have pre-fetched dim1; got: " + beforeOpen
        );
        Assertions.assertTrue(
            beforeOpen.contains(basePrefix + ColumnHolder.TIME_COLUMN_NAME),
            "requiredColumns must pre-fetch __time for a time-ordered base cursor; got: " + beforeOpen
        );

        Assertions.assertNotNull(holder.asCursor());

        final Set<String> afterOpen = mapper.getDownloadedFiles();
        Assertions.assertEquals(
            beforeOpen,
            afterOpen,
            "opening a time-ordered base cursor must not trigger a lazy download; newly downloaded: "
            + Sets.difference(afterOpen, beforeOpen)
        );
      }
    }
  }

  @Test
  void testFilteredWrapperOverPartialUsesAsyncPath() throws IOException
  {
    // A FilteredCursorFactory wrapping a not-fully-downloaded partial cursor factory must route makeCursorHolderAsync
    // through the delegate's async (download-on-demand) path. If FilteredCursorFactory fell back to the CursorFactory
    // default makeCursorHolderAsync (which calls sync makeCursorHolder), the partial delegate would throw because the
    // segment isn't fully downloaded.
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "filtered_wrapper_async")) {
      final PartialQueryableIndex index = opened.index();
      Assertions.assertFalse(index.isFullyDownloaded(), "test setup: segment must not be fully downloaded");

      final PartialQueryableIndexCursorFactory partial = new PartialQueryableIndexCursorFactory(
          index,
          V10TimeBoundaryInspector.forBaseProjection(index.getBaseProjectionMetadata(), index.getDataInterval()),
          noOpAcquirer(directExec())
      );
      final CursorFactory filtered = new FilteredCursorFactory(
          partial,
          new EqualityFilter("dim1", ColumnType.STRING, "a", null)
      );

      // directExec resolves the async download inline; release() returns the built holder. asCursor() exercises the
      // cursor over the on-demand-loaded columns. Would throw here if the wrapper used the sync default.
      try (AsyncCursorHolder asyncHolder = filtered.makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN);
           CursorHolder holder = asyncHolder.release()) {
        Assertions.assertNotNull(holder);
        Assertions.assertNotNull(holder.asCursor(), "async path should build a usable cursor without a sync download");
      }
    }
  }

  @Test
  void testRowSignatureAndCapabilitiesDelegatedToUnderlyingFactory() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "delegate")) {
      final PartialQueryableIndex index = opened.index();
      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          index,
          QueryableIndexTimeBoundaryInspector.create(index),
          noOpAcquirer(directExec())
      );

      // delegating to QueryableIndexCursorFactory which returns the index's row signature
      final RowSignature signature = factory.getRowSignature();
      Assertions.assertNotNull(signature);
      Assertions.assertTrue(signature.size() > 0);

      Assertions.assertNotNull(factory.getColumnCapabilities("dim1"));
    }
  }

  @Test
  void testCloseBeforeReadyCancelsColumnDownloadsAndReleasesHoldOnce() throws IOException
  {
    // When the awaiter closes the holder before it's ready (query cancel/timeout), the canceler must cancel every
    // submitted column-download future and release the bundle hold exactly once (the canceler and the resulting
    // failure callback both try to release it). A recording executor captures each submitted task as a future that
    // never completes and never runs the task, so the holder stays not-ready and we can observe the cancellation.
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "cancel_before_ready")) {
      final PartialQueryableIndex index = opened.index();

      // submitDownload records each task's never-completing future and returns it as an AsyncResource, so the holder
      // stays not-ready and we can observe the cancellation. A bundle hold whose releases we count asserts
      // exactly-once release across the canceler + failure callback.
      final List<ListenableFuture<?>> submitted = new ArrayList<>();
      final AtomicInteger holdReleases = new AtomicInteger(0);
      final PartialBundleAcquirer acquirer = new PartialBundleAcquirer()
      {
        @Override
        public Closeable acquire(String bundleName)
        {
          return holdReleases::incrementAndGet;
        }

        @Override
        public <T> AsyncResource<T> submitDownload(Callable<T> task)
        {
          final SettableFuture<T> future = SettableFuture.create();
          submitted.add(future);
          return AsyncResources.fromFutureUnmanaged(future);
        }
      };

      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          index,
          V10TimeBoundaryInspector.forBaseProjection(index.getBaseProjectionMetadata(), index.getDataInterval()),
          acquirer
      );

      final AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN);

      // Downloads were submitted but never complete, so the holder is not ready and the hold is still held.
      Assertions.assertFalse(asyncHolder.isReady(), "downloads never complete, so the holder must not be ready");
      Assertions.assertFalse(submitted.isEmpty(), "at least one column download should have been submitted");
      submitted.forEach(f -> Assertions.assertFalse(f.isCancelled(), "futures must not be cancelled before close"));
      Assertions.assertEquals(0, holdReleases.get(), "hold must still be held while the cursor is loading");

      // Awaiter cancels (closes) before the holder is ready: every pending download is cancelled and the hold drops.
      asyncHolder.close();

      submitted.forEach(f -> Assertions.assertTrue(f.isCancelled(), "pending column downloads must be cancelled"));
      Assertions.assertEquals(
          1,
          holdReleases.get(),
          "bundle hold must be released exactly once across the canceler and the failure callback"
      );
    }
  }

  @Test
  void testCancelWhileDownloadMidMapFileDefersHoldReleaseUntilBodyFinishes() throws Exception
  {
    // Eviction-during-download safety: releasing the bundle hold makes the bundle entry evictable, and eviction unmaps
    // its containers. A column download still inside mapFile() would then read an unmapped region (a JVM SIGSEGV). So
    // when the awaiter cancels while a download is mid-mapFile(), the canceler must NOT release the hold; the last
    // in-flight body releases it only after its read has finished.
    final ReentrantLock gate = new ReentrantLock();
    final CountDownLatch readStarted = new CountDownLatch(1);
    final AtomicBoolean blockedOnce = new AtomicBoolean(false);
    // Park the first read issued from a download-executor thread, modeling a body parked inside mapFile() while holding
    // the in-flight count. The mapper's create() reads (header + metadata) run on this thread during openIndex and
    // must pass through untouched, so discriminate by thread identity rather than offset. gate.lock() is
    // uninterruptible, modeling the mapper's non-interruptible traditional I/O: future.cancel(true) cannot abort it.
    final Thread mainThread = Thread.currentThread();
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir)
    {
      @Override
      public InputStream readRange(String filename, long offset, long length) throws IOException
      {
        if (Thread.currentThread() != mainThread && blockedOnce.compareAndSet(false, true)) {
          readStarted.countDown();
          gate.lock();
          gate.unlock();
        }
        return super.readRange(filename, offset, length);
      }
    };

    final ExecutorService rawExec = Execs.singleThreaded("partial-cursor-evict-race-%d");
    final ListeningExecutorService blockingExec = MoreExecutors.listeningDecorator(rawExec);
    final AtomicInteger holdReleases = new AtomicInteger(0);
    final CountDownLatch released = new CountDownLatch(1);

    gate.lock(); // hold the gate so the first column download parks inside its read
    try (IndexAndMapper opened = openIndex(rangeReader, "evict_during_download")) {
      final PartialQueryableIndex index = opened.index();
      final PartialBundleAcquirer acquirer = new PartialBundleAcquirer()
      {
        @Override
        public Closeable acquire(String bundleName)
        {
          return () -> {
            holdReleases.incrementAndGet();
            released.countDown();
          };
        }

        @Override
        public <T> AsyncResource<T> submitDownload(Callable<T> task)
        {
          return AsyncResources.fromFutureUnmanaged(blockingExec.submit(task));
        }
      };

      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          index,
          V10TimeBoundaryInspector.forBaseProjection(index.getBaseProjectionMetadata(), index.getDataInterval()),
          acquirer
      );

      final AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN);

      // A download body is now parked mid-mapFile(), holding the in-flight count; the hold is still held.
      Assertions.assertTrue(readStarted.await(5, TimeUnit.SECONDS), "a column download must reach mapFile()");
      Assertions.assertFalse(asyncHolder.isReady(), "download is parked, so the holder must not be ready");
      Assertions.assertEquals(0, holdReleases.get(), "hold must still be held while a download is mid-mapFile");

      // Awaiter cancels while the body is parked inside mapFile().
      asyncHolder.close();

      // The hold must NOT be released yet: the in-flight body is still touching the bundle's container, so releasing
      // now would let eviction unmap it mid-read.
      Assertions.assertEquals(
          0,
          holdReleases.get(),
          "bundle hold must stay held until the in-flight mapFile() body finishes"
      );

      // Let the parked body finish its read; the last body out then releases the hold exactly once.
      gate.unlock();
      Assertions.assertTrue(released.await(5, TimeUnit.SECONDS), "hold must be released once the in-flight body finishes");
      Assertions.assertEquals(1, holdReleases.get(), "bundle hold must be released exactly once");
    }
    finally {
      if (gate.isHeldByCurrentThread()) {
        gate.unlock();
      }
      blockingExec.shutdownNow();
      rawExec.shutdownNow();
    }
  }

  @Test
  void testPrefetchUsesCoalescedRangeReads() throws IOException
  {
    // With per-column file lists in the metadata, the async pre-fetch resolves every required file up front and
    // fetches the base bundle with coalesced range reads: a full scan needs every file in the single base container,
    // and this tiny fixture stays far below the parallel-fetch run-size cap (virtualStorageMaxFetchRunBytes), so the
    // whole span plans as exactly ONE deep-storage request instead of one per file.
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "coalesced_prefetch")) {
      final PartialQueryableIndex index = opened.index();
      Assertions.assertNotNull(
          opened.mapper().getSegmentFileMetadata().getColumnFiles(),
          "fixture must record column file lists"
      );
      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          index,
          V10TimeBoundaryInspector.forBaseProjection(index.getBaseProjectionMetadata(), index.getDataInterval()),
          noOpAcquirer(directExec())
      );
      rangeReader.resetCount();

      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN);
           CursorHolder holder = asyncHolder.release()) {
        Assertions.assertEquals(
            1,
            rangeReader.getReadCount(),
            "full scan of the single-container base bundle must coalesce into one range read"
        );

        // and the coalesced bytes are the right bytes
        final Cursor cursor = holder.asCursor();
        final ColumnValueSelector<?> dim1 = cursor.getColumnSelectorFactory().makeColumnValueSelector("dim1");
        final List<Object> values = new ArrayList<>();
        while (!cursor.isDone()) {
          values.add(dim1.getObject());
          cursor.advance();
        }
        Assertions.assertEquals(List.of("a", "a", "b", "b"), values);
      }
    }
  }

  @Test
  void testNestedColumnPrefetchCoversFieldFiles() throws IOException
  {
    // Nested-format columns store per-field data in `<column>.__field_N` files that the serde reads lazily at
    // cursor-read time. Without file lists those were synchronous deep-storage reads on the cursor's thread; the
    // recorded lists let the pre-fetch cover them, so cursor iteration triggers ZERO further range reads.
    final File nestedSegmentDir = buildNestedSegment();
    final CountingRangeReader rangeReader = new CountingRangeReader(nestedSegmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "nested_prefetch")) {
      final PartialQueryableIndex index = opened.index();
      final PartialSegmentFileMapperV10 mapper = opened.mapper();
      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          index,
          V10TimeBoundaryInspector.forBaseProjection(index.getBaseProjectionMetadata(), index.getDataInterval()),
          noOpAcquirer(directExec())
      );

      final CursorBuildSpec spec = CursorBuildSpec.builder()
                                                  .setInterval(Intervals.ETERNITY)
                                                  .setVirtualColumns(
                                                      VirtualColumns.create(
                                                          new NestedFieldVirtualColumn("nest", "$.x", "v0")
                                                      )
                                                  )
                                                  .setPhysicalColumns(Set.of("nest"))
                                                  .build();

      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(spec);
           CursorHolder holder = asyncHolder.release()) {
        Assertions.assertTrue(
            mapper.getDownloadedFiles().stream().anyMatch(f -> f.contains(".__field_")),
            "pre-fetch must cover the nested column's field files; got: " + mapper.getDownloadedFiles()
        );
        rangeReader.resetCount();

        final Cursor cursor = holder.asCursor();
        final ColumnValueSelector<?> v0 = cursor.getColumnSelectorFactory().makeColumnValueSelector("v0");
        final List<Object> values = new ArrayList<>();
        while (!cursor.isDone()) {
          values.add(v0.getObject());
          cursor.advance();
        }
        Assertions.assertEquals(List.of(1L, 2L, 3L, 4L), values);
        Assertions.assertEquals(
            0,
            rangeReader.getReadCount(),
            "reading nested fields must not trigger lazy downloads on the cursor's thread"
        );
      }
    }
  }

  @Test
  void testOldMetadataWithoutColumnFilesFallsBackToWholeBundleDownload() throws IOException
  {
    // Segments written before columnFiles existed have no per-column lists to resolve a precise fetch set from; the
    // async path degrades to downloading the matched bundle's containers whole, losing only column-level pruning
    // within the bundle, and produces correct results. Request-count-wise that plans as one range read per container
    // span up to the run-size cap (virtualStorageMaxFetchRunBytes), which for this tiny fixture means exactly one.
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "old_metadata_fallback")) {
      final SegmentFileMetadata withLists = opened.mapper().getSegmentFileMetadata();
      final SegmentFileMetadata stripped = new SegmentFileMetadata(
          withLists.getContainers(),
          withLists.getFiles(),
          withLists.getInterval(),
          withLists.getColumnDescriptors(),
          null,
          withLists.getProjections(),
          withLists.getBitmapEncoding()
      );
      final PartialQueryableIndex index = new PartialQueryableIndex(stripped, opened.mapper(), COLUMN_CONFIG);
      Assertions.assertNull(stripped.getColumnFiles(), "test setup: stripped metadata must have no file lists");

      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          index,
          V10TimeBoundaryInspector.forBaseProjection(index.getBaseProjectionMetadata(), index.getDataInterval()),
          noOpAcquirer(directExec())
      );
      rangeReader.resetCount();

      // narrow spec (dim1 only) to prove the fallback fetches the whole base bundle, not just the listed column
      final CursorBuildSpec scanSpec = CursorBuildSpec.builder()
                                                      .setInterval(Intervals.ETERNITY)
                                                      .setPhysicalColumns(Set.of("dim1"))
                                                      .build();
      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(scanSpec);
           CursorHolder holder = asyncHolder.release()) {
        // one read for the base bundle's single (sub-cap) container, covering every base file
        Assertions.assertEquals(1, rangeReader.getReadCount(), "fallback downloads the bundle's container whole");
        final String basePrefix = Projections.BASE_TABLE_PROJECTION_NAME + "/";
        for (String file : List.of("dim1", "metric1", ColumnHolder.TIME_COLUMN_NAME)) {
          Assertions.assertTrue(
              opened.mapper().getDownloadedFiles().contains(basePrefix + file),
              "whole-bundle fallback must cover " + file
          );
        }
        // the projection's bundle was not matched, so it must stay untouched
        Assertions.assertFalse(
            opened.mapper().getDownloadedFiles().contains(PROJECTION_NAME + "/dim1"),
            "unmatched projection bundle must not download"
        );

        final Cursor cursor = holder.asCursor();
        final ColumnValueSelector<?> dim1 = cursor.getColumnSelectorFactory().makeColumnValueSelector("dim1");
        final List<Object> values = new ArrayList<>();
        while (!cursor.isDone()) {
          values.add(dim1.getObject());
          cursor.advance();
        }
        Assertions.assertEquals(List.of("a", "a", "b", "b"), values);
      }
    }
  }

  private File buildNestedSegment()
  {
    final File tmpDir = new File(perTestTempDir, "build_nested_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    final List<InputRow> rows = new ArrayList<>();
    for (int i = 1; i <= 4; i++) {
      rows.add(
          new MapBasedInputRow(
              TIME.plusMinutes(i),
              List.of("dim1", "nest"),
              Map.of("dim1", i <= 2 ? "a" : "b", "nest", Map.of("x", (long) i, "y", "s" + i))
          )
      );
    }
    return IndexBuilder.create()
                       .useV10()
                       .tmpDir(tmpDir)
                       .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                       .schema(
                           IncrementalIndexSchema.builder()
                                                 .withDimensionsSpec(
                                                     DimensionsSpec.builder()
                                                                   .setDimensions(
                                                                       List.of(
                                                                           new StringDimensionSchema("dim1"),
                                                                           new AutoTypeColumnSchema("nest", null, null)
                                                                       )
                                                                   )
                                                                   .build()
                                                 )
                                                 .withRollup(false)
                                                 .withMinTimestamp(TIME.getMillis())
                                                 .build()
                       )
                       .indexSpec(IndexSpec.builder().withMetadataCompression(CompressionStrategy.NONE).build())
                       .rows(rows)
                       .buildMMappedIndexFile();
  }

  private File buildTimeOrderedProjectionSegment(String projectionName)
  {
    final List<AggregateProjectionSpec> projections = Collections.singletonList(
        AggregateProjectionSpec.builder(projectionName)
                               .virtualColumns(Granularities.toVirtualColumn(Granularities.HOUR, "__gran"))
                               .groupingColumns(
                                   new LongDimensionSchema("__gran"),
                                   new StringDimensionSchema("dim1")
                               )
                               .aggregators(
                                   new LongSumAggregatorFactory("_metric1_sum", "metric1"),
                                   new CountAggregatorFactory("_count")
                               )
                               .build()
    );
    final File tmpDir = new File(perTestTempDir, "build_time_ordered_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    return IndexBuilder.create()
                       .useV10()
                       .tmpDir(tmpDir)
                       .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                       .schema(
                           IncrementalIndexSchema.builder()
                                                 .withDimensionsSpec(
                                                     DimensionsSpec.builder()
                                                                   .setDimensions(
                                                                       List.of(
                                                                           new StringDimensionSchema("dim1"),
                                                                           new LongDimensionSchema("metric1")
                                                                       )
                                                                   )
                                                                   .build()
                                                 )
                                                 .withRollup(false)
                                                 .withMinTimestamp(TIME.getMillis())
                                                 .withProjections(projections)
                                                 .build()
                       )
                       .indexSpec(IndexSpec.builder().withMetadataCompression(CompressionStrategy.NONE).build())
                       .rows(ROWS)
                       .buildMMappedIndexFile();
  }
}
