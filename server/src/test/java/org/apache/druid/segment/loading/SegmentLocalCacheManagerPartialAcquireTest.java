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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.ListBasedInputRow;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.guice.LocalDataStorageDruidModule;
import org.apache.druid.jackson.SegmentizerModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.PartialQueryableIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TimeBoundaryInspector;
import org.apache.druid.segment.V10TimeBoundaryInspector;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class SegmentLocalCacheManagerPartialAcquireTest
{
  private static final SegmentId SEGMENT_ID = SegmentId.of("test", Intervals.of("2025/2026"), "v1", 0);
  private static final DateTime TIME = DateTimes.of("2025-01-01");
  private static final String AGG_BUNDLE = "dim1_metric1_sum";

  private static final RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                                .add("dim1", ColumnType.STRING)
                                                                .add("metric1", ColumnType.LONG)
                                                                .build();

  private static final List<AggregateProjectionSpec> PROJECTIONS = Collections.singletonList(
      AggregateProjectionSpec.builder(AGG_BUNDLE)
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
  static File SHARED_TEMP_DIR;

  private static File DEEP_STORAGE_DIR;

  @TempDir
  File perTestTempDir;

  private ObjectMapper jsonMapper;
  private File cacheRoot;
  private SegmentLocalCacheManager manager;
  private DataSegment partialSegment;

  @BeforeAll
  static void buildSegment()
  {
    final File tmp = new File(SHARED_TEMP_DIR, "build_" + ThreadLocalRandom.current().nextInt());
    DEEP_STORAGE_DIR = IndexBuilder.create()
                                   .useV10()
                                   .tmpDir(tmp)
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
                                   .indexSpec(IndexSpec.builder()
                                                       .withMetadataCompression(CompressionStrategy.NONE)
                                                       .build())
                                   .rows(ROWS)
                                   .buildMMappedIndexFile();
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
  }

  @BeforeEach
  void setup() throws IOException
  {
    jsonMapper = TestHelper.makeJsonMapper();
    jsonMapper.registerSubtypes(new NamedType(LocalLoadSpec.class, "local"));
    jsonMapper.registerModule(new SegmentizerModule());
    jsonMapper.registerModules(new LocalDataStorageDruidModule().getJacksonModules());
    jsonMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(LocalDataSegmentPuller.class, new LocalDataSegmentPuller())
            .addValue(IndexIO.class, TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT))
            .addValue(ObjectMapper.class, jsonMapper)
            .addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT)
            .addValue(ExprMacroTable.class, TestExprMacroTable.INSTANCE)
    );

    cacheRoot = new File(perTestTempDir, "cache_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    FileUtils.mkdirp(cacheRoot);

    final StorageLocationConfig locConfig = new StorageLocationConfig(cacheRoot, 1024L * 1024L * 1024L, null);
    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig()
        .setLocations(List.of(locConfig))
        .setVirtualStorage(true, false);
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    manager = new SegmentLocalCacheManager(
        storageLocations,
        loaderConfig,
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    )
    {
      // Tripwire: the partial-aware acquire paths should be self-contained; they must not fall through to the
      // eager-extract acquireCachedSegment API (which is the non-partial branch of acquireCachedSegment-style lookups).
      @Override
      public Optional<Segment> acquireCachedSegment(SegmentId segmentId)
      {
        Assertions.fail("should not fallback to acquireCachedSegment");
        return super.acquireCachedSegment(segmentId);
      }
    };

    // DataSegment with a LocalLoadSpec pointing at the deep storage directory (unzipped V10 layout).
    partialSegment = DataSegment.builder()
                                .dataSource(SEGMENT_ID.getDataSource())
                                .interval(SEGMENT_ID.getInterval())
                                .version(SEGMENT_ID.getVersion())
                                .shardSpec(NoneShardSpec.instance())
                                .loadSpec(Map.of("type", "local", "path", DEEP_STORAGE_DIR.getAbsolutePath()))
                                .size(0)
                                .build();
  }

  @AfterEach
  void tearDown()
  {
    // Drop the segment to release any partial cache entries this test mounted (closes file mappers, unmaps containers
    // and the header bitmap, deletes on-disk artifacts). Safe to call even when the test never acquired the segment:
    // drop is a no-op for missing entries. Then shut down the manager's executors so threads don't leak across tests.
    if (manager != null) {
      manager.drop(partialSegment);
      manager.shutdown();
    }
  }

  @Test
  void testAcquirePartialSegmentReturnsPartialAwareSegment() throws ExecutionException, InterruptedException, IOException
  {
    try (AcquireSegmentAction action = manager.acquirePartialSegment(partialSegment)) {
      final AcquireSegmentResult result = action.getSegmentFuture().get();
      try (Segment segment = result.getReferenceProvider().acquireReference().orElseThrow()) {
        Assertions.assertEquals(SEGMENT_ID, segment.getId());
        Assertions.assertInstanceOf(PartialQueryableIndexSegment.class, segment);

        // The time boundary inspector reads from metadata, so requesting bounds should not download any columns.
        final TimeBoundaryInspector inspector = segment.as(TimeBoundaryInspector.class);
        Assertions.assertNotNull(inspector);
        Assertions.assertInstanceOf(V10TimeBoundaryInspector.class, inspector);
        Assertions.assertTrue(inspector.isMinMaxExact());
        Assertions.assertEquals(TIME, inspector.getMinTime());
        Assertions.assertEquals(TIME.plusMinutes(3), inspector.getMaxTime());
      }
    }
  }

  @Test
  void testAcquirePartialAsyncCursorHolderProducesWorkingCursor()
      throws ExecutionException, InterruptedException, IOException
  {
    try (AcquireSegmentAction action = manager.acquirePartialSegment(partialSegment)) {
      final AcquireSegmentResult result = action.getSegmentFuture().get();
      try (Segment segment = result.getReferenceProvider().acquireReference().orElseThrow()) {
        final CursorFactory factory = segment.as(CursorFactory.class);
        Assertions.assertNotNull(factory);
        // Drive the async path; with the manager's executor the future will complete in the background.
        try (var asyncHolder = factory.makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN)) {
          // Block in the test for readiness via a busy callback latch.
          final CountDownLatch ready = new CountDownLatch(1);
          asyncHolder.addReadyCallback(ready::countDown);
          Assertions.assertTrue(
              ready.await(15, TimeUnit.SECONDS),
              "async cursor must materialize within the test timeout"
          );
          try (var cursorHolder = asyncHolder.release()) {
            Assertions.assertNotNull(cursorHolder);
          }
        }
      }
    }
  }

  @Test
  void testSecondAcquireReturnsCachedSegment() throws ExecutionException, InterruptedException, IOException
  {
    try (AcquireSegmentAction first = manager.acquirePartialSegment(partialSegment)) {
      try (Segment ignored = first.getSegmentFuture().get().getReferenceProvider().acquireReference().orElseThrow()) {
        // entry is registered + mounted
      }
    }

    // Second acquire should find the existing mounted entry (cached fast path).
    final Optional<Segment> cached = manager.acquireCachedPartialSegment(SEGMENT_ID);
    try {
      Assertions.assertTrue(cached.isPresent(), "second acquire should hit the cached fast path");
    }
    finally {
      cached.ifPresent(CloseableUtils::closeAndWrapExceptions);
    }
  }

  @Test
  void testAsyncCursorHoldsBundleHoldUntilCursorClose()
      throws ExecutionException, InterruptedException, IOException
  {
    final StorageLocation loc = manager.getLocations().get(0);
    final PartialSegmentBundleCacheEntryIdentifier baseBundleId = new PartialSegmentBundleCacheEntryIdentifier(
        SEGMENT_ID,
        Projections.BASE_TABLE_PROJECTION_NAME
    );
    final long initialHoldBytes = loc.getWeakStats().getHoldBytes();

    try (AcquireSegmentAction action = manager.acquirePartialSegment(partialSegment)) {
      final AcquireSegmentResult result = action.getSegmentFuture().get();
      // The action holds a SIEVE-protective hold on the metadata entry for its entire lifetime.
      Assertions.assertTrue(
          loc.getWeakStats().getHoldBytes() > initialHoldBytes,
          "metadata storage-location hold must be active for the acquire action's lifetime"
      );
      try (Segment segment = result.getReferenceProvider().acquireReference().orElseThrow()) {
        try (var asyncHolder = segment.as(CursorFactory.class).makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN)) {
          final CountDownLatch ready = new CountDownLatch(1);
          asyncHolder.addReadyCallback(ready::countDown);
          Assertions.assertTrue(ready.await(15, TimeUnit.SECONDS));
          final long duringCursorHoldBytes;
          try (var cursorHolder = asyncHolder.release()) {
            duringCursorHoldBytes = loc.getWeakStats().getHoldBytes();
            // Bundle's own SIEVE hold (from bundleAcquirer.acquire) adds to hold-bytes for the cursor's lifetime,
            // on top of the metadata hold and the bundle's persistent SIEVE hold on the metadata acquired at mount.
            Assertions.assertTrue(
                duringCursorHoldBytes > initialHoldBytes,
                "cursor lifecycle must add storage-location holds beyond the initial baseline"
            );
            Assertions.assertNotNull(cursorHolder);
          }
          // After cursor close the cursor-owned bundle hold is released. The bundle entry's persistent SIEVE hold on
          // the metadata (acquired during bundle.mount) is still alive until the bundle itself is unmounted by SIEVE,
          // so hold-bytes drop but don't return all the way to the pre-cursor baseline.
          Assertions.assertTrue(
              loc.getWeakStats().getHoldBytes() < duringCursorHoldBytes,
              "cursor close must release the cursor-owned bundle hold"
          );
        }
      }
      Assertions.assertTrue(loc.isWeakReserved(baseBundleId), "bundle entry remains registered for re-use");
    }
  }

  @Test
  void testAsyncCursorBuildMountsMatchedProjectionBundle()
      throws ExecutionException, InterruptedException, IOException
  {
    try (AcquireSegmentAction action = manager.acquirePartialSegment(partialSegment)) {
      final AcquireSegmentResult result = action.getSegmentFuture().get();
      try (Segment segment = result.getReferenceProvider().acquireReference().orElseThrow()) {
        // base-table cursor: drives the base bundle to mount via the cache layer
        final CursorBuildSpec scanSpec = CursorBuildSpec.FULL_SCAN;
        try (var asyncHolder = segment.as(CursorFactory.class).makeCursorHolderAsync(scanSpec)) {
          final CountDownLatch ready = new CountDownLatch(1);
          asyncHolder.addReadyCallback(ready::countDown);
          Assertions.assertTrue(ready.await(15, TimeUnit.SECONDS));
          try (var cursorHolder = asyncHolder.release()) {
            Assertions.assertNotNull(cursorHolder);
          }
        }
      }
    }

    // Confirm a bundle entry was registered on the storage location for the base projection
    final StorageLocation loc = manager.getLocations().get(0);
    final PartialSegmentBundleCacheEntryIdentifier baseBundleId = new PartialSegmentBundleCacheEntryIdentifier(
        SEGMENT_ID,
        Projections.BASE_TABLE_PROJECTION_NAME
    );
    Assertions.assertTrue(
        loc.isWeakReserved(baseBundleId),
        "base bundle should be registered with the storage location after the async cursor build"
    );
  }

  @Test
  void testAcquireSegmentForcesFullDownloadOnPartialEligible()
      throws ExecutionException, InterruptedException, IOException
  {
    // acquireSegment (eager API) on a partial-eligible segment should route through the partial machinery and
    // force-download every internal file so the returned segment supports sync makeCursorHolder.
    try (AcquireSegmentAction action = manager.acquireSegment(partialSegment)) {
      final AcquireSegmentResult result = action.getSegmentFuture().get();
      try (Segment segment = result.getReferenceProvider().acquireReference().orElseThrow()) {
        Assertions.assertInstanceOf(PartialQueryableIndexSegment.class, segment);

        // Sync makeCursorHolder must succeed, everything was force-downloaded during acquire.
        final CursorFactory factory = segment.as(CursorFactory.class);
        Assertions.assertNotNull(factory);
        try (CursorHolder holder = factory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
          Assertions.assertNotNull(holder);
        }
      }
    }

    // Confirm the metadata entry exists on the location and reports fully downloaded.
    final StorageLocation loc = manager.getLocations().get(0);
    final CacheEntry entry = loc.getCacheEntry(new SegmentCacheEntryIdentifier(SEGMENT_ID));
    Assertions.assertInstanceOf(PartialSegmentMetadataCacheEntry.class, entry);
    Assertions.assertTrue(
        ((PartialSegmentMetadataCacheEntry) entry).isFullyDownloaded(),
        "force-download path must leave the segment fully downloaded after acquire returns"
    );
  }

  @Test
  void testPartialDownloadsDisabledFallsBackToEager()
      throws ExecutionException, InterruptedException, IOException
  {
    // Rebuild the manager with partial downloads disabled. Both partial-aware acquire APIs must fall through to their
    // eager counterparts: the returned segment is an eager QueryableIndexSegment (NOT PartialQueryableIndexSegment),
    // and no PartialSegmentMetadataCacheEntry is registered on the location.
    final StorageLocationConfig locConfig = new StorageLocationConfig(cacheRoot, 1024L * 1024L * 1024L, null);
    final SegmentLoaderConfig disabledConfig = new SegmentLoaderConfig()
        .setLocations(List.of(locConfig))
        .setVirtualStorage(true, false)
        .setVirtualStoragePartialDownloadsEnabled(false);
    final List<StorageLocation> storageLocations = disabledConfig.toStorageLocations();
    final SegmentLocalCacheManager disabledManager = new SegmentLocalCacheManager(
        storageLocations,
        disabledConfig,
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    );

    try {
      try (AcquireSegmentAction action = disabledManager.acquirePartialSegment(partialSegment)) {
        final AcquireSegmentResult result = action.getSegmentFuture().get();
        try (Segment segment = result.getReferenceProvider().acquireReference().orElseThrow()) {
          Assertions.assertEquals(SEGMENT_ID, segment.getId());
          Assertions.assertFalse(
              segment instanceof PartialQueryableIndexSegment,
              "partial downloads disabled, acquirePartialSegment must fall back to eager and return a non-partial segment"
          );
        }
      }

      // No partial metadata entry should have been created on the location.
      Assertions.assertFalse(
          disabledManager.getLocations().get(0).getCacheEntry(new SegmentCacheEntryIdentifier(SEGMENT_ID))
              instanceof PartialSegmentMetadataCacheEntry,
          "partial downloads disabled, no PartialSegmentMetadataCacheEntry should be registered"
      );

      // Cached lookup via the partial API also degrades to the eager cached path.
      try (Segment cached = disabledManager.acquireCachedPartialSegment(SEGMENT_ID).orElseThrow()) {
        Assertions.assertEquals(SEGMENT_ID, cached.getId());
        Assertions.assertFalse(
            cached instanceof PartialQueryableIndexSegment,
            "partial downloads disabled, acquireCachedPartialSegment must return the eager cached segment"
        );
      }
    }
    finally {
      // Local manager, not picked up by tearDown(); drop + shut down here to release reservations and stop threads.
      disabledManager.drop(partialSegment);
      disabledManager.shutdown();
    }
  }

  @Test
  void testGetCachedSegmentsThenBootstrapMountsPartialEntry()
      throws IOException, SegmentLoadingException
  {
    // Simulate process-restart state: prime the partial on-disk layout (header file + sparse-allocated containers)
    // and the segment info file BEFORE getCachedSegments runs, then verify the new two-phase contract:
    //   - getCachedSegments() reserves the metadata entry on the location but doesn't mount it
    //   - bootstrap(DataSegment) is what triggers the actual mount + bundle restore via polymorphic dispatch
    final File partialDir = new File(cacheRoot, SEGMENT_ID.toString());
    FileUtils.mkdirp(partialDir);
    primePartialOnDiskState(partialDir);
    manager.storeInfoFile(partialSegment);

    final List<DataSegment> cached = manager.getCachedSegments();
    Assertions.assertEquals(List.of(partialSegment), cached);

    final SegmentCacheEntryIdentifier id = new SegmentCacheEntryIdentifier(SEGMENT_ID);
    final StorageLocation location = manager.getLocations().get(0);
    final CacheEntry reserved = location.getCacheEntry(id);
    Assertions.assertInstanceOf(
        PartialSegmentMetadataCacheEntry.class,
        reserved,
        "getCachedSegments must reserve a partial metadata entry on the location"
    );
    final PartialSegmentMetadataCacheEntry partial = (PartialSegmentMetadataCacheEntry) reserved;
    Assertions.assertFalse(
        partial.isMounted(),
        "metadata entry should NOT be mounted after getCachedSegments; mount is deferred to bootstrap()"
    );

    // bootstrap() dispatches polymorphically: partial entry -> metadata.mount(location), which cascades into bundle
    // restore via PartialSegmentCacheBootstrap.restoreBundlesFromDisk.
    manager.bootstrap(partialSegment, SegmentLazyLoadFailCallback.NOOP);

    Assertions.assertTrue(partial.isMounted(), "metadata entry must be mounted after bootstrap()");
    Assertions.assertFalse(
        partial.snapshotLinkedBundles().isEmpty(),
        "bundle restore must have linked at least one bundle to the metadata entry"
    );
    final Set<String> restoredBundles = partial.snapshotLinkedBundles().stream()
        .map(PartialSegmentBundleCacheEntry::getBundleName)
        .collect(Collectors.toSet());
    Assertions.assertTrue(
        restoredBundles.contains(Projections.BASE_TABLE_PROJECTION_NAME),
        "base bundle must be restored by bootstrap; got " + restoredBundles
    );
  }

  /**
   * Lay down the on-disk artifacts a previous process run would have left behind in the given partial directory:
   * a V10 header file and sparse-allocated container files for every container the segment metadata declares. The
   * temporary file mapper used to write the header is closed immediately afterward, it does not delete its files
   * on close, so the artifacts persist for the test to bootstrap-restore.
   */
  private void primePartialOnDiskState(File partialDir) throws IOException
  {
    try (PartialSegmentFileMapperV10 seed =
             PartialSegmentFileMapperV10.create(
                 new DirectoryBackedRangeReader(DEEP_STORAGE_DIR),
                 jsonMapper,
                 partialDir,
                 IndexIO.V10_FILE_NAME,
                 List.of()
             )) {
      final int numContainers = seed.getSegmentFileMetadata().getContainers().size();
      for (int i = 0; i < numContainers; i++) {
        seed.initializeContainer(i);
      }
    }
  }
}
