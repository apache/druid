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
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestHelper;
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
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Manager-level tests for the partial-load-rule {@link SegmentLocalCacheManager#load} path: a segment whose
 * {@code loadSpec} is a {@link PartialLoadSpec} wrapper should mount the metadata as a STATIC cache entry, eagerly
 * download the rule-selected bundles as STATIC, and leave non-selected bundles unmounted (subject to weak/on-demand
 * acquisition at query time). Drop on a static partial cleanly removes both metadata and bundles.
 */
class SegmentLocalCacheManagerPartialRuleLoadTest
{
  private static final SegmentId SEGMENT_ID = SegmentId.of("test", Intervals.of("2025/2026"), "v1", 0);
  private static final DateTime TIME = DateTimes.of("2025-01-01");
  private static final String AGG_BUNDLE = "dim1_metric1_sum";
  private static final String OTHER_AGG_BUNDLE = "dim1_count";
  private static final String FINGERPRINT = "v1:rule-bundle-test";

  private static final RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                                .add("dim1", ColumnType.STRING)
                                                                .add("metric1", ColumnType.LONG)
                                                                .build();

  private static final List<AggregateProjectionSpec> PROJECTIONS = Arrays.asList(
      AggregateProjectionSpec.builder(AGG_BUNDLE)
                             .groupingColumns(new StringDimensionSchema("dim1"))
                             .aggregators(
                                 new LongSumAggregatorFactory("_metric1_sum", "metric1"),
                                 new CountAggregatorFactory("_count")
                             )
                             .build(),
      // A second aggregate projection so tests can verify that non-selected bundles are NOT pre-mounted while
      // selected + parent bundles ARE.
      AggregateProjectionSpec.builder(OTHER_AGG_BUNDLE)
                             .groupingColumns(new StringDimensionSchema("dim1"))
                             .aggregators(new CountAggregatorFactory("_count"))
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
    jsonMapper.registerSubtypes(new NamedType(PartialProjectionLoadSpec.class, PartialProjectionLoadSpec.TYPE));
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
  }

  @AfterEach
  void tearDown()
  {
    if (manager != null) {
      manager.shutdown();
    }
  }

  private SegmentLocalCacheManager makeManager(boolean virtualStorage, boolean partialDownloadsEnabled)
  {
    return makeManagerAtLocations(virtualStorage, partialDownloadsEnabled, List.of(cacheRoot));
  }

  private SegmentLocalCacheManager makeManagerAtLocations(
      boolean virtualStorage,
      boolean partialDownloadsEnabled,
      List<File> locationRoots
  )
  {
    final List<StorageLocationConfig> locConfigs = locationRoots.stream()
        .map(root -> new StorageLocationConfig(root, 1024L * 1024L * 1024L, null))
        .toList();
    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig()
        .setLocations(locConfigs)
        .setVirtualStorage(virtualStorage)
        .setVirtualStoragePartialDownloadsEnabled(partialDownloadsEnabled);
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    return new SegmentLocalCacheManager(
        storageLocations,
        loaderConfig,
        StorageLoadingThreadPool.createFromConfig(loaderConfig),
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestHelper.getTestIndexIO(jsonMapper, ColumnConfig.DEFAULT),
        jsonMapper
    );
  }

  private DataSegment partialWrapperSegment(List<String> selectedProjections)
  {
    return partialWrapperSegment(selectedProjections, FINGERPRINT);
  }

  private DataSegment partialWrapperSegment(List<String> selectedProjections, String fingerprint)
  {
    final Map<String, Object> delegate = Map.of(
        "type", "local",
        "path", DEEP_STORAGE_DIR.getAbsolutePath()
    );
    final Map<String, Object> wrapperWire =
        PartialProjectionLoadSpec.wireForm(delegate, selectedProjections, fingerprint);
    return DataSegment.builder(SEGMENT_ID)
                      .shardSpec(NoneShardSpec.instance())
                      .loadSpec(wrapperWire)
                      .size(0)
                      .build();
  }

  /**
   * A wrapper whose inner LoadSpec resolves via {@code LocalLoadSpec} against a directory that holds no V10 file, so
   * {@code openRangeReader()} returns {@code null}. Simulates the "backend doesn't support range reads" case (e.g. a
   * historical that received a partial-load rule pointing at zipped deep storage).
   */
  private DataSegment partialWrapperSegmentWithNullRangeReader(List<String> selectedProjections, String fingerprint)
      throws IOException
  {
    final File noRangeReaderDir = new File(
        perTestTempDir,
        "no_range_reader_" + fingerprint.replace(':', '_').replace('.', '_')
    );
    FileUtils.mkdirp(noRangeReaderDir);
    final Map<String, Object> delegate = Map.of(
        "type", "local",
        "path", noRangeReaderDir.getAbsolutePath()
    );
    final Map<String, Object> wrapperWire =
        PartialProjectionLoadSpec.wireForm(delegate, selectedProjections, fingerprint);
    return DataSegment.builder(SEGMENT_ID)
                      .shardSpec(NoneShardSpec.instance())
                      .loadSpec(wrapperWire)
                      .size(0)
                      .build();
  }

  /**
   * Post-load lookup of the mounted partial metadata entry. loadPartial is synchronous — by the time load() returns,
   * the entry is either fully mounted or the call threw; tests can inspect state immediately.
   */
  private static PartialSegmentMetadataCacheEntry mountedMetadata(StorageLocation location, SegmentId segmentId)
  {
    final CacheEntry entry = location.getStaticCacheEntry(new SegmentCacheEntryIdentifier(segmentId));
    Assertions.assertInstanceOf(PartialSegmentMetadataCacheEntry.class, entry);
    final PartialSegmentMetadataCacheEntry partial = (PartialSegmentMetadataCacheEntry) entry;
    Assertions.assertTrue(partial.isMounted(), "metadata entry for " + segmentId + " should be mounted after load()");
    return partial;
  }

  private static PartialSegmentBundleCacheEntry mountedBundle(
      StorageLocation location,
      SegmentId segmentId,
      String bundleName
  )
  {
    final CacheEntry entry = location.getStaticCacheEntry(
        new PartialSegmentBundleCacheEntryIdentifier(segmentId, bundleName)
    );
    Assertions.assertInstanceOf(PartialSegmentBundleCacheEntry.class, entry);
    final PartialSegmentBundleCacheEntry bundle = (PartialSegmentBundleCacheEntry) entry;
    Assertions.assertTrue(bundle.isMounted(), "bundle " + bundleName + " should be mounted after load()");
    return bundle;
  }

  @Test
  void testLoadPinsMetadataAndSelectedBundleAsStatic() throws Exception
  {
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);
    final DataSegment segment = partialWrapperSegment(List.of(AGG_BUNDLE));

    manager.load(segment);

    final PartialSegmentMetadataCacheEntry metadata = mountedMetadata(location, SEGMENT_ID);
    final PartialSegmentBundleCacheEntry agg = mountedBundle(location, SEGMENT_ID, AGG_BUNDLE);

    final SegmentCacheEntryIdentifier metaId = new SegmentCacheEntryIdentifier(SEGMENT_ID);
    final PartialSegmentBundleCacheEntryIdentifier aggId =
        new PartialSegmentBundleCacheEntryIdentifier(SEGMENT_ID, AGG_BUNDLE);
    Assertions.assertTrue(location.isReserved(metaId), "metadata entry should be in staticCacheEntries");
    Assertions.assertTrue(location.isReserved(aggId), "selected bundle should be in staticCacheEntries");
    Assertions.assertFalse(location.isWeakReserved(metaId), "metadata entry should NOT be in weakCacheEntries");
    Assertions.assertFalse(location.isWeakReserved(aggId), "selected bundle should NOT be in weakCacheEntries");

    Assertions.assertFalse(agg.getContainerRefs().isEmpty(), "selected bundle must own at least one container");
    // staticBundleNames on the metadata entry is intentionally empty on the fresh-load path (loadPartial reserves
    // statics directly after mount); the field communicates rule intent only across the bootstrap-restore boundary.
    Assertions.assertTrue(metadata.getStaticBundleNames().isEmpty());
  }

  @Test
  void testLoadDoesNotPreMountNonSelectedBundles() throws Exception
  {
    // Select one of the two aggregate projections. The selected one + its parent (__base) must be mounted; the other
    // aggregate is neither selected nor a parent so it must stay unmounted.
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);
    final DataSegment segment = partialWrapperSegment(List.of(AGG_BUNDLE));

    manager.load(segment);
    mountedMetadata(location, SEGMENT_ID);
    mountedBundle(location, SEGMENT_ID, AGG_BUNDLE);
    mountedBundle(location, SEGMENT_ID, Projections.BASE_TABLE_PROJECTION_NAME);

    final PartialSegmentBundleCacheEntryIdentifier otherAggId =
        new PartialSegmentBundleCacheEntryIdentifier(SEGMENT_ID, OTHER_AGG_BUNDLE);
    Assertions.assertNull(
        location.getCacheEntry(otherAggId),
        "non-selected, non-parent bundle must not be mounted at load time"
    );
  }

  @Test
  void testLoadEagerlyDownloadsSelectedBundleContainers() throws Exception
  {
    // With sync loadPartial, ensureBundleDownloaded has already run on the calling thread by the time load() returns.
    // A second ensureBundleDownloaded call must be a pure no-op — no new bytes fetched from deep storage.
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);
    final DataSegment segment = partialWrapperSegment(List.of(AGG_BUNDLE));

    manager.load(segment);
    final PartialSegmentMetadataCacheEntry metadata = mountedMetadata(location, SEGMENT_ID);
    mountedBundle(location, SEGMENT_ID, AGG_BUNDLE);

    final PartialSegmentFileMapperV10 fileMapper = metadata.getFileMapper();
    Assertions.assertNotNull(fileMapper, "metadata mount should produce a file mapper");
    final long downloadedBefore = fileMapper.getDownloadedBytes();
    fileMapper.ensureBundleDownloaded(AGG_BUNDLE);
    Assertions.assertEquals(
        downloadedBefore,
        fileMapper.getDownloadedBytes(),
        "ensureBundleDownloaded should be a no-op since loadPartial already downloaded the bundle eagerly"
    );
  }

  @Test
  void testLoadWithoutVirtualStorageFallsThroughToEagerFullLoad() throws Exception
  {
    // virtualStorage=false: the partial wrapper unwraps to its inner delegate (full download via CompleteSegmentCacheEntry,
    // static). No partial machinery involved.
    manager = makeManager(false, false);
    final StorageLocation location = manager.getLocations().get(0);
    final DataSegment segment = partialWrapperSegment(List.of(AGG_BUNDLE));

    manager.load(segment);

    final SegmentCacheEntryIdentifier metaId = new SegmentCacheEntryIdentifier(SEGMENT_ID);
    final CacheEntry entry = location.getCacheEntry(metaId);
    Assertions.assertNotNull(entry, "complete-load static entry must exist");
    Assertions.assertFalse(
        entry instanceof PartialSegmentMetadataCacheEntry,
        "virtualStorage=false must NOT take the partial machinery path"
    );
    Assertions.assertTrue(location.isReserved(metaId), "entry should be in staticCacheEntries");
  }

  @Test
  void testLoadWithPartialsDisabledNoOps() throws Exception
  {
    // virtualStorage=true but partialDownloads=false: today's current behavior, load() is a noop and the segment will
    // be weakly-loaded at query time via the existing PartialLoadSpec.loadSegment delegate-to-full fallback.
    manager = makeManager(true, false);
    final StorageLocation location = manager.getLocations().get(0);
    final DataSegment segment = partialWrapperSegment(List.of(AGG_BUNDLE));

    manager.load(segment);

    final SegmentCacheEntryIdentifier metaId = new SegmentCacheEntryIdentifier(SEGMENT_ID);
    Assertions.assertNull(
        location.getCacheEntry(metaId),
        "with partial downloads disabled, load() should not eagerly reserve anything"
    );
  }

  @Test
  void testDropReleasesStaticMetadataAndBundles() throws Exception
  {
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);
    final DataSegment segment = partialWrapperSegment(List.of(AGG_BUNDLE));

    manager.load(segment);
    final PartialSegmentMetadataCacheEntry metadata = mountedMetadata(location, SEGMENT_ID);
    mountedBundle(location, SEGMENT_ID, AGG_BUNDLE);

    final SegmentCacheEntryIdentifier metaId = new SegmentCacheEntryIdentifier(SEGMENT_ID);
    final PartialSegmentBundleCacheEntryIdentifier aggId =
        new PartialSegmentBundleCacheEntryIdentifier(SEGMENT_ID, AGG_BUNDLE);
    Assertions.assertTrue(location.isReserved(metaId));
    Assertions.assertTrue(location.isReserved(aggId));

    manager.drop(segment);

    Assertions.assertFalse(location.isReserved(metaId), "metadata entry should be released from staticCacheEntries");
    Assertions.assertFalse(location.isReserved(aggId), "selected bundle should be released from staticCacheEntries");
    Assertions.assertFalse(metadata.isMounted(), "metadata entry should be unmounted after drop cascade");

    final File headerFile = new File(
        cacheRoot,
        SEGMENT_ID.toString() + "/" + IndexIO.V10_FILE_NAME + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX
    );
    Assertions.assertFalse(headerFile.exists(), "header file should be deleted after drop");
  }

  @Test
  void testSecondLoadWithSameFingerprintIsIdempotent() throws Exception
  {
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);
    final DataSegment segment = partialWrapperSegment(List.of(AGG_BUNDLE));

    manager.load(segment);
    final PartialSegmentMetadataCacheEntry firstEntry = mountedMetadata(location, SEGMENT_ID);

    // Second load() with the same fingerprint must not release + re-reserve the metadata; the operation is a no-op.
    manager.load(segment);
    final PartialSegmentMetadataCacheEntry secondEntry = mountedMetadata(location, SEGMENT_ID);
    Assertions.assertSame(firstEntry, secondEntry, "same-fingerprint re-load should not create a new metadata entry");
  }

  @Test
  void testSecondLoadWithChangedFingerprintReconcilesViaDropAndReload() throws Exception
  {
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);

    // First load: rule pins only the aggregate projection.
    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE), "v1:rule-original"));
    final PartialSegmentMetadataCacheEntry firstEntry = mountedMetadata(location, SEGMENT_ID);
    Assertions.assertEquals("v1:rule-original", firstEntry.getFingerprint());

    // Second load: rule now pins the OTHER aggregate. Different fingerprint → reconciliation drops the first entry
    // and reserves a fresh one with the new selection.
    manager.load(partialWrapperSegment(List.of(OTHER_AGG_BUNDLE), "v1:rule-updated"));
    final PartialSegmentMetadataCacheEntry secondEntry = mountedMetadata(location, SEGMENT_ID);
    Assertions.assertNotSame(firstEntry, secondEntry, "changed fingerprint should trigger a fresh metadata entry");
    Assertions.assertEquals("v1:rule-updated", secondEntry.getFingerprint());
    Assertions.assertFalse(firstEntry.isMounted(), "old entry should be unmounted after reconciliation");

    // The old rule's bundle is no longer pinned; the new rule's bundle is.
    final PartialSegmentBundleCacheEntryIdentifier oldBundleId =
        new PartialSegmentBundleCacheEntryIdentifier(SEGMENT_ID, AGG_BUNDLE);
    final PartialSegmentBundleCacheEntryIdentifier newBundleId =
        new PartialSegmentBundleCacheEntryIdentifier(SEGMENT_ID, OTHER_AGG_BUNDLE);
    Assertions.assertNull(location.getCacheEntry(oldBundleId), "old rule's bundle should be released");
    Assertions.assertTrue(location.isReserved(newBundleId), "new rule's bundle should be statically pinned");
  }

  @Test
  void testRangeReaderNullDropsStaleStaticEntryOnFingerprintChange() throws Exception
  {
    // without range reads a partial load cannot be honored, but a stale static entry from a prior rule must still
    // be reconciled — otherwise the historical silently keeps serving the OLD rule after the coordinator changed the
    // rule fingerprint.
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);
    // First load: normal wrapper, real range reader, static entry installed under fingerprint v1.
    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE), "v1:rule-original"));
    mountedMetadata(location, SEGMENT_ID);
    final SegmentCacheEntryIdentifier metaId = new SegmentCacheEntryIdentifier(SEGMENT_ID);
    Assertions.assertTrue(location.isReserved(metaId));

    // Second load: same segment, different rule fingerprint, but the delegate now points at a directory without a V10
    // file so LocalLoadSpec.openRangeReader() returns null. The rule can't be applied — but the stale static entry
    // for the OLD rule must be dropped so the coordinator's rule change isn't silently ignored.
    manager.load(partialWrapperSegmentWithNullRangeReader(List.of(AGG_BUNDLE), "v2:rule-updated"));

    Assertions.assertFalse(
        location.isReserved(metaId),
        "stale static partial entry for the old rule should be dropped when the new rule cannot be applied"
    );
  }

  @Test
  void testRangeReaderNullWithMatchingFingerprintPreservesEntry() throws Exception
  {
    // The rangeReader-null path must still short-circuit on a same-fingerprint re-issue — otherwise a transient
    // range-reader failure on a coordinator retry would drop a still-valid static entry.
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);
    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE), "v1:rule-original"));
    final PartialSegmentMetadataCacheEntry first = mountedMetadata(location, SEGMENT_ID);

    manager.load(partialWrapperSegmentWithNullRangeReader(List.of(AGG_BUNDLE), "v1:rule-original"));

    final PartialSegmentMetadataCacheEntry second = mountedMetadata(location, SEGMENT_ID);
    Assertions.assertSame(
        first,
        second,
        "same-fingerprint re-issue on the rangeReader-null path should preserve the existing entry"
    );
  }

  @Test
  void testReconciliationNukesLeftoverPartialDirectoryAcrossLocations() throws Exception
  {
    // reconciliation must move-and-delete stale per-segment directories at every location before the new reserve picks
    // a location. Without this, an initial load on location A followed by a reconciliation whose new reserve lands on
    // location B leaves A's now-empty partial dir on disk indefinitely — drop()'s cascade only deletes tracked files
    // (header via metadata unmount, containers via bundle eviction), not the enclosing dir.
    final File locationA = new File(perTestTempDir, "loc_a_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    final File locationB = new File(perTestTempDir, "loc_b_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    FileUtils.mkdirp(locationA);
    FileUtils.mkdirp(locationB);
    manager = makeManagerAtLocations(true, true, List.of(locationA, locationB));

    // First load: LeastBytesUsedStorageLocationSelectorStrategy picks locationA (both empty, first wins). Verify A
    // received the segment dir, and neither the __drop dir nor B's segment dir exist yet.
    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE), "v1:rule-original"));
    final File aSegDir = new File(locationA, SEGMENT_ID.toString());
    final File bSegDir = new File(locationB, SEGMENT_ID.toString());
    Assertions.assertTrue(aSegDir.isDirectory(), "first load must create the segment dir on locationA");
    Assertions.assertFalse(bSegDir.exists(), "locationB should be untouched by the first load");
    Assertions.assertFalse(new File(locationA, "__drop").exists());
    Assertions.assertFalse(new File(locationB, "__drop").exists());

    // Second load: same segment, different fingerprint. Reconciliation drops the entry on locationA, then the nuke
    // step atomicMoveAndDeletes A's leftover dir. The new reserve picks the least-used location — which is A again
    // in this configuration, so mkdirp recreates A/segId fresh. The observable signal that the nuke ran is that A's
    // __drop directory now exists (atomicMoveAndDeleteCacheEntryDirectory creates it lazily on first use).
    manager.load(partialWrapperSegment(List.of(OTHER_AGG_BUNDLE), "v2:rule-updated"));
    Assertions.assertTrue(
        new File(locationA, "__drop").exists(),
        "reconciliation must have moved the stale dir into locationA's __drop dir, proving the nuke was invoked"
    );
    // New entry present at whichever location the strategy picked — verify the segment is loaded somewhere.
    Assertions.assertTrue(
        aSegDir.exists() || bSegDir.exists(),
        "reconciliation must have installed the new entry on one of the locations"
    );
  }

  @Test
  void testReserveMkdirpFailureFallsThroughToNextLocation() throws Exception
  {
    // a per-location mkdirp failure (inode exhaustion, permission drop, transient EIO on one disk) must not abort the
    // whole load when another location would accept the segment. Setup:
    //  - writable first in list so info_dir defaults to writable/info_dir (write-tolerant path).
    //  - Pre-fill writable with a dummy reservation so LeastBytesUsed picks the read-only location FIRST.
    //  - Read-only location: mkdirp of the per-segment subdir throws IOException → release the reservation and
    //    fall through to the writable location → mount lands there.
    final File writable = new File(perTestTempDir, "loc_rw_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    final File readOnly = new File(perTestTempDir, "loc_ro_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    FileUtils.mkdirp(writable);
    FileUtils.mkdirp(readOnly);
    manager = makeManagerAtLocations(true, true, List.of(writable, readOnly));

    // Bump writable's currentSize above 0 so LeastBytesUsed iterates readOnly (usage 0) first.
    final SegmentId dummyId = SegmentId.of("dummy", Intervals.of("2020/2021"), "v", 0);
    Assertions.assertTrue(
        manager.getLocations().get(0).reserveWeak(stubCacheEntry(new SegmentCacheEntryIdentifier(dummyId), 4096L)),
        "dummy pre-fill on writable must succeed"
    );

    Assertions.assertTrue(readOnly.setReadOnly(), "test setup must be able to make readOnly location read-only");
    try {
      manager.load(partialWrapperSegment(List.of(AGG_BUNDLE)));

      final File roSegDir = new File(readOnly, SEGMENT_ID.toString());
      final File rwSegDir = new File(writable, SEGMENT_ID.toString());
      Assertions.assertFalse(roSegDir.exists(), "read-only location must NOT receive the segment");
      Assertions.assertTrue(rwSegDir.isDirectory(), "load must fall through to the writable location");
      final SegmentCacheEntryIdentifier metaId = new SegmentCacheEntryIdentifier(SEGMENT_ID);
      Assertions.assertTrue(
          manager.getLocations().get(0).isReserved(metaId),
          "static metadata reservation must land on the writable location after readOnly's mkdirp fails"
      );
      Assertions.assertFalse(
          manager.getLocations().get(1).isReserved(metaId),
          "the reservation attempted on the readOnly location must have been released on mkdirp failure"
      );
    }
    finally {
      Assertions.assertTrue(readOnly.setWritable(true), "test teardown must restore write permission");
    }
  }

  @Test
  void testLoadThrowsWhenOldStaticEntryHasOutstandingReferences() throws Exception
  {
    // a running query holding a metadata reference on the OLD static partial would defer its doActualUnmount past
    // drop(). Its onUnmount hook (deleteSegmentInfoFile) plus deleteHeaderFiles would fire later — after the new
    // entry's storeInfoFile + mount have written fresh on-disk state — and corrupt the new entry
    // (segment ID + partial dir are shared). Fail the load explicitly BEFORE drop so the old entry stays fully
    // installed for a subsequent coordinator retry.
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);
    final SegmentCacheEntryIdentifier metaId = new SegmentCacheEntryIdentifier(SEGMENT_ID);
    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE), "v1:rule-original"));
    final PartialSegmentMetadataCacheEntry oldEntry = mountedMetadata(location, SEGMENT_ID);

    // Pin the old entry with an outstanding metadata reference (a running query would hold one of these via a Segment).
    final Closeable heldRef = oldEntry.acquireMetadataReference();
    try {
      final SegmentLoadingException thrown = Assertions.assertThrows(
          SegmentLoadingException.class,
          () -> manager.load(partialWrapperSegment(List.of(OTHER_AGG_BUNDLE), "v1:rule-updated"))
      );
      Assertions.assertTrue(
          thrown.getMessage().contains("outstanding references"),
          "unexpected message: " + thrown.getMessage()
      );
      // Old entry survives the failed reconciliation WITHOUT dropping — the precheck refused to touch static state,
      // so the old entry is fully installed and mounted, not just deferred-mounted.
      Assertions.assertTrue(
          oldEntry.isMounted(),
          "held reference should keep the old entry mounted"
      );
      Assertions.assertTrue(
          location.isReserved(metaId),
          "reconciliation must not remove the old static entry when it refuses to proceed"
      );
    }
    finally {
      heldRef.close();
    }
    // The old entry is still fully installed (no drop happened). Once the external reference releases, retry
    // succeeds — the precheck sees no external references and proceeds with drop+reserve as usual.
    Assertions.assertTrue(oldEntry.isMounted(), "old entry stays mounted between failed reconciliation and retry");
    manager.load(partialWrapperSegment(List.of(OTHER_AGG_BUNDLE), "v1:rule-updated"));
    final PartialSegmentMetadataCacheEntry newEntry = mountedMetadata(location, SEGMENT_ID);
    Assertions.assertNotSame(oldEntry, newEntry, "retry after reference release should install a fresh entry");
    Assertions.assertEquals("v1:rule-updated", newEntry.getFingerprint());
    Assertions.assertFalse(oldEntry.isMounted(), "old entry finally unmounts as part of the successful retry drop");
  }

  @Test
  void testLoadThrowsWhenOldStaticBundleHasOutstandingReferences() throws Exception
  {
    // outstanding references on a LINKED BUNDLE (not just the metadata directly) also defer cleanup past drop(). This
    // is caught transitively because PartialSegmentBundleCacheEntry.doMount holds a
    // metadataEntry.acquireMetadataReference for the bundle's lifetime — so while any bundle has outstanding
    // references, metadata.isMounted() still returns true and the reconciliation guard fires. This test proves the
    // transitive relationship survives a code change that decouples bundle-and-metadata refs.
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);
    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE), "v1:rule-original"));
    final PartialSegmentMetadataCacheEntry oldMeta = mountedMetadata(location, SEGMENT_ID);
    final PartialSegmentBundleCacheEntry oldAgg = mountedBundle(location, SEGMENT_ID, AGG_BUNDLE);

    // Pin the BUNDLE only (not the metadata directly). A running query walking the aggregate projection would hold
    // this reference via its Segment; the mount-time metadata ref carried by the bundle keeps the metadata alive
    // transitively.
    final Closeable heldBundleRef = oldAgg.acquireReference();
    try {
      final SegmentLoadingException thrown = Assertions.assertThrows(
          SegmentLoadingException.class,
          () -> manager.load(partialWrapperSegment(List.of(OTHER_AGG_BUNDLE), "v2:rule-updated"))
      );
      Assertions.assertTrue(
          thrown.getMessage().contains("outstanding references"),
          "unexpected message: " + thrown.getMessage()
      );
      // Both the pinned bundle and its metadata still report mounted (both cleanup paths are deferred).
      Assertions.assertTrue(oldAgg.isMounted(), "held bundle reference must keep bundle mounted");
      Assertions.assertTrue(
          oldMeta.isMounted(),
          "bundle's mount-time metadata reference must keep metadata mounted transitively"
      );
    }
    finally {
      heldBundleRef.close();
    }
    // Once the bundle reference releases, its dependency-ref release chains into the metadata's cleanup. Retry.
    manager.load(partialWrapperSegment(List.of(OTHER_AGG_BUNDLE), "v2:rule-updated"));
    final PartialSegmentMetadataCacheEntry newEntry = mountedMetadata(location, SEGMENT_ID);
    Assertions.assertNotSame(oldMeta, newEntry, "retry after bundle reference release should install a fresh entry");
    Assertions.assertEquals("v2:rule-updated", newEntry.getFingerprint());
  }

  @Test
  void testLoadEvictsPreExistingUnheldWeakEntry() throws Exception
  {
    // a weak reservation for this segment ID (e.g., installed by bootstrap when no rule fingerprint was persisted,
    // or by a prior on-demand acquireSegment) must be evicted before loadPartial reserves the new static entry.
    // drop() only clears staticCacheEntries, so without explicit weak-eviction the two would coexist on the same
    // on-disk directory.
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);
    final SegmentCacheEntryIdentifier id = new SegmentCacheEntryIdentifier(SEGMENT_ID);
    Assertions.assertTrue(location.reserveWeak(stubCacheEntry(id, 1024L)));
    Assertions.assertTrue(location.isWeakReserved(id), "precondition: weak entry should be installed");

    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE)));

    Assertions.assertFalse(
        location.isWeakReserved(id),
        "reconciliation must evict the pre-existing weak entry before installing the static partial"
    );
    Assertions.assertTrue(location.isReserved(id), "static partial entry should be installed after reconciliation");
    mountedMetadata(location, SEGMENT_ID);
  }

  @Test
  void testLoadThrowsWhenPreExistingWeakEntryHasActiveHold() throws Exception
  {
    // A held weak entry (a running query) cannot be evicted synchronously without breaking that query's on-disk
    // reads. Fail the load explicitly so the coordinator retries once the query completes, rather than stacking a
    // static reservation on top of a live weak entry.
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);
    final SegmentCacheEntryIdentifier id = new SegmentCacheEntryIdentifier(SEGMENT_ID);
    final CacheEntry stub = stubCacheEntry(id, 1024L);
    final StorageLocation.ReservationHold<CacheEntry> hold = location.addWeakReservationHold(id, () -> stub);
    Assertions.assertNotNull(hold, "precondition: weak reservation with hold should succeed");
    try {
      final SegmentLoadingException thrown = Assertions.assertThrows(
          SegmentLoadingException.class,
          () -> manager.load(partialWrapperSegment(List.of(AGG_BUNDLE)))
      );
      Assertions.assertTrue(
          thrown.getMessage().contains("active holds"),
          "unexpected message: " + thrown.getMessage()
      );
      Assertions.assertTrue(
          location.isWeakReserved(id),
          "held weak entry must survive the failed reconciliation attempt"
      );
      Assertions.assertFalse(
          location.isReserved(id),
          "no static partial entry should be installed alongside a held weak entry"
      );
    }
    finally {
      hold.close();
    }
  }

  private static CacheEntry stubCacheEntry(CacheEntryIdentifier id, long size)
  {
    return new CacheEntry()
    {
      @Override
      public CacheEntryIdentifier getId()
      {
        return id;
      }

      @Override
      public long getSize()
      {
        return size;
      }

      @Override
      public boolean isMounted()
      {
        return false;
      }

      @Override
      public void mount(StorageLocation location)
      {
      }

      @Override
      public void unmount()
      {
      }
    };
  }

  @Test
  void testLoadFailureCascadeReleasesEverything() throws Exception
  {
    // Defensive check exercise: a wire form referring to a projection this segment doesn't have would only happen
    // on matcher/reader drift or a coding bug, but the sync loadPartial flow must still cascade-release everything
    // and let load() throw so the failure is visible instead of leaving a silent zombie entry.
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);
    final DataSegment segment = partialWrapperSegment(List.of("does_not_exist"));

    final SegmentLoadingException thrown = Assertions.assertThrows(
        SegmentLoadingException.class,
        () -> manager.load(segment)
    );
    Assertions.assertTrue(
        thrown.getMessage().contains("Failed to load partial segment"),
        "unexpected message: " + thrown.getMessage()
    );
    // The underlying cause carries the specific validation failure — a rule projection name not in this segment.
    Assertions.assertNotNull(thrown.getCause(), "expected an underlying cause");
    Assertions.assertTrue(
        thrown.getCause().getMessage().contains("does not contain projection[does_not_exist]"),
        "unexpected cause message: " + thrown.getCause().getMessage()
    );

    final SegmentCacheEntryIdentifier metaId = new SegmentCacheEntryIdentifier(SEGMENT_ID);
    Assertions.assertFalse(
        location.isReserved(metaId),
        "metadata reservation should be rolled back after a rule failure"
    );
    Assertions.assertFalse(
        location.isWeakReserved(metaId),
        "no weak fallback should be left behind either"
    );
    // The info file was written before the async work started; the metadata's onUnmount hook should have deleted it
    // as part of the cascade release.
    final File infoFile = new File(new File(cacheRoot, "info_dir"), SEGMENT_ID.toString());
    Assertions.assertFalse(infoFile.exists(), "info file should be deleted by the metadata's onUnmount hook");
  }
}
