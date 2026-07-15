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
import org.apache.druid.client.DataSegmentAndLoadProfile;
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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.coordinator.loading.PartialLoadProfile;
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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Manager-level tests for the partial-load-rule {@link SegmentLocalCacheManager#load} path: a segment whose
 * {@code loadSpec} is a {@link PartialLoadSpec} wrapper drives
 * {@link PartialSegmentMetadataCacheEntry#applyRule} on the metadata entry, which installs a self-referential
 * {@link StorageLocation.ReservationHold} to keep the metadata resident and holds on each selected bundle already
 * registered. Selected bundles not yet registered get eager-downloads submitted to the loading pool; {@code load}
 * blocks until every eager download completes so the announced fingerprint reflects "rule fully realized". On any
 * eager-download failure the rule state is cleared and a {@link SegmentLoadingException} propagates so the
 * coordinator's load queue retries. The cache entries themselves live in {@code weakCacheEntries} exactly as they
 * would from an on-demand acquire; the rule-holds keep them from being SIEVE-evicted for as long as the coordinator
 * considers the segment loaded.
 * <p>
 * {@code drop} calls {@link PartialSegmentMetadataCacheEntry#clearRule} — entries become unheld weak (subject to any
 * remaining query holds) and SIEVE reclaims them naturally.
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
      // Drop the segment to release rule-holds and unmount mmap'd bundle files before @TempDir tries to clean up
      // the cache directory. Without this, on some platforms the temp-dir cleanup fails on still-mapped files.
      try {
        manager.drop(partialWrapperSegment(List.of(AGG_BUNDLE)));
      }
      catch (Throwable ignored) {
        // best-effort — some tests never installed a rule on this segment
      }
      manager.shutdown();
    }
  }

  @Test
  void testLoadInstallsRuleHoldsOnMetadataAndSelectedBundle() throws Exception
  {
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);

    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE)));

    // The metadata entry now carries the rule state.
    final PartialSegmentMetadataCacheEntry metadata = weakReservedMetadata(location, SEGMENT_ID);
    Assertions.assertTrue(metadata.isRuleHeld(), "rule must be applied to the metadata entry");
    Assertions.assertEquals(FINGERPRINT, metadata.getRuleFingerprint(), "fingerprint must be stored on the entry");
    // Metadata + selected bundle + its base parent are weak-reserved on the location; NOT static.
    final SegmentCacheEntryIdentifier metaId = new SegmentCacheEntryIdentifier(SEGMENT_ID);
    final PartialSegmentBundleCacheEntryIdentifier aggId =
        new PartialSegmentBundleCacheEntryIdentifier(SEGMENT_ID, AGG_BUNDLE);
    final PartialSegmentBundleCacheEntryIdentifier baseId =
        new PartialSegmentBundleCacheEntryIdentifier(SEGMENT_ID, Projections.BASE_TABLE_PROJECTION_NAME);
    Assertions.assertTrue(location.isWeakReserved(metaId), "metadata entry should be weak-reserved");
    Assertions.assertTrue(location.isWeakReserved(aggId), "selected bundle should be weak-reserved");
    Assertions.assertTrue(location.isWeakReserved(baseId), "base dependency should be weak-reserved");
    Assertions.assertFalse(location.isReserved(metaId), "metadata entry should NOT be in staticCacheEntries");
    Assertions.assertFalse(location.isReserved(aggId), "selected bundle should NOT be in staticCacheEntries");
  }

  @Test
  void testLoadDoesNotReserveNonSelectedBundles() throws Exception
  {
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);

    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE)));

    final PartialSegmentBundleCacheEntryIdentifier otherAggId =
        new PartialSegmentBundleCacheEntryIdentifier(SEGMENT_ID, OTHER_AGG_BUNDLE);
    Assertions.assertFalse(
        location.isWeakReserved(otherAggId),
        "non-selected bundle must not be reserved by loadPartial"
    );
    Assertions.assertNull(
        location.getCacheEntry(otherAggId),
        "non-selected bundle must not be in the cache at all after load"
    );
  }

  @Test
  void testLoadEagerlyDownloadsSelectedBundleContainers() throws Exception
  {
    // ensureBundleDownloaded should be a no-op after load — the eager-download pool task already downloaded the
    // bundle.
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);

    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE)));

    final PartialSegmentMetadataCacheEntry metadata = weakReservedMetadata(location, SEGMENT_ID);
    final PartialSegmentFileMapperV10 mapper = metadata.getFileMapper();
    Assertions.assertNotNull(mapper, "metadata mount should produce a file mapper");
    final long downloadedBefore = mapper.getDownloadedBytes();
    mapper.ensureBundleDownloaded(AGG_BUNDLE);
    Assertions.assertEquals(
        downloadedBefore,
        mapper.getDownloadedBytes(),
        "ensureBundleDownloaded must be a no-op — the eager task already downloaded the bundle"
    );
  }

  @Test
  void testLoadEagerlyDownloadsBaseDependencyOfSelectedProjection() throws Exception
  {
    // when the rule selects a projection bundle, __base is an implicit mount-time dependency (parent hold + sparse
    // allocation) but its container bytes are NOT downloaded by ensureBundleDownloaded(projection).
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);

    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE)));

    final PartialSegmentMetadataCacheEntry metadata = weakReservedMetadata(location, SEGMENT_ID);
    final PartialSegmentFileMapperV10 mapper = metadata.getFileMapper();
    Assertions.assertNotNull(mapper, "metadata mount should produce a file mapper");
    Assertions.assertTrue(
        mapper.isBundleFullyDownloaded(Projections.BASE_TABLE_PROJECTION_NAME),
        "base dependency of the selected projection must be fully downloaded eagerly, not just sparse-mounted"
    );
  }

  @Test
  void testLoadPartialReturnsDataSegmentAndLoadProfileWithRealizedBytes() throws Exception
  {
    // load() on the partial-load path returns a DataSegmentAndLoadProfile carrying a PartialLoadProfile.forLoaded with
    // the actual on-disk footprint. Under a projection rule, that footprint is metadata header + rule-selected
    // projection bundle + __base (transitive dependency pinned via the projection's parent-hold). Anything less
    // would under-report to the coordinator: the historical downloads __base as part of the rule (see
    // awaitEagerDownloadsOrClearRule's dep expansion), so its bytes belong in the announcement's loadedBytes.
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);

    final DataSegment loaded = manager.load(partialWrapperSegment(List.of(AGG_BUNDLE)));

    Assertions.assertInstanceOf(
        DataSegmentAndLoadProfile.class,
        loaded,
        "load on the partial-rule path must return a DataSegmentAndLoadProfile so the announcer can report actual footprint"
    );
    final PartialLoadProfile profile = ((DataSegmentAndLoadProfile) loaded).profile();
    Assertions.assertEquals(FINGERPRINT, profile.fingerprint(), "profile's fingerprint must match the applied rule");

    // Sum the parts we expect: metadata + AGG_BUNDLE (rule-selected) + __base (transitive dep).
    final PartialSegmentMetadataCacheEntry metadata = weakReservedMetadata(location, SEGMENT_ID);
    final PartialSegmentBundleCacheEntry aggEntry = location.getCacheEntry(
        new PartialSegmentBundleCacheEntryIdentifier(SEGMENT_ID, AGG_BUNDLE)
    );
    final PartialSegmentBundleCacheEntry baseEntry = location.getCacheEntry(
        new PartialSegmentBundleCacheEntryIdentifier(SEGMENT_ID, Projections.BASE_TABLE_PROJECTION_NAME)
    );
    Assertions.assertNotNull(aggEntry, "rule-selected projection bundle should be present");
    Assertions.assertNotNull(baseEntry, "transitive __base dependency should be present");
    final long expectedRealizedBytes = metadata.getSize() + aggEntry.getSize() + baseEntry.getSize();

    Assertions.assertEquals(
        Long.valueOf(expectedRealizedBytes),
        profile.loadedBytes(),
        "profile's loadedBytes must include metadata + rule-selected projection + __base dependency; missing any of "
        + "these would under-report the on-disk footprint to the coordinator"
    );
    Assertions.assertTrue(
        baseEntry.getSize() > 0,
        "sanity check: __base's size should be non-zero (so the dep-inclusion assertion is meaningful)"
    );
  }
  @Test
  void testRealizedBytesIncludesPinnedBaseDependency() throws Exception
  {
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);

    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE)));

    final PartialSegmentMetadataCacheEntry metadata = weakReservedMetadata(location, SEGMENT_ID);
    final long metadataBytes = metadata.getSize();
    final long aggBytes = bundleEntry(location, AGG_BUNDLE).getSize();
    final long baseBytes = bundleEntry(location, Projections.BASE_TABLE_PROJECTION_NAME).getSize();

    Assertions.assertTrue(baseBytes > 0, "base dependency must occupy real on-disk bytes");

    final long realized = metadata.getRealizedBytes();

    Assertions.assertEquals(
        metadataBytes + aggBytes + baseBytes,
        realized,
        StringUtils.format(
            "realizedBytes must include the pinned __base dependency: metadata=%d + selected=%d + base=%d = %d, "
            + "but got %d (short by %d, exactly the base footprint)",
            metadataBytes, aggBytes, baseBytes, metadataBytes + aggBytes + baseBytes,
            realized, (metadataBytes + aggBytes + baseBytes) - realized
        )
    );
  }

  @Test
  void testRealizedBytesCountsSharedBaseDependencyOnce() throws Exception
  {
    // Two projections in a single rule both pin the same __base. The base bytes must be counted exactly once.
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);

    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE, OTHER_AGG_BUNDLE)));

    final PartialSegmentMetadataCacheEntry metadata = weakReservedMetadata(location, SEGMENT_ID);
    final long metadataBytes = metadata.getSize();
    final long aggBytes = bundleEntry(location, AGG_BUNDLE).getSize();
    final long otherBytes = bundleEntry(location, OTHER_AGG_BUNDLE).getSize();
    final long baseBytes = bundleEntry(location, Projections.BASE_TABLE_PROJECTION_NAME).getSize();

    Assertions.assertEquals(
        metadataBytes + aggBytes + otherBytes + baseBytes,
        metadata.getRealizedBytes(),
        "realizedBytes must include the shared __base exactly once across both dependent projections"
    );
  }

  @Test
  void testLoadWithoutVirtualStorageDoesNotInstallRuleHolds() throws Exception
  {
    // virtualStorage=false: partial-load rules aren't a concept here. load() takes the eager full-load path (via
    // PartialLoadSpec.loadSegment → the materialized inner LocalLoadSpec), and no rule state is created.
    manager = makeManager(false, false);
    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE)));
    Assertions.assertNull(
        manager.getRuleFingerprintForSegment(SEGMENT_ID),
        "no rule state on the eager path"
    );
  }

  @Test
  void testLoadWithPartialsDisabledIsNoop() throws Exception
  {
    manager = makeManager(true, false);
    final StorageLocation location = manager.getLocations().get(0);

    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE)));

    Assertions.assertNull(
        manager.getRuleFingerprintForSegment(SEGMENT_ID),
        "with partial downloads disabled, loadPartial must not run"
    );
    Assertions.assertFalse(
        location.isWeakReserved(new SegmentCacheEntryIdentifier(SEGMENT_ID)),
        "with partial downloads disabled, no reservation should be installed at load time"
    );
  }

  @Test
  void testSameFingerprintReIssueIsIdempotent() throws Exception
  {
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);

    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE)));
    final PartialSegmentMetadataCacheEntry firstMeta = weakReservedMetadata(location, SEGMENT_ID);

    // Second load with the same fingerprint must be idempotent — same metadata entry, unchanged rule state.
    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE)));
    final PartialSegmentMetadataCacheEntry secondMeta = weakReservedMetadata(location, SEGMENT_ID);
    Assertions.assertSame(firstMeta, secondMeta, "same-fingerprint re-issue must not create a new metadata entry");
    Assertions.assertEquals(FINGERPRINT, secondMeta.getRuleFingerprint(), "fingerprint unchanged after re-issue");
  }

  @Test
  void testRuleChangeSwapsHoldsWithoutDroppingSharedMetadata() throws Exception
  {
    // Same segment, different fingerprint: the rule's bundle selection changes but the underlying metadata entry is
    // the same weak entry (its self-hold is never released during the swap; only bundle rule-holds diff).
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);

    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE), "v1:rule-original"));
    final PartialSegmentMetadataCacheEntry meta = weakReservedMetadata(location, SEGMENT_ID);
    Assertions.assertEquals("v1:rule-original", meta.getRuleFingerprint());

    manager.load(partialWrapperSegment(List.of(OTHER_AGG_BUNDLE), "v2:rule-updated"));

    // Metadata entry is the SAME instance across the swap.
    final PartialSegmentMetadataCacheEntry metaAfter = weakReservedMetadata(location, SEGMENT_ID);
    Assertions.assertSame(meta, metaAfter, "rule change must reuse the underlying metadata entry");
    Assertions.assertEquals("v2:rule-updated", metaAfter.getRuleFingerprint(), "fingerprint must swap");
    // New rule's bundle is rule-held.
    Assertions.assertTrue(
        location.isWeakReserved(new PartialSegmentBundleCacheEntryIdentifier(SEGMENT_ID, OTHER_AGG_BUNDLE)),
        "new rule's bundle should be reserved"
    );
    Assertions.assertFalse(metaAfter.isBundleRuleHeld(AGG_BUNDLE));
  }

  @Test
  void testRuleSwapRewritesInfoFileOnDisk() throws Exception
  {
    // findOrReservePartial returns the existing entry on a rule swap, make sure we rewrite the
    // info file.
    manager = makeManager(true, true);

    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE), "v1:rule-original"));
    manager.load(partialWrapperSegment(List.of(OTHER_AGG_BUNDLE), "v2:rule-updated"));

    final File infoDir = new File(cacheRoot, "info_dir");
    final File infoFile = new File(infoDir, SEGMENT_ID.toString());
    Assertions.assertTrue(infoFile.exists(), "info file must exist after loadPartial");
    final DataSegment onDisk = jsonMapper.readValue(infoFile, DataSegment.class);
    Assertions.assertEquals(
        "v2:rule-updated",
        onDisk.getLoadSpec().get("fingerprint"),
        "info file on disk must carry the latest rule's fingerprint after a swap, not the prior rule's"
    );
    Assertions.assertEquals(
        List.of(OTHER_AGG_BUNDLE),
        onDisk.getLoadSpec().get("projections"),
        "info file on disk must carry the latest rule's projection selection"
    );
  }

  @Test
  void testDropClearsRule() throws Exception
  {
    manager = makeManager(true, true);
    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE)));
    Assertions.assertNotNull(manager.getRuleFingerprintForSegment(SEGMENT_ID));

    manager.drop(partialWrapperSegment(List.of(AGG_BUNDLE)));

    Assertions.assertNull(
        manager.getRuleFingerprintForSegment(SEGMENT_ID),
        "drop must clear the rule state on the metadata entry"
    );
  }

  @Test
  void testRangeReaderNullDropsPriorRule() throws Exception
  {
    // Prior rule installed. Second load with a wrapper whose delegate can't produce a range reader releases the rule
    // via clearRule so subsequent queries fall through to weak-full-load semantics.
    manager = makeManager(true, true);
    manager.load(partialWrapperSegment(List.of(AGG_BUNDLE), "v1:rule-original"));
    Assertions.assertNotNull(manager.getRuleFingerprintForSegment(SEGMENT_ID));

    manager.load(partialWrapperSegmentWithNullRangeReader(List.of(AGG_BUNDLE), "v2:rule-updated"));

    Assertions.assertNull(
        manager.getRuleFingerprintForSegment(SEGMENT_ID),
        "null range reader must clear the prior rule"
    );
  }

  @Test
  void testRangeReaderNullNoopWhenNoPriorRule() throws Exception
  {
    // No prior rule, no range reader → no rule installed, but also no failure. Segment simply doesn't get a rule;
    // a later query would drive weak-full-load via the on-demand path.
    manager = makeManager(true, true);
    manager.load(partialWrapperSegmentWithNullRangeReader(List.of(AGG_BUNDLE), "v1:rule-noop"));
    Assertions.assertNull(manager.getRuleFingerprintForSegment(SEGMENT_ID));
  }

  @Test
  void testLoadFailureLeavesNoRuleApplied() throws Exception
  {
    // Wrapper referring to a non-existent projection — wrapper.getSelectedBundleNames throws before applyRule fires.
    // In the v2 design there is no rollback-nuke of the partial dir on failure; the transient hold releases and the
    // now-unheld weak metadata entry becomes SIEVE-eligible naturally (info-file cleanup fires via the entry's
    // onUnmount hook when SIEVE eventually reclaims it). The important correctness invariant is: no stranded rule
    // state, no partial rule fingerprint applied.
    manager = makeManager(true, true);
    final DataSegment segment = partialWrapperSegment(List.of("does_not_exist"));

    final Throwable thrown = Assertions.assertThrows(
        Throwable.class,
        () -> manager.load(segment)
    );
    Assertions.assertTrue(
        thrown.getMessage() != null && thrown.getMessage().contains("does not contain projection[does_not_exist]")
        || thrown.getCause() != null
           && thrown.getCause().getMessage().contains("does not contain projection[does_not_exist]"),
        "unexpected throwable: " + thrown
    );
    Assertions.assertNull(
        manager.getRuleFingerprintForSegment(SEGMENT_ID),
        "failed load must not leave a rule applied on the metadata entry"
    );
  }

  @Test
  void testBootstrapReinstallsRuleHoldsFromPersistedInfoFile() throws Exception
  {
    // on historical restart, getCachedSegments+bootstrap must reapply the rule using the persisted DataSegment's
    // loadSpec (the wire-form PartialLoadSpec is stored in the info file), so a rule-pinned segment is protected from
    // eviction as soon as bootstrap completes
    manager = makeManager(true, true);
    final DataSegment segment = partialWrapperSegment(List.of(AGG_BUNDLE));

    manager.load(segment);
    Assertions.assertEquals(FINGERPRINT, manager.getRuleFingerprintForSegment(SEGMENT_ID));
    manager.shutdown();
    manager = null;

    // Fresh manager over the same cacheRoot — simulates a restart.
    final SegmentLocalCacheManager restarted = makeManager(true, true);
    try {
      final List<DataSegment> cached = restarted.getCachedSegments();
      Assertions.assertTrue(
          cached.stream().anyMatch(s -> s.getId().equals(SEGMENT_ID)),
          "restarted historical must rediscover the segment via its info file"
      );

      restarted.bootstrap(segment, SegmentLazyLoadFailCallback.NOOP);

      // Fingerprint round-trips through the info file → wrapper → applyRule.
      Assertions.assertEquals(
          FINGERPRINT,
          restarted.getRuleFingerprintForSegment(SEGMENT_ID),
          "bootstrap must reapply the persisted PartialLoadSpec wrapper's fingerprint"
      );
      // Selected bundle + its base dependency are held after bootstrap restore (they were on disk and got
      // restored by the metadata mount's PartialSegmentCacheBootstrap.restoreBundlesFromDisk, which register with
      // the metadata; applyRule picks up their rule-holds from the linkedBundles state).
      final StorageLocation loc = restarted.getLocations().get(0);
      Assertions.assertTrue(
          loc.isWeakReserved(new PartialSegmentBundleCacheEntryIdentifier(SEGMENT_ID, AGG_BUNDLE)),
          "selected bundle must be reserved after bootstrap reapply"
      );
      Assertions.assertTrue(
          loc.isWeakReserved(
              new PartialSegmentBundleCacheEntryIdentifier(SEGMENT_ID, Projections.BASE_TABLE_PROJECTION_NAME)
          ),
          "base dependency must be reserved after bootstrap reapply"
      );
    }
    finally {
      restarted.shutdown();
    }
  }

  @Test
  void testReserveMkdirpFailureFallsThroughToNextLocation() throws Exception
  {
    // Setup: writable first (so info_dir defaults there), read-only second, dummy reservation on writable so
    // LeastBytesUsed picks the read-only location first. mkdirp on read-only fails → release + continue to writable.
    final File writable = new File(perTestTempDir, "loc_rw_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    final File readOnly = new File(perTestTempDir, "loc_ro_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    FileUtils.mkdirp(writable);
    FileUtils.mkdirp(readOnly);
    manager = makeManagerAtLocations(true, true, List.of(writable, readOnly));

    // Bump writable's usage so LeastBytesUsed picks readOnly first.
    final SegmentId dummy = SegmentId.of("dummy", Intervals.of("2020/2021"), "v", 0);
    Assertions.assertTrue(
        manager.getLocations().get(0).reserveWeak(stubCacheEntry(new SegmentCacheEntryIdentifier(dummy), 4096L))
    );
    Assertions.assertTrue(readOnly.setReadOnly(), "test setup must be able to make readOnly location read-only");
    try {
      manager.load(partialWrapperSegment(List.of(AGG_BUNDLE)));

      Assertions.assertFalse(
          new File(readOnly, SEGMENT_ID.toString()).exists(),
          "read-only location must NOT receive the segment"
      );
      Assertions.assertTrue(
          new File(writable, SEGMENT_ID.toString()).isDirectory(),
          "load must fall through to the writable location"
      );
      Assertions.assertNotNull(manager.getRuleFingerprintForSegment(SEGMENT_ID));
    }
    finally {
      Assertions.assertTrue(readOnly.setWritable(true), "test teardown must restore write permission");
    }
  }

  @Test
  void testLoadReusesExistingWeakEntryHeldByConcurrentQuery() throws Exception
  {
    // A concurrent query is holding a weak metadata entry from an on-demand acquire when the coordinator issues a
    // partial-load rule for the same segment. loadPartial must reuse that entry — addWeakReservationHoldIfExists
    // adds another party to the same phaser rather than installing a fresh entry alongside.
    manager = makeManager(true, true);
    final StorageLocation location = manager.getLocations().get(0);
    final SegmentCacheEntryIdentifier id = new SegmentCacheEntryIdentifier(SEGMENT_ID);

    final DataSegment segment = partialWrapperSegment(List.of(AGG_BUNDLE));
    final File partialDir = new File(cacheRoot, SEGMENT_ID.toString());
    FileUtils.mkdirp(partialDir);
    final PartialSegmentMetadataCacheEntry preExisting = new PartialSegmentMetadataCacheEntry(
        SEGMENT_ID,
        partialDir,
        IndexIO.V10_FILE_NAME,
        List.of(),
        new DirectoryBackedRangeReader(DEEP_STORAGE_DIR),
        jsonMapper,
        null,
        16L * 1024L * 1024L,
        PartialSegmentFileMapperV10.DEFAULT_COALESCE_GAP_BYTES,
        PartialSegmentFileMapperV10.DEFAULT_MAX_FETCH_RUN_BYTES
    );
    final StorageLocation.ReservationHold<PartialSegmentMetadataCacheEntry> queryHold =
        location.addWeakReservationHold(id, () -> preExisting);
    Assertions.assertNotNull(queryHold, "precondition: on-demand-style weak reserve must succeed");
    try {
      Assertions.assertTrue(location.isWeakReserved(id));

      manager.load(segment);

      // The rule hold reuses the pre-existing entry — same instance across the load.
      final PartialSegmentMetadataCacheEntry ruleHeld = weakReservedMetadata(location, SEGMENT_ID);
      Assertions.assertSame(
          preExisting,
          ruleHeld,
          "loadPartial must add its hold to the pre-existing weak entry, not install a fresh one"
      );
      Assertions.assertNotNull(manager.getRuleFingerprintForSegment(SEGMENT_ID));
    }
    finally {
      queryHold.close();
    }
  }

  private static CacheEntry bundleEntry(StorageLocation location, String bundleName)
  {
    final CacheEntry entry =
        location.getCacheEntry(new PartialSegmentBundleCacheEntryIdentifier(SEGMENT_ID, bundleName));
    Assertions.assertNotNull(entry, "bundle[" + bundleName + "] must be resident after load");
    return entry;
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
   * {@code openRangeReader()} returns {@code null}. Simulates the "backend doesn't support range reads" case.
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
   * Post-load lookup of the mounted partial metadata entry, verifying it's in weak — not static. loadPartial's
   * install is synchronous by the time load() returns; the entry is either fully mounted or the call threw.
   */
  private static PartialSegmentMetadataCacheEntry weakReservedMetadata(StorageLocation location, SegmentId segmentId)
  {
    final CacheEntry entry = location.getCacheEntry(new SegmentCacheEntryIdentifier(segmentId));
    Assertions.assertInstanceOf(PartialSegmentMetadataCacheEntry.class, entry);
    final PartialSegmentMetadataCacheEntry partial = (PartialSegmentMetadataCacheEntry) entry;
    Assertions.assertTrue(partial.isMounted(), "metadata entry for " + segmentId + " should be mounted after load()");
    return partial;
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
}
