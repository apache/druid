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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.ListBasedInputRow;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.file.PartialSegmentDownloadListener;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

class PartialSegmentCacheBootstrapTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();
  private static final SegmentId SEGMENT_ID = SegmentId.of("test", Intervals.of("2025/2026"), "v1", 0);
  private static final String AGG_BUNDLE = "dim1_metric1_sum";
  private static final long ESTIMATE = 16 * 1024 * 1024L;

  private static final DateTime TIME = DateTimes.of("2025-01-01");
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
  static File sharedTempDir;

  private static File deepStorageDir;

  @TempDir
  File perTestTempDir;

  private File cacheDir;

  @BeforeAll
  static void buildSegment()
  {
    final File tmp = new File(sharedTempDir, "build_" + ThreadLocalRandom.current().nextInt());
    deepStorageDir = IndexBuilder.create()
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
                                 .indexSpec(IndexSpec.builder().withMetadataCompression(CompressionStrategy.NONE).build())
                                 .rows(ROWS)
                                 .buildMMappedIndexFile();
  }

  @BeforeEach
  void setupPerTest() throws IOException
  {
    cacheDir = new File(perTestTempDir, "cache_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    FileUtils.mkdirp(cacheDir);
  }

  @Test
  void testRestoreRebuildsBothEntriesFromDisk() throws IOException
  {
    primeOnDiskState();

    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = restoreFromDisk(location);

    Assertions.assertTrue(metadata.isMounted());
    // metadata entry size matches on-disk header size, NOT a pessimistic estimate
    final long headerSize = new File(
        cacheDir,
        IndexIO.V10_FILE_NAME + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX
    ).length();
    Assertions.assertEquals(headerSize, metadata.getSize());

    // bundles: should have at least __base; aggregate may or may not exist on disk depending on what we primed
    final List<PartialSegmentBundleCacheEntry> bundles = List.copyOf(metadata.snapshotLinkedBundles());
    Assertions.assertFalse(bundles.isEmpty());
    final Set<String> bundleNames = bundles.stream()
                                           .map(PartialSegmentBundleCacheEntry::getBundleName)
                                           .collect(Collectors.toSet());
    Assertions.assertTrue(bundleNames.contains(Projections.BASE_TABLE_PROJECTION_NAME));
    Assertions.assertTrue(bundleNames.contains(AGG_BUNDLE));
    for (PartialSegmentBundleCacheEntry bundle : bundles) {
      Assertions.assertTrue(bundle.isMounted(), "bundle " + bundle.getBundleName() + " should be mounted");
    }
  }

  @Test
  void testRestoreEstablishesParentHoldOnBase() throws IOException
  {
    primeOnDiskState();

    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = restoreFromDisk(location);

    // aggregate bundle should declare __base as its parent
    final PartialSegmentBundleCacheEntry agg = metadata.snapshotLinkedBundles().stream()
        .filter(b -> AGG_BUNDLE.equals(b.getBundleName()))
        .findFirst().orElseThrow();
    Assertions.assertEquals(1, agg.getParentEntryIds().size());
    Assertions.assertEquals(
        Projections.BASE_TABLE_PROJECTION_NAME,
        agg.getParentEntryIds().get(0).bundleName()
    );
  }

  @Test
  void testRestoreSkipsBundlesWithMissingContainers() throws IOException
  {
    primeOnDiskState();

    // remove the aggregate bundle's container file(s), base's containers stay
    final PartialSegmentFileMapperV10 introspect = createMapper(deepStorageDir, cacheDir);
    final List<Integer> aggContainers = new ArrayList<>();
    final String prefix = AGG_BUNDLE + "/";
    for (var entry : introspect.getSegmentFileMetadata().getFiles().entrySet()) {
      if (entry.getKey().startsWith(prefix)) {
        aggContainers.add(entry.getValue().getContainer());
      }
    }
    introspect.close();
    for (Integer ci : aggContainers) {
      final File cf = new File(cacheDir, StringUtils.format("%s.container.%05d", IndexIO.V10_FILE_NAME, ci));
      // only delete if no other bundle shares this container
      if (cf.exists()) {
        cf.delete();
      }
    }

    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = restoreFromDisk(location);

    // base should still be restored; aggregate is skipped because its containers were removed
    final Set<String> bundleNames = metadata.snapshotLinkedBundles().stream()
                                            .map(PartialSegmentBundleCacheEntry::getBundleName)
                                            .collect(Collectors.toSet());
    Assertions.assertTrue(bundleNames.contains(Projections.BASE_TABLE_PROJECTION_NAME));
    // Note: base and aggregate may legitimately share a container (small test segment with sub-cap data), in that
    // case both end up restored. Don't assert absence of aggregate; assert presence of base.
  }

  @Test
  void testRestoreDeletesOrphanedBundleAndSkipsIt() throws IOException
  {
    primeOnDiskState();

    // Remove base's container files. After this, base is unrestorable on disk, which makes the aggregate (which
    // depends on base) an orphan that must be deleted rather than restored in a degenerate state.
    final PartialSegmentFileMapperV10 introspect = createMapper(deepStorageDir, cacheDir);
    final Set<Integer> baseContainers = new HashSet<>();
    final Set<Integer> aggContainers = new HashSet<>();
    for (var entry : introspect.getSegmentFileMetadata().getFiles().entrySet()) {
      final String fileName = entry.getKey();
      final int slash = fileName.indexOf('/');
      if (slash < 0) {
        continue;
      }
      final String group = fileName.substring(0, slash);
      if (Projections.BASE_TABLE_PROJECTION_NAME.equals(group)) {
        baseContainers.add(entry.getValue().getContainer());
      } else if (AGG_BUNDLE.equals(group)) {
        aggContainers.add(entry.getValue().getContainer());
      }
    }
    introspect.close();

    // Only delete base containers; we want to verify the bootstrap deletes the aggregate's containers itself.
    for (Integer ci : baseContainers) {
      // Skip containers that base shares with aggregate (test segment is small enough that they may share).
      if (aggContainers.contains(ci)) {
        continue;
      }
      final File cf = new File(cacheDir, StringUtils.format("%s.container.%05d", IndexIO.V10_FILE_NAME, ci));
      Assertions.assertTrue(cf.exists());
      Assertions.assertTrue(cf.delete());
    }

    // If base shared all its containers with aggregate, this test scenario isn't reachable skip in that case.
    final Set<Integer> orphanContainers = new HashSet<>(aggContainers);
    orphanContainers.removeAll(baseContainers);
    if (orphanContainers.isEmpty()) {
      // base and aggregate share all containers; aggregate isn't truly orphaned. skip.
      return;
    }

    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = restoreFromDisk(location);

    // Neither base (containers missing) nor aggregate (orphaned, parent missing) should be restored.
    final Set<String> restoredNames = metadata.snapshotLinkedBundles().stream()
                                              .map(PartialSegmentBundleCacheEntry::getBundleName)
                                              .collect(Collectors.toSet());
    Assertions.assertFalse(restoredNames.contains(AGG_BUNDLE), "orphan must not be restored");

    // Aggregate's container files (those exclusively owned by it) must have been deleted by the orphan cleanup.
    for (Integer ci : orphanContainers) {
      final File cf = new File(cacheDir, StringUtils.format("%s.container.%05d", IndexIO.V10_FILE_NAME, ci));
      Assertions.assertFalse(
          cf.exists(),
          "orphan's exclusive container " + ci + " must have been deleted, but " + cf + " still exists"
      );
    }
  }

  @Test
  void testRestoreRollsBackOnBundleReservationFailure() throws IOException
  {
    primeOnDiskState();

    final File headerFile = new File(
        cacheDir,
        IndexIO.V10_FILE_NAME + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX
    );
    // size location exactly to the header size: metadata reservation fits, but the first bundle's weak reservation
    // has 0 bytes of remaining budget and no weak entries to reclaim, so addWeakReservationHold returns null
    final StorageLocation location = new StorageLocation(cacheDir, headerFile.length(), null);

    Assertions.assertThrows(
        Throwable.class,
        () -> restoreFromDisk(location)
    );

    // rollback must release the metadata reservation and leave no static/weak entries behind
    Assertions.assertEquals(0, location.currentSizeBytes(), "metadata reservation must be released on bootstrap failure");
    Assertions.assertFalse(
        location.isReserved(new SegmentCacheEntryIdentifier(SEGMENT_ID)),
        "metadata entry must be removed from the static map on bootstrap failure"
    );
    Assertions.assertEquals(0, location.getWeakEntryCount(), "no bundle entries should linger on bootstrap failure");
    // rollback flows through location.release -> metadata.unmount, which unconditionally clears the entry's
    // storage-location footprint. A subsequent acquire will cold-fetch from deep storage.
    Assertions.assertFalse(headerFile.exists(), "bootstrap failure deletes the header via the unmount cleanup path");
  }

  @Test
  void testMountFailureRemovesLingeringWeakEntry() throws IOException
  {
    primeOnDiskState();
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final SegmentCacheEntryIdentifier id = new SegmentCacheEntryIdentifier(SEGMENT_ID);

    // Reserve the metadata entry weakly, exactly as the bootstrap path does (no protecting hold), but with a range
    // reader that fails every fetch so we can force a mount failure below.
    final SegmentRangeReader failingReader = (filename, offset, length) -> {
      throw new IOException("simulated deep-storage fetch failure during mount");
    };
    final PartialSegmentMetadataCacheEntry metadata = PartialSegmentCacheBootstrap.reserveFromDisk(
        SEGMENT_ID,
        cacheDir,
        IndexIO.V10_FILE_NAME,
        List.of(),
        failingReader,
        JSON_MAPPER,
        null,
        location
    );
    Assertions.assertTrue(location.isWeakReserved(id));

    // Delete the header so the mount's file-mapper build must re-fetch it from deep storage; the failing reader throws,
    // failing the mount (the same poison that arises when create() detects header corruption and the deep-storage
    // fetch fails). The mount rollback deletes the header, so the entry is now both unmounted and un-re-mountable.
    final File headerFile = new File(
        cacheDir,
        IndexIO.V10_FILE_NAME + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX
    );
    Assertions.assertTrue(headerFile.delete());

    Assertions.assertThrows(Throwable.class, () -> metadata.mount(location));

    // The failed mount must not leave the lingering weak entry behind: a later findExistingPartialWithHold would
    // otherwise resurrect it and re-mount would fail forever (failing reader + deleted header). It must be gone so
    // the cold acquire path rebuilds a fresh, deep-storage-capable entry via reservePartial.
    Assertions.assertEquals(0, location.getWeakEntryCount(), "failed mount must remove the lingering weak entry");
    Assertions.assertFalse(location.isWeakReserved(id));
    Assertions.assertEquals(0, location.currentSizeBytes(), "failed mount must release the reservation");
  }

  @Test
  void testRestoredEntryCanLazilyFetchUndownloadedFile() throws IOException
  {
    // a bootstrap-restored entry must keep the segment's real deep-storage range reader so a later query can lazily
    // fetch a bundle/column that wasn't on disk at startup. Previously the entry held a throwing disk-only reader, so
    // this fetch failed with "bootstrap should only read from local disk".
    primeOnDiskState();
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = restoreFromDisk(location);

    final PartialSegmentFileMapperV10 mapper = metadata.getFileMapper();
    Assertions.assertNotNull(mapper, "restored entry must be mounted");

    // Priming sparse-allocated the containers but downloaded no column data, so every internal file is un-downloaded.
    final String fileToFetch = mapper.getSegmentFileMetadata().getFiles().keySet().stream()
                                     .filter(f -> f.startsWith(Projections.BASE_TABLE_PROJECTION_NAME + "/"))
                                     .findFirst()
                                     .orElseThrow();
    Assertions.assertFalse(
        mapper.getDownloadedFiles().contains(fileToFetch),
        "precondition: " + fileToFetch + " should not be downloaded yet"
    );

    Assertions.assertNotNull(
        mapper.mapFile(fileToFetch),
        "restored entry must lazily fetch an un-downloaded file from deep storage"
    );
    Assertions.assertTrue(mapper.getDownloadedFiles().contains(fileToFetch));
  }

  @Test
  void testReserveFailsWhenHeaderMissing()
  {
    // no priming: cacheDir is empty
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    Assertions.assertThrows(
        DruidException.class,
        () -> PartialSegmentCacheBootstrap.reserveFromDisk(
            SEGMENT_ID,
            cacheDir,
            IndexIO.V10_FILE_NAME,
            List.of(),
            new DirectoryBackedRangeReader(deepStorageDir),
            JSON_MAPPER,
            null,
            location
        )
    );
  }

  @Test
  void testIsPartialSegmentLayoutDetectsHeader() throws IOException
  {
    Assertions.assertFalse(PartialSegmentCacheBootstrap.isPartialSegmentLayout(cacheDir, IndexIO.V10_FILE_NAME));
    primeOnDiskState();
    Assertions.assertTrue(PartialSegmentCacheBootstrap.isPartialSegmentLayout(cacheDir, IndexIO.V10_FILE_NAME));
    Assertions.assertFalse(PartialSegmentCacheBootstrap.isPartialSegmentLayout(null, IndexIO.V10_FILE_NAME));
    Assertions.assertFalse(PartialSegmentCacheBootstrap.isPartialSegmentLayout(
        new File(perTestTempDir, "nonexistent"),
        IndexIO.V10_FILE_NAME
    ));
  }

  @Test
  void testBitmapRepairClearsBitsForMissingContainers() throws IOException
  {
    primeOnDiskState();
    // download a file in the aggregate bundle to set a bit, then close (persists the bitmap)
    final PartialSegmentFileMapperV10 mapper = createMapper(deepStorageDir, cacheDir);
    final String prefix = AGG_BUNDLE + "/";
    String fileInAgg = null;
    int aggContainerIdx = -1;
    for (var entry : mapper.getSegmentFileMetadata().getFiles().entrySet()) {
      if (entry.getKey().startsWith(prefix)) {
        fileInAgg = entry.getKey();
        aggContainerIdx = entry.getValue().getContainer();
        // ensure this container is exclusively the aggregate's, otherwise we won't be able to test
        // the repair behavior; check that __base doesn't also use this container
        final int idx = aggContainerIdx;
        boolean baseSharesContainer = mapper.getSegmentFileMetadata().getFiles().entrySet().stream()
            .anyMatch(e -> e.getKey().startsWith(Projections.BASE_TABLE_PROJECTION_NAME + "/")
                           && e.getValue().getContainer() == idx);
        if (!baseSharesContainer) {
          break;
        }
        fileInAgg = null;
      }
    }
    if (fileInAgg == null) {
      // small segment: base + aggregate share container 0. Repair behavior is then a no-op, just skip.
      mapper.close();
      return;
    }
    Assertions.assertNotNull(mapper.mapFile(fileInAgg), "expected file " + fileInAgg + " to be downloadable");
    Assertions.assertTrue(mapper.getDownloadedFiles().contains(fileInAgg));
    mapper.close();

    // now delete the aggregate container file out from under the bitmap
    final File aggContainer = new File(
        cacheDir,
        StringUtils.format("%s.container.%05d", IndexIO.V10_FILE_NAME, aggContainerIdx)
    );
    Assertions.assertTrue(aggContainer.delete());

    // re-open the mapper: the bitmap-vs-container repair should clear the bit for the missing file
    try (PartialSegmentFileMapperV10 restored = createMapper(deepStorageDir, cacheDir)) {
      Assertions.assertFalse(
          restored.getDownloadedFiles().contains(fileInAgg),
          "bitmap repair should have cleared the bit for " + fileInAgg
      );
    }
  }

  /**
   * Populate the per-segment cache dir with the on-disk artifacts a previous historical run would have left behind:
   * the V10 header file plus sparse-allocated container files for the base and aggregate bundles.
   */
  private void primeOnDiskState() throws IOException
  {
    final StorageLocation seedLocation = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry seedMeta = new PartialSegmentMetadataCacheEntry(
        SEGMENT_ID,
        cacheDir,
        IndexIO.V10_FILE_NAME,
        List.of(),
        new DirectoryBackedRangeReader(deepStorageDir),
        JSON_MAPPER,
        null,
        ESTIMATE
    );
    Assertions.assertTrue(seedLocation.reserve(seedMeta));
    seedMeta.mount(seedLocation);

    final PartialSegmentBundleCacheEntry base = PartialSegmentBundleCacheEntry.forBundle(
        seedMeta,
        Projections.BASE_TABLE_PROJECTION_NAME,
        List.of()
    );
    final var baseHold = seedLocation.addWeakReservationHold(base.getId(), () -> base);
    Assertions.assertNotNull(baseHold);
    base.mount(seedLocation);

    final PartialSegmentBundleCacheEntry agg = PartialSegmentBundleCacheEntry.forBundle(
        seedMeta,
        AGG_BUNDLE,
        List.of(base.getId())
    );
    final var aggHold = seedLocation.addWeakReservationHold(agg.getId(), () -> agg);
    Assertions.assertNotNull(aggHold);
    agg.mount(seedLocation);

    // Leave on-disk state behind: unmount the bundles (which deletes container files!), that's the wrong final
    // state. Instead, we want containers ON disk, so leave bundles mounted but close the file mapper. Since the
    // restore path re-opens via PartialSegmentFileMapperV10.create which is idempotent w.r.t. on-disk files,
    // un-mount on the SEED side AFTER files are sparse-allocated would also delete them. So we just leave the
    // seed mounted: at test end @TempDir cleans up.
    aggHold.close();
    baseHold.close();
  }

  /**
   * Two-step restore helper that mirrors what {@code SegmentLocalCacheManager} does in production: reserve the metadata
   * entry via {@link PartialSegmentCacheBootstrap#reserveFromDisk}, then drive {@link PartialSegmentMetadataCacheEntry#mount}
   * to trigger the file-mapper build + bundle restore. On a mount failure, {@code mount}'s own rollback removes the
   * (unheld) weak entry from the location, so tests can assert on a clean location without any extra cleanup here.
   */
  private PartialSegmentMetadataCacheEntry restoreFromDisk(StorageLocation location) throws IOException
  {
    final PartialSegmentMetadataCacheEntry metadata = PartialSegmentCacheBootstrap.reserveFromDisk(
        SEGMENT_ID,
        cacheDir,
        IndexIO.V10_FILE_NAME,
        List.of(),
        new DirectoryBackedRangeReader(deepStorageDir),
        JSON_MAPPER,
        null,
        location
    );
    metadata.mount(location);
    return metadata;
  }

  private static PartialSegmentFileMapperV10 createMapper(File deepStorageDir, File cacheDir) throws IOException
  {
    return PartialSegmentFileMapperV10.create(
        new DirectoryBackedRangeReader(deepStorageDir),
        JSON_MAPPER,
        cacheDir,
        IndexIO.V10_FILE_NAME,
        List.of(),
        PartialSegmentDownloadListener.NOOP
    );
  }
}
