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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
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
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
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
import java.util.concurrent.ThreadLocalRandom;

/**
 * Manager-level tests for {@code drop} / {@code removeInfoFile} on partial segments. Partial entries (metadata +
 * bundles) are weak cache entries; their lifecycle is owned by {@link StorageLocation}'s SIEVE + hold-phaser
 * machinery. Coordinator {@code drop} is effectively a no-op for the partial branch — SIEVE drives the actual
 * eviction once holds drop and capacity is needed. {@code removeInfoFile} still wires info-file deletion via the
 * metadata entry's {@link PartialSegmentMetadataCacheEntry#setOnUnmount} hook so the info entry survives until the
 * (now SIEVE-driven) cleanup actually fires.
 * <p>
 * These tests use {@link StorageLocation#setAreWeakEntriesEphemeral} so hold-release triggers immediate eviction,
 * a deterministic substitute for SIEVE's capacity-pressure-driven reclaim.
 */
class SegmentLocalCacheManagerPartialDropTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();
  private static final SegmentId SEGMENT_ID = SegmentId.of("test", Intervals.of("2025/2026"), "v1", 0);
  private static final long ESTIMATE = 16 * 1024 * 1024L;
  private static final String AGG_BUNDLE = "dim1_metric1_sum";

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
  private File infoDir;
  private SegmentLocalCacheManager manager;
  private StorageLocation location;
  private DataSegment dataSegment;

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
  void setup() throws IOException
  {
    cacheDir = new File(perTestTempDir, "cache_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    FileUtils.mkdirp(cacheDir);
    // SegmentLocalCacheManager defaults the info dir to <firstLocation>/info_dir when not configured.
    infoDir = new File(cacheDir, "info_dir");
    FileUtils.mkdirp(infoDir);

    final StorageLocationConfig locConfig = new StorageLocationConfig(cacheDir, ESTIMATE * 16, null);
    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig().setLocations(List.of(locConfig));
    final List<StorageLocation> locations = loaderConfig.toStorageLocations();
    manager = new SegmentLocalCacheManager(
        locations,
        loaderConfig,
        StorageLoadingThreadPool.createFromConfig(loaderConfig),
        new LeastBytesUsedStorageLocationSelectorStrategy(locations),
        TestHelper.getTestIndexIO(JSON_MAPPER, ColumnConfig.DEFAULT),
        JSON_MAPPER
    );
    location = manager.getLocations().get(0);
    // Drive eviction off hold-release so tests don't need to provoke SIEVE via capacity pressure.
    location.setAreWeakEntriesEphemeral(true);
    dataSegment = DataSegment.builder()
                             .dataSource(SEGMENT_ID.getDataSource())
                             .interval(SEGMENT_ID.getInterval())
                             .version(SEGMENT_ID.getVersion())
                             .shardSpec(NoneShardSpec.instance())
                             .size(0)
                             .build();
  }

  /**
   * Pair of a mounted partial metadata entry and the {@link StorageLocation.ReservationHold} the test holds against
   * it; closing the hold (under {@link StorageLocation#setAreWeakEntriesEphemeral} mode) is what triggers SIEVE
   * cleanup.
   */
  private record HeldMetadata(
      PartialSegmentMetadataCacheEntry metadata,
      StorageLocation.ReservationHold<SegmentCacheEntry> hold
  )
  {
  }

  private HeldMetadata registerMountedMetadata()
  {
    final PartialSegmentMetadataCacheEntry metadata = new PartialSegmentMetadataCacheEntry(
        SEGMENT_ID,
        cacheDir,
        IndexIO.V10_FILE_NAME,
        List.of(),
        new DirectoryBackedRangeReader(deepStorageDir),
        JSON_MAPPER,
        null,
        ESTIMATE
    );
    final StorageLocation.ReservationHold<SegmentCacheEntry> hold = location.addWeakReservationHold(
        new SegmentCacheEntryIdentifier(SEGMENT_ID),
        () -> metadata
    );
    Assertions.assertNotNull(hold);
    try {
      metadata.mount(location);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new HeldMetadata(metadata, hold);
  }

  private record HeldBundle(
      PartialSegmentBundleCacheEntry bundle,
      StorageLocation.ReservationHold<SegmentCacheEntry> hold
  )
  {
  }

  private HeldBundle registerMountedBundle(
      PartialSegmentMetadataCacheEntry metadata,
      String bundleName,
      List<PartialSegmentBundleCacheEntryIdentifier> parents
  ) throws IOException
  {
    final PartialSegmentBundleCacheEntry bundle = PartialSegmentBundleCacheEntry.forBundle(metadata, bundleName, parents);
    final StorageLocation.ReservationHold<SegmentCacheEntry> hold =
        location.addWeakReservationHold(bundle.getId(), () -> bundle);
    Assertions.assertNotNull(hold);
    bundle.mount(location);
    return new HeldBundle(bundle, hold);
  }

  @Test
  void testSieveEvictionCascadesThroughBundlesAndMetadata() throws IOException
  {
    final HeldMetadata held = registerMountedMetadata();
    final HeldBundle base = registerMountedBundle(
        held.metadata,
        Projections.BASE_TABLE_PROJECTION_NAME,
        List.of()
    );
    final HeldBundle agg = registerMountedBundle(held.metadata, AGG_BUNDLE, List.of(base.bundle.getId()));
    Assertions.assertEquals(2, held.metadata.snapshotLinkedBundles().size());

    final File headerFile = new File(cacheDir, IndexIO.V10_FILE_NAME + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX);
    Assertions.assertTrue(headerFile.exists(), "header file must exist before eviction");

    // Coordinator drop is a no-op for the partial path — entries linger until SIEVE picks them.
    manager.drop(dataSegment);
    Assertions.assertTrue(held.metadata.isMounted(), "drop alone must not unmount weak partial metadata");
    Assertions.assertTrue(headerFile.exists(), "drop alone must not delete the header file");

    // Release dependent (agg → base → metadata) holds in order. Each release triggers ephemeral-mode eviction of
    // the corresponding entry, which releases that entry's parent holds and metadata reference; once the metadata
    // hold itself is closed and the last reference drops, the file mapper closes and the header is deleted.
    agg.hold.close();
    Assertions.assertFalse(agg.bundle.isMounted(), "agg bundle should unmount once its hold releases");
    base.hold.close();
    Assertions.assertFalse(base.bundle.isMounted(), "base bundle should unmount once its hold releases (agg dropped its parent hold)");
    held.hold.close();
    Assertions.assertFalse(held.metadata.isMounted(), "metadata should unmount once its hold releases and all bundle references are gone");
    Assertions.assertFalse(headerFile.exists(), "header file should be deleted as part of metadata cleanup");
    Assertions.assertTrue(held.metadata.snapshotLinkedBundles().isEmpty(), "metadata should have no linked bundles");
    Assertions.assertFalse(location.isWeakReserved(new SegmentCacheEntryIdentifier(SEGMENT_ID)));
  }

  @Test
  void testRemoveInfoFileDefersInfoDeleteUntilSieveEviction() throws IOException
  {
    final HeldMetadata held = registerMountedMetadata();
    final HeldBundle base = registerMountedBundle(held.metadata, Projections.BASE_TABLE_PROJECTION_NAME, List.of());

    // Simulate the info file that the manager writes when a segment is loaded.
    final File infoFile = new File(infoDir, SEGMENT_ID.toString());
    Assertions.assertTrue(infoFile.createNewFile());

    manager.removeInfoFile(dataSegment);
    Assertions.assertTrue(
        infoFile.exists(),
        "removeInfoFile must defer the actual delete until the partial entry's cleanup runs"
    );

    // Cascading release triggers ephemeral-mode eviction; the onUnmount hook wired by removeInfoFile fires as part
    // of metadata cleanup.
    base.hold.close();
    held.hold.close();
    Assertions.assertFalse(held.metadata.isMounted());
    Assertions.assertFalse(infoFile.exists(), "info file should be deleted as part of metadata cleanup");
  }

  @Test
  void testRemoveInfoFileWithNoCachedEntryDeletesImmediately() throws IOException
  {
    // No registered entry, manager's removeInfoFile fallback should delete on the spot.
    final File infoFile = new File(infoDir, SEGMENT_ID.toString());
    Assertions.assertTrue(infoFile.createNewFile());

    manager.removeInfoFile(dataSegment);
    Assertions.assertFalse(infoFile.exists(), "with no cache entry, removeInfoFile should delete immediately");
  }

  @Test
  void testDropWithNoEntryIsNoOp()
  {
    // Nothing registered for this segment; drop should not throw.
    manager.drop(dataSegment);
  }
}
