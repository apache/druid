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
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
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
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.segment.file.SegmentFileBuilder;
import org.apache.druid.segment.file.SegmentFileBuilderV10;
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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class PartialSegmentBundleCacheEntryTest
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

  private static File segmentDir;

  @TempDir
  File perTestTempDir;

  private File cacheDir;
  private File deepStorageDir;

  @BeforeAll
  static void buildSegment()
  {
    final File tmp = new File(sharedTempDir, "build_" + ThreadLocalRandom.current().nextInt());
    segmentDir = IndexBuilder.create()
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
    deepStorageDir = segmentDir;
    cacheDir = new File(perTestTempDir, "cache_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    FileUtils.mkdirp(cacheDir);
  }

  @Test
  void testForBundleDerivesContainerIndicesAndSize() throws IOException
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = newMetadataEntry();
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);

    final PartialSegmentBundleCacheEntry baseEntry = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        Projections.BASE_TABLE_PROJECTION_NAME,
        List.of()
    );
    Assertions.assertFalse(baseEntry.getContainerRefs().isEmpty());
    Assertions.assertTrue(baseEntry.getSize() > 0);
    Assertions.assertEquals(SEGMENT_ID, baseEntry.getSegmentId());
    Assertions.assertEquals(Projections.BASE_TABLE_PROJECTION_NAME, baseEntry.getBundleName());
  }

  @Test
  void testForBundleFailsIfMetadataNotMounted()
  {
    final PartialSegmentMetadataCacheEntry metadata = newMetadataEntry();
    Assertions.assertThrows(
        DruidException.class,
        () -> PartialSegmentBundleCacheEntry.forBundle(metadata, Projections.BASE_TABLE_PROJECTION_NAME, List.of())
    );
  }

  @Test
  void testForBundleFailsIfBundleUnknown() throws IOException
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = newMetadataEntry();
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);

    Assertions.assertThrows(
        DruidException.class,
        () -> PartialSegmentBundleCacheEntry.forBundle(metadata, "no_such_bundle", List.of())
    );
  }

  @Test
  void testMountSparseAllocatesContainerFiles() throws IOException
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = newMetadataEntry();
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);

    final PartialSegmentBundleCacheEntry baseEntry = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        Projections.BASE_TABLE_PROJECTION_NAME,
        List.of()
    );
    Assertions.assertNotNull(location.addWeakReservationHold(baseEntry.getId(), () -> baseEntry));
    baseEntry.mount(location);

    Assertions.assertTrue(baseEntry.isMounted());
    for (PartialSegmentBundleCacheEntry.BundleContainerRef ref : baseEntry.getContainerRefs()) {
      final String mapperFilename =
          ref.externalFilename() != null ? ref.externalFilename() : IndexIO.V10_FILE_NAME;
      final File containerFile = new File(
          cacheDir,
          StringUtils.format("%s.container.%05d", mapperFilename, ref.containerIndex())
      );
      Assertions.assertTrue(containerFile.exists(), "container " + ref + " should be sparse-allocated");
    }
  }

  @Test
  void testMountAcquiresParentHoldsForAggregateBundle() throws IOException
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = newMetadataEntry();
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);

    final PartialSegmentBundleCacheEntry baseEntry = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        Projections.BASE_TABLE_PROJECTION_NAME,
        List.of()
    );
    final var baseHold = location.addWeakReservationHold(baseEntry.getId(), () -> baseEntry);
    Assertions.assertNotNull(baseHold);
    baseEntry.mount(location);
    // close the bootstrap hold so cache could in principle evict, but the aggregate's transitive hold should keep it
    baseHold.close();

    final PartialSegmentBundleCacheEntry aggEntry = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        AGG_BUNDLE,
        List.of(baseEntry.getId())
    );
    final var aggHold = location.addWeakReservationHold(aggEntry.getId(), () -> aggEntry);
    Assertions.assertNotNull(aggHold);
    aggEntry.mount(location);

    // base must still be held by the aggregate's transitive hold; trying to reclaim its bytes should fail
    final long baseSize = baseEntry.getSize();
    Assertions.assertTrue(
        location.currentSizeBytes() >= baseSize,
        "base entry size should remain charged to the location while held by the aggregate"
    );

    // unmounting the aggregate releases the parent hold; base is then evictable
    aggHold.close();
    aggEntry.unmount();
  }

  @Test
  void testRuntimeAcquireOfProjectionBundleMountsParentBaseFirst() throws IOException
  {
    // Runtime acquire path (the cursor factory's PartialBundleAcquirer): acquiring a projection bundle must first
    // mount its parent __base bundle, since the projection bundle's mount() takes holds/references on its parents and
    // fails with "parent entry not registered" otherwise. Unlike the bootstrap restore path, the runtime path reaches
    // the projection bundle directly with no __base mounted, so the acquirer is responsible for the ordering.
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = newMetadataEntry();
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);

    final PartialSegmentBundleCacheEntryIdentifier baseId =
        new PartialSegmentBundleCacheEntryIdentifier(SEGMENT_ID, Projections.BASE_TABLE_PROJECTION_NAME);
    final PartialSegmentBundleCacheEntryIdentifier aggId =
        new PartialSegmentBundleCacheEntryIdentifier(SEGMENT_ID, AGG_BUNDLE);

    // Precondition: nothing has mounted __base yet; the acquirer must do it as the projection bundle's parent.
    Assertions.assertNull(location.getCacheEntry(baseId), "test setup: __base must not be mounted yet");

    final Closeable handle = metadata.getBundleAcquirer().acquire(AGG_BUNDLE);
    try {
      final PartialSegmentBundleCacheEntry agg = location.getCacheEntry(aggId);
      final PartialSegmentBundleCacheEntry base = location.getCacheEntry(baseId);
      Assertions.assertNotNull(agg, "projection bundle should be registered");
      Assertions.assertTrue(agg.isMounted(), "projection bundle should be mounted");
      Assertions.assertNotNull(base, "parent __base bundle should be auto-registered by the acquirer");
      Assertions.assertTrue(base.isMounted(), "parent __base bundle should be auto-mounted by the acquirer");
    }
    finally {
      handle.close();
    }
  }

  @Test
  void testMountFailsIfMetadataNotRegisteredWithLocation() throws IOException
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = newMetadataEntry();
    // mount metadata standalone without registering with the location, so no hold can be acquired below
    final File anotherDir = new File(perTestTempDir, "adhoc");
    FileUtils.mkdirp(anotherDir);
    final StorageLocation otherLocation = new StorageLocation(anotherDir, ESTIMATE * 8, null);
    Assertions.assertTrue(otherLocation.reserve(metadata));
    metadata.mount(otherLocation);

    final PartialSegmentBundleCacheEntry baseEntry = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        Projections.BASE_TABLE_PROJECTION_NAME,
        List.of()
    );

    Assertions.assertThrows(DruidException.class, () -> baseEntry.mount(location));
    Assertions.assertFalse(baseEntry.isMounted());
  }

  @Test
  void testMountIsIdempotent() throws IOException
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = newMetadataEntry();
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);

    final PartialSegmentBundleCacheEntry baseEntry = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        Projections.BASE_TABLE_PROJECTION_NAME,
        List.of()
    );
    Assertions.assertNotNull(location.addWeakReservationHold(baseEntry.getId(), () -> baseEntry));

    baseEntry.mount(location);
    Assertions.assertTrue(baseEntry.isMounted());
    baseEntry.mount(location);
    Assertions.assertTrue(baseEntry.isMounted());
  }

  @Test
  void testUnmountEvictsContainersAndAllowsRemount() throws IOException
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = newMetadataEntry();
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);

    final PartialSegmentBundleCacheEntry baseEntry = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        Projections.BASE_TABLE_PROJECTION_NAME,
        List.of()
    );
    Assertions.assertNotNull(location.addWeakReservationHold(baseEntry.getId(), () -> baseEntry));
    baseEntry.mount(location);

    // download a file so the bitmap has something set, then verify unmount clears the container file
    final PartialSegmentFileMapperV10 mapper = metadata.getFileMapper();
    Assertions.assertNotNull(mapper);
    final String anyFile = mapper.getInternalFilenames().stream().findFirst().orElseThrow();
    mapper.fetchFiles(Set.of(anyFile));
    Assertions.assertNotNull(mapper.mapFile(anyFile));
    Assertions.assertEquals(1, mapper.getDownloadedFiles().size());
    final PartialSegmentBundleCacheEntry.BundleContainerRef evictedRef = baseEntry.getContainerRefs().getFirst();
    final String evictedMapperFilename =
        evictedRef.externalFilename() != null ? evictedRef.externalFilename() : IndexIO.V10_FILE_NAME;
    final File evictedFile = new File(
        cacheDir,
        StringUtils.format("%s.container.%05d", evictedMapperFilename, evictedRef.containerIndex())
    );
    Assertions.assertTrue(evictedFile.exists());

    baseEntry.unmount();

    Assertions.assertFalse(baseEntry.isMounted());
    Assertions.assertFalse(evictedFile.exists(), "container file should be deleted on unmount");

    // remount works (e.g. after cache eviction + re-acquire)
    Assertions.assertNotNull(location.addWeakReservationHold(baseEntry.getId(), () -> baseEntry));
    baseEntry.mount(location);
    Assertions.assertTrue(baseEntry.isMounted());
  }

  @Test
  void testConcurrentMountIsDeduplicated() throws Exception
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = newMetadataEntry();
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);

    final PartialSegmentBundleCacheEntry baseEntry = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        Projections.BASE_TABLE_PROJECTION_NAME,
        List.of()
    );
    Assertions.assertNotNull(location.addWeakReservationHold(baseEntry.getId(), () -> baseEntry));

    final int threads = 8;
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);
    final AtomicInteger errors = new AtomicInteger();
    final ExecutorService exec = Execs.multiThreaded(threads, "partial-segment-tests-%d");
    try {
      for (int i = 0; i < threads; i++) {
        exec.submit(() -> {
          try {
            start.await();
            baseEntry.mount(location);
          }
          catch (Throwable t) {
            errors.incrementAndGet();
          }
          finally {
            done.countDown();
          }
        });
      }
      start.countDown();
      Assertions.assertTrue(done.await(30, TimeUnit.SECONDS));
      Assertions.assertEquals(0, errors.get());
      Assertions.assertTrue(baseEntry.isMounted());
    }
    finally {
      exec.shutdownNow();
    }
  }

  @Test
  void testFailedMountClearsGateForRetry() throws IOException
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = newMetadataEntry();
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);

    // ask for an aggregate entry but pass a parent that was never registered -> mount should fail
    final PartialSegmentBundleCacheEntry agg = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        AGG_BUNDLE,
        List.of(new PartialSegmentBundleCacheEntryIdentifier(SEGMENT_ID, "ghost"))
    );
    Assertions.assertNotNull(location.addWeakReservationHold(agg.getId(), () -> agg));
    Assertions.assertThrows(DruidException.class, () -> agg.mount(location));
    Assertions.assertFalse(agg.isMounted());

    // a subsequent retry with a valid parent should succeed (gate must have been cleared)
    final PartialSegmentBundleCacheEntry baseEntry = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        Projections.BASE_TABLE_PROJECTION_NAME,
        List.of()
    );
    Assertions.assertNotNull(location.addWeakReservationHold(baseEntry.getId(), () -> baseEntry));
    baseEntry.mount(location);

    final PartialSegmentBundleCacheEntry retry = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        AGG_BUNDLE,
        List.of(baseEntry.getId())
    );
    Assertions.assertNotNull(location.addWeakReservationHold(retry.getId(), () -> retry));
    retry.mount(location);
    Assertions.assertTrue(retry.isMounted());
  }

  @Test
  void testUnmountDefersContainerEvictionWhileReferenceHeld() throws Exception
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = newMetadataEntry();
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);

    final PartialSegmentBundleCacheEntry base = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        Projections.BASE_TABLE_PROJECTION_NAME,
        List.of()
    );
    Assertions.assertNotNull(location.addWeakReservationHold(base.getId(), () -> base));
    base.mount(location);

    // download a file so its container is materialized on disk
    final PartialSegmentFileMapperV10 mapper = metadata.getFileMapper();
    Assertions.assertNotNull(mapper);
    final String anyFile = mapper.getInternalFilenames().stream().findFirst().orElseThrow();
    mapper.fetchFiles(Set.of(anyFile));
    Assertions.assertNotNull(mapper.mapFile(anyFile));
    final PartialSegmentBundleCacheEntry.BundleContainerRef containerRef = base.getContainerRefs().getFirst();
    final String mapperFilename =
        containerRef.externalFilename() != null ? containerRef.externalFilename() : IndexIO.V10_FILE_NAME;
    final File containerFile = new File(
        cacheDir,
        StringUtils.format("%s.container.%05d", mapperFilename, containerRef.containerIndex())
    );
    Assertions.assertTrue(containerFile.exists());

    final Closeable ref = base.acquireReference();
    base.unmount();
    Assertions.assertTrue(
        containerFile.exists(),
        "container file should persist while a reference is held, even after unmount"
    );
    Assertions.assertTrue(base.isMounted(), "bundle should not have been cleaned up while reference is held");

    ref.close();
    Assertions.assertFalse(containerFile.exists(), "container file should be deleted after last reference releases");
    Assertions.assertFalse(base.isMounted());
  }

  @Test
  void testForBundleAcceptsBundleNameContainingSlash() throws IOException
  {
    // Bundle names are matched by exact equality against the container's explicit bundle field, so names with '/'
    // are unambiguous. forBundle should accept them as long as a container exists with that exact bundle name.
    // Build a V10 segment with a slashy bundle name and verify the cache layer attributes containers correctly.
    final File deepDir = writeSlashyGroupSegment("nested/group");
    final File cache = new File(perTestTempDir, "slashy_cache_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    FileUtils.mkdirp(cache);
    final StorageLocation location = new StorageLocation(cache, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = new PartialSegmentMetadataCacheEntry(
        SEGMENT_ID,
        cache,
        IndexIO.V10_FILE_NAME,
        List.of(),
        new DirectoryBackedRangeReader(deepDir),
        JSON_MAPPER,
        null,
        ESTIMATE,
        PartialSegmentFileMapperV10.DEFAULT_COALESCE_GAP_BYTES,
        PartialSegmentFileMapperV10.DEFAULT_MAX_FETCH_RUN_BYTES
    );
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);

    final PartialSegmentBundleCacheEntry bundle = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        "nested/group",
        List.of()
    );
    Assertions.assertEquals("nested/group", bundle.getBundleName());
    Assertions.assertFalse(bundle.getContainerRefs().isEmpty());
  }

  @Test
  void testForBundleSpansMainAndExternalContainers() throws IOException
  {
    // Bundle "proj1" lives in BOTH the main file and an external file. forBundle should pick up containers from
    // both via the explicit bundle field, producing a single logical bundle spanning multiple physical files.
    final String externalName = "ext.segment";
    final File deepDir = new File(perTestTempDir, "multi_deep_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    FileUtils.mkdirp(deepDir);
    try (SegmentFileBuilderV10 builder =
             SegmentFileBuilderV10.create(JSON_MAPPER, deepDir)) {
      // Attach the external builder BEFORE startFileBundle so the group propagates to it.
      final org.apache.druid.segment.file.SegmentFileBuilder external = builder.getExternalBuilder(externalName);
      builder.startFileBundle("proj1");

      final File mainTmp = new File(perTestTempDir, "main-col.bin");
      Files.write(Ints.toByteArray(1), mainTmp);
      builder.add("proj1/main_col", mainTmp);

      final File extTmp = new File(perTestTempDir, "ext-col.bin");
      Files.write(Ints.toByteArray(2), extTmp);
      external.add("proj1/ext_col", extTmp);
    }

    final File cache = new File(perTestTempDir, "multi_cache_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    FileUtils.mkdirp(cache);
    final StorageLocation location = new StorageLocation(cache, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = new PartialSegmentMetadataCacheEntry(
        SEGMENT_ID,
        cache,
        IndexIO.V10_FILE_NAME,
        List.of(externalName),
        new DirectoryBackedRangeReader(deepDir),
        JSON_MAPPER,
        null,
        ESTIMATE,
        PartialSegmentFileMapperV10.DEFAULT_COALESCE_GAP_BYTES,
        PartialSegmentFileMapperV10.DEFAULT_MAX_FETCH_RUN_BYTES
    );
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);

    final PartialSegmentBundleCacheEntry bundle =
        PartialSegmentBundleCacheEntry.forBundle(metadata, "proj1", List.of());
    // Expect exactly two refs: one in main (externalFilename == null), one in external.
    Assertions.assertEquals(2, bundle.getContainerRefs().size());
    final long mainRefCount = bundle.getContainerRefs().stream()
                                    .filter(r -> r.externalFilename() == null).count();
    final long extRefCount = bundle.getContainerRefs().stream()
                                   .filter(r -> externalName.equals(r.externalFilename())).count();
    Assertions.assertEquals(1, mainRefCount, "expected one main-file container ref");
    Assertions.assertEquals(1, extRefCount, "expected one external-file container ref");

    // Mount the bundle and verify both containers are sparse-allocated under their respective targetFilenames.
    Assertions.assertNotNull(location.addWeakReservationHold(bundle.getId(), () -> bundle));
    bundle.mount(location);
    for (PartialSegmentBundleCacheEntry.BundleContainerRef ref : bundle.getContainerRefs()) {
      final String mf = ref.externalFilename() != null ? ref.externalFilename() : IndexIO.V10_FILE_NAME;
      final File cf = new File(cache, StringUtils.format("%s.container.%05d", mf, ref.containerIndex()));
      Assertions.assertTrue(cf.exists(), "expected container file " + cf);
    }
  }

  @Test
  void testForBundleRootOwnsAllUngroupedContainers() throws IOException
  {
    // A V10 segment written without any startFileBundle calls produces containers tagged with ROOT_BUNDLE_NAME.
    // forBundle(ROOT_BUNDLE_NAME) must own every such container.
    final File deepDir = writeRootOnlySegment();
    final File cache = new File(perTestTempDir, "root_cache_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    FileUtils.mkdirp(cache);
    final StorageLocation location = new StorageLocation(cache, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = new PartialSegmentMetadataCacheEntry(
        SEGMENT_ID,
        cache,
        IndexIO.V10_FILE_NAME,
        List.of(),
        new DirectoryBackedRangeReader(deepDir),
        JSON_MAPPER,
        null,
        ESTIMATE,
        PartialSegmentFileMapperV10.DEFAULT_COALESCE_GAP_BYTES,
        PartialSegmentFileMapperV10.DEFAULT_MAX_FETCH_RUN_BYTES
    );
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);

    final PartialSegmentBundleCacheEntry root = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        SegmentFileBuilder.ROOT_BUNDLE_NAME,
        List.of()
    );
    Assertions.assertEquals(
        metadata.getSegmentFileMetadata().getContainers().size(),
        root.getContainerRefs().size(),
        "root bundle should own every container in a no-startFileBundle segment"
    );
  }

  @Test
  void testResolveBundleNameFallsBackToRootWhenRootIsSoleBundle() throws IOException
  {
    // A segment whose every container defaulted to the root bundle (legacy / never called startFileBundle).
    final File deepDir = writeRootOnlySegment();
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = new PartialSegmentMetadataCacheEntry(
        SEGMENT_ID,
        cacheDir,
        IndexIO.V10_FILE_NAME,
        List.of(),
        new DirectoryBackedRangeReader(deepDir),
        JSON_MAPPER,
        null,
        ESTIMATE,
        PartialSegmentFileMapperV10.DEFAULT_COALESCE_GAP_BYTES,
        PartialSegmentFileMapperV10.DEFAULT_MAX_FETCH_RUN_BYTES
    );
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);
    final PartialSegmentFileMapperV10 mapper = metadata.getFileMapper();

    Assertions.assertEquals(
        Set.of(SegmentFileBuilder.ROOT_BUNDLE_NAME),
        PartialSegmentBundleCacheEntry.bundleNames(mapper),
        "an untagged segment has only the root bundle"
    );
    // A request for the base bundle (or any projection) resolves to the root catch-all, since root is the sole bundle.
    Assertions.assertEquals(
        SegmentFileBuilder.ROOT_BUNDLE_NAME,
        PartialSegmentBundleCacheEntry.resolveBundleName(mapper, Projections.BASE_TABLE_PROJECTION_NAME)
    );
    Assertions.assertEquals(
        SegmentFileBuilder.ROOT_BUNDLE_NAME,
        PartialSegmentBundleCacheEntry.resolveBundleName(mapper, "some_projection")
    );
    // A request for the root bundle by name resolves to itself (it exists; no fallback logic needed).
    Assertions.assertEquals(
        SegmentFileBuilder.ROOT_BUNDLE_NAME,
        PartialSegmentBundleCacheEntry.resolveBundleName(mapper, SegmentFileBuilder.ROOT_BUNDLE_NAME)
    );
  }

  @Test
  void testResolveBundleNameNoFallbackWhenNamedBundlesPresent() throws IOException
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = newMetadataEntry();
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);
    final PartialSegmentFileMapperV10 mapper = metadata.getFileMapper();

    final Set<String> present = PartialSegmentBundleCacheEntry.bundleNames(mapper);
    Assertions.assertTrue(present.contains(Projections.BASE_TABLE_PROJECTION_NAME));
    Assertions.assertTrue(present.contains(AGG_BUNDLE));
    Assertions.assertFalse(present.contains(SegmentFileBuilder.ROOT_BUNDLE_NAME), "a tagged segment has no root bundle");

    // Present bundles resolve to themselves.
    Assertions.assertEquals(
        Projections.BASE_TABLE_PROJECTION_NAME,
        PartialSegmentBundleCacheEntry.resolveBundleName(mapper, Projections.BASE_TABLE_PROJECTION_NAME)
    );
    Assertions.assertEquals(AGG_BUNDLE, PartialSegmentBundleCacheEntry.resolveBundleName(mapper, AGG_BUNDLE));
    // An absent request does NOT fall back to root when named bundles are present: the name passes through unchanged
    // so the downstream forBundle fails loudly rather than silently serving the wrong data.
    Assertions.assertEquals(
        "does_not_exist",
        PartialSegmentBundleCacheEntry.resolveBundleName(mapper, "does_not_exist")
    );
  }

  @Test
  void testMountRollsBackIfEntryNoLongerWeakReservedAtLocation() throws Exception
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    // ephemeral mode: when a hold drops and no others remain, the weak entry is evicted from weakCacheEntries
    // immediately. Lets us simulate the race where the bundle's reservation goes away before mount() finishes.
    location.setAreWeakEntriesEphemeral(true);

    final PartialSegmentMetadataCacheEntry metadata = newMetadataEntry();
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);

    final PartialSegmentBundleCacheEntry base = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        Projections.BASE_TABLE_PROJECTION_NAME,
        List.of()
    );

    // Reserve + immediately release the bootstrap hold. In ephemeral mode this evicts the bundle from
    // weakCacheEntries. The bundle itself was never mounted, so its `mounted` flag is still false.
    try (StorageLocation.ReservationHold<?> hold = location.addWeakReservationHold(base.getId(), () -> base)) {
      Assertions.assertNotNull(hold);
    }
    Assertions.assertFalse(location.isWeakReserved(base.getId()), "ephemeral release should have evicted");

    // Mount without re-reserving. doMount's work succeeds (parents are fine, containers sparse-allocate), but the
    // post-mount check must detect that this entry is no longer in the location's weak map and roll back.
    base.mount(location);
    Assertions.assertFalse(
        base.isMounted(),
        "post-mount check should roll back when entry was evicted from the location during mount"
    );
  }

  @Test
  void testAcquireReferenceBeforeMountThrows() throws IOException
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = newMetadataEntry();
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);
    final PartialSegmentBundleCacheEntry base = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        Projections.BASE_TABLE_PROJECTION_NAME,
        List.of()
    );
    Assertions.assertThrows(DruidException.class, base::acquireReference);
  }

  @Test
  void testAggregateBundleHoldsReferenceOnBaseAndMetadata() throws Exception
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = newMetadataEntry();
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);

    final PartialSegmentBundleCacheEntry base = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        Projections.BASE_TABLE_PROJECTION_NAME,
        List.of()
    );
    Assertions.assertNotNull(location.addWeakReservationHold(base.getId(), () -> base));
    base.mount(location);

    final PartialSegmentBundleCacheEntry agg = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        AGG_BUNDLE,
        List.of(base.getId())
    );
    Assertions.assertNotNull(location.addWeakReservationHold(agg.getId(), () -> agg));
    agg.mount(location);

    // Unmount metadata while bundles are still mounted (and holding references on it). Metadata's actual cleanup
    // must defer until the bundles' references on metadata are released.
    final File headerFile = new File(
        cacheDir,
        IndexIO.V10_FILE_NAME + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX
    );
    Assertions.assertTrue(headerFile.exists());
    metadata.unmount();
    Assertions.assertTrue(headerFile.exists(), "metadata header must persist while bundles reference it");
    Assertions.assertTrue(metadata.isMounted(), "metadata must stay mounted while bundles reference it");

    // Unmount base. Cleanup is deferred because agg still references it.
    base.unmount();
    Assertions.assertTrue(base.isMounted(), "base must stay mounted while agg references it");
    Assertions.assertTrue(headerFile.exists(), "metadata still alive via agg's indirect chain");

    // Unmount agg. Its cleanup fires (releasing references on base + metadata), which cascades base's cleanup
    // (releasing its remaining reference on metadata), which finally fires metadata's cleanup.
    agg.unmount();
    Assertions.assertFalse(agg.isMounted());
    Assertions.assertFalse(base.isMounted());
    Assertions.assertFalse(metadata.isMounted());
    Assertions.assertFalse(headerFile.exists(), "metadata header deleted after full cascade");
  }

  @Test
  void testMountedBundleIsLinkedToMetadata() throws IOException
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = newMetadataEntry();
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);

    Assertions.assertTrue(metadata.snapshotLinkedBundles().isEmpty());

    final PartialSegmentBundleCacheEntry base = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        Projections.BASE_TABLE_PROJECTION_NAME,
        List.of()
    );
    Assertions.assertNotNull(location.addWeakReservationHold(base.getId(), () -> base));
    base.mount(location);

    Assertions.assertEquals(1, metadata.snapshotLinkedBundles().size());
    Assertions.assertTrue(metadata.snapshotLinkedBundles().contains(base));

    final PartialSegmentBundleCacheEntry agg = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        AGG_BUNDLE,
        List.of(base.getId())
    );
    Assertions.assertNotNull(location.addWeakReservationHold(agg.getId(), () -> agg));
    agg.mount(location);

    Assertions.assertEquals(2, metadata.snapshotLinkedBundles().size());

    // Unmounting base while agg still holds a dependency reference on it must NOT actually clean up base. agg's
    // ref keeps base alive (deferred cleanup). Both remain in the linked set.
    base.unmount();
    Assertions.assertTrue(
        metadata.snapshotLinkedBundles().contains(base),
        "base must stay linked while agg holds its reference"
    );
    Assertions.assertTrue(metadata.snapshotLinkedBundles().contains(agg));

    // Unmounting agg releases its reference on base; base's deferred cleanup then cascades on the same thread.
    agg.unmount();
    Assertions.assertTrue(metadata.snapshotLinkedBundles().isEmpty());
  }

  @Test
  void testFailedMountDoesNotLeaveDanglingLink() throws IOException
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = newMetadataEntry();
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);

    final PartialSegmentBundleCacheEntry agg = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        AGG_BUNDLE,
        List.of(new PartialSegmentBundleCacheEntryIdentifier(SEGMENT_ID, "ghost"))
    );
    Assertions.assertNotNull(location.addWeakReservationHold(agg.getId(), () -> agg));
    Assertions.assertThrows(DruidException.class, () -> agg.mount(location));

    Assertions.assertTrue(metadata.snapshotLinkedBundles().isEmpty(), "failed mount must not register a link");
  }

  @Test
  void testUnmountIsIdempotent() throws IOException
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 8, null);
    final PartialSegmentMetadataCacheEntry metadata = newMetadataEntry();
    Assertions.assertTrue(location.reserve(metadata));
    metadata.mount(location);

    final PartialSegmentBundleCacheEntry baseEntry = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        Projections.BASE_TABLE_PROJECTION_NAME,
        List.of()
    );
    Assertions.assertNotNull(location.addWeakReservationHold(baseEntry.getId(), () -> baseEntry));
    baseEntry.mount(location);
    baseEntry.unmount();
    baseEntry.unmount(); // no-op
  }

  private PartialSegmentMetadataCacheEntry newMetadataEntry()
  {
    return new PartialSegmentMetadataCacheEntry(
        SEGMENT_ID,
        cacheDir,
        IndexIO.V10_FILE_NAME,
        List.of(),
        new DirectoryBackedRangeReader(deepStorageDir),
        JSON_MAPPER,
        null,
        ESTIMATE,
        PartialSegmentFileMapperV10.DEFAULT_COALESCE_GAP_BYTES,
        PartialSegmentFileMapperV10.DEFAULT_MAX_FETCH_RUN_BYTES
    );
  }

  /**
   * Build a small V10 segment using {@link SegmentFileBuilderV10} directly (i.e.,
   * without going through IndexMergerV10) with a single bundle whose name contains a {@code /}. Used to verify the
   * cache layer attributes containers via the explicit {@code bundle} field and tolerates slashy names.
   */
  private File writeSlashyGroupSegment(String groupName) throws IOException
  {
    final File deepDir = new File(perTestTempDir, "slashy_deep_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    FileUtils.mkdirp(deepDir);
    try (SegmentFileBuilderV10 builder =
             SegmentFileBuilderV10.create(JSON_MAPPER, deepDir)) {
      builder.startFileBundle(groupName);
      for (int i = 0; i < 3; i++) {
        final File tmp = new File(perTestTempDir, "slashy-col" + i + ".bin");
        Files.write(Ints.toByteArray(i), tmp);
        builder.add(groupName + "/col" + i, tmp);
      }
    }
    return deepDir;
  }

  /**
   * Build a small V10 segment using {@link SegmentFileBuilderV10} directly with NO
   * {@code startFileBundle} calls. Containers default to {@link SegmentFileBuilder#ROOT_BUNDLE_NAME}, simulating an
   * older unnamed segment whose containers all live under the root bundle.
   */
  private File writeRootOnlySegment() throws IOException
  {
    final File deepDir = new File(perTestTempDir, "root_deep_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE));
    FileUtils.mkdirp(deepDir);
    try (SegmentFileBuilderV10 builder =
             SegmentFileBuilderV10.create(JSON_MAPPER, deepDir)) {
      // Never call startFileBundle; all writes default to the root bundle.
      for (int i = 0; i < 3; i++) {
        final File tmp = new File(perTestTempDir, "root-col" + i + ".bin");
        Files.write(Ints.toByteArray(i), tmp);
        builder.add("col" + i, tmp);
      }
    }
    return deepDir;
  }

}
