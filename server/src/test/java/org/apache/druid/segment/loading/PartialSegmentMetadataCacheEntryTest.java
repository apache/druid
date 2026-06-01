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
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.file.CountingRangeReader;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.segment.file.SegmentFileBuilderV10;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.timeline.SegmentId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class PartialSegmentMetadataCacheEntryTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();
  private static final SegmentId SEGMENT_ID = SegmentId.of("test", Intervals.of("2025/2026"), "v1", 0);
  private static final long ESTIMATE = 16 * 1024 * 1024L;

  @TempDir
  File tempDir;

  private File segmentFile;
  private File cacheDir;

  @BeforeEach
  void setup() throws IOException
  {
    segmentFile = buildTestSegment(20);
    cacheDir = new File(tempDir, "cache");
    FileUtils.mkdirp(cacheDir);
  }

  @Test
  void testMountFetchesHeaderAndShrinksReservation() throws IOException
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 4, null);
    final PartialSegmentMetadataCacheEntry entry = newEntry(ESTIMATE);
    Assertions.assertTrue(location.reserve(entry));
    Assertions.assertEquals(ESTIMATE, entry.getSize());
    Assertions.assertEquals(ESTIMATE, location.currentSizeBytes());

    entry.mount(location);

    Assertions.assertTrue(entry.isMounted());
    Assertions.assertNotNull(entry.getFileMapper());
    Assertions.assertNotNull(entry.getSegmentFileMetadata());
    final long actualSize = entry.getSize();
    Assertions.assertTrue(actualSize > 0 && actualSize < ESTIMATE, "expected shrink, got " + actualSize);
    Assertions.assertEquals(actualSize, location.currentSizeBytes());

    final File headerFile = new File(cacheDir, IndexIO.V10_FILE_NAME + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX);
    Assertions.assertTrue(headerFile.exists());
    Assertions.assertEquals(headerFile.length(), actualSize);
  }

  @Test
  void testMountFailsWhenActualExceedsEstimate()
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 4, null);
    // estimate of 8 bytes is way too small for any real V10 header
    final PartialSegmentMetadataCacheEntry entry = newEntry(8);
    Assertions.assertTrue(location.reserve(entry));

    final DruidException thrown = Assertions.assertThrows(
        DruidException.class,
        () -> entry.mount(location)
    );
    Assertions.assertTrue(
        thrown.getMessage().contains("virtualStorageMetadataReservationEstimate"),
        "expected operator-facing config hint, got: " + thrown.getMessage()
    );
    Assertions.assertFalse(entry.isMounted());
    Assertions.assertNull(entry.getFileMapper());
    // reservation accounting is unchanged
    Assertions.assertEquals(8, entry.getSize());
    Assertions.assertEquals(8, location.currentSizeBytes());
    // mount failure must delete the on-disk header so a retry starts clean (matches eager SegmentCacheEntry behavior)
    final File headerFile = new File(cacheDir, IndexIO.V10_FILE_NAME + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX);
    Assertions.assertFalse(headerFile.exists(), "mount failure must delete the on-disk header file");
  }

  @Test
  void testMountIsIdempotentInSameLocation() throws IOException
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 4, null);
    final PartialSegmentMetadataCacheEntry entry = newEntry(ESTIMATE);
    Assertions.assertTrue(location.reserve(entry));

    entry.mount(location);
    final PartialSegmentFileMapperV10 firstMapper = entry.getFileMapper();
    Assertions.assertNotNull(firstMapper);

    entry.mount(location);
    Assertions.assertSame(firstMapper, entry.getFileMapper());
  }

  @Test
  void testMountInDifferentLocationThrows() throws IOException
  {
    final StorageLocation location1 = new StorageLocation(cacheDir, ESTIMATE * 4, null);
    final File otherDir = new File(tempDir, "other");
    FileUtils.mkdirp(otherDir);
    final StorageLocation location2 = new StorageLocation(otherDir, ESTIMATE * 4, null);

    final PartialSegmentMetadataCacheEntry entry = newEntry(ESTIMATE);
    Assertions.assertTrue(location1.reserve(entry));
    entry.mount(location1);

    Assertions.assertThrows(DruidException.class, () -> entry.mount(location2));
  }

  @Test
  void testUnmountClearsState() throws IOException
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 4, null);
    final PartialSegmentMetadataCacheEntry entry = newEntry(ESTIMATE);
    Assertions.assertTrue(location.reserve(entry));
    entry.mount(location);
    Assertions.assertTrue(entry.isMounted());

    entry.unmount();

    Assertions.assertFalse(entry.isMounted());
    Assertions.assertNull(entry.getFileMapper());
    Assertions.assertNull(entry.getSegmentFileMetadata());
  }

  @Test
  void testUnmountIsIdempotent() throws IOException
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 4, null);
    final PartialSegmentMetadataCacheEntry entry = newEntry(ESTIMATE);
    Assertions.assertTrue(location.reserve(entry));
    entry.mount(location);
    entry.unmount();
    entry.unmount(); // second call is a no-op
  }

  @Test
  void testUnmountDeletesHeaderFile() throws IOException
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 4, null);
    final PartialSegmentMetadataCacheEntry entry = newEntry(ESTIMATE);
    Assertions.assertTrue(location.reserve(entry));
    entry.mount(location);

    final File headerFile = new File(cacheDir, IndexIO.V10_FILE_NAME + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX);
    Assertions.assertTrue(headerFile.exists());

    entry.unmount();
    Assertions.assertFalse(headerFile.exists(), "unmount must delete the entry's storage-location header file");
  }

  @Test
  void testOnUnmountHookRunsAfterStorageLocationCleanup() throws IOException
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 4, null);
    final PartialSegmentMetadataCacheEntry entry = newEntry(ESTIMATE);
    Assertions.assertTrue(location.reserve(entry));
    entry.mount(location);

    final File headerFile = new File(cacheDir, IndexIO.V10_FILE_NAME + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX);
    final AtomicReference<Boolean> headerExistsWhenHookFired = new AtomicReference<>();
    final AtomicReference<Boolean> hookFired = new AtomicReference<>(false);
    entry.setOnUnmount(() -> {
      hookFired.set(true);
      headerExistsWhenHookFired.set(headerFile.exists());
    });

    entry.unmount();
    Assertions.assertTrue(hookFired.get(), "onUnmount hook must run");
    Assertions.assertEquals(
        Boolean.FALSE,
        headerExistsWhenHookFired.get(),
        "hook must observe header already deleted (storage-location cleanup runs first)"
    );
  }

  @Test
  void testConstructorRejectsNonPositiveEstimate()
  {
    Assertions.assertThrows(
        DruidException.class,
        () -> new PartialSegmentMetadataCacheEntry(
            SEGMENT_ID,
            cacheDir,
            IndexIO.V10_FILE_NAME,
            List.of(),
            new DirectoryBackedRangeReader(segmentFile.getParentFile()),
            JSON_MAPPER,
            null,
            0
        )
    );
  }

  @Test
  void testGettersReturnNullBeforeMount()
  {
    final PartialSegmentMetadataCacheEntry entry = newEntry(ESTIMATE);
    Assertions.assertFalse(entry.isMounted());
    Assertions.assertNull(entry.getFileMapper());
    Assertions.assertNull(entry.getSegmentFileMetadata());
  }

  @Test
  void testUnmountDefersHeaderDeleteWhileReferenceHeld() throws Exception
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 4, null);
    final PartialSegmentMetadataCacheEntry entry = newEntry(ESTIMATE);
    Assertions.assertTrue(location.reserve(entry));
    entry.mount(location);

    final File headerFile = new File(
        cacheDir,
        IndexIO.V10_FILE_NAME + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX
    );
    Assertions.assertTrue(headerFile.exists());

    final Closeable ref = entry.acquireMetadataReference();
    Assertions.assertTrue(entry.isMounted());

    entry.unmount();
    // Header file MUST persist while the reference is held, even though unmount has been called.
    Assertions.assertTrue(headerFile.exists(), "header file should persist while reference is held");
    Assertions.assertTrue(entry.isMounted(), "fileMapper should not be closed while reference is held");

    ref.close();
    // Last reference released, deferred cleanup fires on this thread.
    Assertions.assertFalse(headerFile.exists(), "header file should be deleted after last reference releases");
    Assertions.assertFalse(entry.isMounted());
  }

  @Test
  void testConcurrentMountIsDeduplicated() throws Exception
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 4, null);
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());
    final PartialSegmentMetadataCacheEntry entry = new PartialSegmentMetadataCacheEntry(
        SEGMENT_ID,
        cacheDir,
        IndexIO.V10_FILE_NAME,
        List.of(),
        rangeReader,
        JSON_MAPPER,
        null,
        ESTIMATE
    );
    Assertions.assertTrue(location.reserve(entry));

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
            entry.mount(location);
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
      Assertions.assertTrue(entry.isMounted());
      // Dedup proof: even with 8 concurrent mount() callers, the slow PartialSegmentFileMapperV10.create() path
      // (which range-reads the header) ran exactly once. Without CAS+SettableFuture dedup, every caller would
      // serialize through entryLock and each would still skip the actual fetch (early-return on already-mounted),
      // but the FIRST few callers racing past the pre-check would re-fetch, counting range reads is the cleanest
      // way to assert the slow work was deduped end to end.
      Assertions.assertEquals(
          1,
          rangeReader.getHeaderReadCount(),
          "expected exactly one range-read of the header across 8 concurrent mounters"
      );
    }
    finally {
      exec.shutdownNow();
    }
  }

  @Test
  void testMountRollsBackIfEntryNoLongerReservedAtLocation() throws Exception
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 4, null);
    final PartialSegmentMetadataCacheEntry entry = newEntry(ESTIMATE);
    Assertions.assertTrue(location.reserve(entry));

    // Externally evict the entry by releasing it (without going through entry.unmount() ourselves). This simulates
    // the race where a hold/reservation gets dropped (concurrent cancellation, coordinator drop) and the location
    // no longer knows about the entry by the time mount() finishes its work.
    location.release(entry);
    Assertions.assertFalse(entry.isMounted(), "release should have triggered cleanup");
    Assertions.assertFalse(location.isReserved(entry.getId()));

    // Call mount() again without re-reserving. doMount will succeed (on-disk header is still present and the
    // file mapper opens), but the post-mount check should detect the missing reservation and roll back.
    entry.mount(location);
    Assertions.assertFalse(
        entry.isMounted(),
        "mount must roll back when post-mount check detects the entry is no longer reserved with the location"
    );
    Assertions.assertEquals(0, location.currentSizeBytes(), "no reservation should linger after rollback");
  }

  @Test
  void testAcquireMetadataReferenceBeforeMountThrows()
  {
    final PartialSegmentMetadataCacheEntry entry = newEntry(ESTIMATE);
    Assertions.assertThrows(DruidException.class, entry::acquireMetadataReference);
  }

  @Test
  void testAcquireMetadataReferenceAfterCleanupCompletesThrows() throws Exception
  {
    final StorageLocation location = new StorageLocation(cacheDir, ESTIMATE * 4, null);
    final PartialSegmentMetadataCacheEntry entry = newEntry(ESTIMATE);
    Assertions.assertTrue(location.reserve(entry));
    entry.mount(location);
    entry.unmount(); // no references; cleanup runs synchronously
    Assertions.assertFalse(entry.isMounted());
    Assertions.assertThrows(DruidException.class, entry::acquireMetadataReference);
  }

  @Test
  void testAcquireReferenceBeforeMountReturnsEmpty()
  {
    // Segment-level acquireReference returns empty (not throw) when the entry isn't mounted, matches the
    // SegmentCacheEntry contract used by SegmentLocalCacheManager.acquireCachedSegment to skip locations that don't
    // have the segment.
    final PartialSegmentMetadataCacheEntry entry = newEntry(ESTIMATE);
    Assertions.assertEquals(Optional.empty(), entry.acquireReference());
  }

  @Test
  void testInferParentBundlesForBaseReturnsEmpty()
  {
    final PartialSegmentMetadataCacheEntry entry = newEntry(ESTIMATE);
    Assertions.assertEquals(
        List.of(),
        entry.inferParentBundles(Projections.BASE_TABLE_PROJECTION_NAME)
    );
  }

  @Test
  void testInferParentBundlesForAggregateReturnsBase()
  {
    final PartialSegmentMetadataCacheEntry entry = newEntry(ESTIMATE);
    final List<PartialSegmentBundleCacheEntryIdentifier> parents = entry.inferParentBundles("some_aggregate_projection");
    Assertions.assertEquals(1, parents.size());
    Assertions.assertEquals(SEGMENT_ID, parents.getFirst().segmentId());
    Assertions.assertEquals(
        Projections.BASE_TABLE_PROJECTION_NAME,
        parents.getFirst().bundleName()
    );
  }

  private PartialSegmentMetadataCacheEntry newEntry(long estimate)
  {
    return new PartialSegmentMetadataCacheEntry(
        SEGMENT_ID,
        cacheDir,
        IndexIO.V10_FILE_NAME,
        List.of(),
        new DirectoryBackedRangeReader(segmentFile.getParentFile()),
        JSON_MAPPER,
        null,
        estimate
    );
  }

  private File buildTestSegment(int numFiles) throws IOException
  {
    final File baseDir = new File(tempDir, "deep_storage");
    FileUtils.mkdirp(baseDir);
    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir, CompressionStrategy.NONE)) {
      for (int i = 0; i < numFiles; ++i) {
        File tmpFile = new File(tempDir, StringUtils.format("smoosh-%d.bin", i));
        Files.write(Ints.toByteArray(i), tmpFile);
        builder.add(StringUtils.format("%d", i), tmpFile);
      }
    }
    return new File(baseDir, IndexIO.V10_FILE_NAME);
  }

}
