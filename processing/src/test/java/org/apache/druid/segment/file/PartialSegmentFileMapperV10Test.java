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

package org.apache.druid.segment.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.loading.DirectoryBackedRangeReader;
import org.apache.druid.segment.loading.SegmentRangeReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class PartialSegmentFileMapperV10Test
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @TempDir
  File tempDir;

  @Test
  void testMapFileThrowsUntilFetchedThenSlices() throws IOException
  {
    final File segmentFile = buildTestSegment(20, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("explicit");

    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());

    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir)) {
      // reset count after create fetched metadata
      rangeReader.resetCount();

      // mapFile never downloads: a known-but-unfetched file fails loudly (a silent slice would be all zeros)
      Assertions.assertThrows(DruidException.class, () -> mapper.mapFile("5"));
      Assertions.assertEquals(0, rangeReader.getReadCount(), "the failed map must not have downloaded anything");

      mapper.fetchFiles(Set.of("5"));
      Assertions.assertEquals(1, rangeReader.getReadCount());
      Assertions.assertEquals(4, mapper.getDownloadedBytes());

      ByteBuffer buf = mapper.mapFile("5");
      Assertions.assertNotNull(buf);
      Assertions.assertEquals(0, buf.position());
      Assertions.assertEquals(4, buf.remaining());
      Assertions.assertEquals(5, buf.getInt());

      // mapping again slices the same resident bytes without any further reads
      ByteBuffer buf2 = mapper.mapFile("5");
      Assertions.assertNotNull(buf2);
      Assertions.assertEquals(5, buf2.getInt());
      Assertions.assertEquals(1, rangeReader.getReadCount());
    }
  }

  @Test
  void testMapFileCompressedMetadata() throws IOException
  {
    final File segmentFile = buildTestSegment(20, CompressionStrategy.ZSTD);
    final File cacheDir = newCacheDir("compressed");

    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());

    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir)) {
      mapper.ensureAllDownloaded();
      for (int i = 0; i < 20; ++i) {
        ByteBuffer buf = mapper.mapFile(String.valueOf(i));
        Assertions.assertNotNull(buf);
        Assertions.assertEquals(i, buf.getInt());
      }
    }
  }

  @Test
  void testMapFileReturnsNullForMissingFile() throws IOException
  {
    final File segmentFile = buildTestSegment(5, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("missing");

    final DirectoryBackedRangeReader rangeReader = new DirectoryBackedRangeReader(segmentFile.getParentFile());

    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir)) {
      Assertions.assertNull(mapper.mapFile("nonexistent"));
    }
  }

  @Test
  void testGetInternalFilenames() throws IOException
  {
    final File segmentFile = buildTestSegment(10, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("filenames");

    final DirectoryBackedRangeReader rangeReader = new DirectoryBackedRangeReader(segmentFile.getParentFile());

    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir)) {
      Set<String> expected = new HashSet<>();
      for (int i = 0; i < 10; ++i) {
        expected.add(String.valueOf(i));
      }
      Assertions.assertEquals(expected, mapper.getInternalFilenames());
    }
  }

  @Test
  void testFetchFilesWithDefaultGapTolerance() throws IOException
  {
    final File segmentFile = buildTestSegment(10, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("ensure");

    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());

    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir)) {
      rangeReader.resetCount();

      Set<String> filesToLoad = Set.of("2", "5", "7");
      mapper.fetchFiles(filesToLoad);

      // the tiny gaps (files 3, 4, and 6) are far below the default coalesce tolerance, so the whole span [2..7]
      // fetches in a single range read, and the bridged gap files become resident too
      Assertions.assertEquals(1, rangeReader.getReadCount());
      Assertions.assertEquals(24, mapper.getDownloadedBytes());
      Assertions.assertEquals(Set.of("2", "3", "4", "5", "6", "7"), mapper.getDownloadedFiles());

      // accessing these files should not trigger additional downloads
      for (String name : filesToLoad) {
        ByteBuffer buf = mapper.mapFile(name);
        Assertions.assertNotNull(buf);
        Assertions.assertEquals(Integer.parseInt(name), buf.getInt());
      }
      Assertions.assertEquals(1, rangeReader.getReadCount());
    }
  }

  @Test
  void testFetchFilesCoalescesAdjacentFiles() throws IOException
  {
    final File segmentFile = buildTestSegment(10, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("fetch_adjacent");

    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());

    // gap tolerance 0: only strictly adjacent files coalesce
    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir, 0)) {
      rangeReader.resetCount();

      mapper.fetchFiles(Set.of("3", "4", "5"));

      Assertions.assertEquals(1, rangeReader.getReadCount());
      Assertions.assertEquals(12, rangeReader.getReadBytes());
      Assertions.assertEquals(12, mapper.getDownloadedBytes());
      Assertions.assertEquals(Set.of("3", "4", "5"), mapper.getDownloadedFiles());
    }
  }

  @Test
  void testFetchFilesGapExceedsToleranceSplitsRuns() throws IOException
  {
    final File segmentFile = buildTestSegment(10, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("fetch_split");

    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());

    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir, 0)) {
      rangeReader.resetCount();

      mapper.fetchFiles(Set.of("2", "7"));

      // 16 gap bytes (files 3-6) exceed tolerance 0, so two separate reads with no gap bytes fetched
      Assertions.assertEquals(2, rangeReader.getReadCount());
      Assertions.assertEquals(8, rangeReader.getReadBytes());
      Assertions.assertEquals(8, mapper.getDownloadedBytes());
      Assertions.assertEquals(Set.of("2", "7"), mapper.getDownloadedFiles());
    }
  }

  @Test
  void testFetchFilesBridgesGapWithinTolerance() throws IOException
  {
    final File segmentFile = buildTestSegment(10, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("fetch_bridge");

    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());

    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir, 16)) {
      rangeReader.resetCount();

      mapper.fetchFiles(Set.of("2", "7"));

      // the 16 gap bytes (files 3-6) are within tolerance: one read covering [2..7], bridged files become resident
      Assertions.assertEquals(1, rangeReader.getReadCount());
      Assertions.assertEquals(24, rangeReader.getReadBytes());
      Assertions.assertEquals(24, mapper.getDownloadedBytes());
      Assertions.assertEquals(Set.of("2", "3", "4", "5", "6", "7"), mapper.getDownloadedFiles());

      // bridged files map without further reads, with correct content
      final ByteBuffer buf = mapper.mapFile("4");
      Assertions.assertNotNull(buf);
      Assertions.assertEquals(4, buf.getInt());
      Assertions.assertEquals(1, rangeReader.getReadCount());
    }

    // bridged files persist in the bitmap: a restored mapper sees them as resident
    final CountingRangeReader freshReader = new CountingRangeReader(segmentFile.getParentFile());
    try (PartialSegmentFileMapperV10 restored = createMapper(freshReader, cacheDir, 16)) {
      freshReader.resetCount();
      Assertions.assertEquals(Set.of("2", "3", "4", "5", "6", "7"), restored.getDownloadedFiles());
      final ByteBuffer buf = restored.mapFile("6");
      Assertions.assertNotNull(buf);
      Assertions.assertEquals(6, buf.getInt());
      Assertions.assertEquals(0, freshReader.getReadCount());
    }
  }

  @Test
  void testFetchFilesSkipsResidentSpans() throws IOException
  {
    final File segmentFile = buildTestSegment(10, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("fetch_resident");

    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());

    // large tolerance so only residency, not gaps, can split the span
    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir, 1024)) {
      mapper.fetchFiles(Set.of("4"));
      rangeReader.resetCount();

      mapper.fetchFiles(Set.of("2", "3", "4", "5", "6", "7"));

      // resident file 4 splits the span into [2,3] and [5,6,7]; its bytes are never re-fetched
      Assertions.assertEquals(2, rangeReader.getReadCount());
      Assertions.assertEquals(20, rangeReader.getReadBytes());
      Assertions.assertEquals(24, mapper.getDownloadedBytes());
      Assertions.assertEquals(Set.of("2", "3", "4", "5", "6", "7"), mapper.getDownloadedFiles());
    }
  }

  @Test
  void testFetchFilesFullyResidentOrUnknownIsNoOp() throws IOException
  {
    final File segmentFile = buildTestSegment(10, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("fetch_noop");

    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());

    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir)) {
      mapper.fetchFiles(Set.of("2", "3"));
      rangeReader.resetCount();

      mapper.fetchFiles(Set.of("2", "3"));
      Assertions.assertEquals(0, rangeReader.getReadCount());

      mapper.fetchFiles(Set.of("does-not-exist"));
      Assertions.assertEquals(0, rangeReader.getReadCount());

      mapper.fetchFiles(Set.of());
      Assertions.assertEquals(0, rangeReader.getReadCount());
    }
  }

  @Test
  void testConcurrentFetchFilesSameSet() throws Exception
  {
    final File segmentFile = buildTestSegment(20, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("concurrent_fetch");

    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());

    // tolerance 0 so the request plans as exactly two runs ({3,4,5} and {9,10,11}) with no gap bridging; this test
    // is about concurrent same-set fetches not double-downloading, and exact expectations keep that sharp
    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir, 0)) {
      rangeReader.resetCount();

      final int numThreads = 8;
      final ExecutorService exec = Execs.multiThreaded(numThreads, "fetch-test-%d");
      try {
        final CountDownLatch startLatch = new CountDownLatch(1);
        final List<Future<Void>> futures = new ArrayList<>();
        for (int t = 0; t < numThreads; t++) {
          futures.add(exec.submit(() -> {
            startLatch.await();
            mapper.fetchFiles(Set.of("3", "4", "5", "9", "10", "11"));
            return null;
          }));
        }
        startLatch.countDown();
        for (Future<Void> f : futures) {
          f.get();
        }

        // whichever thread won each run downloaded it; every file is resident exactly once
        Assertions.assertEquals(24, mapper.getDownloadedBytes());
        Assertions.assertEquals(Set.of("3", "4", "5", "9", "10", "11"), mapper.getDownloadedFiles());
        for (String name : mapper.getDownloadedFiles()) {
          final ByteBuffer buf = mapper.mapFile(name);
          Assertions.assertNotNull(buf);
          Assertions.assertEquals(Integer.parseInt(name), buf.getInt());
        }
      }
      finally {
        exec.shutdownNow();
      }
    }
  }

  @Test
  void testMapFileDuringInFlightFetch() throws Exception
  {
    final File segmentFile = buildTestSegment(20, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("concurrent_mixed");

    // Gate the second fetch mid-flight so concurrent mapFile calls genuinely observe an in-progress run.
    // Discriminate by thread identity, not offset: createMapper's header fetch also reads at offset > 0 (the
    // metadata bytes after the fixed preamble) on this thread, and gating that would self-deadlock the test.
    final Thread mainThread = Thread.currentThread();
    final CountDownLatch fetchStarted = new CountDownLatch(1);
    final CountDownLatch releaseFetch = new CountDownLatch(1);
    final AtomicBoolean gated = new AtomicBoolean(true);
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile())
    {
      @Override
      public InputStream readRange(String filename, long offset, long length) throws IOException
      {
        if (Thread.currentThread() != mainThread && gated.compareAndSet(true, false)) {
          fetchStarted.countDown();
          try {
            releaseFetch.await();
          }
          catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        return super.readRange(filename, offset, length);
      }
    };

    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir, 1024)) {
      // make a file outside the run resident up front (on this thread, which the gate ignores)
      mapper.fetchFiles(Set.of("10"));
      rangeReader.resetCount();
      final ExecutorService exec = Execs.multiThreaded(2, "mixed-test-%d");
      try {
        final Future<Void> fetchFuture = exec.submit(() -> {
          mapper.fetchFiles(Set.of("3", "4", "5"));
          return null;
        });
        Assertions.assertTrue(fetchStarted.await(10, TimeUnit.SECONDS));

        // reads never wait on downloads: a resident file slices immediately while the run is parked, and a run
        // member that isn't resident yet fails fast instead of blocking for it
        final ByteBuffer outside = mapper.mapFile("10");
        Assertions.assertNotNull(outside);
        Assertions.assertEquals(10, outside.getInt());
        Assertions.assertThrows(DruidException.class, () -> mapper.mapFile("4"));

        releaseFetch.countDown();
        fetchFuture.get(10, TimeUnit.SECONDS);
        final ByteBuffer member = mapper.mapFile("4");
        Assertions.assertNotNull(member);
        Assertions.assertEquals(4, member.getInt());

        // one read for the run; the throwing mapFile and the resident reads downloaded nothing
        Assertions.assertEquals(1, rangeReader.getReadCount());
        Assertions.assertEquals(16, mapper.getDownloadedBytes());
      }
      finally {
        exec.shutdownNow();
      }
    }
  }

  @Test
  void testFetchFilesListenerAccounting() throws IOException
  {
    final File segmentFile = buildTestSegment(10, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("fetch_listener");

    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());
    final AtomicInteger rangeReads = new AtomicInteger();
    final AtomicLong rangeReadBytes = new AtomicLong();
    final AtomicInteger fileDownloads = new AtomicInteger();
    final AtomicLong fileDownloadBytes = new AtomicLong();
    final PartialSegmentDownloadListener listener = new PartialSegmentDownloadListener()
    {
      @Override
      public void onBytesDownloaded(long bytes)
      {
        fileDownloads.incrementAndGet();
        fileDownloadBytes.addAndGet(bytes);
      }

      @Override
      public void onRangeRead(long bytes, long durationNanos)
      {
        rangeReads.incrementAndGet();
        rangeReadBytes.addAndGet(bytes);
      }
    };

    try (PartialSegmentFileMapperV10 mapper = PartialSegmentFileMapperV10.create(
        rangeReader,
        JSON_MAPPER,
        cacheDir,
        IndexIO.V10_FILE_NAME,
        List.of(),
        listener,
        16,
        PartialSegmentFileMapperV10.DEFAULT_MAX_FETCH_RUN_BYTES
    )) {
      // ignore the header-fetch onBytesDownloaded event
      fileDownloads.set(0);
      fileDownloadBytes.set(0);

      mapper.fetchFiles(Set.of("2", "7"));

      // one wire request for the whole bridged span; one per-file resident event for each of the 6 covered files
      Assertions.assertEquals(1, rangeReads.get());
      Assertions.assertEquals(24, rangeReadBytes.get());
      Assertions.assertEquals(6, fileDownloads.get());
      Assertions.assertEquals(24, fileDownloadBytes.get());
    }
  }

  @Test
  void testPlanFetchRuns()
  {
    // synthetic single-container layout: files a..f tile back-to-back, 10 bytes each
    final Map<String, SegmentInternalFileMetadata> files = new HashMap<>();
    final List<String> byOffset = List.of("a", "b", "c", "d", "e", "f");
    for (int i = 0; i < byOffset.size(); i++) {
      files.put(byOffset.get(i), new SegmentInternalFileMetadata(0, i * 10L, 10));
    }

    // adjacent requested files coalesce regardless of tolerance
    Assertions.assertEquals(
        List.of(new PartialSegmentFileMapperV10.FetchRun(0, 0, 20, List.of("a", "b"))),
        PartialSegmentFileMapperV10.planFetchRuns(0, byOffset, files, Set.of("a", "b"), Set.of(), 0)
    );

    // gap over tolerance splits; trailing gap bytes are never fetched
    Assertions.assertEquals(
        List.of(
            new PartialSegmentFileMapperV10.FetchRun(0, 0, 10, List.of("a")),
            new PartialSegmentFileMapperV10.FetchRun(0, 30, 10, List.of("d"))
        ),
        PartialSegmentFileMapperV10.planFetchRuns(0, byOffset, files, Set.of("a", "d"), Set.of(), 10)
    );

    // gap within tolerance bridges, and the bridged files ride along in the run
    Assertions.assertEquals(
        List.of(new PartialSegmentFileMapperV10.FetchRun(0, 0, 40, List.of("a", "b", "c", "d"))),
        PartialSegmentFileMapperV10.planFetchRuns(0, byOffset, files, Set.of("a", "d"), Set.of(), 20)
    );

    // a resident file always splits, even when the gap around it is within tolerance
    Assertions.assertEquals(
        List.of(
            new PartialSegmentFileMapperV10.FetchRun(0, 0, 10, List.of("a")),
            new PartialSegmentFileMapperV10.FetchRun(0, 30, 10, List.of("d"))
        ),
        PartialSegmentFileMapperV10.planFetchRuns(0, byOffset, files, Set.of("a", "d"), Set.of("b", "c"), 100)
    );

    // requested-but-resident files are skipped entirely
    Assertions.assertEquals(
        List.of(),
        PartialSegmentFileMapperV10.planFetchRuns(0, byOffset, files, Set.of("a"), Set.of("a"), 100)
    );

    // mixed: run bridges over unrequested c but is split by resident e
    Assertions.assertEquals(
        List.of(
            new PartialSegmentFileMapperV10.FetchRun(0, 0, 40, List.of("a", "b", "c", "d")),
            new PartialSegmentFileMapperV10.FetchRun(0, 50, 10, List.of("f"))
        ),
        PartialSegmentFileMapperV10.planFetchRuns(0, byOffset, files, Set.of("a", "b", "d", "f"), Set.of("e"), 100)
    );
  }

  @Test
  void testSplitRuns()
  {
    // synthetic layout: files of sizes 10, 10, 25, 10, 10 tiling one container
    final Map<String, SegmentInternalFileMetadata> files = new HashMap<>();
    files.put("a", new SegmentInternalFileMetadata(0, 0, 10));
    files.put("b", new SegmentInternalFileMetadata(0, 10, 10));
    files.put("big", new SegmentInternalFileMetadata(0, 20, 25));
    files.put("c", new SegmentInternalFileMetadata(0, 45, 10));
    files.put("d", new SegmentInternalFileMetadata(0, 55, 10));
    final PartialSegmentFileMapperV10.FetchRun whole =
        new PartialSegmentFileMapperV10.FetchRun(0, 0, 65, List.of("a", "b", "big", "c", "d"));

    // under the cap: unchanged
    Assertions.assertEquals(
        List.of(whole),
        PartialSegmentFileMapperV10.splitRuns(List.of(whole), files, 65)
    );

    // disabled: unchanged
    Assertions.assertEquals(
        List.of(whole),
        PartialSegmentFileMapperV10.splitRuns(List.of(whole), files, 0)
    );

    // cap 20: cuts at file boundaries; the oversized file forms its own sub-run; sub-runs tile the original span
    Assertions.assertEquals(
        List.of(
            new PartialSegmentFileMapperV10.FetchRun(0, 0, 20, List.of("a", "b")),
            new PartialSegmentFileMapperV10.FetchRun(0, 20, 25, List.of("big")),
            new PartialSegmentFileMapperV10.FetchRun(0, 45, 20, List.of("c", "d"))
        ),
        PartialSegmentFileMapperV10.splitRuns(List.of(whole), files, 20)
    );
  }

  @Test
  void testPlanFetchRunsZeroLengthFiles()
  {
    // all-null columns serialize no payload bytes, leaving zero-length internal files that share a start offset with
    // a neighboring file; run-end tracking must never let one shrink a run below a covered file's bytes (a file must
    // never be marked downloaded without its bytes fetched)
    final Map<String, SegmentInternalFileMetadata> files = new HashMap<>();
    files.put("a", new SegmentInternalFileMetadata(0, 0, 10));
    files.put("nullCol", new SegmentInternalFileMetadata(0, 10, 0));
    files.put("b", new SegmentInternalFileMetadata(0, 10, 20));

    // canonical (startOffset, size) order: the zero-length file rides along between its neighbors
    Assertions.assertEquals(
        List.of(new PartialSegmentFileMapperV10.FetchRun(0, 0, 30, List.of("a", "nullCol", "b"))),
        PartialSegmentFileMapperV10.planFetchRuns(
            0,
            List.of("a", "nullCol", "b"),
            files,
            Set.of("a", "nullCol", "b"),
            Set.of(),
            0
        )
    );

    // adversarial order (zero-length file after the real file whose start it shares): the run must still cover the
    // real file's bytes rather than shrinking to the zero-length file's end
    Assertions.assertEquals(
        List.of(new PartialSegmentFileMapperV10.FetchRun(0, 0, 30, List.of("a", "b", "nullCol"))),
        PartialSegmentFileMapperV10.planFetchRuns(
            0,
            List.of("a", "b", "nullCol"),
            files,
            Set.of("a", "b", "nullCol"),
            Set.of(),
            0
        )
    );

    // a zero-length file alone plans a zero-length run: nothing to read, but it still gets marked downloaded
    Assertions.assertEquals(
        List.of(new PartialSegmentFileMapperV10.FetchRun(0, 10, 0, List.of("nullCol"))),
        PartialSegmentFileMapperV10.planFetchRuns(
            0,
            List.of("a", "nullCol", "b"),
            files,
            Set.of("nullCol"),
            Set.of(),
            0
        )
    );
  }

  @Test
  void testSplitRunsZeroLengthFiles()
  {
    // sub-run end tracking has the same never-shrink obligation as planFetchRuns when a zero-length file shares a
    // start offset with the real file before it
    final Map<String, SegmentInternalFileMetadata> files = new HashMap<>();
    files.put("a", new SegmentInternalFileMetadata(0, 0, 10));
    files.put("b", new SegmentInternalFileMetadata(0, 10, 20));
    files.put("nullCol", new SegmentInternalFileMetadata(0, 10, 0));
    final PartialSegmentFileMapperV10.FetchRun whole =
        new PartialSegmentFileMapperV10.FetchRun(0, 0, 30, List.of("a", "b", "nullCol"));

    Assertions.assertEquals(
        List.of(
            new PartialSegmentFileMapperV10.FetchRun(0, 0, 10, List.of("a")),
            new PartialSegmentFileMapperV10.FetchRun(0, 10, 20, List.of("b", "nullCol"))
        ),
        PartialSegmentFileMapperV10.splitRuns(List.of(whole), files, 15)
    );
  }

  @Test
  void testPlanParallelFetchSplitsLargeRuns() throws IOException
  {
    final File segmentFile = buildTestSegment(10, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("parallel_split");

    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());

    // gap tolerance covers everything, but the parallel plan caps runs at 8 bytes (two 4-byte files each)
    try (PartialSegmentFileMapperV10 mapper = PartialSegmentFileMapperV10.create(
        rangeReader,
        JSON_MAPPER,
        cacheDir,
        IndexIO.V10_FILE_NAME,
        List.of(),
        PartialSegmentDownloadListener.NOOP,
        1024,
        8
    )) {
      rangeReader.resetCount();

      final List<PartialSegmentFileMapperV10.FetchRun> runs =
          mapper.planParallelFetch(Set.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
      Assertions.assertEquals(5, runs.size(), "40 bytes at an 8-byte cap must plan as 5 runs");

      // the sequential plan for the same request stays maximal
      Assertions.assertEquals(
          1,
          mapper.planFetch(Set.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")).size(),
          "sequential plan must not split"
      );

      for (PartialSegmentFileMapperV10.FetchRun run : runs) {
        mapper.fetchRun(run);
      }
      Assertions.assertEquals(5, rangeReader.getReadCount());
      Assertions.assertEquals(40, mapper.getDownloadedBytes());
      for (int i = 0; i < 10; i++) {
        final ByteBuffer buf = mapper.mapFile(String.valueOf(i));
        Assertions.assertNotNull(buf);
        Assertions.assertEquals(i, buf.getInt());
      }
      Assertions.assertEquals(5, rangeReader.getReadCount(), "mapFile must find everything resident");
    }
  }

  @Test
  void testSparseContainerFiles() throws IOException
  {
    final File segmentFile = buildTestSegment(10, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("sparse");

    final DirectoryBackedRangeReader rangeReader = new DirectoryBackedRangeReader(segmentFile.getParentFile());

    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir)) {
      // download just 2 files
      mapper.fetchFiles(Set.of("3"));
      mapper.fetchFiles(Set.of("7"));

      // verify local container files exist (all files share one container in this test)
      File containerFile = new File(cacheDir, IndexIO.V10_FILE_NAME + ".container.00000");
      Assertions.assertTrue(containerFile.exists());

      // the container should be at the full original container size (sparse on supported filesystems)
      try (FileInputStream fis = new FileInputStream(segmentFile)) {
        SegmentFileMetadataReader.Result metadataResult = SegmentFileMetadataReader.read(fis, JSON_MAPPER);
        SegmentFileContainerMetadata containerMeta = metadataResult.getMetadata().getContainers().get(0);
        Assertions.assertEquals(containerMeta.getSize(), containerFile.length());
      }
    }
  }

  @Test
  void testProjectionStyleFileNames() throws IOException
  {
    // test with names like "projectionName/columnName" which is how V10 projections name their files
    final File baseDir = new File(tempDir, "base_" + ThreadLocalRandom.current().nextInt());
    FileUtils.mkdirp(baseDir);

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      for (int i = 0; i < 5; ++i) {
        File tmpFile = new File(tempDir, StringUtils.format("col-%s.bin", i));
        Files.write(Ints.toByteArray(i * 100), tmpFile);
        builder.add(StringUtils.format("myProjection/col_%d", i), tmpFile);
      }
    }

    final File cacheDir = newCacheDir("proj_names");
    final DirectoryBackedRangeReader rangeReader = new DirectoryBackedRangeReader(baseDir);

    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir)) {
      mapper.ensureAllDownloaded();
      for (int i = 0; i < 5; ++i) {
        ByteBuffer buf = mapper.mapFile(StringUtils.format("myProjection/col_%d", i));
        Assertions.assertNotNull(buf);
        Assertions.assertEquals(i * 100, buf.getInt());
      }
    }
  }

  @Test
  void testExternalFilesMatchEagerMapper() throws IOException
  {
    final String externalName = "external.segment";
    final File baseDir = new File(tempDir, "base_" + ThreadLocalRandom.current().nextInt());
    FileUtils.mkdirp(baseDir);

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      for (int i = 0; i < 5; ++i) {
        File tmpFile = new File(tempDir, StringUtils.format("main-%s.bin", i));
        Files.write(Ints.toByteArray(i), tmpFile);
        builder.add(StringUtils.format("%d", i), tmpFile);
      }
      SegmentFileBuilder external = builder.getExternalBuilder(externalName);
      for (int i = 5; i < 10; ++i) {
        File tmpFile = new File(tempDir, StringUtils.format("ext-%s.bin", i));
        Files.write(Ints.toByteArray(i), tmpFile);
        external.add(StringUtils.format("%d", i), tmpFile);
      }
    }

    final File segmentFile = new File(baseDir, IndexIO.V10_FILE_NAME);
    final File cacheDir = newCacheDir("ext_match");
    final DirectoryBackedRangeReader rangeReader = new DirectoryBackedRangeReader(baseDir);

    try (SegmentFileMapperV10 eager = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER, List.of(externalName));
         PartialSegmentFileMapperV10 lazy = createMapperWithExternal(rangeReader, cacheDir, externalName)
    ) {
      lazy.ensureAllDownloaded();
      // verify main files match
      for (int i = 0; i < 5; ++i) {
        ByteBuffer eagerBuf = eager.mapFile(String.valueOf(i));
        ByteBuffer lazyBuf = lazy.mapFile(String.valueOf(i));
        Assertions.assertNotNull(eagerBuf);
        Assertions.assertNotNull(lazyBuf);
        Assertions.assertEquals(eagerBuf, lazyBuf);
      }

      // verify external files match
      for (int i = 5; i < 10; ++i) {
        ByteBuffer eagerBuf = eager.mapExternalFile(externalName, String.valueOf(i));
        ByteBuffer lazyBuf = lazy.mapExternalFile(externalName, String.valueOf(i));
        Assertions.assertNotNull(eagerBuf);
        Assertions.assertNotNull(lazyBuf);
        Assertions.assertEquals(eagerBuf, lazyBuf);
      }
    }
  }

  @Test
  void testCreatePersistsAndRestores() throws IOException
  {
    final File segmentFile = buildTestSegment(10, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("persist");

    final DirectoryBackedRangeReader rangeReader = new DirectoryBackedRangeReader(segmentFile.getParentFile());

    // fetches from range reader and persists header + bitmap; separate single-file fetches so nothing bridges
    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir)) {
      mapper.fetchFiles(Set.of("2"));
      mapper.fetchFiles(Set.of("5"));
      mapper.fetchFiles(Set.of("8"));
      Assertions.assertEquals(12, mapper.getDownloadedBytes());
    }

    // verify persisted header file exists (contains metadata + bitmap)
    Assertions.assertTrue(
        new File(cacheDir, IndexIO.V10_FILE_NAME + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX).exists()
    );

    // reads metadata from local header file, restores downloaded files from bitmap
    final DirectoryBackedRangeReader freshReader = new DirectoryBackedRangeReader(segmentFile.getParentFile());
    try (PartialSegmentFileMapperV10 restored = createMapper(freshReader, cacheDir)) {
      Assertions.assertEquals(12, restored.getDownloadedBytes());

      // previously downloaded files should be available
      ByteBuffer buf2 = restored.mapFile("2");
      Assertions.assertNotNull(buf2);
      Assertions.assertEquals(2, buf2.getInt());

      ByteBuffer buf5 = restored.mapFile("5");
      Assertions.assertNotNull(buf5);
      Assertions.assertEquals(5, buf5.getInt());

      ByteBuffer buf8 = restored.mapFile("8");
      Assertions.assertNotNull(buf8);
      Assertions.assertEquals(8, buf8.getInt());

      // downloading a new file should still work after restore
      restored.fetchFiles(Set.of("0"));
      ByteBuffer buf0 = restored.mapFile("0");
      Assertions.assertNotNull(buf0);
      Assertions.assertEquals(0, buf0.getInt());
      Assertions.assertEquals(16, restored.getDownloadedBytes());
    }
  }

  @Test
  void testCreateWithExternals() throws IOException
  {
    final String externalName = "external.segment";
    final File baseDir = new File(tempDir, "base_" + ThreadLocalRandom.current().nextInt());
    FileUtils.mkdirp(baseDir);

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      for (int i = 0; i < 5; ++i) {
        File tmpFile = new File(tempDir, StringUtils.format("main-%s.bin", i));
        Files.write(Ints.toByteArray(i), tmpFile);
        builder.add(StringUtils.format("%d", i), tmpFile);
      }
      SegmentFileBuilder external = builder.getExternalBuilder(externalName);
      for (int i = 5; i < 10; ++i) {
        File tmpFile = new File(tempDir, StringUtils.format("ext-%s.bin", i));
        Files.write(Ints.toByteArray(i), tmpFile);
        external.add(StringUtils.format("%d", i), tmpFile);
      }
    }

    final File cacheDir = newCacheDir("ext_persist");
    final DirectoryBackedRangeReader rangeReader = new DirectoryBackedRangeReader(baseDir);

    // create with externals, download some files (external files fetch through their own mapper)
    try (PartialSegmentFileMapperV10 mapper = createMapperWithExternal(rangeReader, cacheDir, externalName)) {
      mapper.fetchFiles(Set.of("1"));
      mapper.getExternalMapper(externalName).fetchFiles(Set.of("7"));
    }

    // restore, previously downloaded files should be available
    final DirectoryBackedRangeReader freshReader = new DirectoryBackedRangeReader(baseDir);
    try (PartialSegmentFileMapperV10 restored = createMapperWithExternal(freshReader, cacheDir, externalName)) {
      ByteBuffer buf1 = restored.mapFile("1");
      Assertions.assertNotNull(buf1);
      Assertions.assertEquals(1, buf1.getInt());

      ByteBuffer buf7 = restored.mapExternalFile(externalName, "7");
      Assertions.assertNotNull(buf7);
      Assertions.assertEquals(7, buf7.getInt());
    }
  }

  @Test
  void testCorruptHeaderFileRecovery() throws IOException
  {
    final File segmentFile = buildTestSegment(10, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("corrupt");

    final DirectoryBackedRangeReader rangeReader = new DirectoryBackedRangeReader(segmentFile.getParentFile());

    // populate normally
    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir)) {
      mapper.fetchFiles(Set.of("3"));
    }

    // corrupt the header file
    final File headerFile = new File(cacheDir, IndexIO.V10_FILE_NAME + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX);
    Assertions.assertTrue(headerFile.exists());
    try (RandomAccessFile raf = new RandomAccessFile(headerFile, "rw")) {
      raf.setLength(2); // truncate to something unparseable
    }

    // should detect corruption, re-fetch from deep storage, and work normally
    // (previously downloaded files are lost since bitmap was corrupted too, but new downloads work)
    try (PartialSegmentFileMapperV10 recovered = createMapper(rangeReader, cacheDir)) {
      Assertions.assertEquals(0, recovered.getDownloadedBytes());

      recovered.fetchFiles(Set.of("3"));
      ByteBuffer buf = recovered.mapFile("3");
      Assertions.assertNotNull(buf);
      Assertions.assertEquals(3, buf.getInt());
    }
  }

  @Test
  void testEnsureAllDownloadedReadsOneRangePerContainer() throws IOException
  {
    final int numBundles = 3;
    final int filesPerBundle = 4;
    final File segmentFile = buildMultiBundleSegment(numBundles, filesPerBundle);
    final File cacheDir = newCacheDir("ensure_all_bulk");
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());

    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir)) {
      final int numContainers = mapper.getSegmentFileMetadata().getContainers().size();
      final int numFiles = mapper.getInternalFilenames().size();
      Assertions.assertEquals(numBundles, numContainers, "each tiny bundle should be its own container");
      Assertions.assertEquals(numBundles * filesPerBundle, numFiles);

      rangeReader.resetCount();
      mapper.ensureAllDownloaded();

      // one range read per container, not one per internal file
      Assertions.assertEquals(numContainers, rangeReader.getReadCount());
      Assertions.assertTrue(rangeReader.getReadCount() < numFiles, "bulk download must beat the per-file read count");
      Assertions.assertTrue(mapper.isFullyDownloaded());
      Assertions.assertEquals((long) numFiles * Integer.BYTES, mapper.getDownloadedBytes());

      // a second pass is a no-op: every container is already fully downloaded
      rangeReader.resetCount();
      mapper.ensureAllDownloaded();
      Assertions.assertEquals(0, rangeReader.getReadCount());
    }

    // bulk-downloaded contents match the eager mapper byte-for-byte
    final DirectoryBackedRangeReader freshReader = new DirectoryBackedRangeReader(segmentFile.getParentFile());
    try (SegmentFileMapperV10 eager = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER);
         PartialSegmentFileMapperV10 lazy = createMapper(freshReader, newCacheDir("ensure_all_bulk_match"))) {
      lazy.ensureAllDownloaded();
      Assertions.assertEquals(eager.getInternalFilenames(), lazy.getInternalFilenames());
      for (String name : eager.getInternalFilenames()) {
        Assertions.assertEquals(eager.mapFile(name), lazy.mapFile(name), "file[" + name + "]");
      }
    }
  }

  @Test
  void testEnsureAllDownloadedSkipsResidentSpans() throws IOException
  {
    final int numBundles = 3;
    final int filesPerBundle = 4;
    final File segmentFile = buildMultiBundleSegment(numBundles, filesPerBundle);
    final File cacheDir = newCacheDir("ensure_all_partial");
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());

    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir)) {
      // make one interior file of the middle container resident first
      mapper.fetchFiles(Set.of("b1/col_1"));
      rangeReader.resetCount();

      mapper.ensureAllDownloaded();

      // b0 and b2 fetch in one read each; b1's resident file splits it into two reads, and its 4 bytes
      // are not re-fetched
      Assertions.assertEquals(4, rangeReader.getReadCount());
      final long totalBytes = (long) numBundles * filesPerBundle * Integer.BYTES;
      Assertions.assertEquals(totalBytes - Integer.BYTES, rangeReader.getReadBytes());
      Assertions.assertTrue(mapper.isFullyDownloaded());
      Assertions.assertEquals(totalBytes, mapper.getDownloadedBytes());
    }
  }

  @Test
  void testEnsureBundleDownloadedFetchesOnlyThatBundle() throws IOException
  {
    final File segmentFile = buildMultiBundleSegment(3, 4);
    final File cacheDir = newCacheDir("ensure_bundle");
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());

    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir)) {
      Assertions.assertTrue(mapper.getBundleNames().contains("b1"));
      final int b1Containers = mapper.getContainerIndicesForBundle("b1").size();

      rangeReader.resetCount();
      mapper.ensureBundleDownloaded("b1");

      // one range read per container in the bundle, and nothing fetched from the other bundles
      Assertions.assertEquals(b1Containers, rangeReader.getReadCount());
      Assertions.assertFalse(mapper.isFullyDownloaded(), "only one bundle was materialized");
      final Set<String> downloaded = mapper.getDownloadedFiles();
      for (int i = 0; i < 4; i++) {
        Assertions.assertTrue(downloaded.contains(StringUtils.format("b1/col_%d", i)), "b1 file should be downloaded");
        Assertions.assertFalse(downloaded.contains(StringUtils.format("b0/col_%d", i)), "b0 must stay undownloaded");
        Assertions.assertFalse(downloaded.contains(StringUtils.format("b2/col_%d", i)), "b2 must stay undownloaded");
      }
      // the materialized bundle's files are mappable with the expected content
      for (int i = 0; i < 4; i++) {
        final ByteBuffer buf = mapper.mapFile(StringUtils.format("b1/col_%d", i));
        Assertions.assertNotNull(buf);
        Assertions.assertEquals(100 + i, buf.getInt());
      }

      // an unknown bundle is a no-op
      rangeReader.resetCount();
      mapper.ensureBundleDownloaded("does-not-exist");
      Assertions.assertEquals(0, rangeReader.getReadCount());
    }
  }

  private File newCacheDir(String name) throws IOException
  {
    final File dir = new File(tempDir, name + "_" + ThreadLocalRandom.current().nextInt());
    FileUtils.mkdirp(dir);
    return dir;
  }

  /**
   * Build a segment whose files are spread across {@code numBundles} bundles. Each tiny file stays well under the max
   * container size, so each bundle becomes its own container. Files in bundle {@code b} are named {@code "b{b}/col_{i}"}
   * with content {@code b * 100 + i} (4 bytes).
   */
  private File buildMultiBundleSegment(int numBundles, int filesPerBundle) throws IOException
  {
    final File baseDir = new File(tempDir, "multibundle_" + ThreadLocalRandom.current().nextInt());
    FileUtils.mkdirp(baseDir);

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      for (int b = 0; b < numBundles; b++) {
        builder.startFileBundle("b" + b);
        for (int i = 0; i < filesPerBundle; i++) {
          final File tmpFile = new File(tempDir, StringUtils.format("mb-%d-%d.bin", b, i));
          Files.write(Ints.toByteArray(b * 100 + i), tmpFile);
          builder.add(StringUtils.format("b%d/col_%d", b, i), tmpFile);
        }
      }
    }

    return new File(baseDir, IndexIO.V10_FILE_NAME);
  }

  private File buildTestSegment(int numFiles, CompressionStrategy compression) throws IOException
  {
    final File baseDir = new File(tempDir, "base_" + ThreadLocalRandom.current().nextInt());
    FileUtils.mkdirp(baseDir);

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir, compression)) {
      for (int i = 0; i < numFiles; ++i) {
        File tmpFile = new File(tempDir, StringUtils.format("smoosh-%s.bin", i));
        Files.write(Ints.toByteArray(i), tmpFile);
        builder.add(StringUtils.format("%d", i), tmpFile);
      }
    }

    return new File(baseDir, IndexIO.V10_FILE_NAME);
  }

  private static PartialSegmentFileMapperV10 createMapper(
      SegmentRangeReader rangeReader,
      File localCacheDir
  ) throws IOException
  {
    return PartialSegmentFileMapperV10.create(
        rangeReader,
        JSON_MAPPER,
        localCacheDir,
        IndexIO.V10_FILE_NAME,
        List.of(),
        PartialSegmentDownloadListener.NOOP,
        PartialSegmentFileMapperV10.DEFAULT_COALESCE_GAP_BYTES,
        PartialSegmentFileMapperV10.DEFAULT_MAX_FETCH_RUN_BYTES
    );
  }

  private static PartialSegmentFileMapperV10 createMapper(
      SegmentRangeReader rangeReader,
      File localCacheDir,
      long coalesceGapBytes
  ) throws IOException
  {
    return PartialSegmentFileMapperV10.create(
        rangeReader,
        JSON_MAPPER,
        localCacheDir,
        IndexIO.V10_FILE_NAME,
        List.of(),
        PartialSegmentDownloadListener.NOOP,
        coalesceGapBytes,
        PartialSegmentFileMapperV10.DEFAULT_MAX_FETCH_RUN_BYTES
    );
  }

  private static PartialSegmentFileMapperV10 createMapperWithExternal(
      SegmentRangeReader rangeReader,
      File localCacheDir,
      String externalName
  ) throws IOException
  {
    return PartialSegmentFileMapperV10.create(
        rangeReader,
        JSON_MAPPER,
        localCacheDir,
        IndexIO.V10_FILE_NAME,
        List.of(externalName),
        PartialSegmentDownloadListener.NOOP,
        PartialSegmentFileMapperV10.DEFAULT_COALESCE_GAP_BYTES,
        PartialSegmentFileMapperV10.DEFAULT_MAX_FETCH_RUN_BYTES
    );
  }

}
