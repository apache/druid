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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

class PartialSegmentFileMapperV10Test
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @TempDir
  File tempDir;

  @Test
  void testMapFileDownloadsOnDemand() throws IOException
  {
    final File segmentFile = buildTestSegment(20, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("demand");

    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());

    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir)) {
      // reset count after create fetched metadata
      rangeReader.resetCount();

      // access a single file - should trigger exactly one download
      ByteBuffer buf = mapper.mapFile("5");
      Assertions.assertNotNull(buf);
      Assertions.assertEquals(0, buf.position());
      Assertions.assertEquals(4, buf.remaining());
      Assertions.assertEquals(5, buf.getInt());
      Assertions.assertEquals(1, rangeReader.getReadCount());
      Assertions.assertEquals(4, mapper.getDownloadedBytes());

      // access the same file again - should NOT trigger another download
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
  void testEnsureFilesAvailable() throws IOException
  {
    final File segmentFile = buildTestSegment(10, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("ensure");

    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());

    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir)) {
      rangeReader.resetCount();

      Set<String> filesToLoad = Set.of("2", "5", "7");
      mapper.ensureFilesAvailable(filesToLoad);

      // files 2,5,7 are contiguous (within maxGapBytes) in one container, so they coalesce into a single range read;
      // the span covers files 2..7, all of which are resident and charged afterward (24 bytes)
      Assertions.assertEquals(1, rangeReader.getReadCount());
      Assertions.assertEquals(24, mapper.getDownloadedBytes());

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
  void testCoalesceConfigRejectsInvalidThresholds()
  {
    // negative gap and non-positive chunk are nonsensical
    Assertions.assertThrows(
        DruidException.class,
        () -> new PartialSegmentFileMapperV10.CoalesceConfig(-1, 1 << 20)
    );
    Assertions.assertThrows(
        DruidException.class,
        () -> new PartialSegmentFileMapperV10.CoalesceConfig(0, 0)
    );
    Assertions.assertThrows(
        DruidException.class,
        () -> new PartialSegmentFileMapperV10.CoalesceConfig(0, -5)
    );
    // boundary values are valid: zero gap (merge adjacent files only) and a one-byte chunk (coalescing effectively off)
    Assertions.assertDoesNotThrow(() -> new PartialSegmentFileMapperV10.CoalesceConfig(0, 1));
  }

  @Test
  void testPlanDownloadRunsSplitsWhenGapExceedsThreshold() throws IOException
  {
    final File segmentFile = buildTestSegment(10, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("plan_split");
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());

    // maxGap = 0: no unwanted bytes may be read through, so non-adjacent wanted files never merge
    final PartialSegmentFileMapperV10.CoalesceConfig noGap = new PartialSegmentFileMapperV10.CoalesceConfig(0, 1 << 20);
    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir, noGap)) {
      final Set<String> alternating = Set.of("0", "2", "4", "6", "8");
      final List<PartialSegmentFileMapperV10.DownloadRun> runs = mapper.planDownloadRuns(alternating);

      Assertions.assertEquals(5, runs.size(), "each non-adjacent wanted file should be its own run");
      for (PartialSegmentFileMapperV10.DownloadRun run : runs) {
        Assertions.assertEquals(1, run.wantedFiles().size());
      }

      rangeReader.resetCount();
      mapper.ensureFilesAvailable(alternating);
      Assertions.assertEquals(5, rangeReader.getReadCount(), "one range read per un-coalesced run");
    }
  }

  @Test
  void testPlanDownloadRunsCapsRunAtMaxChunkBytes() throws IOException
  {
    final File segmentFile = buildTestSegment(10, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("plan_chunk");
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());

    // each test file is a 4-byte int; maxChunk = 4 means no two files can share a run even though they are contiguous
    final PartialSegmentFileMapperV10.CoalesceConfig tinyChunk =
        new PartialSegmentFileMapperV10.CoalesceConfig(1 << 20, Integer.BYTES);
    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir, tinyChunk)) {
      final Set<String> all = mapper.getSegmentFileMetadata().getFiles().keySet();
      final List<PartialSegmentFileMapperV10.DownloadRun> runs = mapper.planDownloadRuns(all);

      Assertions.assertEquals(all.size(), runs.size(), "maxChunkBytes should keep each file in its own run");
    }
  }

  @Test
  void testFetchRunMarksEveryFileTheSpanCovers() throws IOException
  {
    final File segmentFile = buildTestSegment(10, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("plan_marks");
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());

    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir)) {
      rangeReader.resetCount();

      // with the generous default gap, files 2,5,7 coalesce into a single run that spans the gap-fill files 3,4,6
      final Set<String> wanted = Set.of("2", "5", "7");
      final List<PartialSegmentFileMapperV10.DownloadRun> runs = mapper.planDownloadRuns(wanted);
      Assertions.assertEquals(1, runs.size());

      mapper.ensureFilesAvailable(wanted);
      Assertions.assertEquals(1, rangeReader.getReadCount(), "the three wanted files coalesce into one range read");
      // every file the span covers is resident, so the gap-fill files (3,4,6) are marked downloaded too, not just the
      // wanted ones; all six files (2..7) are charged
      Assertions.assertEquals(Set.of("2", "3", "4", "5", "6", "7"), mapper.getDownloadedFiles());
      Assertions.assertEquals(24, mapper.getDownloadedBytes());

      // a gap-fill file is already resident, so accessing it triggers no additional range read
      mapper.mapFile("3");
      Assertions.assertEquals(1, rangeReader.getReadCount());

      // a file outside the span was never fetched, so it still downloads on demand
      mapper.mapFile("9");
      Assertions.assertEquals(2, rangeReader.getReadCount());
    }
  }

  @Test
  void testOnRangeReadReportsGapFillBytes() throws IOException
  {
    final File segmentFile = buildTestSegment(10, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("gapfill");
    final DirectoryBackedRangeReader rangeReader = new DirectoryBackedRangeReader(segmentFile.getParentFile());

    // capture (bytes, gapFillBytes) for each range read; onRangeRead does not fire for the header fetch
    final List<long[]> reads = new ArrayList<>();
    final PartialSegmentDownloadListener listener = new PartialSegmentDownloadListener()
    {
      @Override
      public void onBytesDownloaded(long bytes)
      {
        // not under test
      }

      @Override
      public void onRangeRead(long bytes, long gapFillBytes, long nanos)
      {
        reads.add(new long[]{bytes, gapFillBytes});
      }
    };

    try (PartialSegmentFileMapperV10 mapper = PartialSegmentFileMapperV10.createForFile(
        rangeReader,
        JSON_MAPPER,
        cacheDir,
        IndexIO.V10_FILE_NAME,
        listener
    )) {
      mapper.ensureFilesAvailable(Set.of("2", "5", "7"));
    }

    // one coalesced read covers files 2..7 (24 bytes); only 2,5,7 (12 bytes) were requested, so 12 bytes are over-fetch
    Assertions.assertEquals(1, reads.size());
    Assertions.assertEquals(24, reads.get(0)[0]);
    Assertions.assertEquals(12, reads.get(0)[1]);
  }

  @Test
  void testSparseContainerFiles() throws IOException
  {
    final File segmentFile = buildTestSegment(10, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("sparse");

    final DirectoryBackedRangeReader rangeReader = new DirectoryBackedRangeReader(segmentFile.getParentFile());

    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir)) {
      // download just 2 files
      mapper.mapFile("3");
      mapper.mapFile("7");

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
  void testConcurrentMapFile() throws Exception
  {
    final File segmentFile = buildTestSegment(20, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("concurrent");

    final CountingRangeReader rangeReader = new CountingRangeReader(segmentFile.getParentFile());

    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir)) {
      rangeReader.resetCount();

      final int numThreads = 8;
      final ExecutorService exec = Execs.multiThreaded(numThreads, "lazy-test-%d");
      try {
        final CountDownLatch startLatch = new CountDownLatch(1);
        final List<Future<Void>> futures = new ArrayList<>();

        // all threads try to access all 20 files concurrently
        for (int t = 0; t < numThreads; t++) {
          futures.add(exec.submit(() -> {
            startLatch.await();
            for (int i = 0; i < 20; ++i) {
              ByteBuffer buf = mapper.mapFile(String.valueOf(i));
              Assertions.assertNotNull(buf);
              Assertions.assertEquals(4, buf.remaining());
              Assertions.assertEquals(i, buf.getInt());
            }
            return null;
          }));
        }

        startLatch.countDown();

        for (Future<Void> f : futures) {
          f.get();
        }

        // each file should have been downloaded exactly once despite concurrent access
        Assertions.assertEquals(20, rangeReader.getReadCount());
        Assertions.assertEquals(80, mapper.getDownloadedBytes());
      }
      finally {
        exec.shutdownNow();
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
      for (int i = 0; i < 5; ++i) {
        ByteBuffer buf = mapper.mapFile(StringUtils.format("myProjection/col_%d", i));
        Assertions.assertNotNull(buf);
        Assertions.assertEquals(i * 100, buf.getInt());
      }
    }
  }

  @Test
  void testMatchesEagerMapper() throws IOException
  {
    // verify that the lazy mapper produces identical ByteBuffer contents as the eager mapper
    final File segmentFile = buildTestSegment(20, CompressionStrategy.NONE);
    final File cacheDir = newCacheDir("match_eager");

    final DirectoryBackedRangeReader rangeReader = new DirectoryBackedRangeReader(segmentFile.getParentFile());
    try (SegmentFileMapperV10 eager = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER);
         PartialSegmentFileMapperV10 lazy = createMapper(rangeReader, cacheDir)
    ) {
      Assertions.assertEquals(eager.getInternalFilenames(), lazy.getInternalFilenames());
      for (String name : eager.getInternalFilenames()) {
        ByteBuffer eagerBuf = eager.mapFile(name);
        ByteBuffer lazyBuf = lazy.mapFile(name);
        Assertions.assertNotNull(eagerBuf);
        Assertions.assertNotNull(lazyBuf);
        Assertions.assertEquals(eagerBuf, lazyBuf);
      }
    }

  }

  @Test
  void testExternalFiles() throws IOException
  {
    final String externalName = "external.segment";
    final File baseDir = new File(tempDir, "base_" + ThreadLocalRandom.current().nextInt());
    FileUtils.mkdirp(baseDir);

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      for (int i = 0; i < 10; ++i) {
        File tmpFile = new File(tempDir, StringUtils.format("main-%s.bin", i));
        Files.write(Ints.toByteArray(i), tmpFile);
        builder.add(StringUtils.format("%d", i), tmpFile);
      }
      SegmentFileBuilder external = builder.getExternalBuilder(externalName);
      for (int i = 10; i < 20; ++i) {
        File tmpFile = new File(tempDir, StringUtils.format("ext-%s.bin", i));
        Files.write(Ints.toByteArray(i), tmpFile);
        external.add(StringUtils.format("%d", i), tmpFile);
      }
    }

    final File cacheDir = newCacheDir("ext");
    final DirectoryBackedRangeReader rangeReader = new DirectoryBackedRangeReader(baseDir);

    try (PartialSegmentFileMapperV10 mapper = createMapperWithExternal(rangeReader, cacheDir, externalName)) {
      // verify main file internal files
      for (int i = 0; i < 10; ++i) {
        ByteBuffer buf = mapper.mapFile(String.valueOf(i));
        Assertions.assertNotNull(buf);
        Assertions.assertEquals(i, buf.getInt());
      }

      // verify external file internal files via mapExternalFile
      for (int i = 10; i < 20; ++i) {
        ByteBuffer buf = mapper.mapExternalFile(externalName, String.valueOf(i));
        Assertions.assertNotNull(buf);
        Assertions.assertEquals(i, buf.getInt());
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

    // fetches from range reader and persists header + bitmap
    try (PartialSegmentFileMapperV10 mapper = createMapper(rangeReader, cacheDir)) {
      mapper.mapFile("2");
      mapper.mapFile("5");
      mapper.mapFile("8");
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

    // create with externals, download some files
    try (PartialSegmentFileMapperV10 mapper = createMapperWithExternal(rangeReader, cacheDir, externalName)) {
      mapper.mapFile("1");
      mapper.mapExternalFile(externalName, "7");
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
      mapper.mapFile("3");
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
      for (String name : eager.getInternalFilenames()) {
        Assertions.assertEquals(eager.mapFile(name), lazy.mapFile(name), "file[" + name + "]");
      }
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
    return PartialSegmentFileMapperV10.createForFile(
        rangeReader,
        JSON_MAPPER,
        localCacheDir,
        IndexIO.V10_FILE_NAME,
        PartialSegmentDownloadListener.NOOP
    );
  }

  private static PartialSegmentFileMapperV10 createMapper(
      SegmentRangeReader rangeReader,
      File localCacheDir,
      PartialSegmentFileMapperV10.CoalesceConfig coalesceConfig
  ) throws IOException
  {
    return PartialSegmentFileMapperV10.createForFile(
        rangeReader,
        JSON_MAPPER,
        localCacheDir,
        IndexIO.V10_FILE_NAME,
        PartialSegmentDownloadListener.NOOP,
        coalesceConfig
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
        PartialSegmentDownloadListener.NOOP
    );
  }

}
