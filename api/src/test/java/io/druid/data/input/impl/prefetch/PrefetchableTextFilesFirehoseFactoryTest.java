/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.data.input.impl.prefetch;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.CountingOutputStream;
import io.druid.data.input.Firehose;
import io.druid.data.input.Row;
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.StringUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class PrefetchableTextFilesFirehoseFactoryTest
{
  private static final List<File> FIREHOSE_TMP_DIRS = new ArrayList<>();
  private static File TEST_DIR;
  private static long FILE_SIZE = -1;

  private static final StringInputRowParser parser = new StringInputRowParser(
      new CSVParseSpec(
          new TimestampSpec(
              "timestamp",
              "auto",
              null
          ),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(Arrays.asList("timestamp", "a", "b")),
              Lists.newArrayList(),
              Lists.newArrayList()
          ),
          ",",
          Arrays.asList("timestamp", "a", "b"),
          false,
          0
      ),
      Charsets.UTF_8.name()
  );

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void setup() throws IOException
  {
    TEST_DIR = File.createTempFile(PrefetchableTextFilesFirehoseFactoryTest.class.getSimpleName(), "testDir");
    FileUtils.forceDelete(TEST_DIR);
    FileUtils.forceMkdir(TEST_DIR);

    for (int i = 0; i < 100; i++) {
      try (
          CountingOutputStream cos = new CountingOutputStream(
              Files.newOutputStream(new File(TEST_DIR, "test_" + i).toPath())
          );
          Writer writer = new BufferedWriter(new OutputStreamWriter(cos, StandardCharsets.UTF_8))
      ) {
        for (int j = 0; j < 100; j++) {
          final String a = StringUtils.format("%d,%03d,%03d\n", (20171220 + i), i, j);
          writer.write(a);
        }
        writer.flush();
        // Every file size must be same
        if (FILE_SIZE == -1) {
          FILE_SIZE = cos.getCount();
        } else {
          Assert.assertEquals(FILE_SIZE, cos.getCount());
        }
      }
    }
  }

  @AfterClass
  public static void teardown() throws IOException
  {
    FileUtils.forceDelete(TEST_DIR);
    for (File dir : FIREHOSE_TMP_DIRS) {
      FileUtils.forceDelete(dir);
    }
  }

  private static void assertResult(List<Row> rows)
  {
    Assert.assertEquals(10000, rows.size());
    rows.sort((r1, r2) -> {
      int c = r1.getTimestamp().compareTo(r2.getTimestamp());
      if (c != 0) {
        return c;
      }
      c = Integer.valueOf(r1.getDimension("a").get(0)).compareTo(Integer.valueOf(r2.getDimension("a").get(0)));
      if (c != 0) {
        return c;
      }

      return Integer.valueOf(r1.getDimension("b").get(0)).compareTo(Integer.valueOf(r2.getDimension("b").get(0)));
    });

    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < 100; j++) {
        final Row row = rows.get(i * 100 + j);
        Assert.assertEquals(DateTimes.utc(20171220 + i), row.getTimestamp());
        Assert.assertEquals(i, Integer.valueOf(row.getDimension("a").get(0)).intValue());
        Assert.assertEquals(j, Integer.valueOf(row.getDimension("b").get(0)).intValue());
      }
    }
  }

  private static void assertNumRemainingCacheFiles(File firehoseTmpDir, int expectedNumFiles)
  {
    final String[] files = firehoseTmpDir.list();
    Assert.assertNotNull(files);
    Assert.assertEquals(expectedNumFiles, files.length);
  }

  private static File createFirehoseTmpDir(String dirSuffix) throws IOException
  {
    final File firehoseTempDir = File.createTempFile(
        PrefetchableTextFilesFirehoseFactoryTest.class.getSimpleName(),
        dirSuffix
    );
    FileUtils.forceDelete(firehoseTempDir);
    FileUtils.forceMkdir(firehoseTempDir);
    FIREHOSE_TMP_DIRS.add(firehoseTempDir);
    return firehoseTempDir;
  }

  @Test
  public void testWithoutCacheAndFetch() throws IOException
  {
    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.with(TEST_DIR, 0, 0);

    final List<Row> rows = new ArrayList<>();
    final File firehoseTmpDir = createFirehoseTmpDir("testWithoutCacheAndFetch");
    try (Firehose firehose = factory.connect(parser, firehoseTmpDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }

    Assert.assertEquals(0, factory.getCacheManager().getTotalCachedBytes());
    assertResult(rows);
    assertNumRemainingCacheFiles(firehoseTmpDir, 0);
  }

  @Test
  public void testWithoutCache() throws IOException
  {
    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.with(TEST_DIR, 0, 2048);

    final List<Row> rows = new ArrayList<>();
    final File firehoseTmpDir = createFirehoseTmpDir("testWithoutCache");
    try (Firehose firehose = factory.connect(parser, firehoseTmpDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }

    Assert.assertEquals(0, factory.getCacheManager().getTotalCachedBytes());
    assertResult(rows);
    assertNumRemainingCacheFiles(firehoseTmpDir, 0);
  }

  @Test
  public void testWithZeroFetchCapacity() throws IOException
  {
    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.with(TEST_DIR, 2048, 0);

    final List<Row> rows = new ArrayList<>();
    final File firehoseTmpDir = createFirehoseTmpDir("testWithZeroFetchCapacity");
    try (Firehose firehose = factory.connect(parser, firehoseTmpDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }

    assertResult(rows);
    assertNumRemainingCacheFiles(firehoseTmpDir, 2);
  }

  @Test
  public void testWithCacheAndFetch() throws IOException
  {
    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.of(TEST_DIR);

    final List<Row> rows = new ArrayList<>();
    final File firehoseTmpDir = createFirehoseTmpDir("testWithCacheAndFetch");
    try (Firehose firehose = factory.connect(parser, firehoseTmpDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }

    assertResult(rows);
    assertNumRemainingCacheFiles(firehoseTmpDir, 2);
  }

  @Test
  public void testWithLargeCacheAndSmallFetch() throws IOException
  {
    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.with(TEST_DIR, 2048, 1024);

    final List<Row> rows = new ArrayList<>();
    final File firehoseTmpDir = createFirehoseTmpDir("testWithLargeCacheAndSmallFetch");
    try (Firehose firehose = factory.connect(parser, firehoseTmpDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }

    assertResult(rows);
    assertNumRemainingCacheFiles(firehoseTmpDir, 2);
  }

  @Test
  public void testWithSmallCacheAndLargeFetch() throws IOException
  {
    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.with(TEST_DIR, 1024, 2048);

    final List<Row> rows = new ArrayList<>();
    final File firehoseTmpDir = createFirehoseTmpDir("testWithSmallCacheAndLargeFetch");
    try (Firehose firehose = factory.connect(parser, firehoseTmpDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }

    assertResult(rows);
    assertNumRemainingCacheFiles(firehoseTmpDir, 1);
  }

  @Test
  public void testRetry() throws IOException
  {
    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.withOpenExceptions(TEST_DIR, 1);

    final List<Row> rows = new ArrayList<>();
    final File firehoseTmpDir = createFirehoseTmpDir("testRetry");
    try (Firehose firehose = factory.connect(parser, firehoseTmpDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }

    assertResult(rows);
    assertNumRemainingCacheFiles(firehoseTmpDir, 2);
  }

  @Test
  public void testMaxRetry() throws IOException
  {
    expectedException.expect(RuntimeException.class);
    expectedException.expectCause(CoreMatchers.instanceOf(ExecutionException.class));
    expectedException.expectMessage("Exception for retry test");

    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.withOpenExceptions(TEST_DIR, 5);

    try (Firehose firehose = factory.connect(parser, createFirehoseTmpDir("testMaxRetry"))) {
      while (firehose.hasMore()) {
        firehose.nextRow();
      }
    }
  }

  @Test
  public void testTimeout() throws IOException
  {
    expectedException.expect(RuntimeException.class);
    expectedException.expectCause(CoreMatchers.instanceOf(TimeoutException.class));

    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.withSleepMillis(TEST_DIR, 1000);

    try (Firehose firehose = factory.connect(parser, createFirehoseTmpDir("testTimeout"))) {
      while (firehose.hasMore()) {
        firehose.nextRow();
      }
    }
  }

  @Test
  public void testReconnectWithCacheAndPrefetch() throws IOException
  {
    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.of(TEST_DIR);
    final File firehoseTmpDir = createFirehoseTmpDir("testReconnectWithCacheAndPrefetch");

    for (int i = 0; i < 5; i++) {
      final List<Row> rows = new ArrayList<>();
      try (Firehose firehose = factory.connect(parser, firehoseTmpDir)) {
        if (i > 0) {
          Assert.assertEquals(FILE_SIZE * 2, factory.getCacheManager().getTotalCachedBytes());
        }

        while (firehose.hasMore()) {
          rows.add(firehose.nextRow());
        }
      }
      assertResult(rows);
      assertNumRemainingCacheFiles(firehoseTmpDir, 2);
    }
  }

  @Test
  public void testReconnectWithCache() throws IOException
  {
    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.with(TEST_DIR, 2048, 0);
    final File firehoseTmpDir = createFirehoseTmpDir("testReconnectWithCache");

    for (int i = 0; i < 5; i++) {
      final List<Row> rows = new ArrayList<>();
      try (Firehose firehose = factory.connect(parser, firehoseTmpDir)) {
        if (i > 0) {
          Assert.assertEquals(FILE_SIZE * 2, factory.getCacheManager().getTotalCachedBytes());
        }

        while (firehose.hasMore()) {
          rows.add(firehose.nextRow());
        }
      }
      assertResult(rows);
      assertNumRemainingCacheFiles(firehoseTmpDir, 2);
    }
  }

  static class TestPrefetchableTextFilesFirehoseFactory extends PrefetchableTextFilesFirehoseFactory<File>
  {
    private static final long defaultTimeout = 1000;
    private final long sleepMillis;
    private final File baseDir;
    private int openExceptionCount;

    static TestPrefetchableTextFilesFirehoseFactory with(File baseDir, long cacheCapacity, long fetchCapacity)
    {
      return new TestPrefetchableTextFilesFirehoseFactory(
          baseDir,
          1024,
          cacheCapacity,
          fetchCapacity,
          defaultTimeout,
          3,
          0,
          0
      );
    }

    static TestPrefetchableTextFilesFirehoseFactory of(File baseDir)
    {
      return new TestPrefetchableTextFilesFirehoseFactory(
          baseDir,
          1024,
          2048,
          2048,
          defaultTimeout,
          3,
          0,
          0
      );
    }

    static TestPrefetchableTextFilesFirehoseFactory withOpenExceptions(File baseDir, int count)
    {
      return new TestPrefetchableTextFilesFirehoseFactory(
          baseDir,
          1024,
          2048,
          2048,
          defaultTimeout,
          3,
          count,
          0
      );
    }

    static TestPrefetchableTextFilesFirehoseFactory withSleepMillis(File baseDir, long ms)
    {
      return new TestPrefetchableTextFilesFirehoseFactory(
          baseDir,
          1024,
          2048,
          2048,
          100,
          3,
          0,
          ms
      );
    }

    public TestPrefetchableTextFilesFirehoseFactory(
        File baseDir,
        long prefetchTriggerThreshold,
        long maxCacheCapacityBytes,
        long maxFetchCapacityBytes,
        long timeout,
        int maxRetry,
        int openExceptionCount,
        long sleepMillis
    )
    {
      super(
          maxCacheCapacityBytes,
          maxFetchCapacityBytes,
          prefetchTriggerThreshold,
          timeout,
          maxRetry
      );
      this.openExceptionCount = openExceptionCount;
      this.sleepMillis = sleepMillis;
      this.baseDir = baseDir;
    }

    @Override
    protected Collection<File> initObjects()
    {
      return FileUtils.listFiles(
          Preconditions.checkNotNull(baseDir).getAbsoluteFile(),
          TrueFileFilter.INSTANCE,
          TrueFileFilter.INSTANCE
      );
    }

    @Override
    protected InputStream openObjectStream(File object) throws IOException
    {
      if (openExceptionCount > 0) {
        openExceptionCount--;
        throw new IOException("Exception for retry test");
      }
      if (sleepMillis > 0) {
        try {
          Thread.sleep(sleepMillis);
        }
        catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      return FileUtils.openInputStream(object);
    }

    @Override
    protected InputStream wrapObjectStream(File object, InputStream stream) throws IOException
    {
      return stream;
    }
  }
}
