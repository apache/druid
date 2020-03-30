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

package org.apache.druid.data.input.impl.prefetch;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.io.CountingOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.SocketException;
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
  private static long FILE_SIZE = -1;

  private static final StringInputRowParser PARSER = new StringInputRowParser(
      new CSVParseSpec(
          new TimestampSpec(
              "timestamp",
              "auto",
              null
          ),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(Arrays.asList("timestamp", "a", "b")),
              new ArrayList<>(),
              new ArrayList<>()
          ),
          ",",
          Arrays.asList("timestamp", "a", "b"),
          false,
          0
      ),
      StandardCharsets.UTF_8.name()
  );

  @ClassRule
  public static TemporaryFolder tempDir = new TemporaryFolder();
  private static File TEST_DIR;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void setup() throws IOException
  {
    NullHandling.initializeForTests();
    TEST_DIR = tempDir.newFolder();
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

  private static File createFirehoseTmpDir(String dirPrefix) throws IOException
  {
    return Files.createTempDirectory(tempDir.getRoot().toPath(), dirPrefix).toFile();
  }

  @Test
  public void testWithoutCacheAndFetch() throws IOException
  {
    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.with(TEST_DIR, 0, 0);

    final List<Row> rows = new ArrayList<>();
    final File firehoseTmpDir = createFirehoseTmpDir("testWithoutCacheAndFetch");
    try (Firehose firehose = factory.connect(PARSER, firehoseTmpDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }

    Assert.assertEquals(0, factory.getCacheManager().getTotalCachedBytes());
    assertResult(rows);
    assertNumRemainingCacheFiles(firehoseTmpDir, 0);
  }

  @Test
  public void testWithoutCacheAndFetchAgainstConnectionReset() throws IOException
  {
    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.withConnectionResets(TEST_DIR, 0, 0, 2);

    final List<Row> rows = new ArrayList<>();
    final File firehoseTmpDir = createFirehoseTmpDir("testWithoutCacheAndFetch");
    try (Firehose firehose = factory.connect(PARSER, firehoseTmpDir)) {
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
    try (Firehose firehose = factory.connect(PARSER, firehoseTmpDir)) {
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
    try (Firehose firehose = factory.connect(PARSER, firehoseTmpDir)) {
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
    try (Firehose firehose = factory.connect(PARSER, firehoseTmpDir)) {
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
    try (Firehose firehose = factory.connect(PARSER, firehoseTmpDir)) {
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
    try (Firehose firehose = factory.connect(PARSER, firehoseTmpDir)) {
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
    try (Firehose firehose = factory.connect(PARSER, firehoseTmpDir)) {
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

    try (Firehose firehose = factory.connect(PARSER, createFirehoseTmpDir("testMaxRetry"))) {
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

    try (Firehose firehose = factory.connect(PARSER, createFirehoseTmpDir("testTimeout"))) {
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
      try (Firehose firehose = factory.connect(PARSER, firehoseTmpDir)) {
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
      try (Firehose firehose = factory.connect(PARSER, firehoseTmpDir)) {
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
    private final long sleepMillis;
    private final File baseDir;
    private int numOpenExceptions;
    private int maxConnectionResets;

    static TestPrefetchableTextFilesFirehoseFactory with(File baseDir, long cacheCapacity, long fetchCapacity)
    {
      return new TestPrefetchableTextFilesFirehoseFactory(
          baseDir,
          1024,
          cacheCapacity,
          fetchCapacity,
          60_000, // fetch timeout
          3,
          0,
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
          3,
          0,
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
          3,
          count,
          0,
          0
      );
    }

    static TestPrefetchableTextFilesFirehoseFactory withConnectionResets(
        File baseDir,
        long cacheCapacity,
        long fetchCapacity,
        int numConnectionResets
    )
    {
      return new TestPrefetchableTextFilesFirehoseFactory(
          baseDir,
          fetchCapacity / 2,
          cacheCapacity,
          fetchCapacity,
          3,
          0,
          numConnectionResets,
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
          0,
          ms
      );
    }

    private static long computeTimeout(int maxRetry)
    {
      // See RetryUtils.nextRetrySleepMillis()
      final double maxFuzzyMultiplier = 2.;
      return (long) Math.min(
          RetryUtils.MAX_SLEEP_MILLIS,
          RetryUtils.BASE_SLEEP_MILLIS * Math.pow(2, maxRetry - 1) * maxFuzzyMultiplier
      );
    }

    TestPrefetchableTextFilesFirehoseFactory(
        File baseDir,
        long prefetchTriggerThreshold,
        long maxCacheCapacityBytes,
        long maxFetchCapacityBytes,
        int maxRetry,
        int numOpenExceptions,
        int numConnectionResets,
        long sleepMillis
    )
    {
      this(
          baseDir,
          prefetchTriggerThreshold,
          maxCacheCapacityBytes,
          maxFetchCapacityBytes,
          computeTimeout(maxRetry),
          maxRetry,
          numOpenExceptions,
          numConnectionResets,
          sleepMillis
      );
    }

    TestPrefetchableTextFilesFirehoseFactory(
        File baseDir,
        long prefetchTriggerThreshold,
        long maxCacheCapacityBytes,
        long maxFetchCapacityBytes,
        long fetchTimeout,
        int maxRetry,
        int numOpenExceptions,
        int maxConnectionResets,
        long sleepMillis
    )
    {
      super(
          maxCacheCapacityBytes,
          maxFetchCapacityBytes,
          prefetchTriggerThreshold,
          fetchTimeout,
          maxRetry
      );
      this.numOpenExceptions = numOpenExceptions;
      this.maxConnectionResets = maxConnectionResets;
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
      if (numOpenExceptions > 0) {
        numOpenExceptions--;
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
      return maxConnectionResets > 0 ?
             new TestInputStream(FileUtils.openInputStream(object), maxConnectionResets) :
             FileUtils.openInputStream(object);
    }

    @Override
    protected InputStream wrapObjectStream(File object, InputStream stream)
    {
      return stream;
    }

    @Override
    protected Predicate<Throwable> getRetryCondition()
    {
      return e -> e instanceof IOException;
    }

    @Override
    protected InputStream openObjectStream(File object, long start) throws IOException
    {
      if (numOpenExceptions > 0) {
        numOpenExceptions--;
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

      final InputStream in = FileUtils.openInputStream(object);
      in.skip(start);

      return maxConnectionResets > 0 ? new TestInputStream(in, maxConnectionResets) : in;
    }

    private int readCount;
    private int numConnectionResets;

    @Override
    public FiniteFirehoseFactory<StringInputRowParser, File> withSplit(InputSplit<File> split)
    {
      throw new UnsupportedOperationException();
    }

    private class TestInputStream extends InputStream
    {
      private static final int NUM_READ_COUNTS_BEFORE_ERROR = 10;
      private final InputStream delegate;
      private final int maxConnectionResets;

      TestInputStream(
          InputStream delegate,
          int maxConnectionResets
      )
      {
        this.delegate = delegate;
        this.maxConnectionResets = maxConnectionResets;
      }

      @Override
      public int read() throws IOException
      {
        if (readCount++ % NUM_READ_COUNTS_BEFORE_ERROR == 0) {
          if (numConnectionResets++ < maxConnectionResets) {
            // Simulate connection reset
            throw new SocketException("Test Connection reset");
          }
        }
        return delegate.read();
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException
      {
        if (readCount++ % NUM_READ_COUNTS_BEFORE_ERROR == 0) {
          if (numConnectionResets++ < maxConnectionResets) {
            // Simulate connection reset
            throw new SocketException("Test Connection reset");
          }
        }
        return delegate.read(b, off, len);
      }
    }
  }
}
