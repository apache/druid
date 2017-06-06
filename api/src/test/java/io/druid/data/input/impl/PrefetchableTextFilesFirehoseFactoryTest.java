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

package io.druid.data.input.impl;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.data.input.Firehose;
import io.druid.data.input.Row;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.hamcrest.CoreMatchers;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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
  private static File testDir;
  private static File firehoseTempDir;

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
    testDir = File.createTempFile(PrefetchableTextFilesFirehoseFactoryTest.class.getSimpleName(), "testDir");
    FileUtils.forceDelete(testDir);
    FileUtils.forceMkdir(testDir);

    firehoseTempDir = File.createTempFile(PrefetchableTextFilesFirehoseFactoryTest.class.getSimpleName(), "baseDir");
    FileUtils.forceDelete(firehoseTempDir);
    FileUtils.forceMkdir(firehoseTempDir);

    for (int i = 0; i < 10; i++) {
      // Each file is 1390 bytes
      try (final Writer writer = Files.newBufferedWriter(new File(testDir, "test_" + i).toPath(), StandardCharsets.UTF_8)) {
        for (int j = 0; j < 100; j++) {
          final String a = (20171220 + i) + "," + i + "," + j + "\n";
          writer.write(a);
        }
      }
    }
  }

  @AfterClass
  public static void teardown() throws IOException
  {
    FileUtils.forceDelete(testDir);
    FileUtils.forceDelete(firehoseTempDir);
  }

  private static void assertResult(List<Row> rows)
  {
    Assert.assertEquals(1000, rows.size());
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

    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 100; j++) {
        final Row row = rows.get(i * 100 + j);
        Assert.assertEquals(new DateTime(20171220 + i), row.getTimestamp());
        Assert.assertEquals(String.valueOf(i), row.getDimension("a").get(0));
        Assert.assertEquals(String.valueOf(j), row.getDimension("b").get(0));
      }
    }
  }

  @Test
  public void testWithoutCacheAndFetch() throws IOException
  {
    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.with(testDir, 0, 0);

    final List<Row> rows = new ArrayList<>();
    try (Firehose firehose = factory.connect(parser, firehoseTempDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }

    assertResult(rows);
  }

  @Test
  public void testWithoutCache() throws IOException
  {
    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.with(testDir, 0, 2048);

    final List<Row> rows = new ArrayList<>();
    try (Firehose firehose = factory.connect(parser, firehoseTempDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }

    assertResult(rows);
  }

  @Test
  public void testWithZeroFetchCapacity() throws IOException
  {
    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.with(testDir, 2048, 0);

    final List<Row> rows = new ArrayList<>();
    try (Firehose firehose = factory.connect(parser, firehoseTempDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }

    assertResult(rows);
  }

  @Test
  public void testWithCacheAndFetch() throws IOException
  {
    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.of(testDir);

    final List<Row> rows = new ArrayList<>();
    try (Firehose firehose = factory.connect(parser, firehoseTempDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }

    assertResult(rows);
  }

  @Test
  public void testWithLargeCacheAndSmallFetch() throws IOException
  {
    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.with(testDir, 2048, 1024);

    final List<Row> rows = new ArrayList<>();
    try (Firehose firehose = factory.connect(parser, firehoseTempDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }

    assertResult(rows);
  }

  @Test
  public void testWithSmallCacheAndLargeFetch() throws IOException
  {
    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.with(testDir, 1024, 2048);

    final List<Row> rows = new ArrayList<>();
    try (Firehose firehose = factory.connect(parser, firehoseTempDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }

    assertResult(rows);
  }

  @Test
  public void testRetry() throws IOException
  {
    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.withOpenExceptions(testDir, 1);

    final List<Row> rows = new ArrayList<>();
    try (Firehose firehose = factory.connect(parser, firehoseTempDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }

    assertResult(rows);
  }

  @Test
  public void testMaxRetry() throws IOException
  {
    expectedException.expect(RuntimeException.class);
    expectedException.expectCause(CoreMatchers.instanceOf(ExecutionException.class));
    expectedException.expectMessage("Exception for retry test");

    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.withOpenExceptions(testDir, 5);

    final List<Row> rows = new ArrayList<>();
    try (Firehose firehose = factory.connect(parser, firehoseTempDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }
  }

  @Test
  public void testTimeout() throws IOException
  {
    expectedException.expect(RuntimeException.class);
    expectedException.expectCause(CoreMatchers.instanceOf(TimeoutException.class));

    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.withSleepMillis(testDir, 1000);

    final List<Row> rows = new ArrayList<>();
    try (Firehose firehose = factory.connect(parser, firehoseTempDir)) {
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
    }
  }

  @Test
  public void testReconnect() throws IOException
  {
    final TestPrefetchableTextFilesFirehoseFactory factory =
        TestPrefetchableTextFilesFirehoseFactory.of(testDir);

    for (int i = 0; i < 5; i++) {
      final List<Row> rows = new ArrayList<>();
      try (Firehose firehose = factory.connect(parser, firehoseTempDir)) {
        while (firehose.hasMore()) {
          rows.add(firehose.nextRow());
        }
      }
      assertResult(rows);
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
