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

package org.apache.druid.indexing.firehose;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SegmentLoaderFactory;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class IngestSegmentFirehoseFactoryTimelineTest
{
  private static final String DATA_SOURCE = "foo";
  private static final String TIME_COLUMN = "t";
  private static final String[] DIMENSIONS = new String[]{"d1"};
  private static final String[] METRICS = new String[]{"m1"};

  // Must decorate the parser, since IngestSegmentFirehoseFactory will undecorate it.
  private static final InputRowParser<Map<String, Object>> ROW_PARSER = TransformSpec.NONE.decorate(
      new MapInputRowParser(
          new JSONParseSpec(
              new TimestampSpec(TIME_COLUMN, "auto", null),
              new DimensionsSpec(
                  DimensionsSpec.getDefaultSchemas(Arrays.asList(DIMENSIONS)),
                  null,
                  null
              ),
              null,
              null
          )
      )
  );

  private final IngestSegmentFirehoseFactory factory;
  private final File tmpDir;
  private final int expectedCount;
  private final long expectedSum;
  private final int segmentCount;

  private static final ObjectMapper MAPPER;
  private static final IndexIO INDEX_IO;
  private static final IndexMergerV9 INDEX_MERGER_V9;

  static {
    TestUtils testUtils = new TestUtils();
    MAPPER = IngestSegmentFirehoseFactoryTest.setupInjectablesInObjectMapper(testUtils.getTestObjectMapper());
    INDEX_IO = testUtils.getTestIndexIO();
    INDEX_MERGER_V9 = testUtils.getTestIndexMergerV9();
  }

  public IngestSegmentFirehoseFactoryTimelineTest(
      String name,
      IngestSegmentFirehoseFactory factory,
      File tmpDir,
      int expectedCount,
      long expectedSum,
      int segmentCount
  )
  {
    this.factory = factory;
    this.tmpDir = tmpDir;
    this.expectedCount = expectedCount;
    this.expectedSum = expectedSum;
    this.segmentCount = segmentCount;
  }

  @Test
  public void test() throws Exception
  {
    // Junit 4.12 doesn't have a good way to run tearDown after multiple tests in a Parameterized
    // class run. (Junit 4.13 adds @AfterParam but isn't released yet.) Fake it by just running
    // "tests" in series inside one @Test.
    testSimple();
    testSplit();
  }

  private void testSimple() throws Exception
  {
    int count = 0;
    long sum = 0;

    try (final Firehose firehose = factory.connect(ROW_PARSER, tmpDir)) {
      while (firehose.hasMore()) {
        final InputRow row = firehose.nextRow();
        count++;
        sum += row.getMetric(METRICS[0]).longValue();
      }
    }

    Assert.assertEquals("count", expectedCount, count);
    Assert.assertEquals("sum", expectedSum, sum);
  }

  private void testSplit() throws Exception
  {
    Assert.assertTrue(factory.isSplittable());
    final int numSplits = factory.getNumSplits(null);
    // We set maxInputSegmentBytesPerSplit to 2 so each segment should become a byte.
    Assert.assertEquals(segmentCount, numSplits);
    final List<InputSplit<List<WindowedSegmentId>>> splits =
        factory.getSplits(null).collect(Collectors.toList());
    Assert.assertEquals(numSplits, splits.size());

    int count = 0;
    long sum = 0;

    for (InputSplit<List<WindowedSegmentId>> split : splits) {
      final FiniteFirehoseFactory<InputRowParser, List<WindowedSegmentId>> splitFactory =
          factory.withSplit(split);
      try (final Firehose firehose = splitFactory.connect(ROW_PARSER, tmpDir)) {
        while (firehose.hasMore()) {
          final InputRow row = firehose.nextRow();
          count++;
          sum += row.getMetric(METRICS[0]).longValue();
        }
      }
    }

    Assert.assertEquals("count", expectedCount, count);
    Assert.assertEquals("sum", expectedSum, sum);

  }

  @After
  public void tearDown() throws Exception
  {
    FileUtils.deleteDirectory(tmpDir);
  }

  private static TestCase tc(
      String intervalString,
      int expectedCount,
      long expectedSum,
      DataSegmentMaker... segmentMakers
  )
  {
    final File tmpDir = Files.createTempDir();
    final Set<DataSegment> segments = new HashSet<>();
    for (DataSegmentMaker segmentMaker : segmentMakers) {
      segments.add(segmentMaker.make(tmpDir));
    }

    return new TestCase(
        tmpDir,
        Intervals.of(intervalString),
        expectedCount,
        expectedSum,
        segments
    );
  }

  private static DataSegmentMaker ds(
      String intervalString,
      String version,
      int partitionNum,
      InputRow... rows
  )
  {
    return new DataSegmentMaker(Intervals.of(intervalString), version, partitionNum, Arrays.asList(rows));
  }

  private static InputRow ir(String timeString, long metricValue)
  {
    return new MapBasedInputRow(
        DateTimes.of(timeString).getMillis(),
        Arrays.asList(DIMENSIONS),
        ImmutableMap.of(
            TIME_COLUMN, DateTimes.of(timeString).toString(),
            DIMENSIONS[0], "bar",
            METRICS[0], metricValue
        )
    );
  }

  private static Map<String, Object> persist(File tmpDir, InputRow... rows)
  {
    final File persistDir = new File(tmpDir, UUID.randomUUID().toString());
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(JodaUtils.MIN_INSTANT)
        .withDimensionsSpec(ROW_PARSER)
        .withMetrics(new LongSumAggregatorFactory(METRICS[0], METRICS[0]))
        .build();
    final IncrementalIndex index = new IncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(rows.length)
        .buildOnheap();

    for (InputRow row : rows) {
      try {
        index.add(row);
      }
      catch (IndexSizeExceededException e) {
        throw new RuntimeException(e);
      }
    }

    try {
      INDEX_MERGER_V9.persist(index, persistDir, new IndexSpec(), null);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    return ImmutableMap.of(
        "type", "local",
        "path", persistDir.getAbsolutePath()
    );
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder()
  {
    final List<TestCase> testCases = ImmutableList.of(
        tc(
            "2000/2000T02", 3, 7,
            ds("2000/2000T01", "v1", 0, ir("2000", 1), ir("2000T00:01", 2)),
            ds("2000T01/2000T02", "v1", 0, ir("2000T01", 4))
        ) /* Adjacent segments */,
        tc(
            "2000/2000T02", 3, 7,
            ds("2000/2000T02", "v1", 0, ir("2000", 1), ir("2000T00:01", 2), ir("2000T01", 8)),
            ds("2000T01/2000T02", "v2", 0, ir("2000T01:01", 4))
        ) /* 1H segment overlaid on top of 2H segment */,
        tc(
            "2000/2000-01-02", 4, 23,
            ds("2000/2000-01-02", "v1", 0, ir("2000", 1), ir("2000T00:01", 2), ir("2000T01", 8), ir("2000T02", 16)),
            ds("2000T01/2000T02", "v2", 0, ir("2000T01:01", 4))
        ) /* 1H segment overlaid on top of 1D segment */,
        tc(
            "2000/2000T02", 4, 15,
            ds("2000/2000T02", "v1", 0, ir("2000", 1), ir("2000T00:01", 2), ir("2000T01", 8)),
            ds("2000/2000T02", "v1", 1, ir("2000T01:01", 4))
        ) /* Segment set with two segments for the same interval */,
        tc(
            "2000T01/2000T02", 1, 2,
            ds("2000/2000T03", "v1", 0, ir("2000", 1), ir("2000T01", 2), ir("2000T02", 4))
        ) /* Segment wider than desired interval */,
        tc(
            "2000T02/2000T04", 2, 12,
            ds("2000/2000T03", "v1", 0, ir("2000", 1), ir("2000T01", 2), ir("2000T02", 4)),
            ds("2000T03/2000T04", "v1", 0, ir("2000T03", 8))
        ) /* Segment intersecting desired interval */
    );

    final List<Object[]> constructors = new ArrayList<>();

    for (final TestCase testCase : testCases) {
      SegmentHandoffNotifierFactory notifierFactory = EasyMock.createNiceMock(SegmentHandoffNotifierFactory.class);
      EasyMock.replay(notifierFactory);
      final SegmentLoaderFactory slf = new SegmentLoaderFactory(null, MAPPER);
      final RetryPolicyFactory retryPolicyFactory = new RetryPolicyFactory(new RetryPolicyConfig());
      final CoordinatorClient cc = new CoordinatorClient(null, null)
      {
        @Override
        public List<DataSegment> getDatabaseSegmentDataSourceSegments(String dataSource, List<Interval> intervals)
        {
          // Expect the interval we asked for
          if (intervals.equals(ImmutableList.of(testCase.interval))) {
            return ImmutableList.copyOf(testCase.segments);
          } else {
            throw new IllegalArgumentException("WTF");
          }
        }

        @Override
        public DataSegment getDatabaseSegmentDataSourceSegment(String dataSource, String segmentId)
        {
          return testCase.segments
              .stream()
              .filter(s -> s.getId().toString().equals(segmentId))
              .findAny()
              .get();  // throwing if not found is exactly what the real code does
        }
      };
      final IngestSegmentFirehoseFactory factory = new IngestSegmentFirehoseFactory(
          DATA_SOURCE,
          testCase.interval,
          null,
          new TrueDimFilter(),
          Arrays.asList(DIMENSIONS),
          Arrays.asList(METRICS),
          // Split as much as possible
          1L,
          INDEX_IO,
          cc,
          slf,
          retryPolicyFactory
      );

      constructors.add(
          new Object[]{
              testCase.toString(),
              factory,
              testCase.tmpDir,
              testCase.expectedCount,
              testCase.expectedSum,
              testCase.segments.size()
          }
      );
    }

    return constructors;
  }

  private static class TestCase
  {
    final File tmpDir;
    final Interval interval;
    final int expectedCount;
    final long expectedSum;
    final Set<DataSegment> segments;

    public TestCase(
        File tmpDir,
        Interval interval,
        int expectedCount,
        long expectedSum,
        Set<DataSegment> segments
    )
    {
      this.tmpDir = tmpDir;
      this.interval = interval;
      this.expectedCount = expectedCount;
      this.expectedSum = expectedSum;
      this.segments = segments;
    }

    @Override
    public String toString()
    {
      final List<SegmentId> segmentIds = new ArrayList<>();
      for (DataSegment segment : segments) {
        segmentIds.add(segment.getId());
      }
      return "TestCase{" +
             "interval=" + interval +
             ", expectedCount=" + expectedCount +
             ", expectedSum=" + expectedSum +
             ", segments=" + segmentIds +
             '}';
    }
  }

  private static class DataSegmentMaker
  {
    final Interval interval;
    final String version;
    final int partitionNum;
    final List<InputRow> rows;

    public DataSegmentMaker(
        Interval interval,
        String version,
        int partitionNum,
        List<InputRow> rows
    )
    {
      this.interval = interval;
      this.version = version;
      this.partitionNum = partitionNum;
      this.rows = rows;
    }

    public DataSegment make(File tmpDir)
    {
      final Map<String, Object> loadSpec = persist(tmpDir, Iterables.toArray(rows, InputRow.class));

      return new DataSegment(
          DATA_SOURCE,
          interval,
          version,
          loadSpec,
          Arrays.asList(DIMENSIONS),
          Arrays.asList(METRICS),
          new LinearShardSpec(partitionNum),
          -1,
          2L
      );
    }
  }
}
