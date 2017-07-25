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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.actions.LockAcquireAction;
import io.druid.indexing.common.actions.LockListAction;
import io.druid.indexing.common.actions.SegmentAllocateAction;
import io.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import io.druid.indexing.common.actions.TaskAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import io.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import io.druid.indexing.overlord.SegmentPublishResult;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.firehose.LocalFirehoseFactory;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndexTaskTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static final ParseSpec DEFAULT_PARSE_SPEC = new CSVParseSpec(
      new TimestampSpec(
          "ts",
          "auto",
          null
      ),
      new DimensionsSpec(
          DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "dim")),
          Lists.newArrayList(),
          Lists.newArrayList()
      ),
      null,
      Arrays.asList("ts", "dim", "val"),
      false,
      0
  );

  private static final IndexSpec indexSpec = new IndexSpec();
  private final ObjectMapper jsonMapper;
  private IndexMergerV9 indexMergerV9;
  private IndexIO indexIO;
  private volatile int segmentAllocatePartitionCounter;

  public IndexTaskTest()
  {
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
    indexMergerV9 = testUtils.getTestIndexMergerV9();
    indexIO = testUtils.getTestIndexIO();
  }

  @Test
  public void testDeterminePartitions() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();

    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("2014-01-01T00:00:10Z,a,1\n");
      writer.write("2014-01-01T01:00:20Z,b,1\n");
      writer.write("2014-01-01T02:00:30Z,c,1\n");
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        createIngestionSpec(
            tmpDir,
            null,
            null,
            createTuningConfig(2, null, false, true),
            false
        ),
        null
    );

    final List<DataSegment> segments = runTask(indexTask);

    Assert.assertEquals(2, segments.size());

    Assert.assertEquals("test", segments.get(0).getDataSource());
    Assert.assertEquals(new Interval("2014/P1D"), segments.get(0).getInterval());
    Assert.assertEquals(HashBasedNumberedShardSpec.class, segments.get(0).getShardSpec().getClass());
    Assert.assertEquals(0, segments.get(0).getShardSpec().getPartitionNum());
    Assert.assertEquals(2, ((NumberedShardSpec) segments.get(0).getShardSpec()).getPartitions());

    Assert.assertEquals("test", segments.get(1).getDataSource());
    Assert.assertEquals(new Interval("2014/P1D"), segments.get(1).getInterval());
    Assert.assertEquals(HashBasedNumberedShardSpec.class, segments.get(1).getShardSpec().getClass());
    Assert.assertEquals(1, segments.get(1).getShardSpec().getPartitionNum());
    Assert.assertEquals(2, ((NumberedShardSpec) segments.get(1).getShardSpec()).getPartitions());
  }

  @Test
  public void testForceExtendableShardSpecs() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();

    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("2014-01-01T00:00:10Z,a,1\n");
      writer.write("2014-01-01T01:00:20Z,b,1\n");
      writer.write("2014-01-01T02:00:30Z,c,1\n");
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        createIngestionSpec(
            tmpDir,
            null,
            null,
            createTuningConfig(2, null, true, false),
            false
        ),
        null
    );

    Assert.assertEquals(indexTask.getId(), indexTask.getGroupId());

    final List<DataSegment> segments = runTask(indexTask);

    Assert.assertEquals(2, segments.size());

    Assert.assertEquals("test", segments.get(0).getDataSource());
    Assert.assertEquals(new Interval("2014/P1D"), segments.get(0).getInterval());
    Assert.assertEquals(NumberedShardSpec.class, segments.get(0).getShardSpec().getClass());
    Assert.assertEquals(0, segments.get(0).getShardSpec().getPartitionNum());

    Assert.assertEquals("test", segments.get(1).getDataSource());
    Assert.assertEquals(new Interval("2014/P1D"), segments.get(1).getInterval());
    Assert.assertEquals(NumberedShardSpec.class, segments.get(1).getShardSpec().getClass());
    Assert.assertEquals(1, segments.get(1).getShardSpec().getPartitionNum());
  }

  @Test
  public void testWithArbitraryGranularity() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();

    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("2014-01-01T00:00:10Z,a,1\n");
      writer.write("2014-01-01T01:00:20Z,b,1\n");
      writer.write("2014-01-01T02:00:30Z,c,1\n");
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        createIngestionSpec(
            tmpDir,
            null,
            new ArbitraryGranularitySpec(
                Granularities.MINUTE,
                Collections.singletonList(new Interval("2014/2015"))
            ),
            createTuningConfig(10, null, false, true),
            false
        ),
        null
    );

    List<DataSegment> segments = runTask(indexTask);

    Assert.assertEquals(1, segments.size());
  }

  @Test
  public void testIntervalBucketing() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();

    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("2015-03-01T07:59:59.977Z,a,1\n");
      writer.write("2015-03-01T08:00:00.000Z,b,1\n");
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        createIngestionSpec(
            tmpDir,
            null,
            new UniformGranularitySpec(
                Granularities.HOUR,
                Granularities.HOUR,
                Collections.singletonList(new Interval("2015-03-01T08:00:00Z/2015-03-01T09:00:00Z"))
            ),
            createTuningConfig(50, null, false, true),
            false
        ),
        null
    );

    final List<DataSegment> segments = runTask(indexTask);

    Assert.assertEquals(1, segments.size());
  }

  @Test
  public void testNumShardsProvided() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();
    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("2014-01-01T00:00:10Z,a,1\n");
      writer.write("2014-01-01T01:00:20Z,b,1\n");
      writer.write("2014-01-01T02:00:30Z,c,1\n");
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        createIngestionSpec(
            tmpDir,
            null,
            null,
            createTuningConfig(null, 1, false, true),
            false
        ),
        null
    );

    final List<DataSegment> segments = runTask(indexTask);

    Assert.assertEquals(1, segments.size());

    Assert.assertEquals("test", segments.get(0).getDataSource());
    Assert.assertEquals(new Interval("2014/P1D"), segments.get(0).getInterval());
    Assert.assertTrue(segments.get(0).getShardSpec().getClass().equals(NoneShardSpec.class));
    Assert.assertEquals(0, segments.get(0).getShardSpec().getPartitionNum());
  }

  @Test
  public void testAppendToExisting() throws Exception
  {
    segmentAllocatePartitionCounter = 0;
    File tmpDir = temporaryFolder.newFolder();
    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("2014-01-01T00:00:10Z,a,1\n");
      writer.write("2014-01-01T01:00:20Z,b,1\n");
      writer.write("2014-01-01T02:00:30Z,c,1\n");
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        createIngestionSpec(
            tmpDir,
            null,
            null,
            createTuningConfig(2, null, false, false),
            true
        ),
        null
    );

    Assert.assertEquals("index_append_test", indexTask.getGroupId());

    final List<DataSegment> segments = runTask(indexTask);

    Assert.assertEquals(2, segmentAllocatePartitionCounter);
    Assert.assertEquals(2, segments.size());

    Assert.assertEquals("test", segments.get(0).getDataSource());
    Assert.assertEquals(new Interval("2014/P1D"), segments.get(0).getInterval());
    Assert.assertTrue(segments.get(0).getShardSpec().getClass().equals(NumberedShardSpec.class));
    Assert.assertEquals(0, segments.get(0).getShardSpec().getPartitionNum());

    Assert.assertEquals("test", segments.get(1).getDataSource());
    Assert.assertEquals(new Interval("2014/P1D"), segments.get(1).getInterval());
    Assert.assertTrue(segments.get(1).getShardSpec().getClass().equals(NumberedShardSpec.class));
    Assert.assertEquals(1, segments.get(1).getShardSpec().getPartitionNum());
  }

  @Test
  public void testIntervalNotSpecified() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();
    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("2014-01-01T00:00:10Z,a,1\n");
      writer.write("2014-01-01T01:00:20Z,b,1\n");
      writer.write("2014-01-01T02:00:30Z,c,1\n");
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        createIngestionSpec(
            tmpDir,
            null,
            new UniformGranularitySpec(
                Granularities.HOUR,
                Granularities.MINUTE,
                null
            ),
            createTuningConfig(2, null, false, true),
            false
        ),
        null
    );

    final List<DataSegment> segments = runTask(indexTask);

    Assert.assertEquals(3, segments.size());

    Assert.assertEquals("test", segments.get(0).getDataSource());
    Assert.assertEquals(new Interval("2014-01-01T00/PT1H"), segments.get(0).getInterval());
    Assert.assertTrue(segments.get(0).getShardSpec().getClass().equals(NoneShardSpec.class));
    Assert.assertEquals(0, segments.get(0).getShardSpec().getPartitionNum());

    Assert.assertEquals("test", segments.get(1).getDataSource());
    Assert.assertEquals(new Interval("2014-01-01T01/PT1H"), segments.get(1).getInterval());
    Assert.assertTrue(segments.get(1).getShardSpec().getClass().equals(NoneShardSpec.class));
    Assert.assertEquals(0, segments.get(1).getShardSpec().getPartitionNum());

    Assert.assertEquals("test", segments.get(2).getDataSource());
    Assert.assertEquals(new Interval("2014-01-01T02/PT1H"), segments.get(2).getInterval());
    Assert.assertTrue(segments.get(2).getShardSpec().getClass().equals(NoneShardSpec.class));
    Assert.assertEquals(0, segments.get(2).getShardSpec().getPartitionNum());
  }

  @Test
  public void testCSVFileWithHeader() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();

    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("time,d,val\n");
      writer.write("2014-01-01T00:00:10Z,a,1\n");
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        createIngestionSpec(
            tmpDir,
            new CSVParseSpec(
                new TimestampSpec(
                    "time",
                    "auto",
                    null
                ),
                new DimensionsSpec(
                    null,
                    Lists.<String>newArrayList(),
                    Lists.<SpatialDimensionSchema>newArrayList()
                ),
                null,
                null,
                true,
                0
            ),
            null,
            createTuningConfig(2, null, false, true),
            false
        ),
        null
    );

    final List<DataSegment> segments = runTask(indexTask);

    Assert.assertEquals(1, segments.size());

    Assert.assertEquals(Arrays.asList("d"), segments.get(0).getDimensions());
    Assert.assertEquals(Arrays.asList("val"), segments.get(0).getMetrics());
    Assert.assertEquals(new Interval("2014/P1D"), segments.get(0).getInterval());
  }

  @Test
  public void testCSVFileWithHeaderColumnOverride() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();

    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("time,d,val\n");
      writer.write("2014-01-01T00:00:10Z,a,1\n");
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        createIngestionSpec(
            tmpDir,
            new CSVParseSpec(
                new TimestampSpec(
                    "time",
                    "auto",
                    null
                ),
                new DimensionsSpec(
                    null,
                    Lists.<String>newArrayList(),
                    Lists.<SpatialDimensionSchema>newArrayList()
                ),
                null,
                Arrays.asList("time", "dim", "val"),
                true,
                0
            ),
            null,
            createTuningConfig(2, null, false, true),
            false
        ),
        null
    );

    final List<DataSegment> segments = runTask(indexTask);

    Assert.assertEquals(1, segments.size());

    Assert.assertEquals(Arrays.asList("d"), segments.get(0).getDimensions());
    Assert.assertEquals(Arrays.asList("val"), segments.get(0).getMetrics());
    Assert.assertEquals(new Interval("2014/P1D"), segments.get(0).getInterval());
  }

  @Test
  public void testWithSmallMaxTotalRows() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();
    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("2014-01-01T00:00:10Z,a,1\n");
      writer.write("2014-01-01T00:00:10Z,b,2\n");
      writer.write("2014-01-01T00:00:10Z,c,3\n");
      writer.write("2014-01-01T01:00:20Z,a,1\n");
      writer.write("2014-01-01T01:00:20Z,b,2\n");
      writer.write("2014-01-01T01:00:20Z,c,3\n");
      writer.write("2014-01-01T02:00:30Z,a,1\n");
      writer.write("2014-01-01T02:00:30Z,b,2\n");
      writer.write("2014-01-01T02:00:30Z,c,3\n");
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        createIngestionSpec(
            tmpDir,
            null,
            new UniformGranularitySpec(
                Granularities.HOUR,
                Granularities.MINUTE,
                null
            ),
            createTuningConfig(2, 2, 2, null, false, false, true),
            false
        ),
        null
    );

    final List<DataSegment> segments = runTask(indexTask);

    Assert.assertEquals(6, segments.size());

    for (int i = 0; i < 6; i++) {
      final DataSegment segment = segments.get(i);
      final Interval expectedInterval = new Interval(StringUtils.format("2014-01-01T0%d/PT1H", (i / 2)));
      final int expectedPartitionNum = i % 2;

      Assert.assertEquals("test", segment.getDataSource());
      Assert.assertEquals(expectedInterval, segment.getInterval());
      Assert.assertEquals(NumberedShardSpec.class, segment.getShardSpec().getClass());
      Assert.assertEquals(expectedPartitionNum, segment.getShardSpec().getPartitionNum());
    }
  }

  @Test
  public void testPerfectRollup() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();
    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    populateRollupTestData(tmpFile);

    IndexTask indexTask = new IndexTask(
        null,
        null,
        createIngestionSpec(
            tmpDir,
            null,
            new UniformGranularitySpec(
                Granularities.DAY,
                Granularities.DAY,
                true,
                null
            ),
            createTuningConfig(3, 2, 2, null, false, true, true),
            false
        ),
        null
    );

    final List<DataSegment> segments = runTask(indexTask);

    Assert.assertEquals(3, segments.size());

    for (int i = 0; i < 3; i++) {
      final DataSegment segment = segments.get(i);
      final Interval expectedInterval = new Interval("2014-01-01T00:00:00.000Z/2014-01-02T00:00:00.000Z");

      Assert.assertEquals("test", segment.getDataSource());
      Assert.assertEquals(expectedInterval, segment.getInterval());
      Assert.assertTrue(segment.getShardSpec().getClass().equals(HashBasedNumberedShardSpec.class));
      Assert.assertEquals(i, segment.getShardSpec().getPartitionNum());
    }
  }

  @Test
  public void testBestEffortRollup() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();
    File tmpFile = File.createTempFile("druid", "index", tmpDir);

   populateRollupTestData(tmpFile);

    IndexTask indexTask = new IndexTask(
        null,
        null,
        createIngestionSpec(
            tmpDir,
            null,
            new UniformGranularitySpec(
                Granularities.DAY,
                Granularities.DAY,
                true,
                null
            ),
            createTuningConfig(3, 2, 2, null, false, false, true),
            false
        ),
        null
    );

    final List<DataSegment> segments = runTask(indexTask);

    Assert.assertEquals(5, segments.size());

    for (int i = 0; i < 5; i++) {
      final DataSegment segment = segments.get(i);
      final Interval expectedInterval = new Interval("2014-01-01T00:00:00.000Z/2014-01-02T00:00:00.000Z");

      Assert.assertEquals("test", segment.getDataSource());
      Assert.assertEquals(expectedInterval, segment.getInterval());
      Assert.assertEquals(NumberedShardSpec.class, segment.getShardSpec().getClass());
      Assert.assertEquals(i, segment.getShardSpec().getPartitionNum());
    }
  }

  private static void populateRollupTestData(File tmpFile) throws IOException
  {
    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("2014-01-01T00:00:10Z,a,1\n");
      writer.write("2014-01-01T01:00:20Z,a,1\n");
      writer.write("2014-01-01T00:00:10Z,b,2\n");
      writer.write("2014-01-01T00:00:10Z,c,3\n");
      writer.write("2014-01-01T01:00:20Z,b,2\n");
      writer.write("2014-01-01T02:00:30Z,a,1\n");
      writer.write("2014-01-01T02:00:30Z,b,2\n");
      writer.write("2014-01-01T01:00:20Z,c,3\n");
      writer.write("2014-01-01T02:00:30Z,c,3\n");
    }
  }

  @Test
  public void testIgnoreParseException() throws Exception
  {
    final File tmpDir = temporaryFolder.newFolder();

    final File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("time,d,val\n");
      writer.write("unparseable,a,1\n");
      writer.write("2014-01-01T00:00:10Z,a,1\n");
    }

    // GranularitySpec.intervals and numShards must be null to verify reportParseException=false is respected both in
    // IndexTask.determineShardSpecs() and IndexTask.generateAndPublishSegments()
    final IndexIngestionSpec parseExceptionIgnoreSpec = createIngestionSpec(
        tmpDir,
        new CSVParseSpec(
            new TimestampSpec(
                "time",
                "auto",
                null
            ),
            new DimensionsSpec(
                null,
                Lists.<String>newArrayList(),
                Lists.<SpatialDimensionSchema>newArrayList()
            ),
            null,
            Arrays.asList("time", "dim", "val"),
            true,
            0
        ),
        null,
        createTuningConfig(2, null, null, null, false, false, false), // ignore parse exception,
        false
    );

    IndexTask indexTask = new IndexTask(
        null,
        null,
        parseExceptionIgnoreSpec,
        null
    );

    final List<DataSegment> segments = runTask(indexTask);

    Assert.assertEquals(Arrays.asList("d"), segments.get(0).getDimensions());
    Assert.assertEquals(Arrays.asList("val"), segments.get(0).getMetrics());
    Assert.assertEquals(new Interval("2014/P1D"), segments.get(0).getInterval());
  }

  @Test
  public void testReportParseException() throws Exception
  {
    expectedException.expect(ParseException.class);
    expectedException.expectMessage("Unparseable timestamp found!");

    final File tmpDir = temporaryFolder.newFolder();

    final File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("time,d,val\n");
      writer.write("unparseable,a,1\n");
      writer.write("2014-01-01T00:00:10Z,a,1\n");
    }

    final IndexIngestionSpec parseExceptionIgnoreSpec = createIngestionSpec(
        tmpDir,
        new CSVParseSpec(
            new TimestampSpec(
                "time",
                "auto",
                null
            ),
            new DimensionsSpec(
                null,
                Lists.<String>newArrayList(),
                Lists.<SpatialDimensionSchema>newArrayList()
            ),
            null,
            Arrays.asList("time", "dim", "val"),
            true,
            0
        ),
        null,
        createTuningConfig(2, null, null, null, false, false, true), // report parse exception
        false
    );

    IndexTask indexTask = new IndexTask(
        null,
        null,
        parseExceptionIgnoreSpec,
        null
    );

    runTask(indexTask);
  }

  @Test
  public void testCsvWithHeaderOfEmptyColumns() throws Exception
  {
    final File tmpDir = temporaryFolder.newFolder();

    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("time,,\n");
      writer.write("2014-01-01T00:00:10Z,a,1\n");
    }

    tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("time,dim,\n");
      writer.write("2014-01-01T00:00:10Z,a,1\n");
    }

    tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("time,,val\n");
      writer.write("2014-01-01T00:00:10Z,a,1\n");
    }

    final IndexIngestionSpec parseExceptionIgnoreSpec = createIngestionSpec(
        tmpDir,
        new CSVParseSpec(
            new TimestampSpec(
                "time",
                "auto",
                null
            ),
            new DimensionsSpec(
                null,
                null,
                null
            ),
            null,
            null,
            true,
            0
        ),
        null,
        createTuningConfig(2, 1, null, null, false, true, true), // report parse exception
        false
    );

    IndexTask indexTask = new IndexTask(
        null,
        null,
        parseExceptionIgnoreSpec,
        null
    );

    final List<DataSegment> segments = runTask(indexTask);
    // the order of result segments can be changed because hash shardSpec is used.
    // the below loop is to make this test deterministic.
    Assert.assertEquals(2, segments.size());
    Assert.assertNotEquals(segments.get(0), segments.get(1));

    for (DataSegment segment : segments) {
      System.out.println(segment.getDimensions());
    }

    for (int i = 0; i < 2; i++) {
      final DataSegment segment = segments.get(i);
      final Set<String> dimensions = new HashSet<>(segment.getDimensions());

      Assert.assertTrue(
          StringUtils.format("Actual dimensions: %s", dimensions),
          dimensions.equals(Sets.newHashSet("dim", "column_3")) ||
          dimensions.equals(Sets.newHashSet("column_2", "column_3"))
      );

      Assert.assertEquals(Arrays.asList("val"), segment.getMetrics());
      Assert.assertEquals(new Interval("2014/P1D"), segment.getInterval());
    }
  }

  @Test
  public void testCsvWithHeaderOfEmptyTimestamp() throws Exception
  {
    expectedException.expect(ParseException.class);
    expectedException.expectMessage("Unparseable timestamp found!");

    final File tmpDir = temporaryFolder.newFolder();

    final File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write(",,\n");
      writer.write("2014-01-01T00:00:10Z,a,1\n");
    }

    final IndexIngestionSpec parseExceptionIgnoreSpec = createIngestionSpec(
        tmpDir,
        new CSVParseSpec(
            new TimestampSpec(
                "time",
                "auto",
                null
            ),
            new DimensionsSpec(
                null,
                Lists.<String>newArrayList(),
                Lists.<SpatialDimensionSchema>newArrayList()
            ),
            null,
            Arrays.asList("time", "", ""),
            true,
            0
        ),
        null,
        createTuningConfig(2, null, null, null, false, false, true), // report parse exception
        false
    );

    IndexTask indexTask = new IndexTask(
        null,
        null,
        parseExceptionIgnoreSpec,
        null
    );

    runTask(indexTask);
  }

  private final List<DataSegment> runTask(final IndexTask indexTask) throws Exception
  {
    final List<DataSegment> segments = Lists.newArrayList();

    indexTask.run(
        new TaskToolbox(
            null, new TaskActionClient()
        {
          @Override
          public <RetType> RetType submit(TaskAction<RetType> taskAction) throws IOException
          {
            if (taskAction instanceof LockListAction) {
              return (RetType) Collections.singletonList(
                  new TaskLock(
                      "", "", null, new DateTime().toString()
                  )
              );
            }

            if (taskAction instanceof LockAcquireAction) {
              return (RetType) new TaskLock(
                  "groupId",
                  "test",
                  ((LockAcquireAction) taskAction).getInterval(),
                  new DateTime().toString()
              );
            }

            if (taskAction instanceof SegmentTransactionalInsertAction) {
              return (RetType) new SegmentPublishResult(
                  ((SegmentTransactionalInsertAction) taskAction).getSegments(),
                  true
              );
            }

            if (taskAction instanceof SegmentAllocateAction) {
              SegmentAllocateAction action = (SegmentAllocateAction) taskAction;
              Interval interval = action.getPreferredSegmentGranularity().bucket(action.getTimestamp());
              ShardSpec shardSpec = new NumberedShardSpec(segmentAllocatePartitionCounter++, 0);
              return (RetType) new SegmentIdentifier(action.getDataSource(), interval, "latestVersion", shardSpec);
            }

            return null;
          }
        }, null, new DataSegmentPusher()
        {
          @Deprecated
          @Override
          public String getPathForHadoop(String dataSource)
          {
            return getPathForHadoop();
          }

          @Override
          public String getPathForHadoop()
          {
            return null;
          }

          @Override
          public DataSegment push(File file, DataSegment segment) throws IOException
          {
            segments.add(segment);
            return segment;
          }

          @Override
          public Map<String, Object> makeLoadSpec(URI uri)
          {
            throw new UnsupportedOperationException();
          }
        }, null, null, null, null, null, null, null, null, null, null, jsonMapper, temporaryFolder.newFolder(),
            indexIO, null, null, indexMergerV9
        )
    );

    Collections.sort(segments);

    return segments;
  }

  private IndexTask.IndexIngestionSpec createIngestionSpec(
      File baseDir,
      ParseSpec parseSpec,
      GranularitySpec granularitySpec,
      IndexTuningConfig tuningConfig,
      boolean appendToExisting
  )
  {
    return new IndexTask.IndexIngestionSpec(
        new DataSchema(
            "test",
            jsonMapper.convertValue(
                new StringInputRowParser(
                    parseSpec != null ? parseSpec : DEFAULT_PARSE_SPEC,
                    null
                ),
                Map.class
            ),
            new AggregatorFactory[]{
                new LongSumAggregatorFactory("val", "val")
            },
            granularitySpec != null ? granularitySpec : new UniformGranularitySpec(
                Granularities.DAY,
                Granularities.MINUTE,
                Arrays.asList(new Interval("2014/2015"))
            ),
            jsonMapper
        ),
        new IndexTask.IndexIOConfig(
            new LocalFirehoseFactory(
                baseDir,
                "druid*",
                null
            ),
            appendToExisting
        ),
        tuningConfig
    );
  }

  private static IndexTuningConfig createTuningConfig(
      Integer targetPartitionSize,
      Integer numShards,
      boolean forceExtendableShardSpecs,
      boolean forceGuaranteedRollup
  )
  {
    return createTuningConfig(
        targetPartitionSize,
        1,
        null,
        numShards,
        forceExtendableShardSpecs,
        forceGuaranteedRollup,
        true
    );
  }

  private static IndexTuningConfig createTuningConfig(
      Integer targetPartitionSize,
      Integer maxRowsInMemory,
      Integer maxTotalRows,
      Integer numShards,
      boolean forceExtendableShardSpecs,
      boolean forceGuaranteedRollup,
      boolean reportParseException
  )
  {
    return new IndexTask.IndexTuningConfig(
        targetPartitionSize,
        maxRowsInMemory,
        maxTotalRows,
        null,
        numShards,
        indexSpec,
        null,
        true,
        forceExtendableShardSpecs,
        forceGuaranteedRollup,
        reportParseException,
        null
    );
  }
}
