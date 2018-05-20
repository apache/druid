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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.FloatDimensionSchema;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.LongDimensionSchema;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.indexer.TaskMetricsUtils;
import io.druid.indexer.TaskState;
import io.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskLockType;
import io.druid.indexing.common.TaskReport;
import io.druid.indexing.common.TaskReportFileWriter;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.actions.LockAcquireAction;
import io.druid.indexing.common.actions.LockListAction;
import io.druid.indexing.common.actions.LockTryAcquireAction;
import io.druid.indexing.common.actions.SegmentAllocateAction;
import io.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import io.druid.indexing.common.actions.TaskAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import io.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import io.druid.indexing.overlord.SegmentPublishResult;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.math.expr.ExprMacroTable;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.firehose.LocalFirehoseFactory;
import io.druid.segment.transform.ExpressionTransform;
import io.druid.segment.transform.TransformSpec;
import io.druid.server.security.AuthTestUtils;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
  private File reportsFile;

  public IndexTaskTest()
  {
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
    indexMergerV9 = testUtils.getTestIndexMergerV9();
    indexIO = testUtils.getTestIndexIO();
  }

  @Before
  public void setup() throws IOException
  {
    reportsFile = File.createTempFile("IndexTaskTestReports-" + System.currentTimeMillis(), "json");
  }

  @After
  public void teardown()
  {
    reportsFile.delete();
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
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(2, segments.size());

    Assert.assertEquals("test", segments.get(0).getDataSource());
    Assert.assertEquals(Intervals.of("2014/P1D"), segments.get(0).getInterval());
    Assert.assertEquals(HashBasedNumberedShardSpec.class, segments.get(0).getShardSpec().getClass());
    Assert.assertEquals(0, segments.get(0).getShardSpec().getPartitionNum());
    Assert.assertEquals(2, ((NumberedShardSpec) segments.get(0).getShardSpec()).getPartitions());

    Assert.assertEquals("test", segments.get(1).getDataSource());
    Assert.assertEquals(Intervals.of("2014/P1D"), segments.get(1).getInterval());
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
            createTuningConfig(2, null, true, true),
            false
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    Assert.assertEquals(indexTask.getId(), indexTask.getGroupId());

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(2, segments.size());

    Assert.assertEquals("test", segments.get(0).getDataSource());
    Assert.assertEquals(Intervals.of("2014/P1D"), segments.get(0).getInterval());
    Assert.assertEquals(NumberedShardSpec.class, segments.get(0).getShardSpec().getClass());
    Assert.assertEquals(0, segments.get(0).getShardSpec().getPartitionNum());

    Assert.assertEquals("test", segments.get(1).getDataSource());
    Assert.assertEquals(Intervals.of("2014/P1D"), segments.get(1).getInterval());
    Assert.assertEquals(NumberedShardSpec.class, segments.get(1).getShardSpec().getClass());
    Assert.assertEquals(1, segments.get(1).getShardSpec().getPartitionNum());
  }

  @Test
  public void testTransformSpec() throws Exception
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
            new TransformSpec(
                new SelectorDimFilter("dim", "b", null),
                ImmutableList.of(
                    new ExpressionTransform("dimt", "concat(dim,dim)", ExprMacroTable.nil())
                )
            ),
            null,
            createTuningConfig(2, null, true, false),
            false
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    Assert.assertEquals(indexTask.getId(), indexTask.getGroupId());

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(1, segments.size());

    Assert.assertEquals("test", segments.get(0).getDataSource());
    Assert.assertEquals(Intervals.of("2014/P1D"), segments.get(0).getInterval());
    Assert.assertEquals(NumberedShardSpec.class, segments.get(0).getShardSpec().getClass());
    Assert.assertEquals(0, segments.get(0).getShardSpec().getPartitionNum());
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
                Collections.singletonList(Intervals.of("2014-01-01/2014-01-02"))
            ),
            createTuningConfig(10, null, false, true),
            false
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(1, segments.size());
  }

  @Test
  public void testIntervalBucketing() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();

    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("2014-01-01T07:59:59.977Z,a,1\n");
      writer.write("2014-01-01T08:00:00.000Z,b,1\n");
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
                Collections.singletonList(Intervals.of("2014-01-01T08:00:00Z/2014-01-01T09:00:00Z"))
            ),
            createTuningConfig(50, null, false, true),
            false
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    final List<DataSegment> segments = runTask(indexTask).rhs;

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
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(1, segments.size());

    Assert.assertEquals("test", segments.get(0).getDataSource());
    Assert.assertEquals(Intervals.of("2014/P1D"), segments.get(0).getInterval());
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
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    Assert.assertEquals("index_append_test", indexTask.getGroupId());

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(2, segmentAllocatePartitionCounter);
    Assert.assertEquals(2, segments.size());

    Assert.assertEquals("test", segments.get(0).getDataSource());
    Assert.assertEquals(Intervals.of("2014/P1D"), segments.get(0).getInterval());
    Assert.assertTrue(segments.get(0).getShardSpec().getClass().equals(NumberedShardSpec.class));
    Assert.assertEquals(0, segments.get(0).getShardSpec().getPartitionNum());

    Assert.assertEquals("test", segments.get(1).getDataSource());
    Assert.assertEquals(Intervals.of("2014/P1D"), segments.get(1).getInterval());
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
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(3, segments.size());

    Assert.assertEquals("test", segments.get(0).getDataSource());
    Assert.assertEquals(Intervals.of("2014-01-01T00/PT1H"), segments.get(0).getInterval());
    Assert.assertTrue(segments.get(0).getShardSpec().getClass().equals(NoneShardSpec.class));
    Assert.assertEquals(0, segments.get(0).getShardSpec().getPartitionNum());

    Assert.assertEquals("test", segments.get(1).getDataSource());
    Assert.assertEquals(Intervals.of("2014-01-01T01/PT1H"), segments.get(1).getInterval());
    Assert.assertTrue(segments.get(1).getShardSpec().getClass().equals(NoneShardSpec.class));
    Assert.assertEquals(0, segments.get(1).getShardSpec().getPartitionNum());

    Assert.assertEquals("test", segments.get(2).getDataSource());
    Assert.assertEquals(Intervals.of("2014-01-01T02/PT1H"), segments.get(2).getInterval());
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
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(1, segments.size());

    Assert.assertEquals(Arrays.asList("d"), segments.get(0).getDimensions());
    Assert.assertEquals(Arrays.asList("val"), segments.get(0).getMetrics());
    Assert.assertEquals(Intervals.of("2014/P1D"), segments.get(0).getInterval());
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
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(1, segments.size());

    Assert.assertEquals(Arrays.asList("d"), segments.get(0).getDimensions());
    Assert.assertEquals(Arrays.asList("val"), segments.get(0).getMetrics());
    Assert.assertEquals(Intervals.of("2014/P1D"), segments.get(0).getInterval());
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
            createTuningConfig(2, 2, null, 2L, null, false, false, true),
            false
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(6, segments.size());

    for (int i = 0; i < 6; i++) {
      final DataSegment segment = segments.get(i);
      final Interval expectedInterval = Intervals.of(StringUtils.format("2014-01-01T0%d/PT1H", (i / 2)));
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
            createTuningConfig(3, 2, null, 2L, null, false, true, true),
            false
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(3, segments.size());

    for (int i = 0; i < 3; i++) {
      final DataSegment segment = segments.get(i);
      final Interval expectedInterval = Intervals.of("2014-01-01T00:00:00.000Z/2014-01-02T00:00:00.000Z");

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
            createTuningConfig(3, 2, null, 2L, null, false, false, true),
            false
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(5, segments.size());

    for (int i = 0; i < 5; i++) {
      final DataSegment segment = segments.get(i);
      final Interval expectedInterval = Intervals.of("2014-01-01T00:00:00.000Z/2014-01-02T00:00:00.000Z");

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
        createTuningConfig(2, null, null, null, null, false, false, false), // ignore parse exception,
        false
    );

    IndexTask indexTask = new IndexTask(
        null,
        null,
        parseExceptionIgnoreSpec,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(Arrays.asList("d"), segments.get(0).getDimensions());
    Assert.assertEquals(Arrays.asList("val"), segments.get(0).getMetrics());
    Assert.assertEquals(Intervals.of("2014/P1D"), segments.get(0).getInterval());
  }

  @Test
  public void testReportParseException() throws Exception
  {
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
        createTuningConfig(2, null, null, null, null, false, false, true), // report parse exception
        false
    );

    IndexTask indexTask = new IndexTask(
        null,
        null,
        parseExceptionIgnoreSpec,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    TaskStatus status = runTask(indexTask).lhs;
    Assert.assertEquals(TaskState.FAILED, status.getStatusCode());
    checkTaskStatusErrorMsgForParseExceptionsExceeded(status);

    Map<String, Object> expectedUnparseables = ImmutableMap.of(
        "determinePartitions",
        new ArrayList<>(),
        "buildSegments",
        Arrays.asList("Unparseable timestamp found! Event: {time=unparseable, d=a, val=1}")
    );
    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();
    Assert.assertEquals(expectedUnparseables, reportData.getUnparseableEvents());
  }

  @Test
  public void testMultipleParseExceptionsSuccess() throws Exception
  {
    final File tmpDir = temporaryFolder.newFolder();

    final File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("{\"time\":\"unparseable\",\"dim\":\"a\",\"dimLong\":2,\"dimFloat\":3.0,\"val\":1}\n"); // unparseable time
      writer.write("{\"time\":\"2014-01-01T00:00:10Z\",\"dim\":\"a\",\"dimLong\":2,\"dimFloat\":3.0,\"val\":1}\n"); // valid row
      writer.write("{\"time\":\"2014-01-01T00:00:10Z\",\"dim\":\"b\",\"dimLong\":\"notnumber\",\"dimFloat\":3.0,\"val\":1}\n"); // row with invalid long dimension
      writer.write("{\"time\":\"2014-01-01T00:00:10Z\",\"dim\":\"b\",\"dimLong\":2,\"dimFloat\":\"notnumber\",\"val\":1}\n"); // row with invalid float dimension
      writer.write("{\"time\":\"2014-01-01T00:00:10Z\",\"dim\":\"b\",\"dimLong\":2,\"dimFloat\":4.0,\"val\":\"notnumber\"}\n"); // row with invalid metric
      writer.write("{\"time\":9.0x,\"dim\":\"a\",\"dimLong\":2,\"dimFloat\":3.0,\"val\":1}\n"); // invalid JSON
      writer.write("{\"time\":\"3014-03-01T00:00:10Z\",\"dim\":\"outsideofinterval\",\"dimLong\":2,\"dimFloat\":3.0,\"val\":1}\n"); // thrown away
      writer.write("{\"time\":\"99999999999-01-01T00:00:10Z\",\"dim\":\"b\",\"dimLong\":2,\"dimFloat\":3.0,\"val\":1}\n"); // unparseable time
      writer.write("this is not JSON\n"); // invalid JSON
    }

    final IndexTask.IndexTuningConfig tuningConfig = new IndexTask.IndexTuningConfig(
        2,
        null,
        null,
        null,
        null,
        null,
        indexSpec,
        null,
        true,
        false,
        true,
        false,
        null,
        null,
        null,
        true,
        7,
        7
    );

    final IndexIngestionSpec parseExceptionIgnoreSpec = createIngestionSpec(
        tmpDir,
        new JSONParseSpec(
            new TimestampSpec(
                "time",
                "auto",
                null
            ),
            new DimensionsSpec(
                Arrays.asList(
                    new StringDimensionSchema("dim"),
                    new LongDimensionSchema("dimLong"),
                    new FloatDimensionSchema("dimFloat")
                ),
                Lists.<String>newArrayList(),
                Lists.<SpatialDimensionSchema>newArrayList()
            ),
            null,
            null
        ),
        null,
        tuningConfig,
        false
    );

    IndexTask indexTask = new IndexTask(
        null,
        null,
        parseExceptionIgnoreSpec,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    TaskStatus status = runTask(indexTask).lhs;
    Assert.assertEquals(TaskState.SUCCESS, status.getStatusCode());
    Assert.assertEquals(null, status.getErrorMsg());

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    Map<String, Object> expectedMetrics = ImmutableMap.of(
        "determinePartitions",
        ImmutableMap.of(
            TaskMetricsUtils.ROWS_PROCESSED_WITH_ERRORS, 0,
            TaskMetricsUtils.ROWS_PROCESSED, 4,
            TaskMetricsUtils.ROWS_UNPARSEABLE, 4,
            TaskMetricsUtils.ROWS_THROWN_AWAY, 1
        ),
        "buildSegments",
        ImmutableMap.of(
            TaskMetricsUtils.ROWS_PROCESSED_WITH_ERRORS, 3,
            TaskMetricsUtils.ROWS_PROCESSED, 1,
            TaskMetricsUtils.ROWS_UNPARSEABLE, 4,
            TaskMetricsUtils.ROWS_THROWN_AWAY, 1
        )
    );
    Assert.assertEquals(expectedMetrics, reportData.getRowStats());

    Map<String, Object> expectedUnparseables = ImmutableMap.of(
        "determinePartitions",
        Arrays.asList(
            "Unable to parse row [this is not JSON]",
            "Unparseable timestamp found! Event: {time=99999999999-01-01T00:00:10Z, dim=b, dimLong=2, dimFloat=3.0, val=1}",
            "Unable to parse row [{\"time\":9.0x,\"dim\":\"a\",\"dimLong\":2,\"dimFloat\":3.0,\"val\":1}]",
            "Unparseable timestamp found! Event: {time=unparseable, dim=a, dimLong=2, dimFloat=3.0, val=1}"
        ),
        "buildSegments",
        Arrays.asList(
            "Unable to parse row [this is not JSON]",
            "Unparseable timestamp found! Event: {time=99999999999-01-01T00:00:10Z, dim=b, dimLong=2, dimFloat=3.0, val=1}",
            "Unable to parse row [{\"time\":9.0x,\"dim\":\"a\",\"dimLong\":2,\"dimFloat\":3.0,\"val\":1}]",
            "Found unparseable columns in row: [MapBasedInputRow{timestamp=2014-01-01T00:00:10.000Z, event={time=2014-01-01T00:00:10Z, dim=b, dimLong=2, dimFloat=4.0, val=notnumber}, dimensions=[dim, dimLong, dimFloat]}], exceptions: [Unable to parse value[notnumber] for field[val],]",
            "Found unparseable columns in row: [MapBasedInputRow{timestamp=2014-01-01T00:00:10.000Z, event={time=2014-01-01T00:00:10Z, dim=b, dimLong=2, dimFloat=notnumber, val=1}, dimensions=[dim, dimLong, dimFloat]}], exceptions: [could not convert value [notnumber] to float,]",
            "Found unparseable columns in row: [MapBasedInputRow{timestamp=2014-01-01T00:00:10.000Z, event={time=2014-01-01T00:00:10Z, dim=b, dimLong=notnumber, dimFloat=3.0, val=1}, dimensions=[dim, dimLong, dimFloat]}], exceptions: [could not convert value [notnumber] to long,]",
            "Unparseable timestamp found! Event: {time=unparseable, dim=a, dimLong=2, dimFloat=3.0, val=1}"
        )
    );

    Assert.assertEquals(expectedUnparseables, reportData.getUnparseableEvents());
  }

  @Test
  public void testMultipleParseExceptionsFailure() throws Exception
  {
    final File tmpDir = temporaryFolder.newFolder();

    final File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("time,dim,dimLong,dimFloat,val\n");
      writer.write("unparseable,a,2,3.0,1\n"); // unparseable
      writer.write("2014-01-01T00:00:10Z,a,2,3.0,1\n"); // valid row
      writer.write("9.0,a,2,3.0,1\n"); // unparseable
      writer.write("3014-03-01T00:00:10Z,outsideofinterval,2,3.0,1\n"); // thrown away
      writer.write("99999999999-01-01T00:00:10Z,b,2,3.0,1\n"); // unparseable
    }

    // Allow up to 3 parse exceptions, and save up to 2 parse exceptions
    final IndexTask.IndexTuningConfig tuningConfig = new IndexTask.IndexTuningConfig(
        2,
        null,
        null,
        null,
        null,
        null,
        indexSpec,
        null,
        true,
        false,
        false,
        false,
        null,
        null,
        null,
        true,
        2,
        5
    );

    final IndexIngestionSpec parseExceptionIgnoreSpec = createIngestionSpec(
        tmpDir,
        new CSVParseSpec(
            new TimestampSpec(
                "time",
                "auto",
                null
            ),
            new DimensionsSpec(
                Arrays.asList(
                    new StringDimensionSchema("dim"),
                    new LongDimensionSchema("dimLong"),
                    new FloatDimensionSchema("dimFloat")
                ),
                Lists.<String>newArrayList(),
                Lists.<SpatialDimensionSchema>newArrayList()
            ),
            null,
            Arrays.asList("time", "dim", "dimLong", "dimFloat", "val"),
            true,
            0
        ),
        null,
        tuningConfig,
        false
    );

    IndexTask indexTask = new IndexTask(
        null,
        null,
        parseExceptionIgnoreSpec,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    TaskStatus status = runTask(indexTask).lhs;
    Assert.assertEquals(TaskState.FAILED, status.getStatusCode());
    checkTaskStatusErrorMsgForParseExceptionsExceeded(status);

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    Map<String, Object> expectedMetrics = ImmutableMap.of(
        "buildSegments",
        ImmutableMap.of(
            TaskMetricsUtils.ROWS_PROCESSED_WITH_ERRORS, 0,
            TaskMetricsUtils.ROWS_PROCESSED, 1,
            TaskMetricsUtils.ROWS_UNPARSEABLE, 3,
            TaskMetricsUtils.ROWS_THROWN_AWAY, 2
        )
    );

    Assert.assertEquals(expectedMetrics, reportData.getRowStats());

    Map<String, Object> expectedUnparseables = ImmutableMap.of(
        "determinePartitions",
        new ArrayList<>(),
        "buildSegments",
        Arrays.asList(
            "Unparseable timestamp found! Event: {time=99999999999-01-01T00:00:10Z, dim=b, dimLong=2, dimFloat=3.0, val=1}",
            "Unparseable timestamp found! Event: {time=9.0, dim=a, dimLong=2, dimFloat=3.0, val=1}",
            "Unparseable timestamp found! Event: {time=unparseable, dim=a, dimLong=2, dimFloat=3.0, val=1}"
        )
    );

    Assert.assertEquals(expectedUnparseables, reportData.getUnparseableEvents());
  }

  @Test
  public void testMultipleParseExceptionsFailureAtDeterminePartitions() throws Exception
  {
    final File tmpDir = temporaryFolder.newFolder();

    final File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("time,dim,dimLong,dimFloat,val\n");
      writer.write("unparseable,a,2,3.0,1\n"); // unparseable
      writer.write("2014-01-01T00:00:10Z,a,2,3.0,1\n"); // valid row
      writer.write("9.0,a,2,3.0,1\n"); // unparseable
      writer.write("3014-03-01T00:00:10Z,outsideofinterval,2,3.0,1\n"); // thrown away
      writer.write("99999999999-01-01T00:00:10Z,b,2,3.0,1\n"); // unparseable
    }

    // Allow up to 3 parse exceptions, and save up to 2 parse exceptions
    final IndexTask.IndexTuningConfig tuningConfig = new IndexTask.IndexTuningConfig(
        2,
        null,
        null,
        null,
        null,
        null,
        indexSpec,
        null,
        true,
        false,
        true,
        false,
        null,
        null,
        null,
        true,
        2,
        5
    );

    final IndexIngestionSpec parseExceptionIgnoreSpec = createIngestionSpec(
        tmpDir,
        new CSVParseSpec(
            new TimestampSpec(
                "time",
                "auto",
                null
            ),
            new DimensionsSpec(
                Arrays.asList(
                    new StringDimensionSchema("dim"),
                    new LongDimensionSchema("dimLong"),
                    new FloatDimensionSchema("dimFloat")
                ),
                Lists.<String>newArrayList(),
                Lists.<SpatialDimensionSchema>newArrayList()
            ),
            null,
            Arrays.asList("time", "dim", "dimLong", "dimFloat", "val"),
            true,
            0
        ),
        null,
        tuningConfig,
        false
    );

    IndexTask indexTask = new IndexTask(
        null,
        null,
        parseExceptionIgnoreSpec,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    TaskStatus status = runTask(indexTask).lhs;
    Assert.assertEquals(TaskState.FAILED, status.getStatusCode());
    checkTaskStatusErrorMsgForParseExceptionsExceeded(status);

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    Map<String, Object> expectedMetrics = ImmutableMap.of(
        "determinePartitions",
        ImmutableMap.of(
            TaskMetricsUtils.ROWS_PROCESSED_WITH_ERRORS, 0,
            TaskMetricsUtils.ROWS_PROCESSED, 1,
            TaskMetricsUtils.ROWS_UNPARSEABLE, 3,
            TaskMetricsUtils.ROWS_THROWN_AWAY, 2
        )
    );

    Assert.assertEquals(expectedMetrics, reportData.getRowStats());

    Map<String, Object> expectedUnparseables = ImmutableMap.of(
        "determinePartitions",
        Arrays.asList(
            "Unparseable timestamp found! Event: {time=99999999999-01-01T00:00:10Z, dim=b, dimLong=2, dimFloat=3.0, val=1}",
            "Unparseable timestamp found! Event: {time=9.0, dim=a, dimLong=2, dimFloat=3.0, val=1}",
            "Unparseable timestamp found! Event: {time=unparseable, dim=a, dimLong=2, dimFloat=3.0, val=1}"
        ),
        "buildSegments",
        new ArrayList<>()
    );

    Assert.assertEquals(expectedUnparseables, reportData.getUnparseableEvents());
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
        createTuningConfig(2, 1, null, null, null, false, true, true), // report parse exception
        false
    );

    IndexTask indexTask = new IndexTask(
        null,
        null,
        parseExceptionIgnoreSpec,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    final List<DataSegment> segments = runTask(indexTask).rhs;
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
      Assert.assertEquals(Intervals.of("2014/P1D"), segment.getInterval());
    }
  }

  @Test
  public void testCsvWithHeaderOfEmptyTimestamp() throws Exception
  {
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
        createTuningConfig(2, null, null, null, null, false, false, true), // report parse exception
        false
    );

    IndexTask indexTask = new IndexTask(
        null,
        null,
        parseExceptionIgnoreSpec,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    TaskStatus status = runTask(indexTask).lhs;
    Assert.assertEquals(TaskState.FAILED, status.getStatusCode());

    checkTaskStatusErrorMsgForParseExceptionsExceeded(status);

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    Map<String, Object> expectedUnparseables = ImmutableMap.of(
        "determinePartitions",
        new ArrayList<>(),
        "buildSegments",
        Arrays.asList("Unparseable timestamp found! Event: {column_1=2014-01-01T00:00:10Z, column_2=a, column_3=1}")
    );
    Assert.assertEquals(expectedUnparseables, reportData.getUnparseableEvents());
  }

  public static void checkTaskStatusErrorMsgForParseExceptionsExceeded(TaskStatus status)
  {
    // full stacktrace will be too long and make tests brittle (e.g. if line # changes), just match the main message
    Assert.assertTrue(status.getErrorMsg().contains("Max parse exceptions exceeded, terminating task..."));
  }

  private Pair<TaskStatus, List<DataSegment>> runTask(IndexTask indexTask) throws Exception
  {
    final List<DataSegment> segments = Lists.newArrayList();

    final TaskActionClient actionClient = new TaskActionClient()
    {
      @Override
      public <RetType> RetType submit(TaskAction<RetType> taskAction)
      {
        if (taskAction instanceof LockListAction) {
          return (RetType) Collections.singletonList(
              new TaskLock(
                  TaskLockType.EXCLUSIVE,
                  "",
                  "",
                  Intervals.of("2014/P1Y"), DateTimes.nowUtc().toString(),
                  Tasks.DEFAULT_BATCH_INDEX_TASK_PRIORITY
              )
          );
        }

        if (taskAction instanceof LockAcquireAction) {
          return (RetType) new TaskLock(
              TaskLockType.EXCLUSIVE, "groupId",
              "test",
              ((LockAcquireAction) taskAction).getInterval(),
              DateTimes.nowUtc().toString(),
              Tasks.DEFAULT_BATCH_INDEX_TASK_PRIORITY
          );
        }

        if (taskAction instanceof LockTryAcquireAction) {
          return (RetType) new TaskLock(
              TaskLockType.EXCLUSIVE,
              "groupId",
              "test",
              ((LockTryAcquireAction) taskAction).getInterval(),
              DateTimes.nowUtc().toString(),
              Tasks.DEFAULT_BATCH_INDEX_TASK_PRIORITY
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
    };

    final DataSegmentPusher pusher = new DataSegmentPusher()
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
      public DataSegment push(File file, DataSegment segment, boolean useUniquePath)
      {
        segments.add(segment);
        return segment;
      }

      @Override
      public Map<String, Object> makeLoadSpec(URI uri)
      {
        throw new UnsupportedOperationException();
      }
    };

    final DataSegmentKiller killer = new DataSegmentKiller()
    {
      @Override
      public void kill(DataSegment segment)
      {

      }

      @Override
      public void killAll()
      {

      }
    };

    final TaskToolbox box = new TaskToolbox(
        null,
        actionClient,
        null,
        pusher,
        killer,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        jsonMapper,
        temporaryFolder.newFolder(),
        indexIO,
        null,
        null,
        indexMergerV9,
        null,
        null,
        null,
        null,
        new TaskReportFileWriter(reportsFile)
    );

    indexTask.isReady(box.getTaskActionClient());
    TaskStatus status = indexTask.run(box);

    Collections.sort(segments);

    return Pair.of(status, segments);
  }

  private IndexTask.IndexIngestionSpec createIngestionSpec(
      File baseDir,
      ParseSpec parseSpec,
      GranularitySpec granularitySpec,
      IndexTuningConfig tuningConfig,
      boolean appendToExisting
  )
  {
    return createIngestionSpec(baseDir, parseSpec, TransformSpec.NONE, granularitySpec, tuningConfig, appendToExisting);
  }

  private IndexTask.IndexIngestionSpec createIngestionSpec(
      File baseDir,
      ParseSpec parseSpec,
      TransformSpec transformSpec,
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
                Arrays.asList(Intervals.of("2014/2015"))
            ),
            transformSpec,
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
      Long maxBytesInMemory,
      Long maxTotalRows,
      Integer numShards,
      boolean forceExtendableShardSpecs,
      boolean forceGuaranteedRollup,
      boolean reportParseException
  )
  {
    return new IndexTask.IndexTuningConfig(
        targetPartitionSize,
        maxRowsInMemory,
        maxBytesInMemory,
        maxTotalRows,
        null,
        numShards,
        indexSpec,
        null,
        true,
        forceExtendableShardSpecs,
        forceGuaranteedRollup,
        reportParseException,
        null,
        null,
        null,
        null,
        null,
        1
    );
  }

  private IngestionStatsAndErrorsTaskReportData getTaskReportData() throws IOException
  {
    Map<String, TaskReport> taskReports = jsonMapper.readValue(
        reportsFile,
        new TypeReference<Map<String, TaskReport>>()
        {
        }
    );
    return IngestionStatsAndErrorsTaskReportData.getPayloadFromTaskReports(
        taskReports
    );
  }
}
