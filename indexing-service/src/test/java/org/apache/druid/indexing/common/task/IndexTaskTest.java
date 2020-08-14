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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.stats.RowIngestionMeters;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.IndexTask.IndexIOConfig;
import org.apache.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import org.apache.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoaderLocalCacheManager;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.LocalFirehoseFactory;
import org.apache.druid.segment.realtime.firehose.WindowedStorageAdapter;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.NumberedOverwriteShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionIds;
import org.hamcrest.CoreMatchers;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(Parameterized.class)
public class IndexTaskTest extends IngestionTestBase
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static final TimestampSpec DEFAULT_TIMESTAMP_SPEC = new TimestampSpec("ts", "auto", null);
  private static final DimensionsSpec DEFAULT_DIMENSIONS_SPEC = new DimensionsSpec(
      DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "dim"))
  );
  private static final ParseSpec DEFAULT_PARSE_SPEC = new CSVParseSpec(
      DEFAULT_TIMESTAMP_SPEC,
      DEFAULT_DIMENSIONS_SPEC,
      null,
      Arrays.asList("ts", "dim", "val"),
      false,
      0
  );
  private static final InputFormat DEFAULT_INPUT_FORMAT = new CsvInputFormat(
      Arrays.asList("ts", "dim", "val"),
      null,
      null,
      false,
      0
  );

  @Parameterized.Parameters(name = "{0}, useInputFormatApi={1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK, false},
        new Object[]{LockGranularity.TIME_CHUNK, true},
        new Object[]{LockGranularity.SEGMENT, true}
    );
  }

  private static final IndexSpec INDEX_SPEC = new IndexSpec();
  private final ObjectMapper jsonMapper;
  private final IndexIO indexIO;
  private final RowIngestionMetersFactory rowIngestionMetersFactory;
  private final LockGranularity lockGranularity;
  private final boolean useInputFormatApi;

  private AppenderatorsManager appenderatorsManager;
  private SegmentLoader segmentLoader;
  private TestTaskRunner taskRunner;

  public IndexTaskTest(LockGranularity lockGranularity, boolean useInputFormatApi)
  {
    this.jsonMapper = getObjectMapper();
    this.indexIO = getIndexIO();
    this.rowIngestionMetersFactory = getRowIngestionMetersFactory();
    this.lockGranularity = lockGranularity;
    this.useInputFormatApi = useInputFormatApi;
  }

  @Before
  public void setup() throws IOException
  {
    appenderatorsManager = new TestAppenderatorsManager();

    final File cacheDir = temporaryFolder.newFolder();
    segmentLoader = new SegmentLoaderLocalCacheManager(
        indexIO,
        new SegmentLoaderConfig()
        {
          @Override
          public List<StorageLocationConfig> getLocations()
          {
            return Collections.singletonList(
                new StorageLocationConfig(cacheDir, null, null)
            );
          }
        },
        jsonMapper
    );
    taskRunner = new TestTaskRunner();
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
        createDefaultIngestionSpec(
            jsonMapper,
            tmpDir,
            null,
            null,
            createTuningConfigWithMaxRowsPerSegment(2, true),
            false
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
    );

    Assert.assertFalse(indexTask.supportsQueries());

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(2, segments.size());

    Assert.assertEquals("test", segments.get(0).getDataSource());
    Assert.assertEquals(Intervals.of("2014/P1D"), segments.get(0).getInterval());
    Assert.assertEquals(HashBasedNumberedShardSpec.class, segments.get(0).getShardSpec().getClass());
    Assert.assertEquals(0, segments.get(0).getShardSpec().getPartitionNum());
    Assert.assertEquals(2, segments.get(0).getShardSpec().getNumCorePartitions());

    Assert.assertEquals("test", segments.get(1).getDataSource());
    Assert.assertEquals(Intervals.of("2014/P1D"), segments.get(1).getInterval());
    Assert.assertEquals(HashBasedNumberedShardSpec.class, segments.get(1).getShardSpec().getClass());
    Assert.assertEquals(1, segments.get(1).getShardSpec().getPartitionNum());
    Assert.assertEquals(2, segments.get(1).getShardSpec().getNumCorePartitions());
  }

  @Test
  public void testTransformSpec() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();

    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("2014-01-01T00:00:10Z,a,an|array,1|2|3,1\n");
      writer.write("2014-01-01T01:00:20Z,b,another|array,3|4,1\n");
      writer.write("2014-01-01T02:00:30Z,c,and|another,0|1,1\n");
    }

    final DimensionsSpec dimensionsSpec = new DimensionsSpec(
        DimensionsSpec.getDefaultSchemas(
            Arrays.asList(
                "ts",
                "dim",
                "dim_array",
                "dim_num_array",
                "dimt",
                "dimtarray1",
                "dimtarray2",
                "dimtnum_array"
            )
        )
    );
    final List<String> columns = Arrays.asList("ts", "dim", "dim_array", "dim_num_array", "val");
    final String listDelimiter = "|";
    final TransformSpec transformSpec = new TransformSpec(
        new SelectorDimFilter("dim", "b", null),
        ImmutableList.of(
            new ExpressionTransform("dimt", "concat(dim,dim)", ExprMacroTable.nil()),
            new ExpressionTransform("dimtarray1", "array(dim, dim)", ExprMacroTable.nil()),
            new ExpressionTransform(
                "dimtarray2",
                "map(d -> concat(d, 'foo'), dim_array)",
                ExprMacroTable.nil()
            ),
            new ExpressionTransform("dimtnum_array", "map(d -> d + 3, dim_num_array)", ExprMacroTable.nil())
        )
    );
    final IndexTuningConfig tuningConfig = createTuningConfigWithMaxRowsPerSegment(2, false);
    final IndexIngestionSpec indexIngestionSpec;
    if (useInputFormatApi) {
      indexIngestionSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          DEFAULT_TIMESTAMP_SPEC,
          dimensionsSpec,
          new CsvInputFormat(columns, listDelimiter, null, false, 0),
          transformSpec,
          null,
          tuningConfig,
          false
      );
    } else {
      indexIngestionSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          new CSVParseSpec(DEFAULT_TIMESTAMP_SPEC, dimensionsSpec, listDelimiter, columns, false, 0),
          transformSpec,
          null,
          tuningConfig,
          false
      );
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        indexIngestionSpec,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
    );

    Assert.assertEquals(indexTask.getId(), indexTask.getGroupId());

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(1, segments.size());
    DataSegment segment = segments.get(0);
    final File segmentFile = segmentLoader.getSegmentFiles(segment);

    final WindowedStorageAdapter adapter = new WindowedStorageAdapter(
        new QueryableIndexStorageAdapter(indexIO.loadIndex(segmentFile)),
        segment.getInterval()
    );
    final Sequence<Cursor> cursorSequence = adapter.getAdapter().makeCursors(
        null,
        segment.getInterval(),
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    );
    final List<Map<String, Object>> transforms = cursorSequence
        .map(cursor -> {
          final DimensionSelector selector1 = cursor.getColumnSelectorFactory()
                                                    .makeDimensionSelector(new DefaultDimensionSpec("dimt", "dimt"));
          final DimensionSelector selector2 = cursor.getColumnSelectorFactory()
                                                    .makeDimensionSelector(new DefaultDimensionSpec(
                                                        "dimtarray1",
                                                        "dimtarray1"
                                                    ));
          final DimensionSelector selector3 = cursor.getColumnSelectorFactory()
                                                    .makeDimensionSelector(new DefaultDimensionSpec(
                                                        "dimtarray2",
                                                        "dimtarray2"
                                                    ));
          final DimensionSelector selector4 = cursor.getColumnSelectorFactory()
                                                    .makeDimensionSelector(new DefaultDimensionSpec(
                                                        "dimtnum_array",
                                                        "dimtnum_array"
                                                    ));


          Map<String, Object> row = new HashMap<>();
          row.put("dimt", selector1.defaultGetObject());
          row.put("dimtarray1", selector2.defaultGetObject());
          row.put("dimtarray2", selector3.defaultGetObject());
          row.put("dimtnum_array", selector4.defaultGetObject());
          cursor.advance();
          return row;
        })
        .toList();
    Assert.assertEquals(1, transforms.size());
    Assert.assertEquals("bb", transforms.get(0).get("dimt"));
    Assert.assertEquals(ImmutableList.of("b", "b"), transforms.get(0).get("dimtarray1"));
    Assert.assertEquals(ImmutableList.of("anotherfoo", "arrayfoo"), transforms.get(0).get("dimtarray2"));
    Assert.assertEquals(ImmutableList.of("6.0", "7.0"), transforms.get(0).get("dimtnum_array"));

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
        createDefaultIngestionSpec(
            jsonMapper,
            tmpDir,
            new ArbitraryGranularitySpec(
                Granularities.MINUTE,
                Collections.singletonList(Intervals.of("2014-01-01/2014-01-02"))
            ),
            null,
            createTuningConfigWithMaxRowsPerSegment(10, true),
            false
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
    );

    final List<DataSegment> segments = runTask(indexTask).rhs;

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
        createDefaultIngestionSpec(
            jsonMapper,
            tmpDir,
            new UniformGranularitySpec(
                Granularities.HOUR,
                Granularities.HOUR,
                Collections.singletonList(Intervals.of("2014-01-01T08:00:00Z/2014-01-01T09:00:00Z"))
            ),
            null,
            createTuningConfigWithMaxRowsPerSegment(50, true),
            false
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
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
        createDefaultIngestionSpec(
            jsonMapper,
            tmpDir,
            null,
            null,
            createTuningConfigWithPartitionsSpec(new HashedPartitionsSpec(null, 1, null), true),
            false
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
    );

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(1, segments.size());

    Assert.assertEquals("test", segments.get(0).getDataSource());
    Assert.assertEquals(Intervals.of("2014/P1D"), segments.get(0).getInterval());
    Assert.assertEquals(HashBasedNumberedShardSpec.class, segments.get(0).getShardSpec().getClass());
    Assert.assertEquals(0, segments.get(0).getShardSpec().getPartitionNum());
  }

  @Test
  public void testNumShardsAndPartitionDimensionsProvided() throws Exception
  {
    final File tmpDir = temporaryFolder.newFolder();
    final File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("2014-01-01T00:00:10Z,a,1\n");
      writer.write("2014-01-01T01:00:20Z,b,1\n");
      writer.write("2014-01-01T02:00:30Z,c,1\n");
    }

    final IndexTask indexTask = new IndexTask(
        null,
        null,
        createDefaultIngestionSpec(
            jsonMapper,
            tmpDir,
            null,
            null,
            createTuningConfigWithPartitionsSpec(new HashedPartitionsSpec(null, 2, ImmutableList.of("dim")), true),
            false
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
    );

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(2, segments.size());

    for (DataSegment segment : segments) {
      Assert.assertEquals("test", segment.getDataSource());
      Assert.assertEquals(Intervals.of("2014/P1D"), segment.getInterval());
      Assert.assertEquals(HashBasedNumberedShardSpec.class, segment.getShardSpec().getClass());

      final File segmentFile = segmentLoader.getSegmentFiles(segment);

      final WindowedStorageAdapter adapter = new WindowedStorageAdapter(
          new QueryableIndexStorageAdapter(indexIO.loadIndex(segmentFile)),
          segment.getInterval()
      );

      final Sequence<Cursor> cursorSequence = adapter.getAdapter().makeCursors(
          null,
          segment.getInterval(),
          VirtualColumns.EMPTY,
          Granularities.ALL,
          false,
          null
      );
      final List<Integer> hashes = cursorSequence
          .map(cursor -> {
            final DimensionSelector selector = cursor.getColumnSelectorFactory()
                                                     .makeDimensionSelector(new DefaultDimensionSpec("dim", "dim"));
            try {
              final int hash = HashBasedNumberedShardSpec.hash(
                  jsonMapper,
                  Collections.singletonList(selector.getObject())
              );
              cursor.advance();
              return hash;
            }
            catch (JsonProcessingException e) {
              throw new RuntimeException(e);
            }
          })
          .toList();

      Assert.assertTrue(hashes.stream().allMatch(h -> h.intValue() == hashes.get(0)));
    }
  }

  @Test
  public void testWriteNewSegmentsWithAppendToExistingWithLinearPartitioningSuccessfullyAppend() throws Exception
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
        createDefaultIngestionSpec(
            jsonMapper,
            tmpDir,
            null,
            null,
            createTuningConfigWithMaxRowsPerSegment(2, false),
            true
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
    );

    Assert.assertEquals("index_append_test", indexTask.getGroupId());

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(2, taskRunner.getTaskActionClient().getActionCount(SegmentAllocateAction.class));
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
        createDefaultIngestionSpec(
            jsonMapper,
            tmpDir,
            new UniformGranularitySpec(
                Granularities.HOUR,
                Granularities.MINUTE,
                null
            ),
            null,
            createTuningConfigWithMaxRowsPerSegment(2, true),
            false
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
    );

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(3, segments.size());

    Assert.assertEquals("test", segments.get(0).getDataSource());
    Assert.assertEquals(Intervals.of("2014-01-01T00/PT1H"), segments.get(0).getInterval());
    Assert.assertEquals(HashBasedNumberedShardSpec.class, segments.get(0).getShardSpec().getClass());
    Assert.assertEquals(0, segments.get(0).getShardSpec().getPartitionNum());

    Assert.assertEquals("test", segments.get(1).getDataSource());
    Assert.assertEquals(Intervals.of("2014-01-01T01/PT1H"), segments.get(1).getInterval());
    Assert.assertEquals(HashBasedNumberedShardSpec.class, segments.get(1).getShardSpec().getClass());
    Assert.assertEquals(0, segments.get(1).getShardSpec().getPartitionNum());

    Assert.assertEquals("test", segments.get(2).getDataSource());
    Assert.assertEquals(Intervals.of("2014-01-01T02/PT1H"), segments.get(2).getInterval());
    Assert.assertEquals(HashBasedNumberedShardSpec.class, segments.get(2).getShardSpec().getClass());
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

    final TimestampSpec timestampSpec = new TimestampSpec("time", "auto", null);
    final IndexTuningConfig tuningConfig = createTuningConfigWithMaxRowsPerSegment(2, true);
    final IndexIngestionSpec ingestionSpec;
    if (useInputFormatApi) {
      ingestionSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          new CSVParseSpec(timestampSpec, DimensionsSpec.EMPTY, null, null, true, 0),
          null,
          null,
          tuningConfig,
          false
      );
    } else {
      ingestionSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          timestampSpec,
          DimensionsSpec.EMPTY,
          new CsvInputFormat(null, null, null, true, 0),
          null,
          null,
          tuningConfig,
          false
      );
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        ingestionSpec,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
    );

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(1, segments.size());

    Assert.assertEquals(Collections.singletonList("d"), segments.get(0).getDimensions());
    Assert.assertEquals(Collections.singletonList("val"), segments.get(0).getMetrics());
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

    final TimestampSpec timestampSpec = new TimestampSpec("time", "auto", null);
    final List<String> columns = Arrays.asList("time", "dim", "val");
    final IndexTuningConfig tuningConfig = createTuningConfigWithMaxRowsPerSegment(2, true);
    final IndexIngestionSpec ingestionSpec;
    if (useInputFormatApi) {
      ingestionSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          new CSVParseSpec(timestampSpec, DimensionsSpec.EMPTY, null, columns, true, 0),
          null,
          null,
          tuningConfig,
          false
      );
    } else {
      ingestionSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          timestampSpec,
          DimensionsSpec.EMPTY,
          new CsvInputFormat(columns, null, null, true, 0),
          null,
          null,
          tuningConfig,
          false
      );
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        ingestionSpec,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
    );

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(1, segments.size());

    Assert.assertEquals(Collections.singletonList("d"), segments.get(0).getDimensions());
    Assert.assertEquals(Collections.singletonList("val"), segments.get(0).getMetrics());
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
        createDefaultIngestionSpec(
            jsonMapper,
            tmpDir,
            new UniformGranularitySpec(
                Granularities.HOUR,
                Granularities.MINUTE,
                null
            ),
            null,
            createTuningConfig(2, 2, null, 2L, null, false, true),
            false
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
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
        createDefaultIngestionSpec(
            jsonMapper,
            tmpDir,
            new UniformGranularitySpec(
                Granularities.DAY,
                Granularities.DAY,
                true,
                null
            ),
            null,
            createTuningConfig(3, 2, null, 2L, null, true, true),
            false
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
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
        createDefaultIngestionSpec(
            jsonMapper,
            tmpDir,
            new UniformGranularitySpec(
                Granularities.DAY,
                Granularities.DAY,
                true,
                null
            ),
            null,
            createTuningConfig(3, 2, null, 2L, null, false, true),
            false
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
    );

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(5, segments.size());

    final Interval expectedInterval = Intervals.of("2014-01-01T00:00:00.000Z/2014-01-02T00:00:00.000Z");
    for (int i = 0; i < 5; i++) {
      final DataSegment segment = segments.get(i);

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

    final TimestampSpec timestampSpec = new TimestampSpec("time", "auto", null);
    final List<String> columns = Arrays.asList("time", "dim", "val");
    // ignore parse exception
    final IndexTuningConfig tuningConfig = createTuningConfig(2, null, null, null, null, false, false);

    // GranularitySpec.intervals and numShards must be null to verify reportParseException=false is respected both in
    // IndexTask.determineShardSpecs() and IndexTask.generateAndPublishSegments()
    final IndexIngestionSpec parseExceptionIgnoreSpec;
    if (useInputFormatApi) {
      parseExceptionIgnoreSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          timestampSpec,
          DimensionsSpec.EMPTY,
          new CsvInputFormat(columns, null, null, true, 0),
          null,
          null,
          tuningConfig,
          false
      );
    } else {
      parseExceptionIgnoreSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          new CSVParseSpec(timestampSpec, DimensionsSpec.EMPTY, null, columns, true, 0),
          null,
          null,
          tuningConfig,
          false
      );
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        parseExceptionIgnoreSpec,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
    );

    final List<DataSegment> segments = runTask(indexTask).rhs;

    Assert.assertEquals(Collections.singletonList("d"), segments.get(0).getDimensions());
    Assert.assertEquals(Collections.singletonList("val"), segments.get(0).getMetrics());
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

    final TimestampSpec timestampSpec = new TimestampSpec("time", "auto", null);
    final List<String> columns = Arrays.asList("time", "dim", "val");
    // report parse exception
    final IndexTuningConfig tuningConfig = createTuningConfig(2, null, null, null, null, false, true);
    final IndexIngestionSpec indexIngestionSpec;
    if (useInputFormatApi) {
      indexIngestionSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          timestampSpec,
          DimensionsSpec.EMPTY,
          new CsvInputFormat(columns, null, null, true, 0),
          null,
          null,
          tuningConfig,
          false
      );
    } else {
      indexIngestionSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          new CSVParseSpec(timestampSpec, DimensionsSpec.EMPTY, null, columns, true, 0),
          null,
          null,
          tuningConfig,
          false
      );
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        indexIngestionSpec,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
    );

    TaskStatus status = runTask(indexTask).lhs;
    Assert.assertEquals(TaskState.FAILED, status.getStatusCode());
    checkTaskStatusErrorMsgForParseExceptionsExceeded(status);

    Map<String, Object> expectedUnparseables = ImmutableMap.of(
        RowIngestionMeters.DETERMINE_PARTITIONS,
        new ArrayList<>(),
        RowIngestionMeters.BUILD_SEGMENTS,
        Collections.singletonList("Unparseable timestamp found! Event: {time=unparseable, d=a, val=1}")
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
      writer.write(
          "{\"time\":\"2014-01-01T00:00:10Z\",\"dim\":\"b\",\"dimLong\":\"notnumber\",\"dimFloat\":3.0,\"val\":1}\n"); // row with invalid long dimension
      writer.write(
          "{\"time\":\"2014-01-01T00:00:10Z\",\"dim\":\"b\",\"dimLong\":2,\"dimFloat\":\"notnumber\",\"val\":1}\n"); // row with invalid float dimension
      writer.write(
          "{\"time\":\"2014-01-01T00:00:10Z\",\"dim\":\"b\",\"dimLong\":2,\"dimFloat\":4.0,\"val\":\"notnumber\"}\n"); // row with invalid metric
      writer.write("{\"time\":9.0x,\"dim\":\"a\",\"dimLong\":2,\"dimFloat\":3.0,\"val\":1}\n"); // invalid JSON
      writer.write(
          "{\"time\":\"3014-03-01T00:00:10Z\",\"dim\":\"outsideofinterval\",\"dimLong\":2,\"dimFloat\":3.0,\"val\":1}\n"); // thrown away
      writer.write("{\"time\":\"99999999999-01-01T00:00:10Z\",\"dim\":\"b\",\"dimLong\":2,\"dimFloat\":3.0,\"val\":1}\n"); // unparseable time
      writer.write("this is not JSON\n"); // invalid JSON
    }

    final IndexTuningConfig tuningConfig = new IndexTuningConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        new HashedPartitionsSpec(2, null, null),
        INDEX_SPEC,
        null,
        null,
        true,
        false,
        null,
        null,
        null,
        true,
        7,
        7
    );

    final TimestampSpec timestampSpec = new TimestampSpec("time", "auto", null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("dim"),
            new LongDimensionSchema("dimLong"),
            new FloatDimensionSchema("dimFloat")
        )
    );
    final IndexIngestionSpec ingestionSpec;
    if (useInputFormatApi) {
      ingestionSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          timestampSpec,
          dimensionsSpec,
          new JsonInputFormat(null, null, null),
          null,
          null,
          tuningConfig,
          false
      );
    } else {
      ingestionSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          new JSONParseSpec(timestampSpec, dimensionsSpec, null, null, null),
          null,
          null,
          tuningConfig,
          false
      );
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        ingestionSpec,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
    );

    TaskStatus status = runTask(indexTask).lhs;
    Assert.assertEquals(TaskState.SUCCESS, status.getStatusCode());
    Assert.assertEquals(null, status.getErrorMsg());

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    Map<String, Object> expectedMetrics = ImmutableMap.of(
        RowIngestionMeters.DETERMINE_PARTITIONS,
        ImmutableMap.of(
            RowIngestionMeters.PROCESSED_WITH_ERROR, 0,
            RowIngestionMeters.PROCESSED, 4,
            RowIngestionMeters.UNPARSEABLE, 4,
            RowIngestionMeters.THROWN_AWAY, 1
        ),
        RowIngestionMeters.BUILD_SEGMENTS,
        ImmutableMap.of(

            RowIngestionMeters.PROCESSED_WITH_ERROR, 3,
            RowIngestionMeters.PROCESSED, 1,
            RowIngestionMeters.UNPARSEABLE, 4,
            RowIngestionMeters.THROWN_AWAY, 1
        )
    );
    Assert.assertEquals(expectedMetrics, reportData.getRowStats());

    Map<String, Object> expectedUnparseables = ImmutableMap.of(
        RowIngestionMeters.DETERMINE_PARTITIONS,
        Arrays.asList(
            "Unable to parse row [this is not JSON]",
            "Unparseable timestamp found! Event: {time=99999999999-01-01T00:00:10Z, dim=b, dimLong=2, dimFloat=3.0, val=1}",
            "Unable to parse row [{\"time\":9.0x,\"dim\":\"a\",\"dimLong\":2,\"dimFloat\":3.0,\"val\":1}]",
            "Unparseable timestamp found! Event: {time=unparseable, dim=a, dimLong=2, dimFloat=3.0, val=1}"
        ),
        RowIngestionMeters.BUILD_SEGMENTS,
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
    final IndexTuningConfig tuningConfig = new IndexTuningConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        new DynamicPartitionsSpec(2, null),
        INDEX_SPEC,
        null,
        null,
        false,
        false,
        null,
        null,
        null,
        true,
        2,
        5
    );

    final TimestampSpec timestampSpec = new TimestampSpec("time", "auto", null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("dim"),
            new LongDimensionSchema("dimLong"),
            new FloatDimensionSchema("dimFloat")
        )
    );
    final List<String> columns = Arrays.asList("time", "dim", "dimLong", "dimFloat", "val");
    final IndexIngestionSpec ingestionSpec;
    if (useInputFormatApi) {
      ingestionSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          timestampSpec,
          dimensionsSpec,
          new CsvInputFormat(columns, null, null, true, 0),
          null,
          null,
          tuningConfig,
          false
      );
    } else {
      ingestionSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          new CSVParseSpec(timestampSpec, dimensionsSpec, null, columns, true, 0),
          null,
          null,
          tuningConfig,
          false
      );
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        ingestionSpec,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
    );

    TaskStatus status = runTask(indexTask).lhs;
    Assert.assertEquals(TaskState.FAILED, status.getStatusCode());
    checkTaskStatusErrorMsgForParseExceptionsExceeded(status);

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    Map<String, Object> expectedMetrics = ImmutableMap.of(
        RowIngestionMeters.DETERMINE_PARTITIONS,
        ImmutableMap.of(
            RowIngestionMeters.PROCESSED_WITH_ERROR, 0,
            RowIngestionMeters.PROCESSED, 0,
            RowIngestionMeters.UNPARSEABLE, 0,
            RowIngestionMeters.THROWN_AWAY, 0
        ),
        RowIngestionMeters.BUILD_SEGMENTS,
        ImmutableMap.of(
            RowIngestionMeters.PROCESSED_WITH_ERROR, 0,
            RowIngestionMeters.PROCESSED, 1,
            RowIngestionMeters.UNPARSEABLE, 3,
            RowIngestionMeters.THROWN_AWAY, useInputFormatApi ? 1 : 2
        )
    );

    Assert.assertEquals(expectedMetrics, reportData.getRowStats());

    Map<String, Object> expectedUnparseables = ImmutableMap.of(
        RowIngestionMeters.DETERMINE_PARTITIONS,
        new ArrayList<>(),
        RowIngestionMeters.BUILD_SEGMENTS,
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
    final IndexTuningConfig tuningConfig = new IndexTuningConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        new HashedPartitionsSpec(2, null, null),
        INDEX_SPEC,
        null,
        null,
        true,
        false,
        null,
        null,
        null,
        true,
        2,
        5
    );

    final TimestampSpec timestampSpec = new TimestampSpec("time", "auto", null);
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("dim"),
            new LongDimensionSchema("dimLong"),
            new FloatDimensionSchema("dimFloat")
        )
    );
    final List<String> columns = Arrays.asList("time", "dim", "dimLong", "dimFloat", "val");
    final IndexIngestionSpec ingestionSpec;
    if (useInputFormatApi) {
      ingestionSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          timestampSpec,
          dimensionsSpec,
          new CsvInputFormat(columns, null, null, true, 0),
          null,
          null,
          tuningConfig,
          false
      );
    } else {
      ingestionSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          new CSVParseSpec(timestampSpec, dimensionsSpec, null, columns, true, 0),
          null,
          null,
          tuningConfig,
          false
      );
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        ingestionSpec,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
    );

    TaskStatus status = runTask(indexTask).lhs;
    Assert.assertEquals(TaskState.FAILED, status.getStatusCode());
    checkTaskStatusErrorMsgForParseExceptionsExceeded(status);

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    Map<String, Object> expectedMetrics = ImmutableMap.of(
        RowIngestionMeters.DETERMINE_PARTITIONS,
        ImmutableMap.of(
            RowIngestionMeters.PROCESSED_WITH_ERROR, 0,
            RowIngestionMeters.PROCESSED, 1,
            RowIngestionMeters.UNPARSEABLE, 3,
            RowIngestionMeters.THROWN_AWAY, useInputFormatApi ? 1 : 2
        ),
        RowIngestionMeters.BUILD_SEGMENTS,
        ImmutableMap.of(
            RowIngestionMeters.PROCESSED_WITH_ERROR, 0,
            RowIngestionMeters.PROCESSED, 0,
            RowIngestionMeters.UNPARSEABLE, 0,
            RowIngestionMeters.THROWN_AWAY, 0
        )
    );

    Assert.assertEquals(expectedMetrics, reportData.getRowStats());

    Map<String, Object> expectedUnparseables = ImmutableMap.of(
        RowIngestionMeters.DETERMINE_PARTITIONS,
        Arrays.asList(
            "Unparseable timestamp found! Event: {time=99999999999-01-01T00:00:10Z, dim=b, dimLong=2, dimFloat=3.0, val=1}",
            "Unparseable timestamp found! Event: {time=9.0, dim=a, dimLong=2, dimFloat=3.0, val=1}",
            "Unparseable timestamp found! Event: {time=unparseable, dim=a, dimLong=2, dimFloat=3.0, val=1}"
        ),
        RowIngestionMeters.BUILD_SEGMENTS,
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
      writer.write("ts,,\n");
      writer.write("2014-01-01T00:00:10Z,a,1\n");
    }

    tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("ts,dim,\n");
      writer.write("2014-01-01T00:00:10Z,a,1\n");
    }

    tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("ts,,val\n");
      writer.write("2014-01-01T00:00:10Z,a,1\n");
    }

    // report parse exception
    final IndexTuningConfig tuningConfig = createTuningConfig(2, 1, null, null, null, true, true);
    final IndexIngestionSpec ingestionSpec;
    if (useInputFormatApi) {
      ingestionSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          DEFAULT_TIMESTAMP_SPEC,
          DimensionsSpec.EMPTY,
          new CsvInputFormat(null, null, null, true, 0),
          null,
          null,
          tuningConfig,
          false
      );
    } else {
      ingestionSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          new CSVParseSpec(DEFAULT_TIMESTAMP_SPEC, DimensionsSpec.EMPTY, null, null, true, 0),
          null,
          null,
          tuningConfig,
          false
      );
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        ingestionSpec,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
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
          dimensions.equals(Sets.newHashSet("column_2")) ||
          dimensions.equals(Sets.newHashSet("dim", "column_2", "column_3"))
      );

      Assert.assertEquals(Collections.singletonList("val"), segment.getMetrics());
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

    final List<String> columns = Arrays.asList("ts", "", "");
    // report parse exception
    final IndexTuningConfig tuningConfig = createTuningConfig(2, null, null, null, null, false, true);
    final IndexIngestionSpec ingestionSpec;
    if (useInputFormatApi) {
      ingestionSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          DEFAULT_TIMESTAMP_SPEC,
          DimensionsSpec.EMPTY,
          new CsvInputFormat(columns, null, null, true, 0),
          null,
          null,
          tuningConfig,
          false
      );
    } else {
      ingestionSpec = createIngestionSpec(
          jsonMapper,
          tmpDir,
          new CSVParseSpec(DEFAULT_TIMESTAMP_SPEC, DimensionsSpec.EMPTY, null, columns, true, 0),
          null,
          null,
          tuningConfig,
          false
      );
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        ingestionSpec,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
    );

    TaskStatus status = runTask(indexTask).lhs;
    Assert.assertEquals(TaskState.FAILED, status.getStatusCode());

    checkTaskStatusErrorMsgForParseExceptionsExceeded(status);

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    Map<String, Object> expectedUnparseables = ImmutableMap.of(
        RowIngestionMeters.DETERMINE_PARTITIONS,
        new ArrayList<>(),
        RowIngestionMeters.BUILD_SEGMENTS,
        Collections.singletonList(
            "Unparseable timestamp found! Event: {column_1=2014-01-01T00:00:10Z, column_2=a, column_3=1}")
    );
    Assert.assertEquals(expectedUnparseables, reportData.getUnparseableEvents());
  }

  @Test
  public void testOverwriteWithSameSegmentGranularity() throws Exception
  {
    final File tmpDir = temporaryFolder.newFolder();
    final File tmpFile = File.createTempFile("druid", "index", tmpDir);

    populateRollupTestData(tmpFile);

    for (int i = 0; i < 2; i++) {
      final IndexTask indexTask = new IndexTask(
          null,
          null,
          createDefaultIngestionSpec(
              jsonMapper,
              tmpDir,
              new UniformGranularitySpec(
                  Granularities.DAY,
                  Granularities.DAY,
                  true,
                  null
              ),
              null,
              createTuningConfig(3, 2, null, 2L, null, false, true),
              false
          ),
          null,
          AuthTestUtils.TEST_AUTHORIZER_MAPPER,
          null,
          rowIngestionMetersFactory,
          appenderatorsManager
      );

      final List<DataSegment> segments = runTask(indexTask).rhs;

      Assert.assertEquals(5, segments.size());

      final Interval expectedInterval = Intervals.of("2014-01-01T00:00:00.000Z/2014-01-02T00:00:00.000Z");
      for (int j = 0; j < 5; j++) {
        final DataSegment segment = segments.get(j);
        Assert.assertEquals("test", segment.getDataSource());
        Assert.assertEquals(expectedInterval, segment.getInterval());
        if (i == 0) {
          Assert.assertEquals(NumberedShardSpec.class, segment.getShardSpec().getClass());
          Assert.assertEquals(j, segment.getShardSpec().getPartitionNum());
        } else {
          if (lockGranularity == LockGranularity.SEGMENT) {
            Assert.assertEquals(NumberedOverwriteShardSpec.class, segment.getShardSpec().getClass());
            final NumberedOverwriteShardSpec numberedOverwriteShardSpec =
                (NumberedOverwriteShardSpec) segment.getShardSpec();
            Assert.assertEquals(
                j + PartitionIds.NON_ROOT_GEN_START_PARTITION_ID,
                numberedOverwriteShardSpec.getPartitionNum()
            );
            Assert.assertEquals(1, numberedOverwriteShardSpec.getMinorVersion());
            Assert.assertEquals(5, numberedOverwriteShardSpec.getAtomicUpdateGroupSize());
            Assert.assertEquals(0, numberedOverwriteShardSpec.getStartRootPartitionId());
            Assert.assertEquals(5, numberedOverwriteShardSpec.getEndRootPartitionId());
          } else {
            Assert.assertEquals(NumberedShardSpec.class, segment.getShardSpec().getClass());
            final NumberedShardSpec numberedShardSpec = (NumberedShardSpec) segment.getShardSpec();
            Assert.assertEquals(j, numberedShardSpec.getPartitionNum());
          }
        }
      }
    }
  }

  @Test
  public void testOverwriteWithDifferentSegmentGranularity() throws Exception
  {
    final File tmpDir = temporaryFolder.newFolder();
    final File tmpFile = File.createTempFile("druid", "index", tmpDir);

    populateRollupTestData(tmpFile);

    for (int i = 0; i < 2; i++) {
      final Granularity segmentGranularity = i == 0 ? Granularities.DAY : Granularities.MONTH;
      final IndexTask indexTask = new IndexTask(
          null,
          null,
          createDefaultIngestionSpec(
              jsonMapper,
              tmpDir,
              new UniformGranularitySpec(
                  segmentGranularity,
                  Granularities.DAY,
                  true,
                  null
              ),
              null,
              createTuningConfig(3, 2, null, 2L, null, false, true),
              false
          ),
          null,
          AuthTestUtils.TEST_AUTHORIZER_MAPPER,
          null,
          rowIngestionMetersFactory,
          appenderatorsManager
      );

      final List<DataSegment> segments = runTask(indexTask).rhs;

      Assert.assertEquals(5, segments.size());

      final Interval expectedInterval = i == 0
                                        ? Intervals.of("2014-01-01/2014-01-02")
                                        : Intervals.of("2014-01-01/2014-02-01");
      for (int j = 0; j < 5; j++) {
        final DataSegment segment = segments.get(j);
        Assert.assertEquals("test", segment.getDataSource());
        Assert.assertEquals(expectedInterval, segment.getInterval());
        Assert.assertEquals(NumberedShardSpec.class, segment.getShardSpec().getClass());
        Assert.assertEquals(j, segment.getShardSpec().getPartitionNum());
      }
    }
  }

  @Test
  public void testIndexTaskWithSingleDimPartitionsSpecThrowingException() throws Exception
  {
    final File tmpDir = temporaryFolder.newFolder();
    final IndexTask task = new IndexTask(
        null,
        null,
        createDefaultIngestionSpec(
            jsonMapper,
            tmpDir,
            null,
            null,
            createTuningConfigWithPartitionsSpec(new SingleDimensionPartitionsSpec(null, 1, null, false), true),
            false
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory,
        appenderatorsManager
    );
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage(
        "partitionsSpec[org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec] is not supported"
    );
    task.isReady(createActionClient(task));
  }

  public static void checkTaskStatusErrorMsgForParseExceptionsExceeded(TaskStatus status)
  {
    // full stacktrace will be too long and make tests brittle (e.g. if line # changes), just match the main message
    Assert.assertThat(
        status.getErrorMsg(),
        CoreMatchers.containsString("Max parse exceptions exceeded")
    );
  }

  private Pair<TaskStatus, List<DataSegment>> runTask(IndexTask task) throws Exception
  {
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    final TaskStatus status = taskRunner.run(task).get();
    final List<DataSegment> segments = taskRunner.getPublishedSegments();
    return Pair.of(status, segments);
  }

  private static IndexTuningConfig createTuningConfigWithMaxRowsPerSegment(
      int maxRowsPerSegment,
      boolean forceGuaranteedRollup
  )
  {
    return createTuningConfig(
        maxRowsPerSegment,
        1,
        null,
        null,
        null,
        forceGuaranteedRollup,
        true
    );
  }

  private static IndexTuningConfig createTuningConfigWithPartitionsSpec(
      PartitionsSpec partitionsSpec,
      boolean forceGuaranteedRollup
  )
  {
    return createTuningConfig(
        null,
        1,
        null,
        null,
        partitionsSpec,
        forceGuaranteedRollup,
        true
    );
  }

  static IndexTuningConfig createTuningConfig(
      @Nullable Integer maxRowsPerSegment,
      @Nullable Integer maxRowsInMemory,
      @Nullable Long maxBytesInMemory,
      @Nullable Long maxTotalRows,
      @Nullable PartitionsSpec partitionsSpec,
      boolean forceGuaranteedRollup,
      boolean reportParseException
  )
  {
    return new IndexTuningConfig(
        null,
        maxRowsPerSegment,
        maxRowsInMemory,
        maxBytesInMemory,
        maxTotalRows,
        null,
        null,
        null,
        partitionsSpec,
        INDEX_SPEC,
        null,
        null,
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
        taskRunner.getTaskReportsFile(),
        new TypeReference<Map<String, TaskReport>>()
        {
        }
    );
    return IngestionStatsAndErrorsTaskReportData.getPayloadFromTaskReports(
        taskReports
    );
  }

  private IndexIngestionSpec createDefaultIngestionSpec(
      ObjectMapper objectMapper,
      File baseDir,
      @Nullable GranularitySpec granularitySpec,
      @Nullable TransformSpec transformSpec,
      IndexTuningConfig tuningConfig,
      boolean appendToExisting
  )
  {
    if (useInputFormatApi) {
      return createIngestionSpec(
          objectMapper,
          baseDir,
          DEFAULT_TIMESTAMP_SPEC,
          DEFAULT_DIMENSIONS_SPEC,
          DEFAULT_INPUT_FORMAT,
          transformSpec,
          granularitySpec,
          tuningConfig,
          appendToExisting
      );
    } else {
      return createIngestionSpec(
          objectMapper,
          baseDir,
          DEFAULT_PARSE_SPEC,
          transformSpec,
          granularitySpec,
          tuningConfig,
          appendToExisting
      );
    }
  }

  static IndexIngestionSpec createIngestionSpec(
      ObjectMapper objectMapper,
      File baseDir,
      @Nullable ParseSpec parseSpec,
      @Nullable TransformSpec transformSpec,
      @Nullable GranularitySpec granularitySpec,
      IndexTuningConfig tuningConfig,
      boolean appendToExisting
  )
  {
    return createIngestionSpec(
        objectMapper,
        baseDir,
        parseSpec,
        null,
        null,
        null,
        transformSpec,
        granularitySpec,
        tuningConfig,
        appendToExisting
    );
  }

  static IndexIngestionSpec createIngestionSpec(
      ObjectMapper objectMapper,
      File baseDir,
      TimestampSpec timestampSpec,
      DimensionsSpec dimensionsSpec,
      InputFormat inputFormat,
      @Nullable TransformSpec transformSpec,
      @Nullable GranularitySpec granularitySpec,
      IndexTuningConfig tuningConfig,
      boolean appendToExisting
  )
  {
    return createIngestionSpec(
        objectMapper,
        baseDir,
        null,
        timestampSpec,
        dimensionsSpec,
        inputFormat,
        transformSpec,
        granularitySpec,
        tuningConfig,
        appendToExisting
    );
  }

  private static IndexIngestionSpec createIngestionSpec(
      ObjectMapper objectMapper,
      File baseDir,
      @Nullable ParseSpec parseSpec,
      @Nullable TimestampSpec timestampSpec,
      @Nullable DimensionsSpec dimensionsSpec,
      @Nullable InputFormat inputFormat,
      @Nullable TransformSpec transformSpec,
      @Nullable GranularitySpec granularitySpec,
      IndexTuningConfig tuningConfig,
      boolean appendToExisting
  )
  {
    if (inputFormat != null) {
      Preconditions.checkArgument(parseSpec == null, "Can't use parseSpec");
      return new IndexIngestionSpec(
          new DataSchema(
              "test",
              Preconditions.checkNotNull(timestampSpec, "timestampSpec"),
              Preconditions.checkNotNull(dimensionsSpec, "dimensionsSpec"),
              new AggregatorFactory[]{
                  new LongSumAggregatorFactory("val", "val")
              },
              granularitySpec != null ? granularitySpec : new UniformGranularitySpec(
                  Granularities.DAY,
                  Granularities.MINUTE,
                  Collections.singletonList(Intervals.of("2014/2015"))
              ),
              transformSpec
          ),
          new IndexIOConfig(
              null,
              new LocalInputSource(baseDir, "druid*"),
              inputFormat,
              appendToExisting
          ),
          tuningConfig
      );
    } else {
      return new IndexIngestionSpec(
          new DataSchema(
              "test",
              objectMapper.convertValue(
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
                  Collections.singletonList(Intervals.of("2014/2015"))
              ),
              transformSpec,
              objectMapper
          ),
          new IndexIOConfig(
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
  }
}
