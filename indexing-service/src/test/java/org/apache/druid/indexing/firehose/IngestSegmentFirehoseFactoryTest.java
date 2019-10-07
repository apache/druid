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
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.inject.Binder;
import com.google.inject.Module;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.GuiceAnnotationIntrospector;
import org.apache.druid.guice.GuiceInjectableValues;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SegmentLoaderFactory;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.HeapMemoryTaskStorage;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.loading.LocalDataSegmentPuller;
import org.apache.druid.segment.loading.LocalLoadSpec;
import org.apache.druid.segment.realtime.firehose.CombiningFirehoseFactory;
import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.partition.NumberedPartitionChunk;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *
 */
@RunWith(Parameterized.class)
public class IngestSegmentFirehoseFactoryTest
{
  private static final ObjectMapper MAPPER;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  private static final TaskStorage TASK_STORAGE;
  private static final IndexerSQLMetadataStorageCoordinator MDC;
  private static final TaskLockbox TASK_LOCKBOX;
  private static final Task TASK;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  static {
    TestUtils testUtils = new TestUtils();
    MAPPER = setupInjectablesInObjectMapper(TestHelper.makeJsonMapper());
    INDEX_MERGER_V9 = testUtils.getTestIndexMergerV9();
    INDEX_IO = testUtils.getTestIndexIO();
    TASK_STORAGE = new HeapMemoryTaskStorage(
        new TaskStorageConfig(null)
        {
        }
    );
    MDC = new IndexerSQLMetadataStorageCoordinator(null, null, null)
    {
      private final Set<DataSegment> published = new HashSet<>();

      @Override
      public List<DataSegment> getUsedSegmentsForInterval(String dataSource, Interval interval)
      {
        return ImmutableList.copyOf(SEGMENT_SET);
      }

      @Override
      public List<DataSegment> getUsedSegmentsForIntervals(String dataSource, List<Interval> interval)
      {
        return ImmutableList.copyOf(SEGMENT_SET);
      }

      @Override
      public List<DataSegment> getUnusedSegmentsForInterval(String dataSource, Interval interval)
      {
        return ImmutableList.of();
      }

      @Override
      public Set<DataSegment> announceHistoricalSegments(Set<DataSegment> segments)
      {
        Set<DataSegment> added = new HashSet<>();
        for (final DataSegment segment : segments) {
          if (published.add(segment)) {
            added.add(segment);
          }
        }

        return ImmutableSet.copyOf(added);
      }

      @Override
      public void deleteSegments(Set<DataSegment> segments)
      {
        // do nothing
      }
    };
    TASK_LOCKBOX = new TaskLockbox(TASK_STORAGE, MDC);
    TASK = NoopTask.create();
    TASK_LOCKBOX.add(TASK);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder() throws IOException
  {
    final IndexSpec indexSpec = new IndexSpec();

    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(JodaUtils.MIN_INSTANT)
        .withDimensionsSpec(ROW_PARSER)
        .withMetrics(
            new LongSumAggregatorFactory(METRIC_LONG_NAME, DIM_LONG_NAME),
            new DoubleSumAggregatorFactory(METRIC_FLOAT_NAME, DIM_FLOAT_NAME)
        )
        .build();
    final IncrementalIndex index = new IncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(MAX_ROWS * MAX_SHARD_NUMBER)
        .buildOnheap();

    for (Integer i = 0; i < MAX_ROWS; ++i) {
      index.add(ROW_PARSER.parseBatch(buildRow(i.longValue())).get(0));
    }

    if (!PERSIST_DIR.mkdirs() && !PERSIST_DIR.exists()) {
      throw new IOE("Could not create directory at [%s]", PERSIST_DIR.getAbsolutePath());
    }
    INDEX_MERGER_V9.persist(index, PERSIST_DIR, indexSpec, null);

    final CoordinatorClient cc = new CoordinatorClient(null, null)
    {
      @Override
      public List<DataSegment> getDatabaseSegmentDataSourceSegments(String dataSource, List<Interval> intervals)
      {
        return ImmutableList.copyOf(SEGMENT_SET);
      }
    };

    SegmentHandoffNotifierFactory notifierFactory = EasyMock.createNiceMock(SegmentHandoffNotifierFactory.class);
    EasyMock.replay(notifierFactory);

    final SegmentLoaderFactory slf = new SegmentLoaderFactory(null, MAPPER);
    final RetryPolicyFactory retryPolicyFactory = new RetryPolicyFactory(new RetryPolicyConfig());

    Collection<Object[]> values = new ArrayList<>();
    for (InputRowParser parser : Arrays.<InputRowParser>asList(
        ROW_PARSER,
        new MapInputRowParser(
            new JSONParseSpec(
                new TimestampSpec(TIME_COLUMN, "auto", null),
                new DimensionsSpec(
                    DimensionsSpec.getDefaultSchemas(ImmutableList.of()),
                    ImmutableList.of(DIM_FLOAT_NAME, DIM_LONG_NAME),
                    ImmutableList.of()
                ),
                null,
                null
            )
        )
    )) {
      for (List<String> dim_names : Arrays.<List<String>>asList(null, ImmutableList.of(DIM_NAME))) {
        for (List<String> metric_names : Arrays.<List<String>>asList(
            null,
            ImmutableList.of(METRIC_LONG_NAME, METRIC_FLOAT_NAME)
        )) {
          for (Boolean wrapInCombining : Arrays.asList(false, true)) {
            final IngestSegmentFirehoseFactory isfFactory = new IngestSegmentFirehoseFactory(
                TASK.getDataSource(),
                Intervals.ETERNITY,
                null,
                new SelectorDimFilter(DIM_NAME, DIM_VALUE, null),
                dim_names,
                metric_names,
                null,
                INDEX_IO,
                cc,
                slf,
                retryPolicyFactory
            );
            final FirehoseFactory factory = wrapInCombining
                                            ? new CombiningFirehoseFactory(ImmutableList.of(isfFactory))
                                            : isfFactory;
            values.add(
                new Object[]{
                    StringUtils.format(
                        "DimNames[%s]MetricNames[%s]ParserDimNames[%s]WrapInCombining[%s]",
                        dim_names == null ? "null" : "dims",
                        metric_names == null ? "null" : "metrics",
                        parser == ROW_PARSER ? "dims" : "null",
                        wrapInCombining
                    ),
                    factory,
                    parser
                }
            );
          }
        }
      }
    }
    return values;
  }

  public static ObjectMapper setupInjectablesInObjectMapper(ObjectMapper objectMapper)
  {
    objectMapper.registerModule(
        new SimpleModule("testModule").registerSubtypes(LocalLoadSpec.class)
    );

    final GuiceAnnotationIntrospector guiceIntrospector = new GuiceAnnotationIntrospector();
    objectMapper.setAnnotationIntrospectors(
        new AnnotationIntrospectorPair(
            guiceIntrospector,
            objectMapper.getSerializationConfig().getAnnotationIntrospector()
        ),
        new AnnotationIntrospectorPair(
            guiceIntrospector,
            objectMapper.getDeserializationConfig().getAnnotationIntrospector()
        )
    );
    objectMapper.setInjectableValues(
        new GuiceInjectableValues(
            GuiceInjectors.makeStartupInjectorWithModules(
                ImmutableList.of(
                    new Module()
                    {
                      @Override
                      public void configure(Binder binder)
                      {
                        binder.bind(LocalDataSegmentPuller.class);
                      }
                    }
                )
            )
        )
    );
    return objectMapper;
  }

  public IngestSegmentFirehoseFactoryTest(
      String testName,
      FirehoseFactory factory,
      InputRowParser rowParser
  )
  {
    this.factory = factory;

    // Must decorate the parser, since IngestSegmentFirehoseFactory will undecorate it.
    this.rowParser = TransformSpec.NONE.decorate(rowParser);
  }

  private static final Logger log = new Logger(IngestSegmentFirehoseFactoryTest.class);
  private static final String DATA_SOURCE_NAME = "testDataSource";
  private static final String DATA_SOURCE_VERSION = "version";
  private static final Integer BINARY_VERSION = -1;
  private static final String DIM_NAME = "testDimName";
  private static final String DIM_VALUE = "testDimValue";
  private static final String DIM_LONG_NAME = "testDimLongName";
  private static final String DIM_FLOAT_NAME = "testDimFloatName";
  private static final String METRIC_LONG_NAME = "testLongMetric";
  private static final String METRIC_FLOAT_NAME = "testFloatMetric";
  private static final Long METRIC_LONG_VALUE = 1L;
  private static final Float METRIC_FLOAT_VALUE = 1.0f;
  private static final String TIME_COLUMN = "ts";
  private static final Integer MAX_SHARD_NUMBER = 10;
  private static final Integer MAX_ROWS = 10;
  private static final File TMP_DIR = Files.createTempDir();
  private static final File PERSIST_DIR = Paths.get(TMP_DIR.getAbsolutePath(), "indexTestMerger").toFile();
  private static final List<DataSegment> SEGMENT_SET = new ArrayList<>(MAX_SHARD_NUMBER);

  private final FirehoseFactory<InputRowParser> factory;
  private final InputRowParser rowParser;
  private File tempDir;

  private static final InputRowParser<Map<String, Object>> ROW_PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIME_COLUMN, "auto", null),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of(DIM_NAME)),
              ImmutableList.of(DIM_FLOAT_NAME, DIM_LONG_NAME),
              ImmutableList.of()
          )
      )
  );

  private static Map<String, Object> buildRow(Long ts)
  {
    return ImmutableMap.of(
        TIME_COLUMN, ts,
        DIM_NAME, DIM_VALUE,
        DIM_FLOAT_NAME, METRIC_FLOAT_VALUE,
        DIM_LONG_NAME, METRIC_LONG_VALUE
    );
  }

  private static DataSegment buildSegment(Integer shardNumber)
  {
    Preconditions.checkArgument(shardNumber < MAX_SHARD_NUMBER);
    Preconditions.checkArgument(shardNumber >= 0);
    return new DataSegment(
        DATA_SOURCE_NAME,
        Intervals.ETERNITY,
        DATA_SOURCE_VERSION,
        ImmutableMap.of(
            "type", "local",
            "path", PERSIST_DIR.getAbsolutePath()
        ),
        ImmutableList.of(DIM_NAME),
        ImmutableList.of(METRIC_LONG_NAME, METRIC_FLOAT_NAME),
        new NumberedShardSpec(
            shardNumber,
            MAX_SHARD_NUMBER
        ),
        BINARY_VERSION,
        0L
    );
  }

  @BeforeClass
  public static void setUpStatic()
  {
    for (int i = 0; i < MAX_SHARD_NUMBER; ++i) {
      SEGMENT_SET.add(buildSegment(i));
    }
  }

  @AfterClass
  public static void tearDownStatic()
  {
    recursivelyDelete(TMP_DIR);
  }

  private static void recursivelyDelete(final File dir)
  {
    if (dir != null) {
      if (dir.isDirectory()) {
        final File[] files = dir.listFiles();
        if (files != null) {
          for (File file : files) {
            recursivelyDelete(file);
          }
        }
      } else {
        if (!dir.delete()) {
          log.warn("Could not delete file at [%s]", dir.getAbsolutePath());
        }
      }
    }
  }

  @Before
  public void setup() throws IOException
  {
    tempDir = temporaryFolder.newFolder();
  }

  @After
  public void teardown()
  {
    tempDir.delete();
  }

  @Test
  public void sanityTest()
  {
    if (factory instanceof CombiningFirehoseFactory) {
      // This method tests IngestSegmentFirehoseFactory-specific methods.
      return;
    }
    final IngestSegmentFirehoseFactory isfFactory = (IngestSegmentFirehoseFactory) factory;
    Assert.assertEquals(TASK.getDataSource(), isfFactory.getDataSource());
    if (isfFactory.getDimensions() != null) {
      Assert.assertArrayEquals(new String[]{DIM_NAME}, isfFactory.getDimensions().toArray());
    }
    Assert.assertEquals(Intervals.ETERNITY, isfFactory.getInterval());
    if (isfFactory.getMetrics() != null) {
      Assert.assertEquals(
          ImmutableSet.of(METRIC_LONG_NAME, METRIC_FLOAT_NAME),
          ImmutableSet.copyOf(isfFactory.getMetrics())
      );
    }
  }

  @Test
  public void simpleFirehoseReadingTest() throws IOException
  {
    Assert.assertEquals(MAX_SHARD_NUMBER.longValue(), SEGMENT_SET.size());
    Integer rowcount = 0;
    try (final Firehose firehose = factory.connect(rowParser, TMP_DIR)) {
      while (firehose.hasMore()) {
        InputRow row = firehose.nextRow();
        Assert.assertArrayEquals(new String[]{DIM_NAME}, row.getDimensions().toArray());
        Assert.assertArrayEquals(new String[]{DIM_VALUE}, row.getDimension(DIM_NAME).toArray());
        Assert.assertEquals(METRIC_LONG_VALUE.longValue(), row.getMetric(METRIC_LONG_NAME));
        Assert.assertEquals(
            METRIC_FLOAT_VALUE,
            row.getMetric(METRIC_FLOAT_NAME).floatValue(),
            METRIC_FLOAT_VALUE * 0.0001
        );
        ++rowcount;
      }
    }
    Assert.assertEquals((int) MAX_SHARD_NUMBER * MAX_ROWS, (int) rowcount);
  }

  @Test
  public void testTransformSpec() throws IOException
  {
    Assert.assertEquals(MAX_SHARD_NUMBER.longValue(), SEGMENT_SET.size());
    Integer rowcount = 0;
    final TransformSpec transformSpec = new TransformSpec(
        new SelectorDimFilter(ColumnHolder.TIME_COLUMN_NAME, "1", null),
        ImmutableList.of(
            new ExpressionTransform(METRIC_FLOAT_NAME, METRIC_FLOAT_NAME + " * 10", ExprMacroTable.nil())
        )
    );
    int skipped = 0;
    try (final Firehose firehose =
             factory.connect(transformSpec.decorate(rowParser), TMP_DIR)) {
      while (firehose.hasMore()) {
        InputRow row = firehose.nextRow();
        if (row == null) {
          skipped++;
          continue;
        }
        Assert.assertArrayEquals(new String[]{DIM_NAME}, row.getDimensions().toArray());
        Assert.assertArrayEquals(new String[]{DIM_VALUE}, row.getDimension(DIM_NAME).toArray());
        Assert.assertEquals(METRIC_LONG_VALUE.longValue(), row.getMetric(METRIC_LONG_NAME).longValue());
        Assert.assertEquals(
            METRIC_FLOAT_VALUE * 10,
            row.getMetric(METRIC_FLOAT_NAME).floatValue(),
            METRIC_FLOAT_VALUE * 0.0001
        );
        ++rowcount;
      }
    }
    Assert.assertEquals(90, skipped);
    Assert.assertEquals((int) MAX_ROWS, (int) rowcount);
  }

  @Test
  public void testGetUniqueDimensionsAndMetrics()
  {
    final int numSegmentsPerPartitionChunk = 5;
    final int numPartitionChunksPerTimelineObject = 10;
    final int numSegments = numSegmentsPerPartitionChunk * numPartitionChunksPerTimelineObject;
    final Interval interval = Intervals.of("2017-01-01/2017-01-02");
    final String version = "1";

    final List<TimelineObjectHolder<String, DataSegment>> timelineSegments = new ArrayList<>();
    for (int i = 0; i < numPartitionChunksPerTimelineObject; i++) {
      final List<PartitionChunk<DataSegment>> chunks = new ArrayList<>();
      for (int j = 0; j < numSegmentsPerPartitionChunk; j++) {
        final List<String> dims = IntStream.range(i, i + numSegmentsPerPartitionChunk)
                                           .mapToObj(suffix -> "dim" + suffix)
                                           .collect(Collectors.toList());
        final List<String> metrics = IntStream.range(i, i + numSegmentsPerPartitionChunk)
                                              .mapToObj(suffix -> "met" + suffix)
                                              .collect(Collectors.toList());
        final DataSegment segment = new DataSegment(
            "ds",
            interval,
            version,
            ImmutableMap.of(),
            dims,
            metrics,
            new NumberedShardSpec(numPartitionChunksPerTimelineObject, i),
            1,
            1
        );

        final PartitionChunk<DataSegment> partitionChunk = new NumberedPartitionChunk<>(
            i,
            numPartitionChunksPerTimelineObject,
            segment
        );
        chunks.add(partitionChunk);
      }
      final TimelineObjectHolder<String, DataSegment> timelineHolder = new TimelineObjectHolder<>(
          interval,
          version,
          new PartitionHolder<>(chunks)
      );
      timelineSegments.add(timelineHolder);
    }

    final String[] expectedDims = new String[]{
        "dim9",
        "dim10",
        "dim11",
        "dim12",
        "dim13",
        "dim8",
        "dim7",
        "dim6",
        "dim5",
        "dim4",
        "dim3",
        "dim2",
        "dim1",
        "dim0"
    };
    final String[] expectedMetrics = new String[]{
        "met9",
        "met10",
        "met11",
        "met12",
        "met13",
        "met8",
        "met7",
        "met6",
        "met5",
        "met4",
        "met3",
        "met2",
        "met1",
        "met0"
    };
    Assert.assertEquals(
        Arrays.asList(expectedDims),
        IngestSegmentFirehoseFactory.getUniqueDimensions(timelineSegments, null)
    );
    Assert.assertEquals(
        Arrays.asList(expectedMetrics),
        IngestSegmentFirehoseFactory.getUniqueMetrics(timelineSegments)
    );
  }

  private static ServiceEmitter newMockEmitter()
  {
    return new NoopServiceEmitter();
  }
}
