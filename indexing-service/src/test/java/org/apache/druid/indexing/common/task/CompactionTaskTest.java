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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.NoopIndexingServiceClient;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.NoopInputRowParser;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.guice.GuiceAnnotationIntrospector;
import org.apache.druid.guice.GuiceInjectableValues;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SegmentLoaderFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.actions.SegmentListUsedAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.CompactionTask.Builder;
import org.apache.druid.indexing.common.task.CompactionTask.PartitionConfigurationManager;
import org.apache.druid.indexing.common.task.CompactionTask.SegmentProvider;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIOConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIngestionSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.indexing.firehose.IngestSegmentFirehoseFactory;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.first.FloatFirstAggregatorFactory;
import org.apache.druid.query.aggregation.last.DoubleLastAggregatorFactory;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.SimpleQueryableIndex;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.SpatialIndex;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.CompressionFactory.LongEncodingStrategy;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;
import org.apache.druid.segment.transform.TransformingInputRowParser;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.hamcrest.CoreMatchers;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CompactionTaskTest
{
  private static final long SEGMENT_SIZE_BYTES = 100;
  private static final int NUM_ROWS_PER_SEGMENT = 10;
  private static final String DATA_SOURCE = "dataSource";
  private static final String TIMESTAMP_COLUMN = "timestamp";
  private static final String MIXED_TYPE_COLUMN = "string_to_double";
  private static final Interval COMPACTION_INTERVAL = Intervals.of("2017-01-01/2017-07-01");
  private static final List<Interval> SEGMENT_INTERVALS = ImmutableList.of(
      Intervals.of("2017-01-01/2017-02-01"),
      Intervals.of("2017-02-01/2017-03-01"),
      Intervals.of("2017-03-01/2017-04-01"),
      Intervals.of("2017-04-01/2017-05-01"),
      Intervals.of("2017-05-01/2017-06-01"),
      Intervals.of("2017-06-01/2017-07-01")
  );
  private static final Map<Interval, DimensionSchema> MIXED_TYPE_COLUMN_MAP = new HashMap<>();
  private static final ParallelIndexTuningConfig TUNING_CONFIG = createTuningConfig();

  private static final RowIngestionMetersFactory ROW_INGESTION_METERS_FACTORY = new TestUtils()
      .getRowIngestionMetersFactory();
  private static final Map<DataSegment, File> SEGMENT_MAP = new HashMap<>();
  private static final CoordinatorClient COORDINATOR_CLIENT = new TestCoordinatorClient(SEGMENT_MAP);
  private static IndexingServiceClient INDEXING_SERVICE_CLIENT = new NoopIndexingServiceClient();
  private static final AppenderatorsManager APPENDERATORS_MANAGER = new TestAppenderatorsManager();
  private static final ObjectMapper OBJECT_MAPPER = setupInjectablesInObjectMapper(new DefaultObjectMapper());
  private static final RetryPolicyFactory RETRY_POLICY_FACTORY = new RetryPolicyFactory(new RetryPolicyConfig());

  private static Map<String, DimensionSchema> DIMENSIONS;
  private static List<AggregatorFactory> AGGREGATORS;
  private static List<DataSegment> SEGMENTS;

  private TaskToolbox toolbox;
  private SegmentLoaderFactory segmentLoaderFactory;

  @BeforeClass
  public static void setupClass()
  {
    MIXED_TYPE_COLUMN_MAP.put(Intervals.of("2017-01-01/2017-02-01"), new StringDimensionSchema(MIXED_TYPE_COLUMN));
    MIXED_TYPE_COLUMN_MAP.put(Intervals.of("2017-02-01/2017-03-01"), new StringDimensionSchema(MIXED_TYPE_COLUMN));
    MIXED_TYPE_COLUMN_MAP.put(Intervals.of("2017-03-01/2017-04-01"), new StringDimensionSchema(MIXED_TYPE_COLUMN));
    MIXED_TYPE_COLUMN_MAP.put(Intervals.of("2017-04-01/2017-05-01"), new StringDimensionSchema(MIXED_TYPE_COLUMN));
    MIXED_TYPE_COLUMN_MAP.put(Intervals.of("2017-05-01/2017-06-01"), new DoubleDimensionSchema(MIXED_TYPE_COLUMN));
    MIXED_TYPE_COLUMN_MAP.put(Intervals.of("2017-06-01/2017-07-01"), new DoubleDimensionSchema(MIXED_TYPE_COLUMN));

    DIMENSIONS = new HashMap<>();
    AGGREGATORS = new ArrayList<>();

    DIMENSIONS.put(ColumnHolder.TIME_COLUMN_NAME, new LongDimensionSchema(ColumnHolder.TIME_COLUMN_NAME));
    DIMENSIONS.put(TIMESTAMP_COLUMN, new LongDimensionSchema(TIMESTAMP_COLUMN));
    for (int i = 0; i < SEGMENT_INTERVALS.size(); i++) {
      final StringDimensionSchema schema = new StringDimensionSchema(
          "string_dim_" + i,
          null,
          null
      );
      DIMENSIONS.put(schema.getName(), schema);
    }
    for (int i = 0; i < SEGMENT_INTERVALS.size(); i++) {
      final LongDimensionSchema schema = new LongDimensionSchema("long_dim_" + i);
      DIMENSIONS.put(schema.getName(), schema);
    }
    for (int i = 0; i < SEGMENT_INTERVALS.size(); i++) {
      final FloatDimensionSchema schema = new FloatDimensionSchema("float_dim_" + i);
      DIMENSIONS.put(schema.getName(), schema);
    }
    for (int i = 0; i < SEGMENT_INTERVALS.size(); i++) {
      final DoubleDimensionSchema schema = new DoubleDimensionSchema("double_dim_" + i);
      DIMENSIONS.put(schema.getName(), schema);
    }

    AGGREGATORS.add(new CountAggregatorFactory("agg_0"));
    AGGREGATORS.add(new LongSumAggregatorFactory("agg_1", "long_dim_1"));
    AGGREGATORS.add(new LongMaxAggregatorFactory("agg_2", "long_dim_2"));
    AGGREGATORS.add(new FloatFirstAggregatorFactory("agg_3", "float_dim_3"));
    AGGREGATORS.add(new DoubleLastAggregatorFactory("agg_4", "double_dim_4"));

    for (int i = 0; i < SEGMENT_INTERVALS.size(); i++) {
      final Interval segmentInterval = Intervals.of(StringUtils.format("2017-0%d-01/2017-0%d-01", (i + 1), (i + 2)));
      SEGMENT_MAP.put(
          new DataSegment(
              DATA_SOURCE,
              segmentInterval,
              "version",
              ImmutableMap.of(),
              findDimensions(i, segmentInterval),
              AGGREGATORS.stream().map(AggregatorFactory::getName).collect(Collectors.toList()),
              new NumberedShardSpec(0, 1),
              0,
              SEGMENT_SIZE_BYTES
          ),
          new File("file_" + i)
      );
    }
    SEGMENTS = new ArrayList<>(SEGMENT_MAP.keySet());
  }

  private static ObjectMapper setupInjectablesInObjectMapper(ObjectMapper objectMapper)
  {
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
    GuiceInjectableValues injectableValues = new GuiceInjectableValues(
        GuiceInjectors.makeStartupInjectorWithModules(
            ImmutableList.of(
                binder -> {
                  binder.bind(AuthorizerMapper.class).toInstance(AuthTestUtils.TEST_AUTHORIZER_MAPPER);
                  binder.bind(ChatHandlerProvider.class).toInstance(new NoopChatHandlerProvider());
                  binder.bind(RowIngestionMetersFactory.class).toInstance(ROW_INGESTION_METERS_FACTORY);
                  binder.bind(CoordinatorClient.class).toInstance(COORDINATOR_CLIENT);
                  binder.bind(SegmentLoaderFactory.class).toInstance(new SegmentLoaderFactory(null, objectMapper));
                  binder.bind(AppenderatorsManager.class).toInstance(APPENDERATORS_MANAGER);
                  binder.bind(IndexingServiceClient.class).toInstance(INDEXING_SERVICE_CLIENT);
                }
            )
        )
    );
    objectMapper.setInjectableValues(injectableValues);
    objectMapper.registerModule(
        new SimpleModule().registerSubtypes(new NamedType(NumberedShardSpec.class, "NumberedShardSpec"))
    );
    return objectMapper;
  }

  private static List<String> findDimensions(int startIndex, Interval segmentInterval)
  {
    final List<String> dimensions = new ArrayList<>();
    dimensions.add(TIMESTAMP_COLUMN);
    for (int i = 0; i < 6; i++) {
      int postfix = i + startIndex;
      postfix = postfix >= 6 ? postfix - 6 : postfix;
      dimensions.add("string_dim_" + postfix);
      dimensions.add("long_dim_" + postfix);
      dimensions.add("float_dim_" + postfix);
      dimensions.add("double_dim_" + postfix);
    }
    dimensions.add(MIXED_TYPE_COLUMN_MAP.get(segmentInterval).getName());
    return dimensions;
  }

  private static ParallelIndexTuningConfig createTuningConfig()
  {
    return new ParallelIndexTuningConfig(
        null,
        null, // null to compute maxRowsPerSegment automatically
        500000,
        1000000L,
        null,
        null,
        null,
        null,
        new IndexSpec(
            new RoaringBitmapSerdeFactory(true),
            CompressionStrategy.LZ4,
            CompressionStrategy.LZF,
            LongEncodingStrategy.LONGS
        ),
        null,
        null,
        true,
        false,
        5000L,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup()
  {
    final IndexIO testIndexIO = new TestIndexIO(OBJECT_MAPPER, SEGMENT_MAP);
    toolbox = new TestTaskToolbox(
        new TestTaskActionClient(new ArrayList<>(SEGMENT_MAP.keySet())),
        testIndexIO,
        SEGMENT_MAP
    );
    segmentLoaderFactory = new SegmentLoaderFactory(testIndexIO, OBJECT_MAPPER);
  }

  @Test
  public void testSerdeWithInterval() throws IOException
  {
    final Builder builder = new Builder(
        DATA_SOURCE,
        OBJECT_MAPPER,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        ROW_INGESTION_METERS_FACTORY,
        INDEXING_SERVICE_CLIENT,
        COORDINATOR_CLIENT,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY,
        APPENDERATORS_MANAGER
    );
    final CompactionTask task = builder
        .inputSpec(
            new CompactionIntervalSpec(COMPACTION_INTERVAL, SegmentUtils.hashIds(SEGMENTS))
        )
        .tuningConfig(createTuningConfig())
        .context(ImmutableMap.of("testKey", "testContext"))
        .build();

    final byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(task);
    final CompactionTask fromJson = OBJECT_MAPPER.readValue(bytes, CompactionTask.class);
    assertEquals(task, fromJson);
  }

  @Test
  public void testSerdeWithSegments() throws IOException
  {
    final Builder builder = new Builder(
        DATA_SOURCE,
        OBJECT_MAPPER,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        ROW_INGESTION_METERS_FACTORY,
        INDEXING_SERVICE_CLIENT,
        COORDINATOR_CLIENT,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY,
        APPENDERATORS_MANAGER
    );
    final CompactionTask task = builder
        .segments(SEGMENTS)
        .tuningConfig(createTuningConfig())
        .context(ImmutableMap.of("testKey", "testContext"))
        .build();

    final byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(task);
    final CompactionTask fromJson = OBJECT_MAPPER.readValue(bytes, CompactionTask.class);
    assertEquals(task, fromJson);
  }

  @Test
  public void testSerdeWithDimensions() throws IOException
  {
    final Builder builder = new Builder(
        DATA_SOURCE,
        OBJECT_MAPPER,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        ROW_INGESTION_METERS_FACTORY,
        INDEXING_SERVICE_CLIENT,
        COORDINATOR_CLIENT,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY,
        APPENDERATORS_MANAGER
    );

    final CompactionTask task = builder
        .segments(SEGMENTS)
        .dimensionsSpec(
            new DimensionsSpec(
                ImmutableList.of(
                    new StringDimensionSchema("dim1"),
                    new StringDimensionSchema("dim2"),
                    new StringDimensionSchema("dim3")
                )
            )
        )
        .tuningConfig(createTuningConfig())
        .context(ImmutableMap.of("testKey", "testVal"))
        .build();

    final byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(task);
    final CompactionTask fromJson = OBJECT_MAPPER.readValue(bytes, CompactionTask.class);
    assertEquals(task, fromJson);
  }

  private static void assertEquals(CompactionTask expected, CompactionTask actual)
  {
    Assert.assertEquals(expected.getType(), actual.getType());
    Assert.assertEquals(expected.getDataSource(), actual.getDataSource());
    Assert.assertEquals(expected.getIoConfig(), actual.getIoConfig());
    Assert.assertEquals(expected.getDimensionsSpec(), actual.getDimensionsSpec());
    Assert.assertArrayEquals(expected.getMetricsSpec(), actual.getMetricsSpec());
    Assert.assertEquals(expected.getTuningConfig(), actual.getTuningConfig());
    Assert.assertEquals(expected.getContext(), actual.getContext());
  }

  @Test
  public void testCreateIngestionSchema() throws IOException, SegmentLoadingException
  {
    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        toolbox,
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        null,
        null,
        null,
        OBJECT_MAPPER,
        COORDINATOR_CLIENT,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY
    );
    final List<DimensionsSpec> expectedDimensionsSpec = getExpectedDimensionsSpecForAutoGeneration();

    ingestionSpecs.sort(
        (s1, s2) -> Comparators.intervalsByStartThenEnd().compare(
            s1.getDataSchema().getGranularitySpec().inputIntervals().get(0),
            s2.getDataSchema().getGranularitySpec().inputIntervals().get(0)
        )
    );
    Assert.assertEquals(6, ingestionSpecs.size());
    assertIngestionSchema(
        ingestionSpecs,
        expectedDimensionsSpec,
        AGGREGATORS,
        SEGMENT_INTERVALS,
        Granularities.MONTH
    );
  }

  @Test
  public void testCreateIngestionSchemaWithTargetPartitionSize() throws IOException, SegmentLoadingException
  {
    final ParallelIndexTuningConfig tuningConfig = new ParallelIndexTuningConfig(
        100000,
        null,
        500000,
        1000000L,
        null,
        null,
        null,
        null,
        new IndexSpec(
            new RoaringBitmapSerdeFactory(true),
            CompressionStrategy.LZ4,
            CompressionStrategy.LZF,
            LongEncodingStrategy.LONGS
        ),
        null,
        null,
        true,
        false,
        null,
        null,
        null,
        10,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        toolbox,
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(tuningConfig),
        null,
        null,
        null,
        OBJECT_MAPPER,
        COORDINATOR_CLIENT,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY
    );
    final List<DimensionsSpec> expectedDimensionsSpec = getExpectedDimensionsSpecForAutoGeneration();

    ingestionSpecs.sort(
        (s1, s2) -> Comparators.intervalsByStartThenEnd().compare(
            s1.getDataSchema().getGranularitySpec().inputIntervals().get(0),
            s2.getDataSchema().getGranularitySpec().inputIntervals().get(0)
        )
    );
    Assert.assertEquals(6, ingestionSpecs.size());
    assertIngestionSchema(
        ingestionSpecs,
        expectedDimensionsSpec,
        AGGREGATORS,
        SEGMENT_INTERVALS,
        tuningConfig,
        Granularities.MONTH
    );
  }

  @Test
  public void testCreateIngestionSchemaWithMaxTotalRows() throws IOException, SegmentLoadingException
  {
    final ParallelIndexTuningConfig tuningConfig = new ParallelIndexTuningConfig(
        null,
        null,
        500000,
        1000000L,
        1000000L,
        null,
        null,
        null,
        new IndexSpec(
            new RoaringBitmapSerdeFactory(true),
            CompressionStrategy.LZ4,
            CompressionStrategy.LZF,
            LongEncodingStrategy.LONGS
        ),
        null,
        null,
        false,
        false,
        5000L,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        toolbox,
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(tuningConfig),
        null,
        null,
        null,
        OBJECT_MAPPER,
        COORDINATOR_CLIENT,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY
    );
    final List<DimensionsSpec> expectedDimensionsSpec = getExpectedDimensionsSpecForAutoGeneration();

    ingestionSpecs.sort(
        (s1, s2) -> Comparators.intervalsByStartThenEnd().compare(
            s1.getDataSchema().getGranularitySpec().inputIntervals().get(0),
            s2.getDataSchema().getGranularitySpec().inputIntervals().get(0)
        )
    );
    Assert.assertEquals(6, ingestionSpecs.size());
    assertIngestionSchema(
        ingestionSpecs,
        expectedDimensionsSpec,
        AGGREGATORS,
        SEGMENT_INTERVALS,
        tuningConfig,
        Granularities.MONTH
    );
  }

  @Test
  public void testCreateIngestionSchemaWithNumShards() throws IOException, SegmentLoadingException
  {
    final ParallelIndexTuningConfig tuningConfig = new ParallelIndexTuningConfig(
        null,
        null,
        500000,
        1000000L,
        null,
        null,
        null,
        new HashedPartitionsSpec(null, 3, null),
        new IndexSpec(
            new RoaringBitmapSerdeFactory(true),
            CompressionStrategy.LZ4,
            CompressionStrategy.LZF,
            LongEncodingStrategy.LONGS
        ),
        null,
        null,
        true,
        false,
        5000L,
        null,
        null,
        10,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        toolbox,
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(tuningConfig),
        null,
        null,
        null,
        OBJECT_MAPPER,
        COORDINATOR_CLIENT,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY
    );
    final List<DimensionsSpec> expectedDimensionsSpec = getExpectedDimensionsSpecForAutoGeneration();

    ingestionSpecs.sort(
        (s1, s2) -> Comparators.intervalsByStartThenEnd().compare(
            s1.getDataSchema().getGranularitySpec().inputIntervals().get(0),
            s2.getDataSchema().getGranularitySpec().inputIntervals().get(0)
        )
    );
    Assert.assertEquals(6, ingestionSpecs.size());
    assertIngestionSchema(
        ingestionSpecs,
        expectedDimensionsSpec,
        AGGREGATORS,
        SEGMENT_INTERVALS,
        tuningConfig,
        Granularities.MONTH
    );
  }

  @Test
  public void testCreateIngestionSchemaWithCustomDimensionsSpec() throws IOException, SegmentLoadingException
  {
    final DimensionsSpec customSpec = new DimensionsSpec(
        Lists.newArrayList(
            new LongDimensionSchema("timestamp"),
            new StringDimensionSchema("string_dim_0"),
            new StringDimensionSchema("string_dim_1"),
            new StringDimensionSchema("string_dim_2"),
            new StringDimensionSchema("string_dim_3"),
            new StringDimensionSchema("string_dim_4"),
            new LongDimensionSchema("long_dim_0"),
            new LongDimensionSchema("long_dim_1"),
            new LongDimensionSchema("long_dim_2"),
            new LongDimensionSchema("long_dim_3"),
            new LongDimensionSchema("long_dim_4"),
            new FloatDimensionSchema("float_dim_0"),
            new FloatDimensionSchema("float_dim_1"),
            new FloatDimensionSchema("float_dim_2"),
            new FloatDimensionSchema("float_dim_3"),
            new FloatDimensionSchema("float_dim_4"),
            new DoubleDimensionSchema("double_dim_0"),
            new DoubleDimensionSchema("double_dim_1"),
            new DoubleDimensionSchema("double_dim_2"),
            new DoubleDimensionSchema("double_dim_3"),
            new DoubleDimensionSchema("double_dim_4"),
            new StringDimensionSchema(MIXED_TYPE_COLUMN)
        )
    );

    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        toolbox,
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        customSpec,
        null,
        null,
        OBJECT_MAPPER,
        COORDINATOR_CLIENT,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY
    );

    ingestionSpecs.sort(
        (s1, s2) -> Comparators.intervalsByStartThenEnd().compare(
            s1.getDataSchema().getGranularitySpec().inputIntervals().get(0),
            s2.getDataSchema().getGranularitySpec().inputIntervals().get(0)
        )
    );
    Assert.assertEquals(6, ingestionSpecs.size());
    final List<DimensionsSpec> dimensionsSpecs = new ArrayList<>(6);
    IntStream.range(0, 6).forEach(i -> dimensionsSpecs.add(customSpec));
    assertIngestionSchema(
        ingestionSpecs,
        dimensionsSpecs,
        AGGREGATORS,
        SEGMENT_INTERVALS,
        Granularities.MONTH
    );
  }

  @Test
  public void testCreateIngestionSchemaWithCustomMetricsSpec() throws IOException, SegmentLoadingException
  {
    final AggregatorFactory[] customMetricsSpec = new AggregatorFactory[]{
        new CountAggregatorFactory("custom_count"),
        new LongSumAggregatorFactory("custom_long_sum", "agg_1"),
        new FloatMinAggregatorFactory("custom_float_min", "agg_3"),
        new DoubleMaxAggregatorFactory("custom_double_max", "agg_4")
    };

    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        toolbox,
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        null,
        customMetricsSpec,
        null,
        OBJECT_MAPPER,
        COORDINATOR_CLIENT,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY
    );

    final List<DimensionsSpec> expectedDimensionsSpec = getExpectedDimensionsSpecForAutoGeneration();

    ingestionSpecs.sort(
        (s1, s2) -> Comparators.intervalsByStartThenEnd().compare(
            s1.getDataSchema().getGranularitySpec().inputIntervals().get(0),
            s2.getDataSchema().getGranularitySpec().inputIntervals().get(0)
        )
    );
    Assert.assertEquals(6, ingestionSpecs.size());
    assertIngestionSchema(
        ingestionSpecs,
        expectedDimensionsSpec,
        Arrays.asList(customMetricsSpec),
        SEGMENT_INTERVALS,
        Granularities.MONTH
    );
  }

  @Test
  public void testCreateIngestionSchemaWithCustomSegments() throws IOException, SegmentLoadingException
  {
    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        toolbox,
        new SegmentProvider(DATA_SOURCE, SpecificSegmentsSpec.fromSegments(SEGMENTS)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        null,
        null,
        null,
        OBJECT_MAPPER,
        COORDINATOR_CLIENT,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY
    );
    final List<DimensionsSpec> expectedDimensionsSpec = getExpectedDimensionsSpecForAutoGeneration();

    ingestionSpecs.sort(
        (s1, s2) -> Comparators.intervalsByStartThenEnd().compare(
            s1.getDataSchema().getGranularitySpec().inputIntervals().get(0),
            s2.getDataSchema().getGranularitySpec().inputIntervals().get(0)
        )
    );
    Assert.assertEquals(6, ingestionSpecs.size());
    assertIngestionSchema(
        ingestionSpecs,
        expectedDimensionsSpec,
        AGGREGATORS,
        SEGMENT_INTERVALS,
        Granularities.MONTH
    );
  }

  @Test
  public void testCreateIngestionSchemaWithDifferentSegmentSet() throws IOException, SegmentLoadingException
  {
    expectedException.expect(CoreMatchers.instanceOf(IllegalStateException.class));
    expectedException.expectMessage(CoreMatchers.containsString("are different from the current used segments"));

    final List<DataSegment> segments = new ArrayList<>(SEGMENTS);
    Collections.sort(segments);
    // Remove one segment in the middle
    segments.remove(segments.size() / 2);
    CompactionTask.createIngestionSchema(
        toolbox,
        new SegmentProvider(DATA_SOURCE, SpecificSegmentsSpec.fromSegments(segments)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        null,
        null,
        null,
        OBJECT_MAPPER,
        COORDINATOR_CLIENT,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY
    );
  }

  @Test
  public void testMissingMetadata() throws IOException, SegmentLoadingException
  {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(CoreMatchers.startsWith("Index metadata doesn't exist for segment"));

    final TestIndexIO indexIO = (TestIndexIO) toolbox.getIndexIO();
    indexIO.removeMetadata(Iterables.getFirst(indexIO.getQueryableIndexMap().keySet(), null));
    final List<DataSegment> segments = new ArrayList<>(SEGMENTS);
    CompactionTask.createIngestionSchema(
        toolbox,
        new SegmentProvider(DATA_SOURCE, SpecificSegmentsSpec.fromSegments(segments)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        null,
        null,
        null,
        OBJECT_MAPPER,
        COORDINATOR_CLIENT,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY
    );
  }

  @Test
  public void testEmptyInterval()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(CoreMatchers.containsString("must specify a nonempty interval"));

    final Builder builder = new Builder(
        DATA_SOURCE,
        OBJECT_MAPPER,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        ROW_INGESTION_METERS_FACTORY,
        INDEXING_SERVICE_CLIENT,
        COORDINATOR_CLIENT,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY,
        APPENDERATORS_MANAGER
    );

    final CompactionTask task = builder
        .interval(Intervals.of("2000-01-01/2000-01-01"))
        .build();
  }

  @Test
  public void testSegmentGranularity() throws IOException, SegmentLoadingException
  {
    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        toolbox,
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        null,
        null,
        new PeriodGranularity(Period.months(3), null, null),
        OBJECT_MAPPER,
        COORDINATOR_CLIENT,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY
    );
    final List<DimensionsSpec> expectedDimensionsSpec = ImmutableList.of(
        new DimensionsSpec(getDimensionSchema(new DoubleDimensionSchema("string_to_double")))
    );

    ingestionSpecs.sort(
        (s1, s2) -> Comparators.intervalsByStartThenEnd().compare(
            s1.getDataSchema().getGranularitySpec().inputIntervals().get(0),
            s2.getDataSchema().getGranularitySpec().inputIntervals().get(0)
        )
    );
    Assert.assertEquals(1, ingestionSpecs.size());
    assertIngestionSchema(
        ingestionSpecs,
        expectedDimensionsSpec,
        AGGREGATORS,
        Collections.singletonList(COMPACTION_INTERVAL),
        new PeriodGranularity(Period.months(3), null, null)
    );
  }

  @Test
  public void testNullSegmentGranularityAnd() throws IOException, SegmentLoadingException
  {
    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        toolbox,
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        null,
        null,
        null,
        OBJECT_MAPPER,
        COORDINATOR_CLIENT,
        segmentLoaderFactory,
        RETRY_POLICY_FACTORY
    );
    final List<DimensionsSpec> expectedDimensionsSpec = getExpectedDimensionsSpecForAutoGeneration();

    ingestionSpecs.sort(
        (s1, s2) -> Comparators.intervalsByStartThenEnd().compare(
            s1.getDataSchema().getGranularitySpec().inputIntervals().get(0),
            s2.getDataSchema().getGranularitySpec().inputIntervals().get(0)
        )
    );
    Assert.assertEquals(6, ingestionSpecs.size());
    assertIngestionSchema(
        ingestionSpecs,
        expectedDimensionsSpec,
        AGGREGATORS,
        SEGMENT_INTERVALS,
        Granularities.MONTH
    );
  }

  private static List<DimensionsSpec> getExpectedDimensionsSpecForAutoGeneration()
  {
    return ImmutableList.of(
        new DimensionsSpec(getDimensionSchema(new StringDimensionSchema("string_to_double"))),
        new DimensionsSpec(getDimensionSchema(new StringDimensionSchema("string_to_double"))),
        new DimensionsSpec(getDimensionSchema(new StringDimensionSchema("string_to_double"))),
        new DimensionsSpec(getDimensionSchema(new StringDimensionSchema("string_to_double"))),
        new DimensionsSpec(getDimensionSchema(new DoubleDimensionSchema("string_to_double"))),
        new DimensionsSpec(getDimensionSchema(new DoubleDimensionSchema("string_to_double")))
    );
  }

  private static List<DimensionSchema> getDimensionSchema(DimensionSchema mixedTypeColumn)
  {
    return Lists.newArrayList(
        new LongDimensionSchema("timestamp"),
        new StringDimensionSchema("string_dim_4"),
        new LongDimensionSchema("long_dim_4"),
        new FloatDimensionSchema("float_dim_4"),
        new DoubleDimensionSchema("double_dim_4"),
        new StringDimensionSchema("string_dim_0"),
        new LongDimensionSchema("long_dim_0"),
        new FloatDimensionSchema("float_dim_0"),
        new DoubleDimensionSchema("double_dim_0"),
        new StringDimensionSchema("string_dim_1"),
        new LongDimensionSchema("long_dim_1"),
        new FloatDimensionSchema("float_dim_1"),
        new DoubleDimensionSchema("double_dim_1"),
        new StringDimensionSchema("string_dim_2"),
        new LongDimensionSchema("long_dim_2"),
        new FloatDimensionSchema("float_dim_2"),
        new DoubleDimensionSchema("double_dim_2"),
        new StringDimensionSchema("string_dim_3"),
        new LongDimensionSchema("long_dim_3"),
        new FloatDimensionSchema("float_dim_3"),
        new DoubleDimensionSchema("double_dim_3"),
        new StringDimensionSchema("string_dim_5"),
        new LongDimensionSchema("long_dim_5"),
        new FloatDimensionSchema("float_dim_5"),
        new DoubleDimensionSchema("double_dim_5"),
        mixedTypeColumn
    );
  }

  private void assertIngestionSchema(
      List<ParallelIndexIngestionSpec> ingestionSchemas,
      List<DimensionsSpec> expectedDimensionsSpecs,
      List<AggregatorFactory> expectedMetricsSpec,
      List<Interval> expectedSegmentIntervals,
      Granularity expectedSegmentGranularity
  )
  {
    assertIngestionSchema(
        ingestionSchemas,
        expectedDimensionsSpecs,
        expectedMetricsSpec,
        expectedSegmentIntervals,
        new ParallelIndexTuningConfig(
            null,
            null,
            500000,
            1000000L,
            Long.MAX_VALUE,
            null,
            null,
            new HashedPartitionsSpec(5000000, null, null), // automatically computed targetPartitionSize
            new IndexSpec(
                new RoaringBitmapSerdeFactory(true),
                CompressionStrategy.LZ4,
                CompressionStrategy.LZF,
                LongEncodingStrategy.LONGS
            ),
            null,
            null,
            true,
            false,
            5000L,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        ),
        expectedSegmentGranularity
    );
  }

  private void assertIngestionSchema(
      List<ParallelIndexIngestionSpec> ingestionSchemas,
      List<DimensionsSpec> expectedDimensionsSpecs,
      List<AggregatorFactory> expectedMetricsSpec,
      List<Interval> expectedSegmentIntervals,
      ParallelIndexTuningConfig expectedTuningConfig,
      Granularity expectedSegmentGranularity
  )
  {
    Preconditions.checkArgument(
        ingestionSchemas.size() == expectedDimensionsSpecs.size(),
        "ingesionSchemas.size()[%s] should be same with expectedDimensionsSpecs.size()[%s]",
        ingestionSchemas.size(),
        expectedDimensionsSpecs.size()
    );

    for (int i = 0; i < ingestionSchemas.size(); i++) {
      final ParallelIndexIngestionSpec ingestionSchema = ingestionSchemas.get(i);
      final DimensionsSpec expectedDimensionsSpec = expectedDimensionsSpecs.get(i);

      // assert dataSchema
      final DataSchema dataSchema = ingestionSchema.getDataSchema();
      Assert.assertEquals(DATA_SOURCE, dataSchema.getDataSource());

      final InputRowParser parser = OBJECT_MAPPER.convertValue(dataSchema.getParser(), InputRowParser.class);
      Assert.assertTrue(parser instanceof TransformingInputRowParser);
      Assert.assertTrue(((TransformingInputRowParser) parser).getParser() instanceof NoopInputRowParser);
      Assert.assertTrue(parser.getParseSpec() instanceof TimeAndDimsParseSpec);
      Assert.assertEquals(
          new HashSet<>(expectedDimensionsSpec.getDimensions()),
          new HashSet<>(parser.getParseSpec().getDimensionsSpec().getDimensions())
      );

      // metrics
      final List<AggregatorFactory> expectedAggregators = expectedMetricsSpec
          .stream()
          .map(AggregatorFactory::getCombiningFactory)
          .collect(Collectors.toList());
      Assert.assertEquals(expectedAggregators, Arrays.asList(dataSchema.getAggregators()));
      Assert.assertEquals(
          new UniformGranularitySpec(
              expectedSegmentGranularity,
              Granularities.NONE,
              false,
              Collections.singletonList(expectedSegmentIntervals.get(i))
          ),
          dataSchema.getGranularitySpec()
      );

      // assert ioConfig
      final ParallelIndexIOConfig ioConfig = ingestionSchema.getIOConfig();
      Assert.assertFalse(ioConfig.isAppendToExisting());
      final FirehoseFactory firehoseFactory = ioConfig.getFirehoseFactory();
      Assert.assertTrue(firehoseFactory instanceof IngestSegmentFirehoseFactory);
      final IngestSegmentFirehoseFactory ingestSegmentFirehoseFactory = (IngestSegmentFirehoseFactory) firehoseFactory;
      Assert.assertEquals(DATA_SOURCE, ingestSegmentFirehoseFactory.getDataSource());
      Assert.assertEquals(expectedSegmentIntervals.get(i), ingestSegmentFirehoseFactory.getInterval());
      Assert.assertNull(ingestSegmentFirehoseFactory.getDimensionsFilter());

      Assert.assertEquals(
          new HashSet<>(expectedDimensionsSpec.getDimensionNames()),
          new HashSet<>(ingestSegmentFirehoseFactory.getDimensions())
      );

      // assert tuningConfig
      Assert.assertEquals(expectedTuningConfig, ingestionSchema.getTuningConfig());
    }
  }

  private static class TestCoordinatorClient extends CoordinatorClient
  {
    private final Map<DataSegment, File> segmentMap;

    TestCoordinatorClient(Map<DataSegment, File> segmentMap)
    {
      super(null, null);
      this.segmentMap = segmentMap;
    }

    @Override
    public List<DataSegment> getDatabaseSegmentDataSourceSegments(String dataSource, List<Interval> intervals)
    {
      return new ArrayList<>(segmentMap.keySet());
    }
  }

  private static class TestTaskToolbox extends TaskToolbox
  {
    private final Map<DataSegment, File> segmentFileMap;

    TestTaskToolbox(
        TaskActionClient taskActionClient,
        IndexIO indexIO,
        Map<DataSegment, File> segmentFileMap
    )
    {
      super(
          null,
          null,
          taskActionClient,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          indexIO,
          null,
          null,
          null,
          new IndexMergerV9(OBJECT_MAPPER, indexIO, OffHeapMemorySegmentWriteOutMediumFactory.instance()),
          null,
          null,
          null,
          null,
          new NoopTestTaskReportFileWriter(),
          null
      );
      this.segmentFileMap = segmentFileMap;
    }

    @Override
    public Map<DataSegment, File> fetchSegments(List<DataSegment> segments)
    {
      final Map<DataSegment, File> submap = new HashMap<>(segments.size());
      for (DataSegment segment : segments) {
        final File file = Preconditions.checkNotNull(segmentFileMap.get(segment));
        submap.put(segment, file);
      }
      return submap;
    }
  }

  private static class TestTaskActionClient implements TaskActionClient
  {
    private final List<DataSegment> segments;

    TestTaskActionClient(List<DataSegment> segments)
    {
      this.segments = segments;
    }

    @Override
    public <RetType> RetType submit(TaskAction<RetType> taskAction)
    {
      if (!(taskAction instanceof SegmentListUsedAction)) {
        throw new ISE("action[%s] is not supported", taskAction);
      }
      return (RetType) segments;
    }
  }

  private static class TestIndexIO extends IndexIO
  {
    private final Map<File, QueryableIndex> queryableIndexMap;

    TestIndexIO(
        ObjectMapper mapper,
        Map<DataSegment, File> segmentFileMap
    )
    {
      super(mapper, () -> 0);

      queryableIndexMap = new HashMap<>(segmentFileMap.size());
      for (Entry<DataSegment, File> entry : segmentFileMap.entrySet()) {
        final DataSegment segment = entry.getKey();
        final List<String> columnNames = new ArrayList<>(segment.getDimensions().size() + segment.getMetrics().size());
        columnNames.add(ColumnHolder.TIME_COLUMN_NAME);
        columnNames.addAll(segment.getDimensions());
        columnNames.addAll(segment.getMetrics());
        final Map<String, ColumnHolder> columnMap = new HashMap<>(columnNames.size());
        final List<AggregatorFactory> aggregatorFactories = new ArrayList<>(segment.getMetrics().size());

        for (String columnName : columnNames) {
          if (MIXED_TYPE_COLUMN.equals(columnName)) {
            columnMap.put(columnName, createColumn(MIXED_TYPE_COLUMN_MAP.get(segment.getInterval())));
          } else if (DIMENSIONS.containsKey(columnName)) {
            columnMap.put(columnName, createColumn(DIMENSIONS.get(columnName)));
          } else {
            final Optional<AggregatorFactory> maybeMetric = AGGREGATORS.stream()
                                                                       .filter(agg -> agg.getName().equals(columnName))
                                                                       .findAny();
            if (maybeMetric.isPresent()) {
              columnMap.put(columnName, createColumn(maybeMetric.get()));
              aggregatorFactories.add(maybeMetric.get());
            }
          }
        }

        final Metadata metadata = new Metadata(
            null,
            aggregatorFactories.toArray(new AggregatorFactory[0]),
            null,
            null,
            null
        );

        queryableIndexMap.put(
            entry.getValue(),
            new SimpleQueryableIndex(
                segment.getInterval(),
                new ListIndexed<>(segment.getDimensions()),
                null,
                columnMap,
                null,
                metadata
            )
        );
      }
    }

    @Override
    public QueryableIndex loadIndex(File file)
    {
      return queryableIndexMap.get(file);
    }

    void removeMetadata(File file)
    {
      final SimpleQueryableIndex index = (SimpleQueryableIndex) queryableIndexMap.get(file);
      if (index != null) {
        queryableIndexMap.put(
            file,
            new SimpleQueryableIndex(
                index.getDataInterval(),
                index.getColumnNames(),
                index.getAvailableDimensions(),
                index.getBitmapFactoryForDimensions(),
                index.getColumns(),
                index.getFileMapper(),
                null,
                index.getDimensionHandlers()
            )
        );
      }
    }

    Map<File, QueryableIndex> getQueryableIndexMap()
    {
      return queryableIndexMap;
    }
  }

  private static ColumnHolder createColumn(DimensionSchema dimensionSchema)
  {
    return new TestColumn(IncrementalIndex.TYPE_MAP.get(dimensionSchema.getValueType()));
  }

  private static ColumnHolder createColumn(AggregatorFactory aggregatorFactory)
  {
    return new TestColumn(ValueType.fromString(aggregatorFactory.getTypeName()));
  }

  private static class TestColumn implements ColumnHolder
  {
    private final ColumnCapabilities columnCapabilities;

    TestColumn(ValueType type)
    {
      columnCapabilities = new ColumnCapabilitiesImpl()
          .setType(type)
          .setDictionaryEncoded(type == ValueType.STRING) // set a fake value to make string columns
          .setHasBitmapIndexes(type == ValueType.STRING)
          .setHasSpatialIndexes(false)
          .setHasMultipleValues(false);
    }

    @Override
    public ColumnCapabilities getCapabilities()
    {
      return columnCapabilities;
    }

    @Override
    public int getLength()
    {
      return NUM_ROWS_PER_SEGMENT;
    }

    @Override
    public BaseColumn getColumn()
    {
      return null;
    }

    @Override
    public SettableColumnValueSelector makeNewSettableColumnValueSelector()
    {
      return null;
    }

    @Override
    public BitmapIndex getBitmapIndex()
    {
      return null;
    }

    @Override
    public SpatialIndex getSpatialIndex()
    {
      return null;
    }
  }
}
