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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.client.indexing.ClientCompactionTaskTransformSpec;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.GuiceAnnotationIntrospector;
import org.apache.druid.guice.GuiceInjectableValues;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.IndexingServiceTuningConfigModule;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.common.task.CompactionTask.Builder;
import org.apache.druid.indexing.common.task.CompactionTask.PartitionConfigurationManager;
import org.apache.druid.indexing.common.task.CompactionTask.SegmentProvider;
import org.apache.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIOConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIngestionSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.first.FloatFirstAggregatorFactory;
import org.apache.druid.query.aggregation.last.DoubleLastAggregatorFactory;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.DoubleDimensionHandler;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.SimpleQueryableIndex;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.CompressionFactory.LongEncodingStrategy;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.indexing.BatchIOConfig;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.loading.NoopSegmentCacheManager;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;
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
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RunWith(MockitoJUnitRunner.class)
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
      Intervals.of("2017-06-01/2017-07-01"),
      // overlapping intervals
      Intervals.of("2017-06-01/2017-06-02"),
      Intervals.of("2017-06-15/2017-06-16"),
      Intervals.of("2017-06-30/2017-07-01")
  );
  private static final Map<Interval, DimensionSchema> MIXED_TYPE_COLUMN_MAP = new HashMap<>();
  private static final CompactionTask.CompactionTuningConfig TUNING_CONFIG = createTuningConfig();

  private static final TestUtils TEST_UTILS = new TestUtils();
  private static final Map<DataSegment, File> SEGMENT_MAP = new HashMap<>();
  private static final CoordinatorClient COORDINATOR_CLIENT = new TestCoordinatorClient(SEGMENT_MAP);
  private static final ObjectMapper OBJECT_MAPPER = setupInjectablesInObjectMapper(new DefaultObjectMapper());
  private static final RetryPolicyFactory RETRY_POLICY_FACTORY = new RetryPolicyFactory(new RetryPolicyConfig());
  private static final String CONFLICTING_SEGMENT_GRANULARITY_FORMAT =
      "Conflicting segment granularities found %s(segmentGranularity) and %s(granularitySpec.segmentGranularity).\n"
      + "Remove `segmentGranularity` and set the `granularitySpec.segmentGranularity` to the expected granularity";

  private static final ServiceMetricEvent.Builder METRIC_BUILDER = new ServiceMetricEvent.Builder();

  private static Map<String, DimensionSchema> DIMENSIONS;
  private static List<AggregatorFactory> AGGREGATORS;
  private static List<DataSegment> SEGMENTS;

  private TaskToolbox toolbox;
  private SegmentCacheManagerFactory segmentCacheManagerFactory;

  @BeforeClass
  public static void setupClass()
  {
    MIXED_TYPE_COLUMN_MAP.put(Intervals.of("2017-01-01/2017-02-01"), new StringDimensionSchema(MIXED_TYPE_COLUMN));
    MIXED_TYPE_COLUMN_MAP.put(Intervals.of("2017-02-01/2017-03-01"), new StringDimensionSchema(MIXED_TYPE_COLUMN));
    MIXED_TYPE_COLUMN_MAP.put(Intervals.of("2017-03-01/2017-04-01"), new StringDimensionSchema(MIXED_TYPE_COLUMN));
    MIXED_TYPE_COLUMN_MAP.put(Intervals.of("2017-04-01/2017-05-01"), new StringDimensionSchema(MIXED_TYPE_COLUMN));
    MIXED_TYPE_COLUMN_MAP.put(Intervals.of("2017-05-01/2017-06-01"), new DoubleDimensionSchema(MIXED_TYPE_COLUMN));
    MIXED_TYPE_COLUMN_MAP.put(Intervals.of("2017-06-01/2017-07-01"), new DoubleDimensionSchema(MIXED_TYPE_COLUMN));

    MIXED_TYPE_COLUMN_MAP.put(Intervals.of("2017-06-01/2017-06-02"), new DoubleDimensionSchema(MIXED_TYPE_COLUMN));
    MIXED_TYPE_COLUMN_MAP.put(Intervals.of("2017-06-15/2017-06-16"), new DoubleDimensionSchema(MIXED_TYPE_COLUMN));
    MIXED_TYPE_COLUMN_MAP.put(Intervals.of("2017-06-30/2017-07-01"), new DoubleDimensionSchema(MIXED_TYPE_COLUMN));

    DIMENSIONS = new HashMap<>();
    AGGREGATORS = new ArrayList<>();

    DIMENSIONS.put(ColumnHolder.TIME_COLUMN_NAME, new LongDimensionSchema(ColumnHolder.TIME_COLUMN_NAME));
    DIMENSIONS.put(TIMESTAMP_COLUMN, new LongDimensionSchema(TIMESTAMP_COLUMN));
    int numUmbrellaIntervals = 6;
    for (int i = 0; i < numUmbrellaIntervals; i++) {
      final StringDimensionSchema schema = new StringDimensionSchema(
          "string_dim_" + i,
          null,
          null
      );
      DIMENSIONS.put(schema.getName(), schema);
    }
    for (int i = 0; i < numUmbrellaIntervals; i++) {
      final LongDimensionSchema schema = new LongDimensionSchema("long_dim_" + i);
      DIMENSIONS.put(schema.getName(), schema);
    }
    for (int i = 0; i < numUmbrellaIntervals; i++) {
      final FloatDimensionSchema schema = new FloatDimensionSchema("float_dim_" + i);
      DIMENSIONS.put(schema.getName(), schema);
    }
    for (int i = 0; i < numUmbrellaIntervals; i++) {
      final DoubleDimensionSchema schema = new DoubleDimensionSchema("double_dim_" + i);
      DIMENSIONS.put(schema.getName(), schema);
    }

    AGGREGATORS.add(new CountAggregatorFactory("agg_0"));
    AGGREGATORS.add(new LongSumAggregatorFactory("agg_1", "long_dim_1"));
    AGGREGATORS.add(new LongMaxAggregatorFactory("agg_2", "long_dim_2"));
    AGGREGATORS.add(new FloatFirstAggregatorFactory("agg_3", "float_dim_3", null));
    AGGREGATORS.add(new DoubleLastAggregatorFactory("agg_4", "double_dim_4", null));

    for (int i = 0; i < SEGMENT_INTERVALS.size(); i++) {
      SEGMENT_MAP.put(
          new DataSegment(
              DATA_SOURCE,
              SEGMENT_INTERVALS.get(i),
              "version_" + i,
              ImmutableMap.of(),
              findDimensions(i, SEGMENT_INTERVALS.get(i)),
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
                  binder.bind(RowIngestionMetersFactory.class).toInstance(TEST_UTILS.getRowIngestionMetersFactory());
                  binder.bind(CoordinatorClient.class).toInstance(COORDINATOR_CLIENT);
                  binder.bind(SegmentCacheManagerFactory.class)
                        .toInstance(new SegmentCacheManagerFactory(objectMapper));
                  binder.bind(AppenderatorsManager.class).toInstance(new TestAppenderatorsManager());
                }
            )
        )
    );
    objectMapper.setInjectableValues(injectableValues);
    objectMapper.registerModule(
        new SimpleModule().registerSubtypes(new NamedType(NumberedShardSpec.class, "NumberedShardSpec"))
    );
    objectMapper.registerModules(new IndexingServiceTuningConfigModule().getJacksonModules());
    return objectMapper;
  }

  private static List<String> findDimensions(int startIndex, Interval segmentInterval)
  {
    final List<String> dimensions = new ArrayList<>();
    dimensions.add(TIMESTAMP_COLUMN);
    for (int i = 0; i < 6; i++) {
      int postfix = i + startIndex;
      postfix = postfix % 6;
      dimensions.add("string_dim_" + postfix);
      dimensions.add("long_dim_" + postfix);
      dimensions.add("float_dim_" + postfix);
      dimensions.add("double_dim_" + postfix);
    }
    dimensions.add(MIXED_TYPE_COLUMN_MAP.get(segmentInterval).getName());
    return dimensions;
  }

  private static CompactionTask.CompactionTuningConfig createTuningConfig()
  {
    return new CompactionTask.CompactionTuningConfig(
        null,
        null, // null to compute maxRowsPerSegment automatically
        null,
        500000,
        1000000L,
        null,
        null,
        null,
        null,
        null,
        IndexSpec.builder()
                 .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                 .withDimensionCompression(CompressionStrategy.LZ4)
                 .withMetricCompression(CompressionStrategy.LZF)
                 .withLongEncoding(LongEncodingStrategy.LONGS)
                 .build(),
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
        null,
        null,
        null,
        null
    );
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Mock
  private Clock clock;
  private StubServiceEmitter emitter;

  @Before
  public void setup()
  {
    final IndexIO testIndexIO = new TestIndexIO(OBJECT_MAPPER, SEGMENT_MAP);
    emitter = new StubServiceEmitter();
    toolbox = makeTaskToolbox(
        new TestTaskActionClient(new ArrayList<>(SEGMENT_MAP.keySet())),
        testIndexIO,
        SEGMENT_MAP
    );
    Mockito.when(clock.millis()).thenReturn(0L, 10_000L);
    segmentCacheManagerFactory = new SegmentCacheManagerFactory(OBJECT_MAPPER);
  }

  @Test
  public void testCreateCompactionTaskWithGranularitySpec()
  {
    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );
    builder.inputSpec(new CompactionIntervalSpec(COMPACTION_INTERVAL, SegmentUtils.hashIds(SEGMENTS)));
    builder.tuningConfig(createTuningConfig());
    builder.segmentGranularity(Granularities.HOUR);
    final CompactionTask taskCreatedWithSegmentGranularity = builder.build();

    final Builder builder2 = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );
    builder2.inputSpec(new CompactionIntervalSpec(COMPACTION_INTERVAL, SegmentUtils.hashIds(SEGMENTS)));
    builder2.tuningConfig(createTuningConfig());
    builder2.granularitySpec(new ClientCompactionTaskGranularitySpec(Granularities.HOUR, Granularities.DAY, null));
    final CompactionTask taskCreatedWithGranularitySpec = builder2.build();
    Assert.assertEquals(
        taskCreatedWithGranularitySpec.getSegmentGranularity(),
        taskCreatedWithSegmentGranularity.getSegmentGranularity()
    );
  }

  @Test
  public void testCompactionTaskEmitter()
  {
    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );
    builder.inputSpec(new CompactionIntervalSpec(COMPACTION_INTERVAL, SegmentUtils.hashIds(SEGMENTS)));
    builder.tuningConfig(createTuningConfig());
    builder.segmentGranularity(Granularities.HOUR);
    final CompactionTask taskCreatedWithSegmentGranularity = builder.build();

    // null emitter should work
    taskCreatedWithSegmentGranularity.emitCompactIngestionModeMetrics(null, false);
    // non-null should also work
    ServiceEmitter noopEmitter = new ServiceEmitter("service", "host", new NoopEmitter());
    taskCreatedWithSegmentGranularity.emitCompactIngestionModeMetrics(noopEmitter, false);
    taskCreatedWithSegmentGranularity.emitCompactIngestionModeMetrics(noopEmitter, true);
  }

  @Test(expected = IAE.class)
  public void testCreateCompactionTaskWithConflictingGranularitySpecAndSegmentGranularityShouldThrowIAE()
  {
    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );
    builder.inputSpec(new CompactionIntervalSpec(COMPACTION_INTERVAL, SegmentUtils.hashIds(SEGMENTS)));
    builder.tuningConfig(createTuningConfig());
    builder.segmentGranularity(Granularities.HOUR);
    builder.granularitySpec(new ClientCompactionTaskGranularitySpec(Granularities.MINUTE, Granularities.DAY, null));
    try {
      builder.build();
    }
    catch (IAE iae) {
      Assert.assertEquals(
          StringUtils.format(
              CONFLICTING_SEGMENT_GRANULARITY_FORMAT,
              Granularities.HOUR,
              Granularities.MINUTE
          ),
          iae.getMessage()
      );
      throw iae;
    }
    Assert.fail("Should not have reached here!");
  }

  @Test
  public void testCreateCompactionTaskWithTransformSpec()
  {
    ClientCompactionTaskTransformSpec transformSpec =
        new ClientCompactionTaskTransformSpec(new SelectorDimFilter("dim1", "foo", null));
    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );
    builder.inputSpec(new CompactionIntervalSpec(COMPACTION_INTERVAL, SegmentUtils.hashIds(SEGMENTS)));
    builder.tuningConfig(createTuningConfig());
    builder.transformSpec(transformSpec);
    final CompactionTask taskCreatedWithTransformSpec = builder.build();
    Assert.assertEquals(
        transformSpec,
        taskCreatedWithTransformSpec.getTransformSpec()
    );
  }

  @Test
  public void testCreateCompactionTaskWithMetricsSpec()
  {
    AggregatorFactory[] aggregatorFactories = new AggregatorFactory[]{new CountAggregatorFactory("cnt")};
    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );
    builder.inputSpec(new CompactionIntervalSpec(COMPACTION_INTERVAL, SegmentUtils.hashIds(SEGMENTS)));
    builder.tuningConfig(createTuningConfig());
    builder.metricsSpec(aggregatorFactories);
    final CompactionTask taskCreatedWithTransformSpec = builder.build();
    Assert.assertArrayEquals(
        aggregatorFactories,
        taskCreatedWithTransformSpec.getMetricsSpec()
    );
  }

  @Test(expected = IAE.class)
  public void testCreateCompactionTaskWithNullSegmentGranularityInGranularitySpecAndSegmentGranularityShouldSucceed()
  {
    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );
    builder.inputSpec(new CompactionIntervalSpec(COMPACTION_INTERVAL, SegmentUtils.hashIds(SEGMENTS)));
    builder.tuningConfig(createTuningConfig());
    builder.segmentGranularity(Granularities.HOUR);
    builder.granularitySpec(new ClientCompactionTaskGranularitySpec(null, Granularities.DAY, null));
    try {
      builder.build();
    }
    catch (IAE iae) {
      Assert.assertEquals(
          StringUtils.format(
              CONFLICTING_SEGMENT_GRANULARITY_FORMAT,
              Granularities.HOUR,
              null
          ),
          iae.getMessage()
      );
      throw iae;
    }
    Assert.fail("Should not have reached here!");
  }

  @Test
  public void testCreateCompactionTaskWithSameGranularitySpecAndSegmentGranularityShouldSucceed()
  {
    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );
    builder.inputSpec(new CompactionIntervalSpec(COMPACTION_INTERVAL, SegmentUtils.hashIds(SEGMENTS)));
    builder.tuningConfig(createTuningConfig());
    builder.segmentGranularity(Granularities.HOUR);
    builder.granularitySpec(new ClientCompactionTaskGranularitySpec(Granularities.HOUR, Granularities.DAY, null));
    final CompactionTask taskCreatedWithSegmentGranularity = builder.build();
    Assert.assertEquals(Granularities.HOUR, taskCreatedWithSegmentGranularity.getSegmentGranularity());
  }

  @Test
  public void testSerdeWithInterval() throws IOException
  {
    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
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
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
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
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
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

  @Test
  public void testSerdeWithOldTuningConfigSuccessfullyDeserializeToNewOne() throws IOException
  {
    final OldCompactionTaskWithAnyTuningConfigType oldTask = new OldCompactionTaskWithAnyTuningConfigType(
        null,
        null,
        DATA_SOURCE,
        null,
        SEGMENTS,
        null,
        null,
        null,
        null,
        null,
        new IndexTuningConfig(
            null,
            null, // null to compute maxRowsPerSegment automatically
            null,
            500000,
            1000000L,
            null,
            null,
            null,
            null,
            null,
            null,
            IndexSpec.builder()
                     .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                     .withDimensionCompression(CompressionStrategy.LZ4)
                     .withMetricCompression(CompressionStrategy.LZF)
                     .withLongEncoding(LongEncodingStrategy.LONGS)
                     .build(),
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
            null
        ),
        null,
        toolbox.getJsonMapper(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        toolbox.getChatHandlerProvider(),
        toolbox.getRowIngestionMetersFactory(),
        COORDINATOR_CLIENT,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY,
        toolbox.getAppenderatorsManager()
    );

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );

    final CompactionTask expectedFromJson = builder
        .segments(SEGMENTS)
        .tuningConfig(CompactionTask.getTuningConfig(oldTask.getTuningConfig()))
        .build();

    final ObjectMapper mapper = new DefaultObjectMapper((DefaultObjectMapper) OBJECT_MAPPER);
    mapper.registerSubtypes(new NamedType(OldCompactionTaskWithAnyTuningConfigType.class, "compact"));
    final byte[] bytes = mapper.writeValueAsBytes(oldTask);
    final CompactionTask fromJson = mapper.readValue(bytes, CompactionTask.class);
    assertEquals(expectedFromJson, fromJson);
  }

  @Test
  public void testInputSourceResources()
  {
    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );
    final CompactionTask task = builder
        .inputSpec(
            new CompactionIntervalSpec(COMPACTION_INTERVAL, SegmentUtils.hashIds(SEGMENTS))
        )
        .tuningConfig(createTuningConfig())
        .context(ImmutableMap.of("testKey", "testContext"))
        .build();

    Assert.assertTrue(task.getInputSourceResources().isEmpty());
  }

  @Test
  public void testGetTuningConfigWithIndexTuningConfig()
  {
    IndexTuningConfig indexTuningConfig = new IndexTuningConfig(
        null,
        null, // null to compute maxRowsPerSegment automatically
        null,
        500000,
        1000000L,
        null,
        null,
        null,
        null,
        null,
        null,
        IndexSpec.builder()
                 .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                 .withDimensionCompression(CompressionStrategy.LZ4)
                 .withMetricCompression(CompressionStrategy.LZF)
                 .withLongEncoding(LongEncodingStrategy.LONGS)
                 .build(),
        null,
        null,
        true,
        false,
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

    CompactionTask.CompactionTuningConfig compactionTuningConfig = new CompactionTask.CompactionTuningConfig(
        null,
        null,
        null,
        500000,
        1000000L,
        null,
        null,
        null,
        null,
        null,
        IndexSpec.builder()
                 .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                 .withDimensionCompression(CompressionStrategy.LZ4)
                 .withMetricCompression(CompressionStrategy.LZF)
                 .withLongEncoding(LongEncodingStrategy.LONGS)
                 .build(),
        null,
        null,
        true,
        false,
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
        null,
        null
    );

    Assert.assertEquals(compactionTuningConfig, CompactionTask.getTuningConfig(indexTuningConfig));

  }

  @Test
  public void testGetTuningConfigWithParallelIndexTuningConfig()
  {
    ParallelIndexTuningConfig parallelIndexTuningConfig = new ParallelIndexTuningConfig(
        null,
        null, // null to compute maxRowsPerSegment automatically
        null,
        500000,
        1000000L,
        null,
        null,
        null,
        null,
        null,
        IndexSpec.builder()
                 .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                 .withDimensionCompression(CompressionStrategy.LZ4)
                 .withMetricCompression(CompressionStrategy.LZF)
                 .withLongEncoding(LongEncodingStrategy.LONGS)
                 .build(),
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
        null,
        null,
        null,
        null,
        null
    );

    CompactionTask.CompactionTuningConfig compactionTuningConfig = new CompactionTask.CompactionTuningConfig(
        null,
        null, // null to compute maxRowsPerSegment automatically
        null,
        500000,
        1000000L,
        null,
        null,
        null,
        null,
        null,
        IndexSpec.builder()
                 .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                 .withDimensionCompression(CompressionStrategy.LZ4)
                 .withMetricCompression(CompressionStrategy.LZF)
                 .withLongEncoding(LongEncodingStrategy.LONGS)
                 .build(),
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
        null,
        null,
        null,
        null
    );

    Assert.assertEquals(compactionTuningConfig, CompactionTask.getTuningConfig(parallelIndexTuningConfig));
  }

  @Test
  public void testSerdeWithUnknownTuningConfigThrowingError() throws IOException
  {
    final OldCompactionTaskWithAnyTuningConfigType taskWithUnknownTuningConfig =
        new OldCompactionTaskWithAnyTuningConfigType(
            null,
            null,
            DATA_SOURCE,
            null,
            SEGMENTS,
            null,
            null,
            null,
            null,
            null,
            RealtimeTuningConfig.makeDefaultTuningConfig(null),
            null,
            OBJECT_MAPPER,
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            null,
            toolbox.getRowIngestionMetersFactory(),
            COORDINATOR_CLIENT,
            segmentCacheManagerFactory,
            RETRY_POLICY_FACTORY,
            toolbox.getAppenderatorsManager()
        );

    final ObjectMapper mapper = new DefaultObjectMapper((DefaultObjectMapper) OBJECT_MAPPER);
    mapper.registerSubtypes(
        new NamedType(OldCompactionTaskWithAnyTuningConfigType.class, "compact"),
        new NamedType(RealtimeTuningConfig.class, "realtime")
    );
    final byte[] bytes = mapper.writeValueAsBytes(taskWithUnknownTuningConfig);

    expectedException.expect(ValueInstantiationException.class);
    expectedException.expectCause(CoreMatchers.instanceOf(IllegalStateException.class));
    expectedException.expectMessage(
        "Unknown tuningConfig type: [org.apache.druid.segment.indexing.RealtimeTuningConfig]"
    );
    mapper.readValue(bytes, CompactionTask.class);
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
  public void testSegmentProviderFindSegmentsWithEmptySegmentsThrowException()
  {
    final SegmentProvider provider = new SegmentProvider(
        "datasource",
        new CompactionIntervalSpec(Intervals.of("2021-01-01/P1D"), null)
    );

    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage(
        "No segments found for compaction. Please check that datasource name and interval are correct."
    );
    provider.checkSegments(LockGranularity.TIME_CHUNK, ImmutableList.of());
  }
  
  @Test
  public void testCreateIngestionSchema() throws IOException
  {
    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        clock,
        toolbox,
        LockGranularity.TIME_CHUNK,
        new CompactionIOConfig(null, false, null),
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        null,
        null,
        null,
        null,
        COORDINATOR_CLIENT,
        segmentCacheManagerFactory,
        METRIC_BUILDER
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
        AGGREGATORS.stream().map(AggregatorFactory::getCombiningFactory).collect(Collectors.toList()),
        SEGMENT_INTERVALS,
        Granularities.MONTH,
        Granularities.NONE,
        BatchIOConfig.DEFAULT_DROP_EXISTING
    );
  }

  @Test
  public void testCreateIngestionSchemaWithTargetPartitionSize() throws IOException
  {
    final CompactionTask.CompactionTuningConfig tuningConfig = new CompactionTask.CompactionTuningConfig(
        100000,
        null,
        null,
        500000,
        1000000L,
        null,
        null,
        null,
        null,
        null,
        IndexSpec.builder()
                 .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                 .withDimensionCompression(CompressionStrategy.LZ4)
                 .withMetricCompression(CompressionStrategy.LZF)
                 .withLongEncoding(LongEncodingStrategy.LONGS)
                 .build(),
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
        null,
        null,
        null,
        null
    );
    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        clock,
        toolbox,
        LockGranularity.TIME_CHUNK,
        new CompactionIOConfig(null, false, null),
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(tuningConfig),
        null,
        null,
        null,
        null,
        COORDINATOR_CLIENT,
        segmentCacheManagerFactory,
        METRIC_BUILDER
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
        AGGREGATORS.stream().map(AggregatorFactory::getCombiningFactory).collect(Collectors.toList()),
        SEGMENT_INTERVALS,
        tuningConfig,
        Granularities.MONTH,
        Granularities.NONE,
        BatchIOConfig.DEFAULT_DROP_EXISTING
    );
  }

  @Test
  public void testCreateIngestionSchemaWithMaxTotalRows() throws IOException
  {
    final CompactionTask.CompactionTuningConfig tuningConfig = new CompactionTask.CompactionTuningConfig(
        null,
        null,
        null,
        500000,
        1000000L,
        null,
        1000000L,
        null,
        null,
        null,
        IndexSpec.builder()
                 .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                 .withDimensionCompression(CompressionStrategy.LZ4)
                 .withMetricCompression(CompressionStrategy.LZF)
                 .withLongEncoding(LongEncodingStrategy.LONGS)
                 .build(),
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
        null,
        null,
        null,
        null
    );
    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        clock,
        toolbox,
        LockGranularity.TIME_CHUNK,
        new CompactionIOConfig(null, false, null),
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(tuningConfig),
        null,
        null,
        null,
        null,
        COORDINATOR_CLIENT,
        segmentCacheManagerFactory,
        METRIC_BUILDER
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
        AGGREGATORS.stream().map(AggregatorFactory::getCombiningFactory).collect(Collectors.toList()),
        SEGMENT_INTERVALS,
        tuningConfig,
        Granularities.MONTH,
        Granularities.NONE,
        BatchIOConfig.DEFAULT_DROP_EXISTING
    );
  }

  @Test
  public void testCreateIngestionSchemaWithNumShards() throws IOException
  {
    final CompactionTask.CompactionTuningConfig tuningConfig = new CompactionTask.CompactionTuningConfig(
        null,
        null,
        null,
        500000,
        1000000L,
        null,
        null,
        null,
        null,
        new HashedPartitionsSpec(null, 3, null),
        IndexSpec.builder()
                 .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                 .withDimensionCompression(CompressionStrategy.LZ4)
                 .withMetricCompression(CompressionStrategy.LZF)
                 .withLongEncoding(LongEncodingStrategy.LONGS)
                 .build(),
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
        null,
        null,
        null,
        null
    );
    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        clock,
        toolbox,
        LockGranularity.TIME_CHUNK,
        new CompactionIOConfig(null, false, null),
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(tuningConfig),
        null,
        null,
        null,
        null,
        COORDINATOR_CLIENT,
        segmentCacheManagerFactory,
        METRIC_BUILDER
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
        AGGREGATORS.stream().map(AggregatorFactory::getCombiningFactory).collect(Collectors.toList()),
        SEGMENT_INTERVALS,
        tuningConfig,
        Granularities.MONTH,
        Granularities.NONE,
        BatchIOConfig.DEFAULT_DROP_EXISTING
    );
  }

  @Test
  public void testCreateIngestionSchemaWithCustomDimensionsSpec() throws IOException
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
        clock,
        toolbox,
        LockGranularity.TIME_CHUNK,
        new CompactionIOConfig(null, false, null),
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        customSpec,
        null,
        null,
        null,
        COORDINATOR_CLIENT,
        segmentCacheManagerFactory,
        METRIC_BUILDER
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
        AGGREGATORS.stream().map(AggregatorFactory::getCombiningFactory).collect(Collectors.toList()),
        SEGMENT_INTERVALS,
        Granularities.MONTH,
        Granularities.NONE,
        BatchIOConfig.DEFAULT_DROP_EXISTING
    );
  }

  @Test
  public void testCreateIngestionSchemaWithCustomMetricsSpec() throws IOException
  {
    final AggregatorFactory[] customMetricsSpec = new AggregatorFactory[]{
        new CountAggregatorFactory("custom_count"),
        new LongSumAggregatorFactory("custom_long_sum", "agg_1"),
        new FloatMinAggregatorFactory("custom_float_min", "agg_3"),
        new DoubleMaxAggregatorFactory("custom_double_max", "agg_4")
    };

    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        clock,
        toolbox,
        LockGranularity.TIME_CHUNK,
        new CompactionIOConfig(null, false, null),
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        null,
        null,
        customMetricsSpec,
        null,
        COORDINATOR_CLIENT,
        segmentCacheManagerFactory,
        METRIC_BUILDER
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
        Granularities.MONTH,
        Granularities.NONE,
        BatchIOConfig.DEFAULT_DROP_EXISTING
    );
  }

  @Test
  public void testCreateIngestionSchemaWithCustomSegments() throws IOException
  {
    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        clock,
        toolbox,
        LockGranularity.TIME_CHUNK,
        new CompactionIOConfig(null, false, null),
        new SegmentProvider(DATA_SOURCE, SpecificSegmentsSpec.fromSegments(SEGMENTS)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        null,
        null,
        null,
        null,
        COORDINATOR_CLIENT,
        segmentCacheManagerFactory,
        METRIC_BUILDER
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
        AGGREGATORS.stream().map(AggregatorFactory::getCombiningFactory).collect(Collectors.toList()),
        SEGMENT_INTERVALS,
        Granularities.MONTH,
        Granularities.NONE,
        BatchIOConfig.DEFAULT_DROP_EXISTING
    );
  }

  @Test
  public void testCreateIngestionSchemaWithDifferentSegmentSet() throws IOException
  {
    expectedException.expect(CoreMatchers.instanceOf(IllegalStateException.class));
    expectedException.expectMessage(CoreMatchers.containsString("are different from the current used segments"));

    final List<DataSegment> segments = new ArrayList<>(SEGMENTS);
    Collections.sort(segments);
    // Remove one segment in the middle
    segments.remove(segments.size() / 2);
    CompactionTask.createIngestionSchema(
        clock,
        toolbox,
        LockGranularity.TIME_CHUNK,
        new CompactionIOConfig(null, false, null),
        new SegmentProvider(DATA_SOURCE, SpecificSegmentsSpec.fromSegments(segments)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        null,
        null,
        null,
        null,
        COORDINATOR_CLIENT,
        segmentCacheManagerFactory,
        METRIC_BUILDER
    );
  }

  @Test
  public void testMissingMetadata() throws IOException
  {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(CoreMatchers.startsWith("Index metadata doesn't exist for segment"));

    final TestIndexIO indexIO = (TestIndexIO) toolbox.getIndexIO();
    indexIO.removeMetadata(Iterables.getFirst(indexIO.getQueryableIndexMap().keySet(), null));
    final List<DataSegment> segments = new ArrayList<>(SEGMENTS);
    CompactionTask.createIngestionSchema(
        clock,
        toolbox,
        LockGranularity.TIME_CHUNK,
        new CompactionIOConfig(null, false, null),
        new SegmentProvider(DATA_SOURCE, SpecificSegmentsSpec.fromSegments(segments)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        null,
        null,
        null,
        null,
        COORDINATOR_CLIENT,
        segmentCacheManagerFactory,
        METRIC_BUILDER
    );
  }

  @Test
  public void testEmptyInterval()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(CoreMatchers.containsString("must specify a nonempty interval"));

    final Builder builder = new Builder(
        DATA_SOURCE,
        segmentCacheManagerFactory,
        RETRY_POLICY_FACTORY
    );

    @SuppressWarnings("unused")
    final CompactionTask task = builder
        .interval(Intervals.of("2000-01-01/2000-01-01"))
        .build();
  }

  @Test
  public void testSegmentGranularityAndNullQueryGranularity() throws IOException
  {
    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        clock,
        toolbox,
        LockGranularity.TIME_CHUNK,
        new CompactionIOConfig(null, false, null),
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        null,
        null,
        null,
        new ClientCompactionTaskGranularitySpec(new PeriodGranularity(Period.months(3), null, null), null, null),
        COORDINATOR_CLIENT,
        segmentCacheManagerFactory,
        METRIC_BUILDER
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
        AGGREGATORS.stream().map(AggregatorFactory::getCombiningFactory).collect(Collectors.toList()),
        Collections.singletonList(COMPACTION_INTERVAL),
        new PeriodGranularity(Period.months(3), null, null),
        Granularities.NONE,
        BatchIOConfig.DEFAULT_DROP_EXISTING
    );
  }

  @Test
  public void testQueryGranularityAndNullSegmentGranularity() throws IOException
  {
    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        clock,
        toolbox,
        LockGranularity.TIME_CHUNK,
        new CompactionIOConfig(null, false, null),
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        null,
        null,
        null,
        new ClientCompactionTaskGranularitySpec(null, new PeriodGranularity(Period.months(3), null, null), null),
        COORDINATOR_CLIENT,
        segmentCacheManagerFactory,
        METRIC_BUILDER
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
        AGGREGATORS.stream().map(AggregatorFactory::getCombiningFactory).collect(Collectors.toList()),
        SEGMENT_INTERVALS,
        Granularities.MONTH,
        new PeriodGranularity(Period.months(3), null, null),
        BatchIOConfig.DEFAULT_DROP_EXISTING
    );
  }

  @Test
  public void testQueryGranularityAndSegmentGranularityNonNull() throws IOException
  {
    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        clock,
        toolbox,
        LockGranularity.TIME_CHUNK,
        new CompactionIOConfig(null, false, null),
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        null,
        null,
        null,
        new ClientCompactionTaskGranularitySpec(
            new PeriodGranularity(Period.months(3), null, null),
            new PeriodGranularity(Period.months(3), null, null),
            null
        ),
        COORDINATOR_CLIENT,
        segmentCacheManagerFactory,
        METRIC_BUILDER
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
        AGGREGATORS.stream().map(AggregatorFactory::getCombiningFactory).collect(Collectors.toList()),
        Collections.singletonList(COMPACTION_INTERVAL),
        new PeriodGranularity(Period.months(3), null, null),
        new PeriodGranularity(Period.months(3), null, null),
        BatchIOConfig.DEFAULT_DROP_EXISTING
    );
    emitter.verifyValue("compact/segmentAnalyzer/fetchAndProcessMillis", 10_000L);
  }

  @Test
  public void testNullGranularitySpec() throws IOException
  {
    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        clock,
        toolbox,
        LockGranularity.TIME_CHUNK,
        new CompactionIOConfig(null, false, null),
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        null,
        null,
        null,
        null,
        COORDINATOR_CLIENT,
        segmentCacheManagerFactory,
        METRIC_BUILDER
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
        AGGREGATORS.stream().map(AggregatorFactory::getCombiningFactory).collect(Collectors.toList()),
        SEGMENT_INTERVALS,
        Granularities.MONTH,
        Granularities.NONE,
        BatchIOConfig.DEFAULT_DROP_EXISTING
    );
  }

  @Test
  public void testGranularitySpecWithNullQueryGranularityAndNullSegmentGranularity()
      throws IOException
  {
    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        clock,
        toolbox,
        LockGranularity.TIME_CHUNK,
        new CompactionIOConfig(null, false, null),
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        null,
        null,
        null,
        new ClientCompactionTaskGranularitySpec(null, null, null),
        COORDINATOR_CLIENT,
        segmentCacheManagerFactory,
        METRIC_BUILDER
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
        AGGREGATORS.stream().map(AggregatorFactory::getCombiningFactory).collect(Collectors.toList()),
        SEGMENT_INTERVALS,
        Granularities.MONTH,
        Granularities.NONE,
        BatchIOConfig.DEFAULT_DROP_EXISTING
    );
  }

  @Test
  public void testGranularitySpecWithNotNullRollup()
      throws IOException
  {
    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        clock,
        toolbox,
        LockGranularity.TIME_CHUNK,
        new CompactionIOConfig(null, false, null),
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        null,
        null,
        null,
        new ClientCompactionTaskGranularitySpec(null, null, true),
        COORDINATOR_CLIENT,
        segmentCacheManagerFactory,
        METRIC_BUILDER
    );

    Assert.assertEquals(6, ingestionSpecs.size());
    for (ParallelIndexIngestionSpec indexIngestionSpec : ingestionSpecs) {
      Assert.assertTrue(indexIngestionSpec.getDataSchema().getGranularitySpec().isRollup());
    }
  }

  @Test
  public void testGranularitySpecWithNullRollup()
      throws IOException
  {
    final List<ParallelIndexIngestionSpec> ingestionSpecs = CompactionTask.createIngestionSchema(
        clock,
        toolbox,
        LockGranularity.TIME_CHUNK,
        new CompactionIOConfig(null, false, null),
        new SegmentProvider(DATA_SOURCE, new CompactionIntervalSpec(COMPACTION_INTERVAL, null)),
        new PartitionConfigurationManager(TUNING_CONFIG),
        null,
        null,
        null,
        new ClientCompactionTaskGranularitySpec(null, null, null),
        COORDINATOR_CLIENT,
        segmentCacheManagerFactory,
        METRIC_BUILDER
    );
    Assert.assertEquals(6, ingestionSpecs.size());
    for (ParallelIndexIngestionSpec indexIngestionSpec : ingestionSpecs) {
      //Expect false since rollup value in metadata of existing segments are null
      Assert.assertFalse(indexIngestionSpec.getDataSchema().getGranularitySpec().isRollup());
    }
  }

  @Test
  public void testChooseFinestGranularityWithNulls()
  {
    List<Granularity> input = Arrays.asList(
        Granularities.DAY,
        Granularities.SECOND,
        Granularities.MINUTE,
        Granularities.SIX_HOUR,
        Granularities.EIGHT_HOUR,
        Granularities.DAY,
        null,
        Granularities.ALL,
        Granularities.MINUTE
    );
    Assert.assertTrue(Granularities.SECOND.equals(chooseFinestGranularityHelper(input)));
  }

  @Test
  public void testChooseFinestGranularityNone()
  {
    List<Granularity> input = ImmutableList.of(
        Granularities.DAY,
        Granularities.SECOND,
        Granularities.MINUTE,
        Granularities.SIX_HOUR,
        Granularities.EIGHT_HOUR,
        Granularities.NONE,
        Granularities.DAY,
        Granularities.NONE,
        Granularities.MINUTE
    );
    Assert.assertTrue(Granularities.NONE.equals(chooseFinestGranularityHelper(input)));
  }

  @Test
  public void testChooseFinestGranularityAllNulls()
  {
    List<Granularity> input = Arrays.asList(
        null,
        null,
        null,
        null
    );
    Assert.assertNull(chooseFinestGranularityHelper(input));
  }

  private Granularity chooseFinestGranularityHelper(List<Granularity> granularities)
  {
    SettableSupplier<Granularity> queryGranularity = new SettableSupplier<>();
    for (Granularity current : granularities) {
      queryGranularity.set(CompactionTask.ExistingSegmentAnalyzer.compareWithCurrent(queryGranularity.get(), current));
    }
    return queryGranularity.get();
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
      Granularity expectedSegmentGranularity,
      Granularity expectedQueryGranularity,
      boolean expectedDropExisting
  )
  {
    assertIngestionSchema(
        ingestionSchemas,
        expectedDimensionsSpecs,
        expectedMetricsSpec,
        expectedSegmentIntervals,
        new CompactionTask.CompactionTuningConfig(
            null,
            null,
            null,
            500000,
            1000000L,
            null,
            Long.MAX_VALUE,
            null,
            null,
            new HashedPartitionsSpec(5000000, null, null), // automatically computed targetPartitionSize
            IndexSpec.builder()
                     .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                     .withDimensionCompression(CompressionStrategy.LZ4)
                     .withMetricCompression(CompressionStrategy.LZF)
                     .withLongEncoding(LongEncodingStrategy.LONGS)
                     .build(),
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
            null,
            null,
            null,
            null
        ),
        expectedSegmentGranularity,
        expectedQueryGranularity,
        expectedDropExisting
    );
  }

  private void assertIngestionSchema(
      List<ParallelIndexIngestionSpec> ingestionSchemas,
      List<DimensionsSpec> expectedDimensionsSpecs,
      List<AggregatorFactory> expectedMetricsSpec,
      List<Interval> expectedSegmentIntervals,
      CompactionTask.CompactionTuningConfig expectedTuningConfig,
      Granularity expectedSegmentGranularity,
      Granularity expectedQueryGranularity,
      boolean expectedDropExisting
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

      Assert.assertEquals(
          new TimestampSpec(ColumnHolder.TIME_COLUMN_NAME, "millis", null),
          dataSchema.getTimestampSpec()
      );

      Assert.assertEquals(
          new HashSet<>(expectedDimensionsSpec.getDimensions()),
          new HashSet<>(dataSchema.getDimensionsSpec().getDimensions())
      );

      // metrics
      Assert.assertEquals(expectedMetricsSpec, Arrays.asList(dataSchema.getAggregators()));
      Assert.assertEquals(
          new UniformGranularitySpec(
              expectedSegmentGranularity,
              expectedQueryGranularity,
              false,
              Collections.singletonList(expectedSegmentIntervals.get(i))
          ),
          dataSchema.getGranularitySpec()
      );

      // assert ioConfig
      final ParallelIndexIOConfig ioConfig = ingestionSchema.getIOConfig();
      Assert.assertFalse(ioConfig.isAppendToExisting());
      Assert.assertEquals(
          expectedDropExisting,
          ioConfig.isDropExisting()
      );
      final InputSource inputSource = ioConfig.getInputSource();
      Assert.assertTrue(inputSource instanceof DruidInputSource);
      final DruidInputSource druidInputSource = (DruidInputSource) inputSource;
      Assert.assertEquals(DATA_SOURCE, druidInputSource.getDataSource());
      Assert.assertEquals(expectedSegmentIntervals.get(i), druidInputSource.getInterval());
      Assert.assertNull(druidInputSource.getDimFilter());

      // assert tuningConfig
      Assert.assertEquals(expectedTuningConfig, ingestionSchema.getTuningConfig());
    }
  }

  private static class TestCoordinatorClient extends NoopCoordinatorClient
  {
    private final Map<DataSegment, File> segmentMap;

    TestCoordinatorClient(Map<DataSegment, File> segmentMap)
    {
      this.segmentMap = segmentMap;
    }

    @Override
    public ListenableFuture<List<DataSegment>> fetchUsedSegments(
        String dataSource,
        List<Interval> intervals
    )
    {
      return Futures.immediateFuture(ImmutableList.copyOf(segmentMap.keySet()));
    }
  }

  private TaskToolbox makeTaskToolbox(
      TaskActionClient taskActionClient,
      IndexIO indexIO,
      Map<DataSegment, File> segments
  )
  {
    final SegmentCacheManager segmentCacheManager = new NoopSegmentCacheManager()
    {
      @Override
      public File getSegmentFiles(DataSegment segment)
      {
        return Preconditions.checkNotNull(segments.get(segment));
      }

      @Override
      public void cleanup(DataSegment segment)
      {
        // Do nothing.
      }
    };

    final TaskConfig config = new TaskConfigBuilder()
        .setBatchProcessingMode(TaskConfig.BATCH_PROCESSING_MODE_DEFAULT.name())
        .build();
    return new TaskToolbox.Builder()
        .config(config)
        .taskActionClient(taskActionClient)
        .joinableFactory(NoopJoinableFactory.INSTANCE)
        .indexIO(indexIO)
        .indexMergerV9(new IndexMergerV9(
            OBJECT_MAPPER,
            indexIO,
            OffHeapMemorySegmentWriteOutMediumFactory.instance(),
            true
        ))
        .taskReportFileWriter(new NoopTestTaskReportFileWriter())
        .authorizerMapper(AuthTestUtils.TEST_AUTHORIZER_MAPPER)
        .chatHandlerProvider(new NoopChatHandlerProvider())
        .rowIngestionMetersFactory(TEST_UTILS.getRowIngestionMetersFactory())
        .appenderatorsManager(new TestAppenderatorsManager())
        .coordinatorClient(COORDINATOR_CLIENT)
        .segmentCacheManager(segmentCacheManager)
        .taskLogPusher(null)
        .attemptId("1")
        .emitter(emitter)
        .build();
  }

  private static class TestTaskActionClient implements TaskActionClient
  {
    private final List<DataSegment> segments;

    TestTaskActionClient(List<DataSegment> segments)
    {
      this.segments = segments;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <RetType> RetType submit(TaskAction<RetType> taskAction)
    {
      if (!(taskAction instanceof RetrieveUsedSegmentsAction)) {
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
      super(mapper, ColumnConfig.DEFAULT);

      queryableIndexMap = Maps.newHashMapWithExpectedSize(segmentFileMap.size());
      for (Entry<DataSegment, File> entry : segmentFileMap.entrySet()) {
        final DataSegment segment = entry.getKey();
        final List<String> columnNames = new ArrayList<>(segment.getDimensions().size() + segment.getMetrics().size());
        columnNames.add(ColumnHolder.TIME_COLUMN_NAME);
        columnNames.addAll(segment.getDimensions());
        columnNames.addAll(segment.getMetrics());
        final Map<String, Supplier<ColumnHolder>> columnMap = Maps.newHashMapWithExpectedSize(columnNames.size());
        final List<AggregatorFactory> aggregatorFactories = new ArrayList<>(segment.getMetrics().size());

        for (String columnName : columnNames) {
          if (MIXED_TYPE_COLUMN.equals(columnName)) {
            ColumnHolder columnHolder = createColumn(MIXED_TYPE_COLUMN_MAP.get(segment.getInterval()));
            columnMap.put(columnName, () -> columnHolder);
          } else if (DIMENSIONS.containsKey(columnName)) {
            ColumnHolder columnHolder = createColumn(DIMENSIONS.get(columnName));
            columnMap.put(columnName, () -> columnHolder);
          } else {
            final Optional<AggregatorFactory> maybeMetric = AGGREGATORS.stream()
                                                                       .filter(agg -> agg.getName().equals(columnName))
                                                                       .findAny();
            if (maybeMetric.isPresent()) {
              ColumnHolder columnHolder = createColumn(maybeMetric.get());
              columnMap.put(columnName, () -> columnHolder);
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
                metadata,
                false
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
                index.getAvailableDimensions(),
                index.getBitmapFactoryForDimensions(),
                index.getColumns(),
                index.getFileMapper(),
                null,
                false
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
    return new TestColumn(dimensionSchema.getColumnType());
  }

  private static ColumnHolder createColumn(AggregatorFactory aggregatorFactory)
  {
    return new TestColumn(aggregatorFactory.getIntermediateType());
  }

  private static class TestColumn implements ColumnHolder
  {
    private final ColumnCapabilities columnCapabilities;

    TestColumn(ColumnType type)
    {
      columnCapabilities = new ColumnCapabilitiesImpl()
          .setType(type)
          .setDictionaryEncoded(type.is(ValueType.STRING)) // set a fake value to make string columns
          .setHasBitmapIndexes(type.is(ValueType.STRING))
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

    @Nullable
    @Override
    public ColumnIndexSupplier getIndexSupplier()
    {
      return null;
    }

    @Override
    public SettableColumnValueSelector<?> makeNewSettableColumnValueSelector()
    {
      return null;
    }

  }

  /**
   * The compaction task spec in 0.16.0 except for the tuningConfig.
   * The original spec accepts only {@link IndexTuningConfig}, but this class acceps any type of tuningConfig for
   * testing.
   */
  private static class OldCompactionTaskWithAnyTuningConfigType extends AbstractTask
  {
    private final Interval interval;
    private final List<DataSegment> segments;
    @Nullable
    private final DimensionsSpec dimensionsSpec;
    @Nullable
    private final AggregatorFactory[] metricsSpec;
    @Nullable
    private final Granularity segmentGranularity;
    @Nullable
    private final Long targetCompactionSizeBytes;
    @Nullable
    private final TuningConfig tuningConfig;

    @JsonCreator
    public OldCompactionTaskWithAnyTuningConfigType(
        @JsonProperty("id") final String id,
        @JsonProperty("resource") final TaskResource taskResource,
        @JsonProperty("dataSource") final String dataSource,
        @JsonProperty("interval") @Nullable final Interval interval,
        @JsonProperty("segments") @Nullable final List<DataSegment> segments,
        @JsonProperty("dimensions") @Nullable final DimensionsSpec dimensions,
        @JsonProperty("dimensionsSpec") @Nullable final DimensionsSpec dimensionsSpec,
        @JsonProperty("metricsSpec") @Nullable final AggregatorFactory[] metricsSpec,
        @JsonProperty("segmentGranularity") @Nullable final Granularity segmentGranularity,
        @JsonProperty("targetCompactionSizeBytes") @Nullable final Long targetCompactionSizeBytes,
        @JsonProperty("tuningConfig") @Nullable final TuningConfig tuningConfig,
        @JsonProperty("context") @Nullable final Map<String, Object> context,
        @JacksonInject ObjectMapper jsonMapper,
        @JacksonInject AuthorizerMapper authorizerMapper,
        @JacksonInject ChatHandlerProvider chatHandlerProvider,
        @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory,
        @JacksonInject CoordinatorClient coordinatorClient,
        @JacksonInject SegmentCacheManagerFactory segmentCacheManagerFactory,
        @JacksonInject RetryPolicyFactory retryPolicyFactory,
        @JacksonInject AppenderatorsManager appenderatorsManager
    )
    {
      super(
          getOrMakeId(id, "compact", dataSource),
          null,
          taskResource,
          dataSource,
          context,
          IngestionMode.REPLACE_LEGACY
      );
      this.interval = interval;
      this.segments = segments;
      this.dimensionsSpec = dimensionsSpec;
      this.metricsSpec = metricsSpec;
      this.segmentGranularity = segmentGranularity;
      this.targetCompactionSizeBytes = targetCompactionSizeBytes;
      this.tuningConfig = tuningConfig;
    }

    @Override
    public String getType()
    {
      return "compact";
    }

    @Nonnull
    @JsonIgnore
    @Override
    public Set<ResourceAction> getInputSourceResources()
    {
      return ImmutableSet.of();
    }

    @JsonProperty
    public Interval getInterval()
    {
      return interval;
    }

    @JsonProperty
    public List<DataSegment> getSegments()
    {
      return segments;
    }

    @JsonProperty
    @Nullable
    public DimensionsSpec getDimensionsSpec()
    {
      return dimensionsSpec;
    }

    @JsonProperty
    @Nullable
    public AggregatorFactory[] getMetricsSpec()
    {
      return metricsSpec;
    }

    @JsonProperty
    @Nullable
    public Granularity getSegmentGranularity()
    {
      return segmentGranularity;
    }

    @Nullable
    @JsonProperty
    public Long getTargetCompactionSizeBytes()
    {
      return targetCompactionSizeBytes;
    }

    @Nullable
    @JsonProperty
    public TuningConfig getTuningConfig()
    {
      return tuningConfig;
    }

    @Override
    public boolean isReady(TaskActionClient taskActionClient)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void stopGracefully(TaskConfig taskConfig)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public TaskStatus runTask(TaskToolbox toolbox)
    {
      throw new UnsupportedOperationException();
    }
  }

  private static class ExtensionDimensionHandler extends DoubleDimensionHandler
  {
    private static final String TYPE_NAME = "extension-double";

    public ExtensionDimensionHandler(String dimensionName)
    {
      super(dimensionName);
    }

    @Override
    public DimensionSchema getDimensionSchema(ColumnCapabilities capabilities)
    {
      return new ExtensionDimensionSchema(getDimensionName(), getMultivalueHandling(), capabilities.hasBitmapIndexes());
    }
  }

  private static class ExtensionDimensionSchema extends DimensionSchema
  {
    protected ExtensionDimensionSchema(
        String name,
        MultiValueHandling multiValueHandling,
        boolean createBitmapIndex
    )
    {
      super(name, multiValueHandling, createBitmapIndex);
    }

    @Override
    public String getTypeName()
    {
      return ExtensionDimensionHandler.TYPE_NAME;
    }

    @Override
    public ColumnType getColumnType()
    {
      return ColumnType.ofComplex(ExtensionDimensionHandler.TYPE_NAME);
    }
  }
}
