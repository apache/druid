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

package org.apache.druid.msq.indexing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.guice.security.EscalatorModule;
import org.apache.druid.guice.security.PolicyModule;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.task.CompactionIntervalSpec;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.TuningConfigBuilder;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.NestedDataColumnSchema;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.indexing.CombinedDataSchema;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.apache.druid.server.initialization.AuthorizerMapperModule;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MSQCompactionRunnerTest
{
  private static final String DATA_SOURCE = "dataSource";
  private static final Interval COMPACTION_INTERVAL = Intervals.of("2017-01-01/2017-07-01");

  private static final String TIMESTAMP_COLUMN = ColumnHolder.TIME_COLUMN_NAME;
  private static final int TARGET_ROWS_PER_SEGMENT = 100000;
  private static final int MAX_ROWS_PER_SEGMENT = 150000;
  private static final GranularityType SEGMENT_GRANULARITY = GranularityType.HOUR;
  private static final GranularityType QUERY_GRANULARITY = GranularityType.HOUR;

  private static final StringDimensionSchema STRING_DIMENSION = new StringDimensionSchema("string_dim", null, false);
  private static final StringDimensionSchema MV_STRING_DIMENSION = new StringDimensionSchema(
      "mv_string_dim",
      null,
      null
  );
  private static final LongDimensionSchema LONG_DIMENSION = new LongDimensionSchema("long_dim");
  private static final NestedDataColumnSchema NESTED_DIMENSION = new NestedDataColumnSchema("nested_dim", 5);
  private static final AutoTypeColumnSchema AUTO_DIMENSION = AutoTypeColumnSchema.of("auto_dim");
  private static final List<DimensionSchema> DIMENSIONS = ImmutableList.of(
      STRING_DIMENSION,
      LONG_DIMENSION,
      MV_STRING_DIMENSION,
      NESTED_DIMENSION,
      AUTO_DIMENSION
  );
  private static final AggregateProjectionSpec PROJECTION_SPEC =
      AggregateProjectionSpec.builder("projection")
                             .virtualColumns(
                                 Granularities.toVirtualColumn(
                                     Granularities.HOUR,
                                     Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
                                 )
                             )
                             .groupingColumns(STRING_DIMENSION)
                             .aggregators(
                                 new LongSumAggregatorFactory(LONG_DIMENSION.getName(), LONG_DIMENSION.getName())
                             )
                             .build();
  private static final Map<Interval, DataSchema> INTERVAL_DATASCHEMAS = ImmutableMap.of(
      COMPACTION_INTERVAL,
      new CombinedDataSchema(
          DATA_SOURCE,
          new TimestampSpec(TIMESTAMP_COLUMN, null, null),
          new DimensionsSpec(DIMENSIONS),
          null,
          null,
          null,
          null,
          ImmutableSet.of(MV_STRING_DIMENSION.getName())
      )
  );
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final AggregatorFactory AGG1 = new CountAggregatorFactory("agg_0");
  private static final AggregatorFactory AGG2 = new LongSumAggregatorFactory("sum_added", "sum_added");
  private static final List<AggregatorFactory> AGGREGATORS = ImmutableList.of(AGG1, AGG2);
  private static final MSQCompactionRunner MSQ_COMPACTION_RUNNER = new MSQCompactionRunner(
      JSON_MAPPER,
      TestExprMacroTable.INSTANCE,
      new CoreInjectorBuilder(new StartupInjectorBuilder().forTests().build()).addModules(
          new EscalatorModule(),
          new AuthorizerMapperModule(),
          new PolicyModule()
      ).build()
  );
  private static final List<String> PARTITION_DIMENSIONS = Collections.singletonList(STRING_DIMENSION.getName());

  @Test
  public void testMultipleDisjointCompactionIntervalsAreInvalid()
  {
    Map<Interval, DataSchema> intervalDataschemas = new HashMap<>(INTERVAL_DATASCHEMAS);
    intervalDataschemas.put(Intervals.of("2017-07-01/2018-01-01"), null);
    CompactionTask compactionTask = createCompactionTask(
        new HashedPartitionsSpec(3, null, ImmutableList.of("dummy")),
        null,
        Collections.emptyMap(),
        null,
        null
    );
    CompactionConfigValidationResult validationResult = MSQ_COMPACTION_RUNNER.validateCompactionTask(
        compactionTask,
        intervalDataschemas
    );
    Assert.assertFalse(validationResult.isValid());
    Assert.assertEquals(
        StringUtils.format("MSQ: Disjoint compaction intervals[%s] not supported", intervalDataschemas.keySet()),
        validationResult.getReason()
    );
  }

  @Test
  public void testHashedPartitionsSpecIsInvalid()
  {
    CompactionTask compactionTask = createCompactionTask(
        new HashedPartitionsSpec(3, null, ImmutableList.of("dummy")),
        null,
        Collections.emptyMap(),
        null,
        null
    );
    Assert.assertFalse(MSQ_COMPACTION_RUNNER.validateCompactionTask(compactionTask, INTERVAL_DATASCHEMAS).isValid());
  }

  @Test
  public void testStringDimensionInRangePartitionsSpecIsValid()
  {
    CompactionTask compactionTask = createCompactionTask(
        new DimensionRangePartitionsSpec(TARGET_ROWS_PER_SEGMENT, null, PARTITION_DIMENSIONS, false),
        null,
        Collections.emptyMap(),
        null,
        null
    );
    Assert.assertTrue(MSQ_COMPACTION_RUNNER.validateCompactionTask(compactionTask, INTERVAL_DATASCHEMAS).isValid());
  }

  @Test
  public void testLongDimensionInRangePartitionsSpecIsInvalid()
  {
    List<String> longPartitionDimension = Collections.singletonList(LONG_DIMENSION.getName());
    CompactionTask compactionTask = createCompactionTask(
        new DimensionRangePartitionsSpec(TARGET_ROWS_PER_SEGMENT, null, longPartitionDimension, false),
        null,
        Collections.emptyMap(),
        null,
        null
    );

    CompactionConfigValidationResult validationResult = MSQ_COMPACTION_RUNNER.validateCompactionTask(
        compactionTask,
        INTERVAL_DATASCHEMAS
    );
    Assert.assertFalse(validationResult.isValid());
    Assert.assertEquals(
        "MSQ: Non-string partition dimension[long_dim] of type[long] not supported with 'range' partition spec",
        validationResult.getReason()
    );
  }

  @Test
  public void testMultiValuedDimensionInRangePartitionsSpecIsInvalid()
  {
    List<String> mvStringPartitionDimension = Collections.singletonList(MV_STRING_DIMENSION.getName());
    CompactionTask compactionTask = createCompactionTask(
        new DimensionRangePartitionsSpec(TARGET_ROWS_PER_SEGMENT, null, mvStringPartitionDimension, false),
        null,
        Collections.emptyMap(),
        null,
        null
    );

    CompactionConfigValidationResult validationResult = MSQ_COMPACTION_RUNNER.validateCompactionTask(
        compactionTask,
        INTERVAL_DATASCHEMAS
    );
    Assert.assertFalse(validationResult.isValid());
    Assert.assertEquals(
        "MSQ: Multi-valued string partition dimension[mv_string_dim] not supported with 'range' partition spec",
        validationResult.getReason()
    );
  }

  @Test
  public void testMaxTotalRowsIsInvalid()
  {
    CompactionTask compactionTask = createCompactionTask(
        new DynamicPartitionsSpec(3, 3L),
        null,
        Collections.emptyMap(),
        null,
        null
    );
    Assert.assertFalse(MSQ_COMPACTION_RUNNER.validateCompactionTask(compactionTask, INTERVAL_DATASCHEMAS).isValid());
  }

  @Test
  public void testDynamicPartitionsSpecIsValid()
  {
    CompactionTask compactionTask = createCompactionTask(
        new DynamicPartitionsSpec(3, null),
        null,
        Collections.emptyMap(),
        null,
        null
    );
    Assert.assertTrue(MSQ_COMPACTION_RUNNER.validateCompactionTask(compactionTask, INTERVAL_DATASCHEMAS).isValid());
  }

  @Test
  public void testQueryGranularityAllIsValid()
  {
    CompactionTask compactionTask = createCompactionTask(
        new DynamicPartitionsSpec(3, null),
        null,
        Collections.emptyMap(),
        new ClientCompactionTaskGranularitySpec(null, Granularities.ALL, null),
        null
    );
    Assert.assertTrue(MSQ_COMPACTION_RUNNER.validateCompactionTask(compactionTask, INTERVAL_DATASCHEMAS).isValid());
  }

  @Test
  public void testRollupFalseWithMetricsSpecIsInValid()
  {
    CompactionTask compactionTask = createCompactionTask(
        new DynamicPartitionsSpec(3, null),
        null,
        Collections.emptyMap(),
        new ClientCompactionTaskGranularitySpec(null, null, false),
        AGGREGATORS.toArray(new AggregatorFactory[0])
    );
    Assert.assertFalse(MSQ_COMPACTION_RUNNER.validateCompactionTask(compactionTask, INTERVAL_DATASCHEMAS).isValid());
  }

  @Test
  public void testRollupTrueWithoutMetricsSpecIsInvalid()
  {
    CompactionTask compactionTask = createCompactionTask(
        new DynamicPartitionsSpec(3, null),
        null,
        Collections.emptyMap(),
        new ClientCompactionTaskGranularitySpec(null, null, true),
        null
    );
    Assert.assertFalse(MSQ_COMPACTION_RUNNER.validateCompactionTask(compactionTask, INTERVAL_DATASCHEMAS).isValid());
  }

  @Test
  public void testMSQEngineWithUnsupportedMetricsSpecIsInvalid()
  {
    // Aggregators having different input and ouput column names are unsupported.
    final String inputColName = "added";
    final String outputColName = "sum_added";
    CompactionTask compactionTask = createCompactionTask(
        new DynamicPartitionsSpec(3, null),
        null,
        Collections.emptyMap(),
        new ClientCompactionTaskGranularitySpec(null, null, true),
        new AggregatorFactory[]{new LongSumAggregatorFactory(outputColName, inputColName)}
    );
    CompactionConfigValidationResult validationResult = MSQ_COMPACTION_RUNNER.validateCompactionTask(
        compactionTask,
        INTERVAL_DATASCHEMAS
    );
    Assert.assertFalse(validationResult.isValid());
    Assert.assertEquals(
        "MSQ: Aggregator[sum_added] not supported in 'metricsSpec'",
        validationResult.getReason()
    );
  }

  @Test
  public void testRunCompactionTasksWithEmptyTaskListFails() throws Exception
  {
    CompactionTask compactionTask = createCompactionTask(null, null, Collections.emptyMap(), null, null);
    TaskStatus taskStatus = MSQ_COMPACTION_RUNNER.runCompactionTasks(compactionTask, Collections.emptyMap(), null);
    Assert.assertTrue(taskStatus.isFailure());
  }

  @Test
  public void testCompactionConfigWithoutMetricsSpecProducesCorrectSpec() throws JsonProcessingException
  {
    DimFilter dimFilter = new SelectorDimFilter("dim1", "foo", null);

    CompactionTask taskCreatedWithTransformSpec = createCompactionTask(
        new DimensionRangePartitionsSpec(TARGET_ROWS_PER_SEGMENT, null, PARTITION_DIMENSIONS, false),
        dimFilter,
        Collections.emptyMap(),
        null,
        null
    );

    DataSchema dataSchema =
        DataSchema.builder()
                  .withDataSource(DATA_SOURCE)
                  .withTimestamp(new TimestampSpec(TIMESTAMP_COLUMN, null, null))
                  .withDimensions(DIMENSIONS)
                  .withGranularity(
                      new UniformGranularitySpec(
                          SEGMENT_GRANULARITY.getDefaultGranularity(),
                          QUERY_GRANULARITY.getDefaultGranularity(),
                          false,
                          Collections.singletonList(COMPACTION_INTERVAL)
                      )
                  )
                  .withTransform(new TransformSpec(dimFilter, Collections.emptyList()))
                  .build();


    List<MSQControllerTask> msqControllerTasks = MSQ_COMPACTION_RUNNER.createMsqControllerTasks(
        taskCreatedWithTransformSpec,
        Collections.singletonMap(COMPACTION_INTERVAL, dataSchema)
    );

    MSQControllerTask msqControllerTask = Iterables.getOnlyElement(msqControllerTasks);

    LegacyMSQSpec actualMSQSpec = msqControllerTask.getQuerySpec();

    Assert.assertEquals(getExpectedTuningConfig(), actualMSQSpec.getTuningConfig());
    Assert.assertEquals(getExpectedDestination(), actualMSQSpec.getDestination());

    Query<?> query = actualMSQSpec.getQuery();
    Assert.assertTrue(query instanceof ScanQuery);
    ScanQuery scanQuery = (ScanQuery) query;

    List<String> expectedColumns = new ArrayList<>();
    List<ColumnType> expectedColumnTypes = new ArrayList<>();
    // Add __time since this is a time-ordered query which doesn't have __time explicitly defined in dimensionsSpec
    expectedColumns.add(ColumnHolder.TIME_COLUMN_NAME);
    expectedColumnTypes.add(ColumnType.LONG);

    // Add TIME_VIRTUAL_COLUMN since a query granularity is specified
    expectedColumns.add(MSQCompactionRunner.TIME_VIRTUAL_COLUMN);
    expectedColumnTypes.add(ColumnType.LONG);

    expectedColumns.addAll(DIMENSIONS.stream().map(DimensionSchema::getName).collect(Collectors.toList()));
    expectedColumnTypes.addAll(DIMENSIONS.stream().map(DimensionSchema::getColumnType).collect(Collectors.toList()));

    Assert.assertEquals(expectedColumns, scanQuery.getColumns());
    Assert.assertEquals(expectedColumnTypes, scanQuery.getColumnTypes());

    Assert.assertEquals(dimFilter, scanQuery.getFilter());
    Assert.assertEquals(
        JSON_MAPPER.writeValueAsString(SEGMENT_GRANULARITY.toString()),
        msqControllerTask.getContext().get(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY)
    );
    Assert.assertEquals(
        JSON_MAPPER.writeValueAsString(QUERY_GRANULARITY.toString()),
        msqControllerTask.getContext().get(DruidSqlInsert.SQL_INSERT_QUERY_GRANULARITY)
    );
    Assert.assertEquals(WorkerAssignmentStrategy.MAX, actualMSQSpec.getAssignmentStrategy());
    Assert.assertEquals(
        PARTITION_DIMENSIONS.stream().map(OrderBy::ascending).collect(Collectors.toList()),
        scanQuery.getOrderBys()
    );
  }

  @Test
  public void testCompactionConfigWithSortOnNonTimeDimensionsProducesCorrectSpec() throws JsonProcessingException
  {
    List<DimensionSchema> nonTimeSortedDimensions = ImmutableList.of(
        STRING_DIMENSION,
        new LongDimensionSchema(ColumnHolder.TIME_COLUMN_NAME),
        LONG_DIMENSION
    );
    CompactionTask taskCreatedWithTransformSpec = createCompactionTask(
        new DynamicPartitionsSpec(TARGET_ROWS_PER_SEGMENT, null),
        null,
        Collections.emptyMap(),
        null,
        null
    );

    // Set forceSegmentSortByTime=false to enable non-time order
    DimensionsSpec dimensionsSpec = DimensionsSpec.builder()
                                                  .setDimensions(nonTimeSortedDimensions)
                                                  .setForceSegmentSortByTime(false)
                                                  .build();
    DataSchema dataSchema =
        DataSchema.builder()
                  .withDataSource(DATA_SOURCE)
                  .withTimestamp(new TimestampSpec(TIMESTAMP_COLUMN, null, null))
                  .withDimensions(dimensionsSpec)
                  .withGranularity(
                      new UniformGranularitySpec(
                          SEGMENT_GRANULARITY.getDefaultGranularity(),
                          null,
                          false,
                          Collections.singletonList(COMPACTION_INTERVAL)
                      )
                  )
                  .build();

    List<MSQControllerTask> msqControllerTasks = MSQ_COMPACTION_RUNNER.createMsqControllerTasks(
        taskCreatedWithTransformSpec,
        Collections.singletonMap(COMPACTION_INTERVAL, dataSchema)
    );

    LegacyMSQSpec actualMSQSpec = Iterables.getOnlyElement(msqControllerTasks).getQuerySpec();

    Query<?> query = actualMSQSpec.getQuery();
    Assert.assertTrue(query instanceof ScanQuery);
    ScanQuery scanQuery = (ScanQuery) query;

    // Dimensions should already list __time and the order should remain intact
    Assert.assertEquals(
        nonTimeSortedDimensions.stream().map(DimensionSchema::getName).collect(Collectors.toList()),
        scanQuery.getColumns()
    );
  }

  @Test
  public void testCompactionConfigWithMetricsSpecProducesCorrectSpec() throws JsonProcessingException
  {
    DimFilter dimFilter = new SelectorDimFilter("dim1", "foo", null);

    CompactionTask taskCreatedWithTransformSpec = createCompactionTask(
        new DimensionRangePartitionsSpec(null, MAX_ROWS_PER_SEGMENT, PARTITION_DIMENSIONS, false),
        dimFilter,
        Collections.emptyMap(),
        null,
        null
    );

    Set<String> multiValuedDimensions = new HashSet<>();
    multiValuedDimensions.add(MV_STRING_DIMENSION.getName());

    CombinedDataSchema dataSchema = new CombinedDataSchema(
        DATA_SOURCE,
        new TimestampSpec(TIMESTAMP_COLUMN, null, null),
        new DimensionsSpec(DIMENSIONS),
        AGGREGATORS.toArray(new AggregatorFactory[0]),
        new UniformGranularitySpec(
            SEGMENT_GRANULARITY.getDefaultGranularity(),
            QUERY_GRANULARITY.getDefaultGranularity(),
            Collections.singletonList(COMPACTION_INTERVAL)
        ),
        new TransformSpec(dimFilter, Collections.emptyList()),
        null,
        multiValuedDimensions
    );

    List<MSQControllerTask> msqControllerTasks = MSQ_COMPACTION_RUNNER.createMsqControllerTasks(
        taskCreatedWithTransformSpec,
        Collections.singletonMap(COMPACTION_INTERVAL, dataSchema)
    );

    MSQControllerTask msqControllerTask = Iterables.getOnlyElement(msqControllerTasks);

    LegacyMSQSpec actualMSQSpec = msqControllerTask.getQuerySpec();

    Assert.assertEquals(getExpectedTuningConfig(), actualMSQSpec.getTuningConfig());
    Assert.assertEquals(getExpectedDestination(), actualMSQSpec.getDestination());

    Query<?> query = actualMSQSpec.getQuery();
    Assert.assertTrue(query instanceof GroupByQuery);
    GroupByQuery groupByQuery = (GroupByQuery) query;

    Assert.assertEquals(dimFilter, groupByQuery.getFilter());
    Assert.assertEquals(
        JSON_MAPPER.writeValueAsString(SEGMENT_GRANULARITY.toString()),
        msqControllerTask.getContext().get(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY)
    );
    Assert.assertEquals(
        JSON_MAPPER.writeValueAsString(QUERY_GRANULARITY.toString()),
        msqControllerTask.getContext().get(DruidSqlInsert.SQL_INSERT_QUERY_GRANULARITY)
    );
    Assert.assertEquals(WorkerAssignmentStrategy.MAX, actualMSQSpec.getAssignmentStrategy());

    List<DimensionSpec> expectedDimensionSpec = new ArrayList<>();
    expectedDimensionSpec.add(
        new DefaultDimensionSpec(
            MSQCompactionRunner.TIME_VIRTUAL_COLUMN,
            MSQCompactionRunner.TIME_VIRTUAL_COLUMN,
            ColumnType.LONG
        )
    );
    String mvToArrayStringDim = MSQCompactionRunner.ARRAY_VIRTUAL_COLUMN_PREFIX + MV_STRING_DIMENSION.getName();
    // Since only MV_STRING_DIMENSION is indicated to be MVD by the CombinedSchema, conversion to array should happen
    // only for that column.
    expectedDimensionSpec.addAll(DIMENSIONS.stream()
                                           .map(dim ->
                                                    MV_STRING_DIMENSION.getName().equals(dim.getName())
                                                    ? new DefaultDimensionSpec(
                                                        mvToArrayStringDim,
                                                        mvToArrayStringDim,
                                                        ColumnType.STRING_ARRAY
                                                    )
                                                    : new DefaultDimensionSpec(
                                                        dim.getName(),
                                                        dim.getName(),
                                                        dim.getColumnType()
                                                    ))
                                           .collect(Collectors.toList()));
    Assert.assertEquals(expectedDimensionSpec, groupByQuery.getDimensions());
  }

  @Test
  public void testCompactionConfigWithProjectionsProducesCorrectSpec() throws JsonProcessingException
  {
    DimFilter dimFilter = new SelectorDimFilter("dim1", "foo", null);
    CompactionTask taskCreatedWithTransformSpec = createCompactionTask(
        new DimensionRangePartitionsSpec(TARGET_ROWS_PER_SEGMENT, null, PARTITION_DIMENSIONS, false),
        null,
        Collections.emptyMap(),
        null,
        null,
        ImmutableList.of(PROJECTION_SPEC)
    );

    DataSchema dataSchema =
        DataSchema.builder()
                  .withDataSource(DATA_SOURCE)
                  .withTimestamp(new TimestampSpec(TIMESTAMP_COLUMN, null, null))
                  .withDimensions(DIMENSIONS)
                  .withGranularity(
                      new UniformGranularitySpec(
                          SEGMENT_GRANULARITY.getDefaultGranularity(),
                          QUERY_GRANULARITY.getDefaultGranularity(),
                          false,
                          Collections.singletonList(COMPACTION_INTERVAL)
                      )
                  )
                  .withProjections(ImmutableList.of(PROJECTION_SPEC))
                  .withTransform(new TransformSpec(dimFilter, Collections.emptyList()))
                  .build();


    List<MSQControllerTask> msqControllerTasks = MSQ_COMPACTION_RUNNER.createMsqControllerTasks(
        taskCreatedWithTransformSpec,
        Collections.singletonMap(COMPACTION_INTERVAL, dataSchema)
    );

    MSQControllerTask msqControllerTask = Iterables.getOnlyElement(msqControllerTasks);

    LegacyMSQSpec actualMSQSpec = msqControllerTask.getQuerySpec();

    Assert.assertEquals(getExpectedTuningConfig(), actualMSQSpec.getTuningConfig());
    Assert.assertEquals(getExpectedDestinationWithProjections(), actualMSQSpec.getDestination());

    Assert.assertTrue(actualMSQSpec.getQuery() instanceof ScanQuery);
    ScanQuery scanQuery = (ScanQuery) actualMSQSpec.getQuery();

    List<String> expectedColumns = new ArrayList<>();
    List<ColumnType> expectedColumnTypes = new ArrayList<>();
    // Add __time since this is a time-ordered query which doesn't have __time explicitly defined in dimensionsSpec
    expectedColumns.add(ColumnHolder.TIME_COLUMN_NAME);
    expectedColumnTypes.add(ColumnType.LONG);

    // Add TIME_VIRTUAL_COLUMN since a query granularity is specified
    expectedColumns.add(MSQCompactionRunner.TIME_VIRTUAL_COLUMN);
    expectedColumnTypes.add(ColumnType.LONG);

    expectedColumns.addAll(DIMENSIONS.stream().map(DimensionSchema::getName).collect(Collectors.toList()));
    expectedColumnTypes.addAll(DIMENSIONS.stream().map(DimensionSchema::getColumnType).collect(Collectors.toList()));

    Assert.assertEquals(expectedColumns, scanQuery.getColumns());
    Assert.assertEquals(expectedColumnTypes, scanQuery.getColumnTypes());

    Assert.assertEquals(dimFilter, scanQuery.getFilter());
    Assert.assertEquals(
        JSON_MAPPER.writeValueAsString(SEGMENT_GRANULARITY.toString()),
        msqControllerTask.getContext().get(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY)
    );
    Assert.assertEquals(
        JSON_MAPPER.writeValueAsString(QUERY_GRANULARITY.toString()),
        msqControllerTask.getContext().get(DruidSqlInsert.SQL_INSERT_QUERY_GRANULARITY)
    );
    Assert.assertEquals(WorkerAssignmentStrategy.MAX, actualMSQSpec.getAssignmentStrategy());
    Assert.assertEquals(
        PARTITION_DIMENSIONS.stream().map(OrderBy::ascending).collect(Collectors.toList()),
        scanQuery.getOrderBys()
    );
  }

  private CompactionTask createCompactionTask(
      @Nullable PartitionsSpec partitionsSpec,
      @Nullable DimFilter dimFilter,
      Map<String, Object> contextParams,
      @Nullable ClientCompactionTaskGranularitySpec granularitySpec,
      @Nullable AggregatorFactory[] metricsSpec
  )
  {
    return createCompactionTask(partitionsSpec, dimFilter, contextParams, granularitySpec, metricsSpec, null);
  }

  private CompactionTask createCompactionTask(
      @Nullable PartitionsSpec partitionsSpec,
      @Nullable DimFilter dimFilter,
      Map<String, Object> contextParams,
      @Nullable ClientCompactionTaskGranularitySpec granularitySpec,
      @Nullable AggregatorFactory[] metricsSpec,
      @Nullable List<AggregateProjectionSpec> projections
  )
  {
    CompactionTransformSpec transformSpec =
        new CompactionTransformSpec(dimFilter);
    final CompactionTask.Builder builder = new CompactionTask.Builder(
        DATA_SOURCE,
        null
    );
    IndexSpec indexSpec = createIndexSpec();

    Map<String, Object> context = new HashMap<>();
    context.put(MultiStageQueryContext.CTX_MAX_NUM_TASKS, 2);
    context.putAll(contextParams);

    builder
        .inputSpec(new CompactionIntervalSpec(COMPACTION_INTERVAL, null))
        .tuningConfig(createTuningConfig(
            indexSpec,
            partitionsSpec == null ? new DynamicPartitionsSpec(100, null) : partitionsSpec
        ))
        .transformSpec(transformSpec)
        .granularitySpec(granularitySpec)
        .dimensionsSpec(new DimensionsSpec(null))
        .metricsSpec(metricsSpec)
        .projections(projections)
        .compactionRunner(MSQ_COMPACTION_RUNNER)
        .context(context);

    return builder.build();
  }

  private static CompactionTask.CompactionTuningConfig createTuningConfig(
      IndexSpec indexSpec,
      PartitionsSpec partitionsSpec
  )
  {
    return TuningConfigBuilder
        .forCompactionTask()
        .withMaxRowsInMemory(500000)
        .withMaxBytesInMemory(1000000L)
        .withMaxTotalRows(Long.MAX_VALUE)
        .withPartitionsSpec(partitionsSpec)
        .withIndexSpec(indexSpec)
        .withForceGuaranteedRollup(!(partitionsSpec instanceof DynamicPartitionsSpec))
        .withReportParseExceptions(false)
        .withPushTimeout(5000L)
        .build();
  }

  private static IndexSpec createIndexSpec()
  {
    return IndexSpec.builder()
                    .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                    .withDimensionCompression(CompressionStrategy.LZ4)
                    .withMetricCompression(CompressionStrategy.LZF)
                    .withLongEncoding(CompressionFactory.LongEncodingStrategy.LONGS)
                    .build();
  }

  private static DataSourceMSQDestination getExpectedDestination()
  {
    return new DataSourceMSQDestination(
        DATA_SOURCE,
        SEGMENT_GRANULARITY.getDefaultGranularity(),
        null,
        Collections.singletonList(COMPACTION_INTERVAL),
        DIMENSIONS.stream().collect(Collectors.toMap(DimensionSchema::getName, Function.identity())),
        null,
        null
    );
  }

  private static DataSourceMSQDestination getExpectedDestinationWithProjections()
  {
    return new DataSourceMSQDestination(
        DATA_SOURCE,
        SEGMENT_GRANULARITY.getDefaultGranularity(),
        null,
        Collections.singletonList(COMPACTION_INTERVAL),
        DIMENSIONS.stream().collect(Collectors.toMap(DimensionSchema::getName, Function.identity())),
        ImmutableList.of(PROJECTION_SPEC),
        null
    );
  }

  private static MSQTuningConfig getExpectedTuningConfig()
  {
    return new MSQTuningConfig(
        1,
        MultiStageQueryContext.DEFAULT_ROWS_IN_MEMORY,
        MAX_ROWS_PER_SEGMENT,
        null,
        createIndexSpec()
    );
  }
}
