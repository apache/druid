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
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.client.indexing.ClientCompactionTaskTransformSpec;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexing.common.task.CompactionIntervalSpec;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.NativeCompactionRunner;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MSQCompactionRunnerTest
{
  private static final String DATA_SOURCE = "dataSource";
  private static final Interval COMPACTION_INTERVAL = Intervals.of("2017-01-01/2017-07-01");

  private static final String TIMESTAMP_COLUMN = "timestamp";
  private static final int TARGET_ROWS_PER_SEGMENT = 100000;
  private static final GranularityType SEGMENT_GRANULARITY = GranularityType.HOUR;
  private static final GranularityType QUERY_GRANULARITY = GranularityType.HOUR;
  private static List<String> PARTITION_DIMENSIONS;
  private static List<String> SORT_ORDER_DIMENSIONS;

  private static final StringDimensionSchema DIM1 = new StringDimensionSchema(
      "string_dim",
      null,
      null
  );
  private static final LongDimensionSchema DIM2 = new LongDimensionSchema("long_dim");
  private static final List<DimensionSchema> DIMENSIONS = ImmutableList.of(DIM1, DIM2);
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final AggregatorFactory AGG1 = new CountAggregatorFactory("agg_0");
  private static final AggregatorFactory AGG2 = new LongSumAggregatorFactory("agg_1", "long_dim_1");
  private static List<AggregatorFactory> AGGREGATORS = ImmutableList.of(AGG1, AGG2);

  @BeforeClass
  public static void setupClass()
  {
    NullHandling.initializeForTests();

    final StringDimensionSchema stringDimensionSchema = new StringDimensionSchema(
        "string_dim",
        null,
        null
    );

    PARTITION_DIMENSIONS = Collections.singletonList(stringDimensionSchema.getName());

    final LongDimensionSchema longDimensionSchema = new LongDimensionSchema("long_dim");
    SORT_ORDER_DIMENSIONS = Collections.singletonList(longDimensionSchema.getName());


    JSON_MAPPER.setInjectableValues(new InjectableValues.Std().addValue(
        ExprMacroTable.class,
        LookupEnabledTestExprMacroTable.INSTANCE
    ));
  }

  @Test
  public void testCompactionToMSQTasks() throws JsonProcessingException
  {
    DimFilter dimFilter = new SelectorDimFilter("dim1", "foo", null);
    ClientCompactionTaskTransformSpec transformSpec =
        new ClientCompactionTaskTransformSpec(dimFilter);
    final CompactionTask.Builder builder = new CompactionTask.Builder(
        DATA_SOURCE,
        null,
        new MSQCompactionRunner(JSON_MAPPER, null)
    );
    IndexSpec indexSpec = IndexSpec.builder()
                                   .withBitmapSerdeFactory(RoaringBitmapSerdeFactory.getInstance())
                                   .withDimensionCompression(CompressionStrategy.LZ4)
                                   .withMetricCompression(CompressionStrategy.LZF)
                                   .withLongEncoding(CompressionFactory.LongEncodingStrategy.LONGS)
                                   .build();

    Map<String, Object> context = new HashMap<>();
    context.put(MultiStageQueryContext.CTX_SORT_ORDER, JSON_MAPPER.writeValueAsString(SORT_ORDER_DIMENSIONS));
    context.put(MultiStageQueryContext.CTX_MAX_NUM_TASKS, 2);

    builder
        .inputSpec(new CompactionIntervalSpec(COMPACTION_INTERVAL, null))
        .tuningConfig(createTuningConfig(indexSpec))
        .transformSpec(transformSpec)
        .context(context);

    final CompactionTask taskCreatedWithTransformSpec = builder.build();
    Assert.assertEquals(
        transformSpec,
        taskCreatedWithTransformSpec.getTransformSpec()
    );

    DataSchema dataSchema = new DataSchema(
        DATA_SOURCE,
        new TimestampSpec(TIMESTAMP_COLUMN, null, null),
        new DimensionsSpec(DIMENSIONS),
        AGGREGATORS.toArray(new AggregatorFactory[0]),
        new UniformGranularitySpec(
            SEGMENT_GRANULARITY.getDefaultGranularity(),
            QUERY_GRANULARITY.getDefaultGranularity(),
            Collections.singletonList(COMPACTION_INTERVAL)
        ),
        new TransformSpec(dimFilter, Collections.emptyList())
    );

    List<MSQControllerTask> msqControllerTasks = new MSQCompactionRunner(JSON_MAPPER, null)
        .compactionToMSQTasks(
            taskCreatedWithTransformSpec,
            Collections.singletonList(new NonnullPair<>(
                COMPACTION_INTERVAL,
                dataSchema
            ))
        );

    Assert.assertEquals(1, msqControllerTasks.size());

    MSQControllerTask msqControllerTask = msqControllerTasks.get(0);
    MSQSpec actualMSQSpec = msqControllerTask.getQuerySpec();
    Assert.assertEquals(
        new MSQTuningConfig(
            1,
            MultiStageQueryContext.DEFAULT_ROWS_IN_MEMORY,
            TARGET_ROWS_PER_SEGMENT,
            indexSpec
        ),
        actualMSQSpec.getTuningConfig()
    );
    Assert.assertEquals(new DataSourceMSQDestination(
        DATA_SOURCE,
        SEGMENT_GRANULARITY.getDefaultGranularity(),
        SORT_ORDER_DIMENSIONS,
        Collections.singletonList(COMPACTION_INTERVAL)
    ), actualMSQSpec.getDestination());

    Assert.assertEquals(dimFilter, actualMSQSpec.getQuery().getFilter());
    Assert.assertEquals(
        JSON_MAPPER.writeValueAsString(SEGMENT_GRANULARITY.toString()),
        msqControllerTask.getContext().get(DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY)
    );
    Assert.assertEquals(
        JSON_MAPPER.writeValueAsString(QUERY_GRANULARITY.toString()),
        msqControllerTask.getContext().get(DruidSqlInsert.SQL_INSERT_QUERY_GRANULARITY)
    );
    Assert.assertEquals(WorkerAssignmentStrategy.MAX, actualMSQSpec.getAssignmentStrategy());
  }

  private static CompactionTask.CompactionTuningConfig createTuningConfig(IndexSpec indexSpec)
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
        new DimensionRangePartitionsSpec(TARGET_ROWS_PER_SEGMENT, null, PARTITION_DIMENSIONS, false),
        indexSpec,
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
}
