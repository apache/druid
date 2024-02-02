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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.DurableStorageMSQDestination;
import org.apache.druid.msq.indexing.destination.MSQSelectDestination;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.test.CounterSnapshotMatcher;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.operator.ColumnWithDirection;
import org.apache.druid.query.operator.NaivePartitioningOperatorFactory;
import org.apache.druid.query.operator.NaiveSortOperatorFactory;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.operator.window.WindowFrame;
import org.apache.druid.query.operator.window.WindowFramedAggregateProcessor;
import org.apache.druid.query.operator.window.WindowOperatorFactory;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

@RunWith(Parameterized.class)
public class MSQWindowTest extends MSQTestBase
{

  public static final String QUERY_RESULTS_WITH_DURABLE_STORAGE = "query_results_with_durable_storage";

  public static final String QUERY_RESULTS_WITH_DEFAULT = "query_results_with_default_storage";

  public static final Map<String, Object> QUERY_RESULTS_WITH_DURABLE_STORAGE_CONTEXT =
      ImmutableMap.<String, Object>builder()
                  .putAll(DURABLE_STORAGE_MSQ_CONTEXT)
                  .put(MultiStageQueryContext.CTX_ROWS_PER_PAGE, 2)
                  .put(
                      MultiStageQueryContext.CTX_SELECT_DESTINATION,
                      StringUtils.toLowerCase(MSQSelectDestination.DURABLESTORAGE.getName())
                  )
                  .build();


  public static final Map<String, Object> QUERY_RESULTS_WITH_DEFAULT_CONTEXT =
      ImmutableMap.<String, Object>builder()
                  .putAll(DEFAULT_MSQ_CONTEXT)
                  .put(
                      MultiStageQueryContext.CTX_SELECT_DESTINATION,
                      StringUtils.toLowerCase(MSQSelectDestination.DURABLESTORAGE.getName())
                  )
                  .build();
  @Parameterized.Parameter(0)
  public String contextName;
  @Parameterized.Parameter(1)
  public Map<String, Object> context;

  @Parameterized.Parameters(name = "{index}:with context {0}")
  public static Collection<Object[]> data()
  {
    Object[][] data = new Object[][]{
        {DEFAULT, DEFAULT_MSQ_CONTEXT},
        {DURABLE_STORAGE, DURABLE_STORAGE_MSQ_CONTEXT},
        {FAULT_TOLERANCE, FAULT_TOLERANCE_MSQ_CONTEXT},
        {PARALLEL_MERGE, PARALLEL_MERGE_MSQ_CONTEXT},
        {QUERY_RESULTS_WITH_DURABLE_STORAGE, QUERY_RESULTS_WITH_DURABLE_STORAGE_CONTEXT},
        {QUERY_RESULTS_WITH_DEFAULT, QUERY_RESULTS_WITH_DEFAULT_CONTEXT}
    };

    return Arrays.asList(data);
  }


  @Test
  public void testWindowOnFooWithPartitionByAndInnerGroupBy()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cc", ColumnType.DOUBLE)
                                            .build();

    final Query groupByQuery = GroupByQuery.builder()
                                           .setDataSource(CalciteTests.DATASOURCE1)
                                           .setInterval(querySegmentSpec(Filtration
                                                                             .eternity()))
                                           .setGranularity(Granularities.ALL)
                                           .setDimensions(dimensions(
                                               new DefaultDimensionSpec(
                                                   "m1",
                                                   "d0",
                                                   ColumnType.FLOAT
                                               )
                                           ))
                                           .setContext(context)
                                           .build();


    final WindowFrame theFrame = new WindowFrame(WindowFrame.PeerType.ROWS, true, 0, true, 0, null);
    final AggregatorFactory[] theAggs = {
        new DoubleSumAggregatorFactory("w0", "d0")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(groupByQuery),
        new LegacySegmentSpec(Intervals.ETERNITY),
        context,
        RowSignature.builder().add("d0", ColumnType.FLOAT).add("w0", ColumnType.DOUBLE).build(),
        ImmutableList.of(
            new NaivePartitioningOperatorFactory(ImmutableList.of("d0")),
            new WindowOperatorFactory(proc)
        ),
        null
    );
    testSelectQuery()
        .setSql("select m1,SUM(m1) OVER(PARTITION BY m1) cc from foo group by m1")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("d0", "m1"),
                                           new ColumnMapping("w0", "cc")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(isDurableStorageDestination()
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1.0f, 1.0},
            new Object[]{2.0f, 2.0},
            new Object[]{3.0f, 3.0},
            new Object[]{4.0f, 4.0},
            new Object[]{5.0f, 5.0},
            new Object[]{6.0f, 6.0}
        ))
        .setQueryContext(context)
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().totalFiles(1),
            0, 0, "input0"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(6).frames(1),
            0, 0, "output"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(6).frames(1),
            0, 0, "shuffle"
        )
        .verifyResults();
  }

  @Test
  public void testWindowOnFooWithFirstWindowPartitionNextWindowEmpty()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("m2", ColumnType.DOUBLE)
                                            .add("summ2", ColumnType.DOUBLE)
                                            .add("summ1", ColumnType.DOUBLE)
                                            .build();

    final Query groupByQuery = GroupByQuery.builder()
                                           .setDataSource(CalciteTests.DATASOURCE1)
                                           .setInterval(querySegmentSpec(Filtration
                                                                             .eternity()))
                                           .setGranularity(Granularities.ALL)
                                           .setDimensions(dimensions(
                                               new DefaultDimensionSpec(
                                                   "m1",
                                                   "d0",
                                                   ColumnType.FLOAT
                                               ),
                                               new DefaultDimensionSpec(
                                                   "m2",
                                                   "d1",
                                                   ColumnType.DOUBLE
                                               )
                                           ))
                                           .setContext(context)
                                           .build();


    final WindowFrame theFrame = new WindowFrame(WindowFrame.PeerType.ROWS, true, 0, true, 0, null);
    final AggregatorFactory[] theAggs = {
        new DoubleSumAggregatorFactory("w0", "d1")
    };
    final AggregatorFactory[] nextAggs = {
        new DoubleSumAggregatorFactory("w1", "d0")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);
    WindowFramedAggregateProcessor proc1 = new WindowFramedAggregateProcessor(theFrame, nextAggs);

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(groupByQuery),
        new LegacySegmentSpec(Intervals.ETERNITY),
        context,
        RowSignature.builder()
                    .add("d0", ColumnType.FLOAT)
                    .add("d1", ColumnType.DOUBLE)
                    .add("w0", ColumnType.DOUBLE)
                    .add("w1", ColumnType.DOUBLE)
                    .build(),
        ImmutableList.of(
            new NaivePartitioningOperatorFactory(ImmutableList.of("d0")),
            new WindowOperatorFactory(proc),
            new NaivePartitioningOperatorFactory(ImmutableList.of()),
            new WindowOperatorFactory(proc1)
        ),
        null
    );
    testSelectQuery()
        .setSql("SELECT m1, m2,\n"
                + "SUM(m2) OVER(PARTITION BY m1) as summ2\n"
                + ",SUM(m1) OVER() as summ1\n"
                + "from foo\n"
                + "GROUP BY m1,m2")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("d0", "m1"),
                                           new ColumnMapping("d1", "m2"),
                                           new ColumnMapping("w0", "summ2"),
                                           new ColumnMapping("w1", "summ1")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(isDurableStorageDestination()
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1.0f, 1.0, 1.0, 1.0},
            new Object[]{2.0f, 2.0, 2.0, 2.0},
            new Object[]{3.0f, 3.0, 3.0, 3.0},
            new Object[]{4.0f, 4.0, 4.0, 4.0},
            new Object[]{5.0f, 5.0, 5.0, 5.0},
            new Object[]{6.0f, 6.0, 6.0, 6.0}
        ))
        .setQueryContext(context)
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().totalFiles(1),
            0, 0, "input0"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(6).frames(1),
            0, 0, "output"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(6).frames(1),
            0, 0, "shuffle"
        )
        .verifyResults();
  }

  @Test
  public void testWindowOnFooWith2WindowsBothWindowsHavingPartitionByInnerGroupBy()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("m2", ColumnType.DOUBLE)
                                            .add("summ2", ColumnType.DOUBLE)
                                            .add("summ1", ColumnType.DOUBLE)
                                            .build();

    final Query groupByQuery = GroupByQuery.builder()
                                           .setDataSource(CalciteTests.DATASOURCE1)
                                           .setInterval(querySegmentSpec(Filtration
                                                                             .eternity()))
                                           .setGranularity(Granularities.ALL)
                                           .setDimensions(dimensions(
                                               new DefaultDimensionSpec(
                                                   "m1",
                                                   "d0",
                                                   ColumnType.FLOAT
                                               ),
                                               new DefaultDimensionSpec(
                                                   "m2",
                                                   "d1",
                                                   ColumnType.DOUBLE
                                               )
                                           ))
                                           .setContext(context)
                                           .build();


    final WindowFrame theFrame = new WindowFrame(WindowFrame.PeerType.ROWS, true, 0, true, 0, null);
    final AggregatorFactory[] theAggs = {
        new DoubleSumAggregatorFactory("w0", "d1")
    };
    final AggregatorFactory[] nextAggs = {
        new DoubleSumAggregatorFactory("w1", "d0")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);
    WindowFramedAggregateProcessor proc1 = new WindowFramedAggregateProcessor(theFrame, nextAggs);

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(groupByQuery),
        new LegacySegmentSpec(Intervals.ETERNITY),
        context,
        RowSignature.builder()
                    .add("d0", ColumnType.FLOAT)
                    .add("d1", ColumnType.DOUBLE)
                    .add("w0", ColumnType.DOUBLE)
                    .add("w1", ColumnType.DOUBLE)
                    .build(),
        ImmutableList.of(
            new NaivePartitioningOperatorFactory(ImmutableList.of("d0")),
            new WindowOperatorFactory(proc),
            new NaiveSortOperatorFactory(ImmutableList.of(ColumnWithDirection.ascending("d1"))),
            new NaivePartitioningOperatorFactory(ImmutableList.of("d1")),
            new WindowOperatorFactory(proc1)
        ),
        null
    );
    testSelectQuery()
        .setSql("SELECT m1, m2,\n"
                + "SUM(m2) OVER(PARTITION BY m1) as summ2\n"
                + ",SUM(m1) OVER(PARTITION BY m2) as summ1\n"
                + "from foo\n"
                + "GROUP BY m1,m2")
        /**
         *
         */
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("d0", "m1"),
                                           new ColumnMapping("d1", "m2"),
                                           new ColumnMapping("w0", "summ2"),
                                           new ColumnMapping("w1", "summ1")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(isDurableStorageDestination()
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1.0f, 1.0, 1.0, 1.0},
            new Object[]{2.0f, 2.0, 2.0, 2.0},
            new Object[]{3.0f, 3.0, 3.0, 3.0},
            new Object[]{4.0f, 4.0, 4.0, 4.0},
            new Object[]{5.0f, 5.0, 5.0, 5.0},
            new Object[]{6.0f, 6.0, 6.0, 6.0}
        ))
        .setQueryContext(context)
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().totalFiles(1),
            0, 0, "input0"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(6).frames(1),
            0, 0, "output"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(6).frames(1),
            0, 0, "shuffle"
        )
        .verifyResults();
  }

  @Test
  public void testWindowOnFooWith2WindowsBothPartitionByWithOrderReversed()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("m2", ColumnType.DOUBLE)
                                            .add("summ1", ColumnType.DOUBLE)
                                            .add("summ2", ColumnType.DOUBLE)
                                            .build();

    final Query groupByQuery = GroupByQuery.builder()
                                           .setDataSource(CalciteTests.DATASOURCE1)
                                           .setInterval(querySegmentSpec(Filtration.eternity()))
                                           .setGranularity(Granularities.ALL)
                                           .setDimensions(dimensions(
                                               new DefaultDimensionSpec(
                                                   "m1",
                                                   "d0",
                                                   ColumnType.FLOAT
                                               ),
                                               new DefaultDimensionSpec(
                                                   "m2",
                                                   "d1",
                                                   ColumnType.DOUBLE
                                               )
                                           ))
                                           .setContext(context)
                                           .build();


    final WindowFrame theFrame = new WindowFrame(WindowFrame.PeerType.ROWS, true, 0, true, 0, null);
    final AggregatorFactory[] theAggs = {
        new DoubleSumAggregatorFactory("w0", "d0")
    };
    final AggregatorFactory[] nextAggs = {
        new DoubleSumAggregatorFactory("w1", "d1")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);
    WindowFramedAggregateProcessor proc1 = new WindowFramedAggregateProcessor(theFrame, nextAggs);

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(groupByQuery),
        new LegacySegmentSpec(Intervals.ETERNITY),
        context,
        RowSignature.builder()
                    .add("d0", ColumnType.FLOAT)
                    .add("d1", ColumnType.DOUBLE)
                    .add("w0", ColumnType.DOUBLE)
                    .add("w1", ColumnType.DOUBLE)
                    .build(),
        ImmutableList.of(
            new NaiveSortOperatorFactory(ImmutableList.of(ColumnWithDirection.ascending("d1"))),
            new NaivePartitioningOperatorFactory(ImmutableList.of("d1")),
            new WindowOperatorFactory(proc),
            new NaiveSortOperatorFactory(ImmutableList.of(ColumnWithDirection.ascending("d0"))),
            new NaivePartitioningOperatorFactory(ImmutableList.of("d0")),
            new WindowOperatorFactory(proc1)
        ),
        null
    );
    testSelectQuery()
        .setSql("SELECT m1, m2,\n"
                + "SUM(m1) OVER(PARTITION BY m2) as summ1\n"
                + ",SUM(m2) OVER(PARTITION BY m1) as summ2\n"
                + "from foo\n"
                + "GROUP BY m1,m2")
        /**
         *
         */
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("d0", "m1"),
                                           new ColumnMapping("d1", "m2"),
                                           new ColumnMapping("w0", "summ1"),
                                           new ColumnMapping("w1", "summ2")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(isDurableStorageDestination()
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1.0f, 1.0, 1.0, 1.0},
            new Object[]{2.0f, 2.0, 2.0, 2.0},
            new Object[]{3.0f, 3.0, 3.0, 3.0},
            new Object[]{4.0f, 4.0, 4.0, 4.0},
            new Object[]{5.0f, 5.0, 5.0, 5.0},
            new Object[]{6.0f, 6.0, 6.0, 6.0}
        ))
        .setQueryContext(context)
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().totalFiles(1),
            0, 0, "input0"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(6).frames(1),
            0, 0, "output"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(6).frames(1),
            0, 0, "shuffle"
        )
        .verifyResults();
  }

  @Test
  public void testWindowOnFooWithEmptyOverWithGroupBy()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cc", ColumnType.DOUBLE)
                                            .build();

    final Query groupByQuery = GroupByQuery.builder()
                                           .setDataSource(CalciteTests.DATASOURCE1)
                                           .setInterval(querySegmentSpec(Filtration
                                                                             .eternity()))
                                           .setGranularity(Granularities.ALL)
                                           .setDimensions(dimensions(
                                               new DefaultDimensionSpec(
                                                   "m1",
                                                   "d0",
                                                   ColumnType.FLOAT
                                               )
                                           ))
                                           .setContext(context)
                                           .build();


    final WindowFrame theFrame = new WindowFrame(WindowFrame.PeerType.ROWS, true, 0, true, 0, null);
    final AggregatorFactory[] theAggs = {
        new DoubleSumAggregatorFactory("w0", "d0")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(groupByQuery),
        new LegacySegmentSpec(Intervals.ETERNITY),
        context,
        RowSignature.builder().add("d0", ColumnType.FLOAT).add("w0", ColumnType.DOUBLE).build(),
        ImmutableList.of(
            new NaivePartitioningOperatorFactory(ImmutableList.of()),
            new WindowOperatorFactory(proc)
        ),
        null
    );
    testSelectQuery()
        .setSql("select m1,SUM(m1) OVER() cc from foo group by m1")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("d0", "m1"),
                                           new ColumnMapping("w0", "cc")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(isDurableStorageDestination()
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1.0f, 21.0},
            new Object[]{2.0f, 21.0},
            new Object[]{3.0f, 21.0},
            new Object[]{4.0f, 21.0},
            new Object[]{5.0f, 21.0},
            new Object[]{6.0f, 21.0}
        ))
        .setQueryContext(context)
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().totalFiles(1),
            0, 0, "input0"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(6).frames(1),
            0, 0, "output"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(6).frames(1),
            0, 0, "shuffle"
        )
        .verifyResults();
  }

  @Test
  public void testWindowOnFooWithNoGroupByAndPartition()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cc", ColumnType.DOUBLE)
                                            .build();

    final WindowFrame theFrame = new WindowFrame(WindowFrame.PeerType.ROWS, true, 0, true, 0, null);
    final AggregatorFactory[] theAggs = {
        new DoubleSumAggregatorFactory("w0", "m1")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);

    final Map<String, Object> contextWithRowSignature =
        ImmutableMap.<String, Object>builder()
                    .putAll(context)
                    .put(DruidQuery.CTX_SCAN_SIGNATURE, "[{\"name\":\"m1\",\"type\":\"FLOAT\"}]")
                    .build();

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("m1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(contextWithRowSignature)
                .legacy(false)
                .build()),
        new LegacySegmentSpec(Intervals.ETERNITY),
        context,
        RowSignature.builder().add("m1", ColumnType.FLOAT).add("w0", ColumnType.DOUBLE).build(),
        ImmutableList.of(
            new NaiveSortOperatorFactory(ImmutableList.of(ColumnWithDirection.ascending("m1"))),
            new NaivePartitioningOperatorFactory(ImmutableList.of("m1")),
            new WindowOperatorFactory(proc)
        ),
        ImmutableList.of()
    );
    testSelectQuery()
        .setSql("select m1,SUM(m1) OVER(PARTITION BY m1) cc from foo")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("m1", "m1"),
                                           new ColumnMapping("w0", "cc")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(isDurableStorageDestination()
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1.0f, 1.0},
            new Object[]{2.0f, 2.0},
            new Object[]{3.0f, 3.0},
            new Object[]{4.0f, 4.0},
            new Object[]{5.0f, 5.0},
            new Object[]{6.0f, 6.0}
        ))
        .setQueryContext(context)
        .verifyResults();
  }

  @Test
  public void testWindowOnFooWithNoGroupByAndPartitionOnTwoElements()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cc", ColumnType.DOUBLE)
                                            .build();

    final WindowFrame theFrame = new WindowFrame(WindowFrame.PeerType.ROWS, true, 0, true, 0, null);
    final AggregatorFactory[] theAggs = {
        new DoubleSumAggregatorFactory("w0", "m1")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);

    final Map<String, Object> contextWithRowSignature =
        ImmutableMap.<String, Object>builder()
                    .putAll(context)
                    .put(
                        DruidQuery.CTX_SCAN_SIGNATURE,
                        "[{\"name\":\"m1\",\"type\":\"FLOAT\"},{\"name\":\"m2\",\"type\":\"DOUBLE\"}]"
                    )
                    .build();

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("m1", "m2")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(contextWithRowSignature)
                .legacy(false)
                .build()),
        new LegacySegmentSpec(Intervals.ETERNITY),
        context,
        RowSignature.builder().add("m1", ColumnType.FLOAT).add("w0", ColumnType.DOUBLE).build(),
        ImmutableList.of(
            new NaiveSortOperatorFactory(ImmutableList.of(
                ColumnWithDirection.ascending("m1"),
                ColumnWithDirection.ascending("m2")
            )),
            new NaivePartitioningOperatorFactory(ImmutableList.of("m1", "m2")),
            new WindowOperatorFactory(proc)
        ),
        ImmutableList.of()
    );
    testSelectQuery()
        .setSql("select m1,SUM(m1) OVER(PARTITION BY m1,m2) cc from foo")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("m1", "m1"),
                                           new ColumnMapping("w0", "cc")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(isDurableStorageDestination()
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1.0f, 1.0},
            new Object[]{2.0f, 2.0},
            new Object[]{3.0f, 3.0},
            new Object[]{4.0f, 4.0},
            new Object[]{5.0f, 5.0},
            new Object[]{6.0f, 6.0}
        ))
        .setQueryContext(context)
        .verifyResults();
  }

  @Test
  public void testWindowOnFooWithNoGroupByAndPartitionByAnother()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cc", ColumnType.DOUBLE)
                                            .build();

    final WindowFrame theFrame = new WindowFrame(WindowFrame.PeerType.ROWS, true, 0, true, 0, null);
    final AggregatorFactory[] theAggs = {
        new DoubleSumAggregatorFactory("w0", "m1")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);

    final Map<String, Object> contextWithRowSignature =
        ImmutableMap.<String, Object>builder()
                    .putAll(context)
                    .put(
                        DruidQuery.CTX_SCAN_SIGNATURE,
                        "[{\"name\":\"m1\",\"type\":\"FLOAT\"},{\"name\":\"m2\",\"type\":\"DOUBLE\"}]"
                    )
                    .build();

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("m1", "m2")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(contextWithRowSignature)
                .legacy(false)
                .build()),
        new LegacySegmentSpec(Intervals.ETERNITY),
        context,
        RowSignature.builder().add("m1", ColumnType.FLOAT).add("w0", ColumnType.DOUBLE).build(),
        ImmutableList.of(
            new NaiveSortOperatorFactory(ImmutableList.of(ColumnWithDirection.ascending("m2"))),
            new NaivePartitioningOperatorFactory(ImmutableList.of("m2")),
            new WindowOperatorFactory(proc)
        ),
        ImmutableList.of()
    );
    testSelectQuery()
        .setSql("select m1,SUM(m1) OVER(PARTITION BY m2) cc from foo")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("m1", "m1"),
                                           new ColumnMapping("w0", "cc")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(isDurableStorageDestination()
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1.0f, 1.0},
            new Object[]{2.0f, 2.0},
            new Object[]{3.0f, 3.0},
            new Object[]{4.0f, 4.0},
            new Object[]{5.0f, 5.0},
            new Object[]{6.0f, 6.0}
        ))
        .setQueryContext(context)
        .verifyResults();
  }

  @Test
  public void testWindowOnFooWithNoGroupByAndPartitionAndVirtualColumns()
  {
    final Map<String, Object> contextWithRowSignature =
        ImmutableMap.<String, Object>builder()
                    .putAll(context)
                    .put(
                        DruidQuery.CTX_SCAN_SIGNATURE,
                        "[{\"name\":\"m1\",\"type\":\"FLOAT\"},{\"name\":\"v0\",\"type\":\"LONG\"}]"
                    )
                    .build();

    RowSignature rowSignature = RowSignature.builder()
                                            .add("ld", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cc", ColumnType.DOUBLE)
                                            .build();

    final WindowFrame theFrame = new WindowFrame(WindowFrame.PeerType.ROWS, true, 0, true, 0, null);
    final AggregatorFactory[] theAggs = {
        new DoubleSumAggregatorFactory("w0", "m1")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("m1", "v0")
                .virtualColumns(expressionVirtualColumn("v0", "strlen(\"dim1\")", ColumnType.LONG))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(contextWithRowSignature)
                .legacy(false)
                .build()),
        new LegacySegmentSpec(Intervals.ETERNITY),
        context,
        RowSignature.builder()
                    .add("v0", ColumnType.LONG)
                    .add("m1", ColumnType.FLOAT)
                    .add("w0", ColumnType.DOUBLE)
                    .build(),
        ImmutableList.of(
            new NaiveSortOperatorFactory(ImmutableList.of(ColumnWithDirection.ascending("m1"))),
            new NaivePartitioningOperatorFactory(ImmutableList.of("m1")),
            new WindowOperatorFactory(proc)
        ),
        ImmutableList.of()
    );
    testSelectQuery()
        .setSql("select STRLEN(dim1) as ld, m1, SUM(m1) OVER(PARTITION BY m1) cc from foo")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("v0", "ld"),
                                           new ColumnMapping("m1", "m1"),
                                           new ColumnMapping("w0", "cc")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(isDurableStorageDestination()
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{0, 1.0f, 1.0},
            new Object[]{4, 2.0f, 2.0},
            new Object[]{1, 3.0f, 3.0},
            new Object[]{1, 4.0f, 4.0},
            new Object[]{3, 5.0f, 5.0},
            new Object[]{3, 6.0f, 6.0}
        ))
        .setQueryContext(context)
        .verifyResults();
  }


  @Test
  public void testWindowOnFooWithNoGroupByAndEmptyOver()
  {

    final Map<String, Object> contextWithRowSignature =
        ImmutableMap.<String, Object>builder()
                    .putAll(context)
                    .put(DruidQuery.CTX_SCAN_SIGNATURE, "[{\"name\":\"m1\",\"type\":\"FLOAT\"}]")
                    .build();

    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cc", ColumnType.DOUBLE)
                                            .build();

    final WindowFrame theFrame = new WindowFrame(WindowFrame.PeerType.ROWS, true, 0, true, 0, null);
    final AggregatorFactory[] theAggs = {
        new DoubleSumAggregatorFactory("w0", "m1")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("m1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(contextWithRowSignature)
                .legacy(false)
                .build()),
        new LegacySegmentSpec(Intervals.ETERNITY),
        context,
        RowSignature.builder().add("m1", ColumnType.FLOAT).add("w0", ColumnType.DOUBLE).build(),
        ImmutableList.of(
            new NaivePartitioningOperatorFactory(ImmutableList.of()),
            new WindowOperatorFactory(proc)
        ),
        ImmutableList.of()
    );
    testSelectQuery()
        .setSql("select m1,SUM(m1) OVER() cc from foo")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("m1", "m1"),
                                           new ColumnMapping("w0", "cc")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(isDurableStorageDestination()
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1.0f, 21.0},
            new Object[]{2.0f, 21.0},
            new Object[]{3.0f, 21.0},
            new Object[]{4.0f, 21.0},
            new Object[]{5.0f, 21.0},
            new Object[]{6.0f, 21.0}
        ))
        .setQueryContext(context)
        .verifyResults();
  }

  @Test
  public void testWindowOnFooWithPartitionByOrderBYWithJoin()
  {
    final Map<String, Object> contextWithRowSignature =
        ImmutableMap.<String, Object>builder()
                    .putAll(context)
                    .put(
                        DruidQuery.CTX_SCAN_SIGNATURE,
                        "[{\"name\":\"j0.m2\",\"type\":\"DOUBLE\"},{\"name\":\"m1\",\"type\":\"FLOAT\"}]"
                    )
                    .build();

    final Map<String, Object> contextWithRowSignature1 =
        ImmutableMap.<String, Object>builder()
                    .putAll(context)
                    .put(
                        DruidQuery.CTX_SCAN_SIGNATURE,
                        "[{\"name\":\"m2\",\"type\":\"DOUBLE\"},{\"name\":\"v0\",\"type\":\"FLOAT\"}]"
                    )
                    .build();

    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cc", ColumnType.DOUBLE)
                                            .add("m2", ColumnType.DOUBLE)
                                            .build();

    final WindowFrame theFrame = new WindowFrame(
        WindowFrame.PeerType.RANGE,
        true,
        0,
        false,
        0,
        ImmutableList.of(new ColumnWithDirection(
            "m1",
            ColumnWithDirection.Direction.ASC
        ))
    );
    final AggregatorFactory[] theAggs = {
        new DoubleSumAggregatorFactory("w0", "m1")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .virtualColumns(expressionVirtualColumn("v0", "\"m2\"", ColumnType.FLOAT))
                                .columns("m2", "v0")
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(contextWithRowSignature1)
                                .legacy(false)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(
                            DruidExpression.ofColumn(ColumnType.FLOAT, "m1"),
                            DruidExpression.ofColumn(ColumnType.FLOAT, "j0.v0")
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("j0.m2", "m1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(contextWithRowSignature)
                .legacy(false)
                .build()),
        new LegacySegmentSpec(Intervals.ETERNITY),
        context,
        RowSignature.builder()
                    .add("m1", ColumnType.FLOAT)
                    .add("w0", ColumnType.DOUBLE)
                    .add("j0.m2", ColumnType.DOUBLE)
                    .build(),
        ImmutableList.of(
            new NaiveSortOperatorFactory(ImmutableList.of(ColumnWithDirection.ascending("m1"))),
            new NaivePartitioningOperatorFactory(ImmutableList.of("m1")),
            new WindowOperatorFactory(proc)
        ),
        ImmutableList.of()
    );
    testSelectQuery()
        .setSql(
            "select foo.m1,SUM(foo.m1) OVER(PARTITION BY foo.m1 ORDER BY foo.m1) cc, t.m2 from foo JOIN (select * from foo) as t ON foo.m1=t.m2")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("m1", "m1"),
                                           new ColumnMapping("w0", "cc"),
                                           new ColumnMapping("j0.m2", "m2")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(isDurableStorageDestination()
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1.0f, 1.0, 1.0},
            new Object[]{2.0f, 2.0, 2.0},
            new Object[]{3.0f, 3.0, 3.0},
            new Object[]{4.0f, 4.0, 4.0},
            new Object[]{5.0f, 5.0, 5.0},
            new Object[]{6.0f, 6.0, 6.0}
        ))
        .setQueryContext(context)
        .verifyResults();
  }

  @Test
  public void testWindowOnFooWithEmptyOverWithJoin()
  {
    final Map<String, Object> contextWithRowSignature =
        ImmutableMap.<String, Object>builder()
                    .putAll(context)
                    .put(
                        DruidQuery.CTX_SCAN_SIGNATURE,
                        "[{\"name\":\"j0.m2\",\"type\":\"DOUBLE\"},{\"name\":\"m1\",\"type\":\"FLOAT\"}]"
                    )
                    .build();

    final Map<String, Object> contextWithRowSignature1 =
        ImmutableMap.<String, Object>builder()
                    .putAll(context)
                    .put(
                        DruidQuery.CTX_SCAN_SIGNATURE,
                        "[{\"name\":\"m2\",\"type\":\"DOUBLE\"},{\"name\":\"v0\",\"type\":\"FLOAT\"}]"
                    )
                    .build();

    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cc", ColumnType.DOUBLE)
                                            .add("m2", ColumnType.DOUBLE)
                                            .build();

    final WindowFrame theFrame = new WindowFrame(
        WindowFrame.PeerType.ROWS,
        true,
        0,
        true,
        0,
        null
    );
    final AggregatorFactory[] theAggs = {
        new DoubleSumAggregatorFactory("w0", "m1")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .virtualColumns(expressionVirtualColumn("v0", "\"m2\"", ColumnType.FLOAT))
                                .columns("m2", "v0")
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(contextWithRowSignature1)
                                .legacy(false)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(
                            DruidExpression.ofColumn(ColumnType.FLOAT, "m1"),
                            DruidExpression.ofColumn(ColumnType.FLOAT, "j0.v0")
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("j0.m2", "m1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(contextWithRowSignature)
                .legacy(false)
                .build()),
        new LegacySegmentSpec(Intervals.ETERNITY),
        context,
        RowSignature.builder()
                    .add("m1", ColumnType.FLOAT)
                    .add("w0", ColumnType.DOUBLE)
                    .add("j0.m2", ColumnType.DOUBLE)
                    .build(),
        ImmutableList.of(
            new NaivePartitioningOperatorFactory(ImmutableList.of()),
            new WindowOperatorFactory(proc)
        ),
        ImmutableList.of()
    );
    testSelectQuery()
        .setSql(
            "select foo.m1,SUM(foo.m1) OVER() cc, t.m2 from foo JOIN (select * from foo) as t ON foo.m1=t.m2")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("m1", "m1"),
                                           new ColumnMapping("w0", "cc"),
                                           new ColumnMapping("j0.m2", "m2")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(isDurableStorageDestination()
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1.0f, 21.0, 1.0},
            new Object[]{2.0f, 21.0, 2.0},
            new Object[]{3.0f, 21.0, 3.0},
            new Object[]{4.0f, 21.0, 4.0},
            new Object[]{5.0f, 21.0, 5.0},
            new Object[]{6.0f, 21.0, 6.0}
        ))
        .setQueryContext(context)
        .verifyResults();
  }

  @Test
  public void testWindowOnFooWithEmptyOverWithUnnest()
  {
    final Map<String, Object> contextWithRowSignature =
        ImmutableMap.<String, Object>builder()
                    .putAll(context)
                    .put(
                        DruidQuery.CTX_SCAN_SIGNATURE,
                        "[{\"name\":\"j0.unnest\",\"type\":\"STRING\"},{\"name\":\"m1\",\"type\":\"FLOAT\"}]"
                    )
                    .build();


    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cc", ColumnType.DOUBLE)
                                            .add("d3", ColumnType.STRING)
                                            .build();

    final WindowFrame theFrame = new WindowFrame(
        WindowFrame.PeerType.ROWS,
        true,
        0,
        true,
        0,
        null
    );
    final AggregatorFactory[] theAggs = {
        new DoubleSumAggregatorFactory("w0", "m1")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(
            newScanQueryBuilder()
                .dataSource(
                    UnnestDataSource.create(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                        null
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("j0.unnest", "m1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(contextWithRowSignature)
                .legacy(false)
                .build()),
        new LegacySegmentSpec(Intervals.ETERNITY),
        context,
        RowSignature.builder()
                    .add("m1", ColumnType.FLOAT)
                    .add("w0", ColumnType.DOUBLE)
                    .add("j0.unnest", ColumnType.STRING)
                    .build(),
        ImmutableList.of(
            new NaivePartitioningOperatorFactory(ImmutableList.of()),
            new WindowOperatorFactory(proc)
        ),
        ImmutableList.of()
    );
    testSelectQuery()
        .setSql(
            "select m1,SUM(m1) OVER() cc, u.d3 from foo CROSS JOIN UNNEST(MV_TO_ARRAY(dim3)) as u(d3)")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("m1", "m1"),
                                           new ColumnMapping("w0", "cc"),
                                           new ColumnMapping("j0.unnest", "d3")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(isDurableStorageDestination()
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1.0f, 24.0, "a"},
            new Object[]{1.0f, 24.0, "b"},
            new Object[]{2.0f, 24.0, "b"},
            new Object[]{2.0f, 24.0, "c"},
            new Object[]{3.0f, 24.0, "d"},
            new Object[]{4.0f, 24.0, ""},
            new Object[]{5.0f, 24.0, NullHandling.sqlCompatible() ? null : ""},
            new Object[]{6.0f, 24.0, NullHandling.sqlCompatible() ? null : ""}
        ))
        .setQueryContext(context)
        .verifyResults();
  }

  @Test
  public void testWindowOnFooWithPartitionByAndWithUnnest()
  {
    final Map<String, Object> contextWithRowSignature =
        ImmutableMap.<String, Object>builder()
                    .putAll(context)
                    .put(
                        DruidQuery.CTX_SCAN_SIGNATURE,
                        "[{\"name\":\"j0.unnest\",\"type\":\"STRING\"},{\"name\":\"m1\",\"type\":\"FLOAT\"}]"
                    )
                    .build();


    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cc", ColumnType.DOUBLE)
                                            .add("d3", ColumnType.STRING)
                                            .build();

    final WindowFrame theFrame = new WindowFrame(
        WindowFrame.PeerType.ROWS,
        true,
        0,
        true,
        0,
        null
    );
    final AggregatorFactory[] theAggs = {
        new DoubleSumAggregatorFactory("w0", "m1")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(
            newScanQueryBuilder()
                .dataSource(
                    UnnestDataSource.create(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                        null
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("j0.unnest", "m1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(contextWithRowSignature)
                .legacy(false)
                .build()),
        new LegacySegmentSpec(Intervals.ETERNITY),
        context,
        RowSignature.builder()
                    .add("m1", ColumnType.FLOAT)
                    .add("w0", ColumnType.DOUBLE)
                    .add("j0.unnest", ColumnType.STRING)
                    .build(),
        ImmutableList.of(
            new NaiveSortOperatorFactory(ImmutableList.of(ColumnWithDirection.ascending("m1"))),
            new NaivePartitioningOperatorFactory(ImmutableList.of("m1")),
            new WindowOperatorFactory(proc)
        ),
        ImmutableList.of()
    );
    testSelectQuery()
        .setSql(
            "select m1,SUM(m1) OVER(PARTITION BY m1) cc, u.d3 from foo CROSS JOIN UNNEST(MV_TO_ARRAY(dim3)) as u(d3)")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("m1", "m1"),
                                           new ColumnMapping("w0", "cc"),
                                           new ColumnMapping("j0.unnest", "d3")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(isDurableStorageDestination()
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1.0f, 2.0, "a"},
            new Object[]{1.0f, 2.0, "b"},
            new Object[]{2.0f, 4.0, "b"},
            new Object[]{2.0f, 4.0, "c"},
            new Object[]{3.0f, 3.0, "d"},
            new Object[]{4.0f, 4.0, ""},
            new Object[]{5.0f, 5.0, NullHandling.sqlCompatible() ? null : ""},
            new Object[]{6.0f, 6.0, NullHandling.sqlCompatible() ? null : ""}
        ))
        .setQueryContext(context)
        .verifyResults();
  }

  public boolean isDurableStorageDestination()
  {
    return QUERY_RESULTS_WITH_DURABLE_STORAGE.equals(contextName) || QUERY_RESULTS_WITH_DEFAULT_CONTEXT.equals(context);
  }
}
