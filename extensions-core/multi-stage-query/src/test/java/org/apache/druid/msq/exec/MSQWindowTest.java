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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.error.TooManyRowsInAWindowFault;
import org.apache.druid.msq.test.CounterSnapshotMatcher;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
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
import org.apache.druid.timeline.SegmentId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;


public class MSQWindowTest extends MSQTestBase
{
  public static Collection<Object[]> data()
  {
    Object[][] data = new Object[][]{
        {DEFAULT, DEFAULT_MSQ_CONTEXT}
    };

    return Arrays.asList(data);
  }


  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testWindowOnFooWithPartitionByAndInnerGroupBy(String contextName, Map<String, Object> context)
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
            new NaiveSortOperatorFactory(ImmutableList.of(ColumnWithDirection.ascending("d0"))),
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testWindowOnFooWithFirstWindowPartitionNextWindowEmpty(String contextName, Map<String, Object> context)
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
            new NaiveSortOperatorFactory(ImmutableList.of(ColumnWithDirection.ascending("d0"))),
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testWindowOnFooWith2WindowsBothWindowsHavingPartitionByInnerGroupBy(
      String contextName,
      Map<String, Object> context
  )
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
            new NaiveSortOperatorFactory(ImmutableList.of(ColumnWithDirection.ascending("d0"))),
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testWindowOnFooWith2WindowsBothPartitionByWithOrderReversed(
      String contextName,
      Map<String, Object> context
  )
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testWindowOnFooWithEmptyOverWithGroupBy(String contextName, Map<String, Object> context)
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testWindowOnFooWithNoGroupByAndPartition(String contextName, Map<String, Object> context)
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testWindowOnFooWithNoGroupByAndPartitionOnTwoElements(String contextName, Map<String, Object> context)
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testWindowOnFooWithNoGroupByAndPartitionByAnother(String contextName, Map<String, Object> context)
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testWindowOnFooWithGroupByAndInnerLimit(String contextName, Map<String, Object> context)
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cc", ColumnType.DOUBLE)
                                            .build();

    final WindowFrame theFrame = new WindowFrame(WindowFrame.PeerType.ROWS, true, 0, true, 0, null);
    final AggregatorFactory[] theAggs = {
        new DoubleSumAggregatorFactory("w0", "d1")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);


    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(
            GroupByQuery.builder()
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
                        .setLimit(5)
                        .setContext(context)
                        .build()),
        new LegacySegmentSpec(Intervals.ETERNITY),
        context,
        RowSignature.builder().add("d0", ColumnType.FLOAT).add("w0", ColumnType.DOUBLE).build(),
        ImmutableList.of(
            new NaivePartitioningOperatorFactory(ImmutableList.of()),
            new WindowOperatorFactory(proc)
        ),
        ImmutableList.of()
    );
    testSelectQuery()
        .setSql("with t AS (\n"
                + "select m1, m2 from foo GROUP BY 1,2 \n"
                + "LIMIT 5\n"
                + ")\n"
                + "select m1,SUM(m2) OVER() cc from t\n"
                + "GROUP BY m1,m2")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("d0", "m1"),
                                           new ColumnMapping("w0", "cc")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1.0f, 15.0},
            new Object[]{2.0f, 15.0},
            new Object[]{3.0f, 15.0},
            new Object[]{4.0f, 15.0},
            new Object[]{5.0f, 15.0}
        ))
        .setQueryContext(context)
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testWindowOnFooWithNoGroupByAndPartitionAndVirtualColumns(String contextName, Map<String, Object> context)
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


  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testWindowOnFooWithNoGroupByAndEmptyOver(String contextName, Map<String, Object> context)
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testWindowOnFooWithPartitionByOrderBYWithJoin(String contextName, Map<String, Object> context)
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testWindowOnFooWithEmptyOverWithJoin(String contextName, Map<String, Object> context)
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testWindowOnFooWithDim2(String contextName, Map<String, Object> context)
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("dim2", ColumnType.STRING)
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
                        "[{\"name\":\"dim2\",\"type\":\"STRING\"},{\"name\":\"m1\",\"type\":\"FLOAT\"}]"
                    )
                    .build();

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim2", "m1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(contextWithRowSignature)
                .legacy(false)
                .build()),
        new LegacySegmentSpec(Intervals.ETERNITY),
        context,
        RowSignature.builder().add("dim2", ColumnType.STRING).add("w0", ColumnType.DOUBLE).build(),
        ImmutableList.of(
            new NaiveSortOperatorFactory(ImmutableList.of(ColumnWithDirection.ascending("dim2"))),
            new NaivePartitioningOperatorFactory(ImmutableList.of("dim2")),
            new WindowOperatorFactory(proc)
        ),
        ImmutableList.of()
    );
    testSelectQuery()
        .setSql("select dim2, SUM(m1) OVER (PARTITION BY dim2) cc from foo")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("dim2", "dim2"),
                                           new ColumnMapping("w0", "cc")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            NullHandling.replaceWithDefault() ?
            ImmutableList.of(
                new Object[]{"", 11.0},
                new Object[]{"", 11.0},
                new Object[]{"", 11.0},
                new Object[]{"a", 5.0},
                new Object[]{"a", 5.0},
                new Object[]{"abc", 5.0}
            ) :
            ImmutableList.of(
                new Object[]{null, 8.0},
                new Object[]{null, 8.0},
                new Object[]{"", 3.0},
                new Object[]{"a", 5.0},
                new Object[]{"a", 5.0},
                new Object[]{"abc", 5.0}
            ))
        .setQueryContext(context)
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testWindowOnFooWithEmptyOverWithUnnest(String contextName, Map<String, Object> context)
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testWindowOnFooWithPartitionByAndWithUnnest(String contextName, Map<String, Object> context)
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

  // Insert Tests
  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testInsertWithWindow(String contextName, Map<String, Object> context)
  {
    List<Object[]> expectedRows = ImmutableList.of(
        new Object[]{946684800000L, 1.0f, 1.0},
        new Object[]{946771200000L, 2.0f, 2.0},
        new Object[]{946857600000L, 3.0f, 3.0},
        new Object[]{978307200000L, 4.0f, 4.0},
        new Object[]{978393600000L, 5.0f, 5.0},
        new Object[]{978480000000L, 6.0f, 6.0}
    );

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .add("summ1", ColumnType.DOUBLE)
                                            .build();


    testIngestQuery().setSql(
                         "insert into foo1 SELECT __time, m1,\n"
                         + "SUM(m1) OVER(PARTITION BY m1) as summ1\n"
                         + "from foo\n"
                         + "GROUP BY __time, m1 PARTITIONED BY ALL")
                     .setQueryContext(context)
                     .setExpectedResultRows(expectedRows)
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .verifyResults();

  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testInsertWithWindowEmptyOver(String contextName, Map<String, Object> context)
  {
    List<Object[]> expectedRows = ImmutableList.of(
        new Object[]{946684800000L, 1.0f, 21.0},
        new Object[]{946771200000L, 2.0f, 21.0},
        new Object[]{946857600000L, 3.0f, 21.0},
        new Object[]{978307200000L, 4.0f, 21.0},
        new Object[]{978393600000L, 5.0f, 21.0},
        new Object[]{978480000000L, 6.0f, 21.0}
    );

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .add("summ1", ColumnType.DOUBLE)
                                            .build();


    testIngestQuery().setSql(
                         "insert into foo1 SELECT __time, m1,\n"
                         + "SUM(m1) OVER() as summ1\n"
                         + "from foo\n"
                         + "GROUP BY __time, m1 PARTITIONED BY ALL")
                     .setQueryContext(context)
                     .setExpectedResultRows(expectedRows)
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .verifyResults();

  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testInsertWithWindowPartitionByOrderBy(String contextName, Map<String, Object> context)
  {
    List<Object[]> expectedRows = ImmutableList.of(
        new Object[]{946684800000L, 1.0f, 1.0},
        new Object[]{946771200000L, 2.0f, 2.0},
        new Object[]{946857600000L, 3.0f, 3.0},
        new Object[]{978307200000L, 4.0f, 4.0},
        new Object[]{978393600000L, 5.0f, 5.0},
        new Object[]{978480000000L, 6.0f, 6.0}
    );

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .add("summ1", ColumnType.DOUBLE)
                                            .build();


    testIngestQuery().setSql(
                         "insert into foo1 SELECT __time, m1,\n"
                         + "SUM(m1) OVER(PARTITION BY m1 ORDER BY m1 ASC) as summ1\n"
                         + "from foo\n"
                         + "GROUP BY __time, m1 PARTITIONED BY ALL")
                     .setQueryContext(context)
                     .setExpectedResultRows(expectedRows)
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .verifyResults();

  }


  // Replace Tests
  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testReplaceWithWindowsAndUnnest(String contextName, Map<String, Object> context)
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cc", ColumnType.DOUBLE)
                                            .add("d3", ColumnType.STRING)
                                            .build();

    testIngestQuery().setSql(" REPLACE INTO foo1 OVERWRITE ALL\n"
                             + "select __time,m1,SUM(m1) OVER(PARTITION BY m1) cc, u.d3 from foo CROSS JOIN UNNEST(MV_TO_ARRAY(dim3)) as u(d3)\n"
                             + "PARTITIONED BY ALL CLUSTERED BY m1")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(Intervals.ONLY_ETERNITY)
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{946684800000L, 1.0f, 2.0, "a"},
                             new Object[]{946684800000L, 1.0f, 2.0, "b"},
                             new Object[]{946771200000L, 2.0f, 4.0, "b"},
                             new Object[]{946771200000L, 2.0f, 4.0, "c"},
                             new Object[]{946857600000L, 3.0f, 3.0, "d"},
                             new Object[]{978307200000L, 4.0f, 4.0, NullHandling.sqlCompatible() ? "" : null},
                             new Object[]{978393600000L, 5.0f, 5.0, null},
                             new Object[]{978480000000L, 6.0f, 6.0, null}
                         )
                     )
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo1", Intervals.ETERNITY, "test", 0)))
                     .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSimpleWindowWithPartitionBy(String contextName, Map<String, Object> context)
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cc", ColumnType.DOUBLE)
                                            .build();

    testIngestQuery().setSql(" REPLACE INTO foo OVERWRITE ALL\n"
                             + "select __time, m1,SUM(m1) OVER(PARTITION BY m1) cc from foo group by __time, m1\n"
                             + "PARTITIONED BY ALL CLUSTERED BY m1")
                     .setExpectedDataSource("foo")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(Intervals.ONLY_ETERNITY)
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{946684800000L, 1.0f, 1.0},
                             new Object[]{946771200000L, 2.0f, 2.0},
                             new Object[]{946857600000L, 3.0f, 3.0},
                             new Object[]{978307200000L, 4.0f, 4.0},
                             new Object[]{978393600000L, 5.0f, 5.0},
                             new Object[]{978480000000L, 6.0f, 6.0}
                         )
                     )
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo", Intervals.ETERNITY, "test", 0)))
                     .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSimpleWindowWithEmptyOver(String contextName, Map<String, Object> context)
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cc", ColumnType.DOUBLE)
                                            .build();

    testIngestQuery().setSql(" REPLACE INTO foo OVERWRITE ALL\n"
                             + "select __time, m1,SUM(m1) OVER() cc from foo group by __time, m1\n"
                             + "PARTITIONED BY ALL CLUSTERED BY m1")
                     .setExpectedDataSource("foo")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(Intervals.ONLY_ETERNITY)
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{946684800000L, 1.0f, 21.0},
                             new Object[]{946771200000L, 2.0f, 21.0},
                             new Object[]{946857600000L, 3.0f, 21.0},
                             new Object[]{978307200000L, 4.0f, 21.0},
                             new Object[]{978393600000L, 5.0f, 21.0},
                             new Object[]{978480000000L, 6.0f, 21.0}
                         )
                     )
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo", Intervals.ETERNITY, "test", 0)))
                     .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSimpleWindowWithEmptyOverNoGroupBy(String contextName, Map<String, Object> context)
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cc", ColumnType.DOUBLE)
                                            .build();

    testIngestQuery().setSql(" REPLACE INTO foo OVERWRITE ALL\n"
                             + "select __time, m1,SUM(m1) OVER() cc from foo\n"
                             + "PARTITIONED BY ALL CLUSTERED BY m1")
                     .setExpectedDataSource("foo")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(Intervals.ONLY_ETERNITY)
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{946684800000L, 1.0f, 21.0},
                             new Object[]{946771200000L, 2.0f, 21.0},
                             new Object[]{946857600000L, 3.0f, 21.0},
                             new Object[]{978307200000L, 4.0f, 21.0},
                             new Object[]{978393600000L, 5.0f, 21.0},
                             new Object[]{978480000000L, 6.0f, 21.0}
                         )
                     )
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo", Intervals.ETERNITY, "test", 0)))
                     .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSimpleWindowWithDuplicateSelectNode(String contextName, Map<String, Object> context)
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cc", ColumnType.DOUBLE)
                                            .add("cc_dup", ColumnType.DOUBLE)
                                            .build();

    testIngestQuery().setSql(" REPLACE INTO foo OVERWRITE ALL\n"
                             + "select __time, m1,SUM(m1) OVER() cc,SUM(m1) OVER() cc_dup from foo\n"
                             + "PARTITIONED BY ALL CLUSTERED BY m1")
                     .setExpectedDataSource("foo")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(Intervals.ONLY_ETERNITY)
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{946684800000L, 1.0f, 21.0, 21.0},
                             new Object[]{946771200000L, 2.0f, 21.0, 21.0},
                             new Object[]{946857600000L, 3.0f, 21.0, 21.0},
                             new Object[]{978307200000L, 4.0f, 21.0, 21.0},
                             new Object[]{978393600000L, 5.0f, 21.0, 21.0},
                             new Object[]{978480000000L, 6.0f, 21.0, 21.0}
                         )
                     )
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo", Intervals.ETERNITY, "test", 0)))
                     .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSimpleWindowWithJoins(String contextName, Map<String, Object> context)
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cc", ColumnType.DOUBLE)
                                            .add("m2", ColumnType.DOUBLE)
                                            .build();

    testIngestQuery().setSql(" REPLACE INTO foo1 OVERWRITE ALL\n"
                             + "select foo.__time,foo.m1,SUM(foo.m1) OVER(PARTITION BY foo.m1 ORDER BY foo.m1) cc, t.m2 from foo JOIN (select * from foo) as t ON foo.m1=t.m2\n"
                             + "PARTITIONED BY DAY CLUSTERED BY m1")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(Intervals.ONLY_ETERNITY)
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{946684800000L, 1.0f, 1.0, 1.0},
                             new Object[]{946771200000L, 2.0f, 2.0, 2.0},
                             new Object[]{946857600000L, 3.0f, 3.0, 3.0},
                             new Object[]{978307200000L, 4.0f, 4.0, 4.0},
                             new Object[]{978393600000L, 5.0f, 5.0, 5.0},
                             new Object[]{978480000000L, 6.0f, 6.0, 6.0}
                         )
                     )
                     .setExpectedSegment(
                         ImmutableSet.of(
                             SegmentId.of("foo1", Intervals.of("2000-01-01T/P1D"), "test", 0),
                             SegmentId.of("foo1", Intervals.of("2000-01-02T/P1D"), "test", 0),
                             SegmentId.of("foo1", Intervals.of("2000-01-03T/P1D"), "test", 0),
                             SegmentId.of("foo1", Intervals.of("2001-01-01T/P1D"), "test", 0),
                             SegmentId.of("foo1", Intervals.of("2001-01-02T/P1D"), "test", 0),
                             SegmentId.of("foo1", Intervals.of("2001-01-03T/P1D"), "test", 0)
                         )
                     )
                     .verifyResults();
  }

  // Bigger dataset tests
  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectWithWikipedia(String contextName, Map<String, Object> context)
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("cityName", ColumnType.STRING)
                                            .add("added", ColumnType.LONG)
                                            .add("cc", ColumnType.LONG)
                                            .build();

    final WindowFrame theFrame = new WindowFrame(WindowFrame.PeerType.ROWS, true, 0, true, 0, null);
    final AggregatorFactory[] theAggs = {
        new LongSumAggregatorFactory("w0", "added")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);

    final Map<String, Object> contextWithRowSignature =
        ImmutableMap.<String, Object>builder()
                    .putAll(context)
                    .put(
                        DruidQuery.CTX_SCAN_SIGNATURE,
                        "[{\"name\":\"added\",\"type\":\"LONG\"},{\"name\":\"cityName\",\"type\":\"STRING\"}]"
                    )
                    .build();

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(
            newScanQueryBuilder()
                .dataSource(CalciteTests.WIKIPEDIA)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(in("cityName", ImmutableList.of("Ahmedabad", "Albuquerque")))
                .columns("added", "cityName")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(contextWithRowSignature)
                .legacy(false)
                .build()),
        new LegacySegmentSpec(Intervals.ETERNITY),
        context,
        RowSignature.builder().add("cityName", ColumnType.STRING)
                    .add("added", ColumnType.LONG)
                    .add("w0", ColumnType.LONG).build(),
        ImmutableList.of(
            new NaiveSortOperatorFactory(ImmutableList.of(ColumnWithDirection.ascending("cityName"))),
            new NaivePartitioningOperatorFactory(ImmutableList.of("cityName")),
            new WindowOperatorFactory(proc)
        ),
        ImmutableList.of()
    );
    testSelectQuery()
        .setSql(
            "select cityName, added, SUM(added) OVER (PARTITION BY cityName) cc from wikipedia where cityName IN ('Ahmedabad', 'Albuquerque')")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("cityName", "cityName"),
                                           new ColumnMapping("added", "added"),
                                           new ColumnMapping("w0", "cc")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{"Ahmedabad", 0L, 0L},
            new Object[]{"Ahmedabad", 0L, 0L},
            new Object[]{"Albuquerque", 129L, 140L},
            new Object[]{"Albuquerque", 9L, 140L},
            new Object[]{"Albuquerque", 2L, 140L}
        ))
        .setQueryContext(context)
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectWithWikipediaEmptyOverWithCustomContext(String contextName, Map<String, Object> context)
  {
    final Map<String, Object> customContext =
        ImmutableMap.<String, Object>builder()
                    .putAll(context)
                    .put(MultiStageQueryContext.MAX_ROWS_MATERIALIZED_IN_WINDOW, 200)
                    .build();

    testSelectQuery()
        .setSql(
            "select cityName, added, SUM(added) OVER () cc from wikipedia")
        .setQueryContext(customContext)
        .setExpectedMSQFault(new TooManyRowsInAWindowFault(15676, 200))
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectWithWikipediaWithPartitionKeyNotInSelect(String contextName, Map<String, Object> context)
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("cityName", ColumnType.STRING)
                                            .add("added", ColumnType.LONG)
                                            .add("cc", ColumnType.LONG)
                                            .build();

    final WindowFrame theFrame = new WindowFrame(WindowFrame.PeerType.ROWS, true, 0, true, 0, null);
    final AggregatorFactory[] theAggs = {
        new LongSumAggregatorFactory("w0", "added")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);

    final Map<String, Object> innerContextWithRowSignature =
        ImmutableMap.<String, Object>builder()
                    .putAll(context)
                    .put(
                        DruidQuery.CTX_SCAN_SIGNATURE,
                        "[{\"name\":\"added\",\"type\":\"LONG\"},{\"name\":\"cityName\",\"type\":\"STRING\"},{\"name\":\"countryIsoCode\",\"type\":\"STRING\"}]"
                    )
                    .build();

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(
            newScanQueryBuilder()
                .dataSource(CalciteTests.WIKIPEDIA)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(notNull("cityName"))
                .columns("added", "cityName", "countryIsoCode")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(innerContextWithRowSignature)
                .legacy(false)
                .build()),
        new LegacySegmentSpec(Intervals.ETERNITY),
        context,
        RowSignature.builder().add("cityName", ColumnType.STRING)
                    .add("added", ColumnType.LONG)
                    .add("w0", ColumnType.LONG).build(),
        ImmutableList.of(
            new NaiveSortOperatorFactory(ImmutableList.of(ColumnWithDirection.ascending("countryIsoCode"))),
            new NaivePartitioningOperatorFactory(ImmutableList.of("countryIsoCode")),
            new WindowOperatorFactory(proc)
        ),
        ImmutableList.of()
    );

    final Map<String, Object> outerContextWithRowSignature =
        ImmutableMap.<String, Object>builder()
                    .putAll(context)
                    .put(
                        DruidQuery.CTX_SCAN_SIGNATURE,
                        "[{\"name\":\"added\",\"type\":\"LONG\"},{\"name\":\"cityName\",\"type\":\"STRING\"},{\"name\":\"w0\",\"type\":\"LONG\"}]"
                    )
                    .build();
    final Query scanQuery = Druids.newScanQueryBuilder()
                                  .dataSource(new QueryDataSource(query))
                                  .intervals(querySegmentSpec(Filtration.eternity()))
                                  .columns("added", "cityName", "w0")
                                  .limit(5)
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .context(outerContextWithRowSignature)
                                  .legacy(false)
                                  .build();

    testSelectQuery()
        .setSql(
            "select cityName, added, SUM(added) OVER (PARTITION BY countryIsoCode) cc from wikipedia \n"
            + "where cityName is NOT NULL\n"
            + "LIMIT 5")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(scanQuery)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("cityName", "cityName"),
                                           new ColumnMapping("added", "added"),
                                           new ColumnMapping("w0", "cc")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{"Al Ain", 8L, 6334L},
            new Object[]{"Dubai", 3L, 6334L},
            new Object[]{"Dubai", 6323L, 6334L},
            new Object[]{"Tirana", 26L, 26L},
            new Object[]{"Benguela", 0L, 0L}
        ))
        .setQueryContext(context)
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testGroupByWithWikipedia(String contextName, Map<String, Object> context)
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("cityName", ColumnType.STRING)
                                            .add("added", ColumnType.LONG)
                                            .add("cc", ColumnType.LONG)
                                            .build();

    final Query groupByQuery = GroupByQuery.builder()
                                           .setDataSource(CalciteTests.WIKIPEDIA)
                                           .setInterval(querySegmentSpec(Filtration.eternity()))
                                           .setDimFilter(in("cityName", ImmutableList.of("Ahmedabad", "Albuquerque")))
                                           .setGranularity(Granularities.ALL)
                                           .setDimensions(dimensions(
                                               new DefaultDimensionSpec(
                                                   "cityName",
                                                   "d0",
                                                   ColumnType.STRING
                                               ),
                                               new DefaultDimensionSpec(
                                                   "added",
                                                   "d1",
                                                   ColumnType.LONG
                                               )
                                           ))
                                           .setContext(context)
                                           .build();


    final WindowFrame theFrame = new WindowFrame(WindowFrame.PeerType.ROWS, true, 0, true, 0, null);
    final AggregatorFactory[] theAggs = {
        new LongSumAggregatorFactory("w0", "d1")
    };
    WindowFramedAggregateProcessor proc = new WindowFramedAggregateProcessor(theFrame, theAggs);

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(groupByQuery),
        new LegacySegmentSpec(Intervals.ETERNITY),
        context,
        RowSignature.builder().add("d0", ColumnType.STRING)
                    .add("d1", ColumnType.LONG)
                    .add("w0", ColumnType.LONG).build(),
        ImmutableList.of(
            new NaiveSortOperatorFactory(ImmutableList.of(ColumnWithDirection.ascending("d0"))),
            new NaivePartitioningOperatorFactory(ImmutableList.of("d0")),
            new WindowOperatorFactory(proc)
        ),
        ImmutableList.of()
    );
    testSelectQuery()
        .setSql(
            "select cityName, added, SUM(added) OVER (PARTITION BY cityName) cc from wikipedia \n"
            + "where cityName IN ('Ahmedabad', 'Albuquerque')\n"
            + "GROUP BY cityName,added")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("d0", "cityName"),
                                           new ColumnMapping("d1", "added"),
                                           new ColumnMapping("w0", "cc")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{"Ahmedabad", 0L, 0L},
            new Object[]{"Albuquerque", 2L, 140L},
            new Object[]{"Albuquerque", 9L, 140L},
            new Object[]{"Albuquerque", 129L, 140L}
        ))
        .setQueryContext(context)
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testReplaceGroupByOnWikipedia(String contextName, Map<String, Object> context)
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("added", ColumnType.LONG)
                                            .add("cityName", ColumnType.STRING)
                                            .add("cc", ColumnType.LONG)
                                            .build();

    testIngestQuery().setSql(" REPLACE INTO foo1 OVERWRITE ALL\n"
                             + "select cityName, added, SUM(added) OVER (PARTITION BY cityName) cc from wikipedia \n"
                             + "where cityName IN ('Ahmedabad', 'Albuquerque')\n"
                             + "GROUP BY cityName,added\n"
                             + "PARTITIONED BY ALL CLUSTERED BY added")
                     .setExpectedDataSource("foo1")
                     .setExpectedRowSignature(rowSignature)
                     .setQueryContext(context)
                     .setExpectedDestinationIntervals(Intervals.ONLY_ETERNITY)
                     .setExpectedResultRows(
                         ImmutableList.of(
                             new Object[]{0L, 0L, "Ahmedabad", 0L},
                             new Object[]{0L, 2L, "Albuquerque", 140L},
                             new Object[]{0L, 9L, "Albuquerque", 140L},
                             new Object[]{0L, 129L, "Albuquerque", 140L}
                         )
                     )
                     .setExpectedSegment(ImmutableSet.of(SegmentId.of("foo1", Intervals.ETERNITY, "test", 0)))
                     .verifyResults();
  }
}
