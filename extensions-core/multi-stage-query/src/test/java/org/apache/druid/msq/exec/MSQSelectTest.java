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
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.frame.util.DurableStorageUtils;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.error.CannotParseExternalDataFault;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.test.CounterSnapshotMatcher;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.test.MSQTestFileUtils;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.planner.JoinAlgorithm;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.UnsupportedSQLQueryException;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class MSQSelectTest extends MSQTestBase
{

  @Parameterized.Parameters(name = "{index}:with context {0}")
  public static Collection<Object[]> data()
  {
    Object[][] data = new Object[][]{
        {DEFAULT, DEFAULT_MSQ_CONTEXT},
        {DURABLE_STORAGE, DURABLE_STORAGE_MSQ_CONTEXT},
        {FAULT_TOLERANCE, FAULT_TOLERANCE_MSQ_CONTEXT},
        {SEQUENTIAL_MERGE, SEQUENTIAL_MERGE_MSQ_CONTEXT}
    };
    return Arrays.asList(data);
  }

  @Parameterized.Parameter(0)
  public String contextName;

  @Parameterized.Parameter(1)
  public Map<String, Object> context;

  @Test
  public void testCalculator()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("EXPR$0", ColumnType.LONG)
                                               .build();

    testSelectQuery()
        .setSql("select 1 + 1")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(
                               InlineDataSource.fromIterable(
                                   ImmutableList.of(new Object[]{2L}),
                                   resultSignature
                               )
                           )
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .columns("EXPR$0")
                           .context(defaultScanQueryContext(context, resultSignature))
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setExpectedRowSignature(resultSignature)
        .setQueryContext(context)
        .setExpectedResultRows(ImmutableList.of(new Object[]{2})).verifyResults();
  }

  @Test
  public void testSelectOnFoo()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();

    testSelectQuery()
        .setSql("select cnt,dim1 from foo")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE1)
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .columns("cnt", "dim1")
                           .context(defaultScanQueryContext(context, resultSignature))
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setQueryContext(context)
        .setExpectedRowSignature(resultSignature)
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
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1L, ""},
            new Object[]{1L, "10.1"},
            new Object[]{1L, "2"},
            new Object[]{1L, "1"},
            new Object[]{1L, "def"},
            new Object[]{1L, "abc"}
        )).verifyResults();
  }

  @Test
  public void testSelectOnFoo2()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("m1", ColumnType.LONG)
                                               .add("dim2", ColumnType.STRING)
                                               .build();

    testSelectQuery()
        .setSql("select m1,dim2 from foo2")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(newScanQueryBuilder()
                              .dataSource(CalciteTests.DATASOURCE2)
                              .intervals(querySegmentSpec(Filtration.eternity()))
                              .columns("dim2", "m1")
                              .context(defaultScanQueryContext(
                                  context,
                                  RowSignature.builder()
                                              .add("dim2", ColumnType.STRING)
                                              .add("m1", ColumnType.LONG)
                                              .build()
                              ))
                              .build())
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setExpectedRowSignature(resultSignature)
        .setQueryContext(context)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1L, "en"},
            new Object[]{1L, "ru"},
            new Object[]{1L, "he"}
        ))
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().totalFiles(1),
            0, 0, "input0"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(3).frames(1),
            0, 0, "output"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(3).frames(1),
            0, 0, "shuffle"
        )
        .verifyResults();
  }

  @Test
  public void testSelectOnFooDuplicateColumnNames()
  {
    // Duplicate column names are OK in SELECT statements.

    final RowSignature expectedScanSignature =
        RowSignature.builder()
                    .add("cnt", ColumnType.LONG)
                    .add("dim1", ColumnType.STRING)
                    .build();

    final ColumnMappings expectedColumnMappings = new ColumnMappings(
        ImmutableList.of(
            new ColumnMapping("cnt", "x"),
            new ColumnMapping("dim1", "x")
        )
    );

    final List<MSQResultsReport.ColumnAndType> expectedOutputSignature =
        ImmutableList.of(
            new MSQResultsReport.ColumnAndType("x", ColumnType.LONG),
            new MSQResultsReport.ColumnAndType("x", ColumnType.STRING)
        );

    testSelectQuery()
        .setSql("select cnt AS x, dim1 AS x from foo")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE1)
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .columns("cnt", "dim1")
                           .context(defaultScanQueryContext(context, expectedScanSignature))
                           .build()
                   )
                   .columnMappings(expectedColumnMappings)
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setQueryContext(context)
        .setExpectedRowSignature(expectedOutputSignature)
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
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1L, ""},
            new Object[]{1L, "10.1"},
            new Object[]{1L, "2"},
            new Object[]{1L, "1"},
            new Object[]{1L, "def"},
            new Object[]{1L, "abc"}
        )).verifyResults();
  }

  @Test
  public void testSelectOnFooWhereMatchesNoSegments()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();

    // Filter [__time >= timestamp '3000-01-01 00:00:00'] matches no segments at all.
    testSelectQuery()
        .setSql("select cnt,dim1 from foo where __time >= timestamp '3000-01-01 00:00:00'")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE1)
                           .intervals(
                               querySegmentSpec(
                                   Intervals.utc(
                                       DateTimes.of("3000").getMillis(),
                                       Intervals.ETERNITY.getEndMillis()
                                   )
                               )
                           )
                           .columns("cnt", "dim1")
                           .context(defaultScanQueryContext(context, resultSignature))
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setQueryContext(context)
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of())
        .verifyResults();
  }

  @Test
  public void testGroupByOnFoo()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("cnt", ColumnType.LONG)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    testSelectQuery()
        .setSql("select cnt,count(*) as cnt1 from foo group by cnt")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(GroupByQuery.builder()
                                                      .setDataSource(CalciteTests.DATASOURCE1)
                                                      .setInterval(querySegmentSpec(Filtration
                                                                                        .eternity()))
                                                      .setGranularity(Granularities.ALL)
                                                      .setDimensions(dimensions(
                                                          new DefaultDimensionSpec(
                                                              "cnt",
                                                              "d0",
                                                              ColumnType.LONG
                                                          )
                                                      ))
                                                      .setAggregatorSpecs(aggregators(new CountAggregatorFactory(
                                                          "a0")))
                                                      .setContext(context)
                                                      .build())
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("d0", "cnt"),
                                           new ColumnMapping("a0", "cnt1")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(new Object[]{1L, 6L}))
        .setQueryContext(context)
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().totalFiles(1),
            0, 0, "input0"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(1).frames(1),
            0, 0, "output"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(1).frames(1),
            0, 0, "shuffle"
        )
        .verifyResults();
  }

  @Test
  public void testGroupByOrderByDimension()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("cnt", ColumnType.LONG)
                                            .build();

    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setDimensions(dimensions(new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT)))
                    .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                    .setLimitSpec(
                        new DefaultLimitSpec(
                            ImmutableList.of(
                                new OrderByColumnSpec(
                                    "d0",
                                    OrderByColumnSpec.Direction.DESCENDING,
                                    StringComparators.NUMERIC
                                )
                            ),
                            null
                        )
                    )
                    .setContext(context)
                    .build();

    testSelectQuery()
        .setSql("select m1, count(*) as cnt from foo group by m1 order by m1 desc")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(query)
                   .columnMappings(
                       new ColumnMappings(ImmutableList.of(
                           new ColumnMapping("d0", "m1"),
                           new ColumnMapping("a0", "cnt")
                       )
                       ))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build())
        .setExpectedRowSignature(rowSignature)
        .setQueryContext(context)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{6f, 1L},
                new Object[]{5f, 1L},
                new Object[]{4f, 1L},
                new Object[]{3f, 1L},
                new Object[]{2f, 1L},
                new Object[]{1f, 1L}
            )
        )
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
  public void testSelectWithLimit()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();

    testSelectQuery()
        .setSql("select cnt,dim1 from foo limit 10")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE1)
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .columns("cnt", "dim1")
                           .context(defaultScanQueryContext(context, resultSignature))
                           .limit(10)
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setQueryContext(context)
        .setExpectedRowSignature(resultSignature)
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
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1L, ""},
            new Object[]{1L, "10.1"},
            new Object[]{1L, "2"},
            new Object[]{1L, "1"},
            new Object[]{1L, "def"},
            new Object[]{1L, "abc"}
        )).verifyResults();
  }

  @Test
  public void testSelectWithGroupByLimit()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("cnt", ColumnType.LONG)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();


    testSelectQuery()
        .setSql("select cnt,count(*) as cnt1 from foo group by cnt limit 10")
        .setQueryContext(context)
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(GroupByQuery.builder()
                                                      .setDataSource(CalciteTests.DATASOURCE1)
                                                      .setInterval(querySegmentSpec(Filtration
                                                                                        .eternity()))
                                                      .setGranularity(Granularities.ALL)
                                                      .setDimensions(dimensions(
                                                          new DefaultDimensionSpec(
                                                              "cnt",
                                                              "d0",
                                                              ColumnType.LONG
                                                          )
                                                      ))
                                                      .setAggregatorSpecs(aggregators(new CountAggregatorFactory(
                                                          "a0")))
                                                      .setContext(context)
                                                      .setLimit(10)
                                                      .build())
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("d0", "cnt"),
                                           new ColumnMapping("a0", "cnt1")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(new Object[]{1L, 6L}))
        .verifyResults();

  }

  @Test
  public void testSelectLookup()
  {
    final RowSignature rowSignature = RowSignature.builder().add("EXPR$0", ColumnType.LONG).build();

    testSelectQuery()
        .setSql("select count(*) from lookup.lookyloo")
        .setQueryContext(context)
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(
                       GroupByQuery.builder()
                                   .setDataSource(new LookupDataSource("lookyloo"))
                                   .setInterval(querySegmentSpec(Filtration.eternity()))
                                   .setGranularity(Granularities.ALL)
                                   .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                                   .setContext(context)
                                   .build())
                   .columnMappings(new ColumnMappings(ImmutableList.of(new ColumnMapping("a0", "EXPR$0"))))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(new Object[]{4L}))
        .verifyResults();
  }

  @Test
  public void testJoinWithLookup()
  {
    final RowSignature rowSignature =
        RowSignature.builder()
                    .add("v", ColumnType.STRING)
                    .add("cnt", ColumnType.LONG)
                    .build();

    testSelectQuery()
        .setSql("SELECT lookyloo.v, COUNT(*) AS cnt\n"
                + "FROM foo LEFT JOIN lookup.lookyloo ON foo.dim2 = lookyloo.k\n"
                + "WHERE lookyloo.v <> 'xa'\n"
                + "GROUP BY lookyloo.v")
        .setQueryContext(context)
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(
                       GroupByQuery.builder()
                                   .setDataSource(
                                       join(
                                           new TableDataSource(CalciteTests.DATASOURCE1),
                                           new LookupDataSource("lookyloo"),
                                           "j0.",
                                           equalsCondition(
                                               DruidExpression.ofColumn(ColumnType.STRING, "dim2"),
                                               DruidExpression.ofColumn(ColumnType.STRING, "j0.k")
                                           ),
                                           JoinType.LEFT
                                       )
                                   )
                                   .setInterval(querySegmentSpec(Filtration.eternity()))
                                   .setDimFilter(not(selector("j0.v", "xa", null)))
                                   .setGranularity(Granularities.ALL)
                                   .setDimensions(dimensions(new DefaultDimensionSpec("j0.v", "d0")))
                                   .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                                   .setContext(context)
                                   .build())
                   .columnMappings(
                       new ColumnMappings(
                           ImmutableList.of(
                               new ColumnMapping("d0", "v"),
                               new ColumnMapping("a0", "cnt")
                           )
                       )
                   )
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{NullHandling.defaultStringValue(), 3L},
                new Object[]{"xabc", 1L}
            )
        )
        .verifyResults();
  }

  @Test
  public void testSubquery()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .build();

    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(
                        GroupByQuery.builder()
                                    .setDataSource("foo")
                                    .setInterval(querySegmentSpec(Filtration.eternity()))
                                    .setGranularity(Granularities.ALL)
                                    .setDimensions(dimensions(new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT)))
                                    .setContext(context)
                                    .build()
                    )
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                    .setContext(context)
                    .build();

    testSelectQuery()
        .setSql("select count(*) AS cnt from (select distinct m1 from foo)")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(query)
                   .columnMappings(new ColumnMappings(ImmutableList.of(new ColumnMapping("a0", "cnt"))))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of(new Object[]{6L}))
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
  public void testBroadcastJoin()
  {
    testJoin(JoinAlgorithm.BROADCAST);
  }

  @Test
  public void testSortMergeJoin()
  {
    testJoin(JoinAlgorithm.SORT_MERGE);
  }

  private void testJoin(final JoinAlgorithm joinAlgorithm)
  {
    final Map<String, Object> queryContext =
        ImmutableMap.<String, Object>builder()
                    .putAll(context)
                    .put(PlannerContext.CTX_SQL_JOIN_ALGORITHM, joinAlgorithm.toString())
                    .build();

    final RowSignature resultSignature = RowSignature.builder()
                                                     .add("dim2", ColumnType.STRING)
                                                     .add("EXPR$1", ColumnType.DOUBLE)
                                                     .build();

    final ImmutableList<Object[]> expectedResults;

    if (NullHandling.sqlCompatible()) {
      expectedResults = ImmutableList.of(
          new Object[]{null, 4.0},
          new Object[]{"", 3.0},
          new Object[]{"a", 2.5},
          new Object[]{"abc", 5.0}
      );
    } else {
      expectedResults = ImmutableList.of(
          new Object[]{"", 3.6666666666666665},
          new Object[]{"a", 2.5},
          new Object[]{"abc", 5.0}
      );
    }

    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(
                        join(
                            new QueryDataSource(
                                newScanQueryBuilder()
                                    .dataSource(CalciteTests.DATASOURCE1)
                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                    .columns("dim2", "m1", "m2")
                                    .context(
                                        defaultScanQueryContext(
                                            queryContext,
                                            RowSignature.builder()
                                                        .add("dim2", ColumnType.STRING)
                                                        .add("m1", ColumnType.FLOAT)
                                                        .add("m2", ColumnType.DOUBLE)
                                                        .build()
                                        )
                                    )
                                    .limit(10)
                                    .build()
                                    .withOverriddenContext(queryContext)
                            ),
                            new QueryDataSource(
                                newScanQueryBuilder()
                                    .dataSource(CalciteTests.DATASOURCE1)
                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                    .columns(ImmutableList.of("m1"))
                                    .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                    .context(
                                        defaultScanQueryContext(
                                            queryContext,
                                            RowSignature.builder().add("m1", ColumnType.FLOAT).build()
                                        )
                                    )
                                    .build()
                                    .withOverriddenContext(queryContext)
                            ),
                            "j0.",
                            equalsCondition(
                                DruidExpression.ofColumn(ColumnType.FLOAT, "m1"),
                                DruidExpression.ofColumn(ColumnType.FLOAT, "j0.m1")
                            ),
                            JoinType.INNER
                        )
                    )
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setDimensions(new DefaultDimensionSpec("dim2", "d0", ColumnType.STRING))
                    .setGranularity(Granularities.ALL)
                    .setAggregatorSpecs(
                        useDefault
                        ? aggregators(
                            new DoubleSumAggregatorFactory("a0:sum", "m2"),
                            new CountAggregatorFactory("a0:count")
                        )
                        : aggregators(
                            new DoubleSumAggregatorFactory("a0:sum", "m2"),
                            new FilteredAggregatorFactory(
                                new CountAggregatorFactory("a0:count"),
                                not(selector("m2", null, null)),

                                // Not sure why the name is only set in SQL-compatible null mode. Seems strange.
                                // May be due to JSON serialization: name is set on the serialized aggregator even
                                // if it was originally created with no name.
                                NullHandling.sqlCompatible() ? "a0:count" : null
                            )
                        )
                    )
                    .setPostAggregatorSpecs(
                        ImmutableList.of(
                            new ArithmeticPostAggregator(
                                "a0",
                                "quotient",
                                ImmutableList.of(
                                    new FieldAccessPostAggregator(null, "a0:sum"),
                                    new FieldAccessPostAggregator(null, "a0:count")
                                )
                            )
                        )
                    )
                    .setContext(queryContext)
                    .build();

    testSelectQuery()
        .setSql(
            "SELECT t1.dim2, AVG(t1.m2) FROM "
            + "(SELECT * FROM foo LIMIT 10) AS t1 "
            + "INNER JOIN foo AS t2 "
            + "ON t1.m1 = t2.m1 "
            + "GROUP BY t1.dim2"
        )
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(query)
                   .columnMappings(
                       new ColumnMappings(
                           ImmutableList.of(
                               new ColumnMapping("d0", "dim2"),
                               new ColumnMapping("a0", "EXPR$1")
                           )
                       )
                   )
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(expectedResults)
        .setQueryContext(queryContext)
        .setExpectedCountersForStageWorkerChannel(CounterSnapshotMatcher.with().totalFiles(1), 0, 0, "input0")
        .setExpectedCountersForStageWorkerChannel(CounterSnapshotMatcher.with().rows(6).frames(1), 0, 0, "output")
        .setExpectedCountersForStageWorkerChannel(CounterSnapshotMatcher.with().rows(6).frames(1), 0, 0, "shuffle")
        .verifyResults();
  }

  @Test
  public void testGroupByOrderByAggregation()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("sum_m1", ColumnType.DOUBLE)
                                            .build();

    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setDimensions(dimensions(new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT)))
                    .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                    .setLimitSpec(
                        new DefaultLimitSpec(
                            ImmutableList.of(
                                new OrderByColumnSpec(
                                    "a0",
                                    OrderByColumnSpec.Direction.DESCENDING,
                                    StringComparators.NUMERIC
                                )
                            ),
                            null
                        )
                    )
                    .setContext(context)
                    .build();

    testSelectQuery()
        .setSql("select m1, sum(m1) as sum_m1 from foo group by m1 order by sum_m1 desc")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(query)
                   .columnMappings(
                       new ColumnMappings(
                           ImmutableList.of(
                               new ColumnMapping("d0", "m1"),
                               new ColumnMapping("a0", "sum_m1")
                           )
                       )
                   )
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setExpectedRowSignature(rowSignature)
        .setQueryContext(context)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{6f, 6d},
                new Object[]{5f, 5d},
                new Object[]{4f, 4d},
                new Object[]{3f, 3d},
                new Object[]{2f, 2d},
                new Object[]{1f, 1d}
            )
        )
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
  public void testGroupByOrderByAggregationWithLimit()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("sum_m1", ColumnType.DOUBLE)
                                            .build();

    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setDimensions(dimensions(new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT)))
                    .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                    .setLimitSpec(
                        new DefaultLimitSpec(
                            ImmutableList.of(
                                new OrderByColumnSpec(
                                    "a0",
                                    OrderByColumnSpec.Direction.DESCENDING,
                                    StringComparators.NUMERIC
                                )
                            ),
                            3
                        )
                    )
                    .setContext(context)
                    .build();

    testSelectQuery()
        .setSql("select m1, sum(m1) as sum_m1 from foo group by m1 order by sum_m1 desc limit 3")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(query)
                   .columnMappings(
                       new ColumnMappings(
                           ImmutableList.of(
                               new ColumnMapping("d0", "m1"),
                               new ColumnMapping("a0", "sum_m1")
                           )
                       )
                   )
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setExpectedRowSignature(rowSignature)
        .setQueryContext(context)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{6f, 6d},
                new Object[]{5f, 5d},
                new Object[]{4f, 4d}
            )
        )
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
  public void testGroupByOrderByAggregationWithLimitAndOffset()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("m1", ColumnType.FLOAT)
                                            .add("sum_m1", ColumnType.DOUBLE)
                                            .build();

    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setDimensions(dimensions(new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT)))
                    .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                    .setLimitSpec(
                        new DefaultLimitSpec(
                            ImmutableList.of(
                                new OrderByColumnSpec(
                                    "a0",
                                    OrderByColumnSpec.Direction.DESCENDING,
                                    StringComparators.NUMERIC
                                )
                            ),
                            1,
                            2
                        )
                    )
                    .setContext(context)
                    .build();

    testSelectQuery()
        .setSql("select m1, sum(m1) as sum_m1 from foo group by m1 order by sum_m1 desc limit 2 offset 1")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(query)
                   .columnMappings(
                       new ColumnMappings(
                           ImmutableList.of(
                               new ColumnMapping("d0", "m1"),
                               new ColumnMapping("a0", "sum_m1")
                           )
                       )
                   )
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setExpectedRowSignature(rowSignature)
        .setQueryContext(context)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{5f, 5d},
                new Object[]{4f, 4d}
            )
        )
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
  public void testExternSelect1() throws IOException
  {
    final File toRead = MSQTestFileUtils.getResourceAsTemporaryFile(temporaryFolder, this, "/wikipedia-sampled.json");
    final String toReadAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("cnt", ColumnType.LONG)
                                            .build();

    final GroupByQuery expectedQuery =
        GroupByQuery.builder()
                    .setDataSource(
                        new ExternalDataSource(
                            new LocalInputSource(null, null, ImmutableList.of(toRead.getAbsoluteFile())),
                            new JsonInputFormat(null, null, null, null, null),
                            RowSignature.builder()
                                        .add("timestamp", ColumnType.STRING)
                                        .add("page", ColumnType.STRING)
                                        .add("user", ColumnType.STRING)
                                        .build()
                        )
                    )
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setVirtualColumns(
                        new ExpressionVirtualColumn(
                            "v0",
                            "timestamp_floor(timestamp_parse(\"timestamp\",null,'UTC'),'P1D',null,'UTC')",
                            ColumnType.LONG,
                            CalciteTests.createExprMacroTable()
                        )
                    )
                    .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)))
                    .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                    .setContext(context)
                    .build();

    testSelectQuery()
        .setSql("SELECT\n"
                + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                + "  count(*) as cnt\n"
                + "FROM TABLE(\n"
                + "  EXTERN(\n"
                + "    '{ \"files\": [" + toReadAsJson + "],\"type\":\"local\"}',\n"
                + "    '{\"type\": \"json\"}',\n"
                + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                + "  )\n"
                + ") group by 1")
        .setExpectedRowSignature(rowSignature)
        .setQueryContext(context)
        .setExpectedResultRows(ImmutableList.of(new Object[]{1466985600000L, 20L}))
        .setExpectedMSQSpec(
            MSQSpec
                .builder()
                .query(expectedQuery)
                .columnMappings(new ColumnMappings(
                    ImmutableList.of(
                        new ColumnMapping("d0", "__time"),
                        new ColumnMapping("a0", "cnt")
                    )
                ))
                .tuningConfig(MSQTuningConfig.defaultConfig())
                .build()
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(20).bytes(toRead.length()).files(1).totalFiles(1),
            0, 0, "input0"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(1).frames(1),
            0, 0, "output"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(1).frames(1),
            0, 0, "shuffle"
        )
        .verifyResults();
  }

  @Test
  public void testIncorrectSelectQuery()
  {
    testSelectQuery()
        .setSql("select a from ")
        .setExpectedValidationErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(SqlPlanningException.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Encountered \"from <EOF>\""))
        ))
        .setQueryContext(context)
        .verifyPlanningErrors();
  }

  @Test
  public void testSelectOnInformationSchemaSource()
  {
    testSelectQuery()
        .setSql("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA")
        .setQueryContext(context)
        .setExpectedValidationErrorMatcher(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(SqlPlanningException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
                    "Cannot query table INFORMATION_SCHEMA.SCHEMATA with SQL engine 'msq-task'."))
            )
        )
        .verifyPlanningErrors();
  }

  @Test
  public void testSelectOnSysSource()
  {
    testSelectQuery()
        .setSql("SELECT * FROM sys.segments")
        .setQueryContext(context)
        .setExpectedValidationErrorMatcher(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(SqlPlanningException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
                    "Cannot query table sys.segments with SQL engine 'msq-task'."))
            )
        )
        .verifyPlanningErrors();
  }

  @Test
  public void testSelectOnSysSourceWithJoin()
  {
    testSelectQuery()
        .setSql("select s.segment_id, s.num_rows, f.dim1 from sys.segments as s, foo as f")
        .setQueryContext(context)
        .setExpectedValidationErrorMatcher(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(SqlPlanningException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
                    "Cannot query table sys.segments with SQL engine 'msq-task'."))
            )
        )
        .verifyPlanningErrors();
  }

  @Test
  public void testSelectOnSysSourceContainingWith()
  {
    testSelectQuery()
        .setSql("with segment_source as (SELECT * FROM sys.segments) "
                + "select segment_source.segment_id, segment_source.num_rows from segment_source")
        .setQueryContext(context)
        .setExpectedValidationErrorMatcher(
            CoreMatchers.allOf(
                CoreMatchers.instanceOf(SqlPlanningException.class),
                ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
                    "Cannot query table sys.segments with SQL engine 'msq-task'."))
            )
        )
        .verifyPlanningErrors();
  }


  @Test
  public void testSelectOnUserDefinedSourceContainingWith()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("m1", ColumnType.LONG)
                                               .add("dim2", ColumnType.STRING)
                                               .build();

    testSelectQuery()
        .setSql("with sys as (SELECT * FROM foo2) "
                + "select m1, dim2 from sys")
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE2)
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .columns("dim2", "m1")
                           .context(defaultScanQueryContext(
                               context,
                               RowSignature.builder()
                                           .add("dim2", ColumnType.STRING)
                                           .add("m1", ColumnType.LONG)
                                           .build()
                           ))
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build()
        )
        .setExpectedRowSignature(resultSignature)
        .setQueryContext(context)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1L, "en"},
            new Object[]{1L, "ru"},
            new Object[]{1L, "he"}
        ))
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().totalFiles(1),
            0, 0, "input0"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(3).frames(1),
            0, 0, "output"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(3).frames(1),
            0, 0, "shuffle"
        )
        .verifyResults();
  }

  @Test
  public void testScanWithMultiValueSelectQuery()
  {
    RowSignature expectedScanSignature = RowSignature.builder()
                                                     .add("dim3", ColumnType.STRING)
                                                     .add("v0", ColumnType.STRING_ARRAY)
                                                     .build();

    RowSignature expectedResultSignature = RowSignature.builder()
                                                       .add("dim3", ColumnType.STRING)
                                                       .add("dim3_array", ColumnType.STRING_ARRAY)
                                                       .build();

    testSelectQuery()
        .setSql("select dim3, MV_TO_ARRAY(dim3) AS dim3_array from foo")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(newScanQueryBuilder()
                                              .dataSource(CalciteTests.DATASOURCE1)
                                              .intervals(querySegmentSpec(Filtration.eternity()))
                                              .virtualColumns(
                                                  expressionVirtualColumn(
                                                      "v0",
                                                      "mv_to_array(\"dim3\")",
                                                      ColumnType.STRING_ARRAY
                                                  )
                                              )
                                              .columns("dim3", "v0")
                                              .context(defaultScanQueryContext(context, expectedScanSignature))
                                              .build())
                                   .columnMappings(
                                       new ColumnMappings(
                                           ImmutableList.of(
                                               new ColumnMapping("dim3", "dim3"),
                                               new ColumnMapping("v0", "dim3_array")
                                           )
                                       )
                                   )
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .build())
        .setExpectedRowSignature(expectedResultSignature)
        .setQueryContext(context)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{"[\"a\",\"b\"]", ImmutableList.of("a", "b")},
            new Object[]{"[\"b\",\"c\"]", ImmutableList.of("b", "c")},
            new Object[]{"d", ImmutableList.of("d")},
            new Object[]{"", Collections.singletonList(useDefault ? null : "")},
            new Object[]{NullHandling.defaultStringValue(), Collections.singletonList(null)},
            new Object[]{NullHandling.defaultStringValue(), Collections.singletonList(null)}
        )).verifyResults();
  }

  @Test
  public void testHavingOnApproximateCountDistinct()
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("dim2", ColumnType.STRING)
                                               .add("col", ColumnType.LONG)
                                               .build();

    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(CalciteTests.DATASOURCE1)
                                     .setInterval(querySegmentSpec(Filtration.eternity()))
                                     .setGranularity(Granularities.ALL)
                                     .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                     .setAggregatorSpecs(
                                         aggregators(
                                             new CardinalityAggregatorFactory(
                                                 "a0",
                                                 null,
                                                 ImmutableList.of(
                                                     new DefaultDimensionSpec(
                                                         "m1",
                                                         "m1",
                                                         ColumnType.FLOAT
                                                     )
                                                 ),
                                                 false,
                                                 true
                                             )
                                         )
                                     )
                                     .setHavingSpec(
                                         having(
                                             bound(
                                                 "a0",
                                                 "1",
                                                 null,
                                                 true,
                                                 false,
                                                 null,
                                                 StringComparators.NUMERIC
                                             )
                                         )
                                     )
                                     .setContext(context)
                                     .build();

    testSelectQuery()
        .setSql("SELECT dim2, COUNT(DISTINCT m1) as col FROM foo GROUP BY dim2 HAVING COUNT(DISTINCT m1) > 1")
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(query)
                                   .columnMappings(new ColumnMappings(ImmutableList.of(
                                       new ColumnMapping("d0", "dim2"),
                                       new ColumnMapping("a0", "col")
                                   )))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .build())
        .setQueryContext(context)
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(
            NullHandling.replaceWithDefault()
            ? ImmutableList.of(new Object[]{"", 3L}, new Object[]{"a", 2L})
            : ImmutableList.of(new Object[]{null, 2L}, new Object[]{"a", 2L})

        )
        .verifyResults();
  }

  @Test
  public void testGroupByWithMultiValue()
  {
    Map<String, Object> localContext = enableMultiValueUnnesting(context, true);
    RowSignature rowSignature = RowSignature.builder()
                                            .add("dim3", ColumnType.STRING)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    testSelectQuery()
        .setSql("select dim3, count(*) as cnt1 from foo group by dim3")
        .setQueryContext(localContext)
        .setExpectedMSQSpec(
            MSQSpec.builder()
                   .query(
                       GroupByQuery.builder()
                                   .setDataSource(CalciteTests.DATASOURCE1)
                                   .setInterval(querySegmentSpec(Filtration.eternity()))
                                   .setGranularity(Granularities.ALL)
                                   .setDimensions(
                                       dimensions(
                                           new DefaultDimensionSpec(
                                               "dim3",
                                               "d0"
                                           )
                                       )
                                   )
                                   .setAggregatorSpecs(aggregators(new CountAggregatorFactory(
                                       "a0")))
                                   .setContext(localContext)
                                   .build()
                   )
                   .columnMappings(
                       new ColumnMappings(ImmutableList.of(
                           new ColumnMapping("d0", "dim3"),
                           new ColumnMapping("a0", "cnt1")
                       )
                       ))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(expectedMultiValueFooRowsGroup())
        .verifyResults();
  }


  @Test
  public void testGroupByWithMultiValueWithoutGroupByEnable()
  {
    Map<String, Object> localContext = enableMultiValueUnnesting(context, false);

    testSelectQuery()
        .setSql("select dim3, count(*) as cnt1 from foo group by dim3")
        .setQueryContext(localContext)
        .setExpectedExecutionErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(ISE.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                "Column [dim3] is a multi-value string. Please wrap the column using MV_TO_ARRAY() to proceed further.")
            )
        ))
        .verifyExecutionError();
  }

  @Test
  public void testGroupByWithMultiValueMvToArray()
  {
    Map<String, Object> localContext = enableMultiValueUnnesting(context, true);

    RowSignature rowSignature = RowSignature.builder()
                                            .add("EXPR$0", ColumnType.STRING_ARRAY)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    testSelectQuery()
        .setSql("select MV_TO_ARRAY(dim3), count(*) as cnt1 from foo group by dim3")
        .setQueryContext(localContext)
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(GroupByQuery.builder()
                                                      .setDataSource(CalciteTests.DATASOURCE1)
                                                      .setInterval(querySegmentSpec(Filtration.eternity()))
                                                      .setGranularity(Granularities.ALL)
                                                      .setDimensions(
                                                          dimensions(
                                                              new DefaultDimensionSpec(
                                                                  "dim3",
                                                                  "d0"
                                                              )
                                                          )
                                                      )
                                                      .setAggregatorSpecs(aggregators(new CountAggregatorFactory(
                                                          "a0")))
                                                      .setPostAggregatorSpecs(
                                                          ImmutableList.of(new ExpressionPostAggregator(
                                                                               "p0",
                                                                               "mv_to_array(\"d0\")",
                                                                               null, ExprMacroTable.nil()
                                                                           )
                                                          )
                                                      )
                                                      .setContext(localContext)
                                                      .build()
                                   )
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("p0", "EXPR$0"),
                                           new ColumnMapping("a0", "cnt1")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            expectedMultiValueFooRowsGroupByList()
        )
        .verifyResults();
  }

  @Test
  public void testGroupByArrayWithMultiValueMvToArray()
  {
    Map<String, Object> localContext = enableMultiValueUnnesting(context, true);

    RowSignature rowSignature = RowSignature.builder()
                                            .add("EXPR$0", ColumnType.STRING_ARRAY)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    ArrayList<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{Collections.singletonList(null), !useDefault ? 2L : 3L});
    if (!useDefault) {
      expected.add(new Object[]{Collections.singletonList(""), 1L});
    }
    expected.addAll(ImmutableList.of(
        new Object[]{Arrays.asList("a", "b"), 1L},
        new Object[]{Arrays.asList("b", "c"), 1L},
        new Object[]{Collections.singletonList("d"), 1L}
    ));

    testSelectQuery()
        .setSql("select MV_TO_ARRAY(dim3), count(*) as cnt1 from foo group by MV_TO_ARRAY(dim3)")
        .setQueryContext(localContext)
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(GroupByQuery.builder()
                                                      .setDataSource(CalciteTests.DATASOURCE1)
                                                      .setInterval(querySegmentSpec(Filtration.eternity()))
                                                      .setGranularity(Granularities.ALL)
                                                      .setDimensions(
                                                          dimensions(
                                                              new DefaultDimensionSpec(
                                                                  "v0",
                                                                  "d0",
                                                                  ColumnType.STRING_ARRAY
                                                              )
                                                          )
                                                      )
                                                      .setVirtualColumns(
                                                          new ExpressionVirtualColumn(
                                                              "v0",
                                                              "mv_to_array(\"dim3\")",
                                                              ColumnType.STRING_ARRAY,
                                                              TestExprMacroTable.INSTANCE
                                                          )
                                                      )
                                                      .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                                                      .setContext(localContext)
                                                      .build()
                                   )
                                   .columnMappings(
                                       new ColumnMappings(
                                           ImmutableList.of(
                                               new ColumnMapping("d0", "EXPR$0"),
                                               new ColumnMapping("a0", "cnt1")
                                           )
                                       )
                                   )
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(expected)
        .verifyResults();
  }

  @Test
  public void testTimeColumnAggregationFromExtern() throws IOException
  {
    final File toRead = MSQTestFileUtils.getResourceAsTemporaryFile(temporaryFolder, this, "/wikipedia-sampled.json");
    final String toReadAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("cnt", ColumnType.LONG)
                                            .build();

    testSelectQuery()
        .setSql("WITH\n"
                + "kttm_data AS (\n"
                + "SELECT * FROM TABLE(\n"
                + "  EXTERN(\n"
                + "    '{ \"files\": [" + toReadAsJson + "],\"type\":\"local\"}',\n"
                + "    '{\"type\":\"json\"}',\n"
                + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                + "  )\n"
                + "))\n"
                + "\n"
                + "SELECT\n"
                + "  FLOOR(TIME_PARSE(\"timestamp\") TO MINUTE) AS __time,\n"
                + "  LATEST(\"page\") AS \"page\"\n"
                + "FROM kttm_data "
                + "GROUP BY 1")
        .setExpectedValidationErrorMatcher(
            ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                "LATEST() aggregator depends on __time column"))
        )
        .setExpectedRowSignature(rowSignature)
        .verifyPlanningErrors();
  }

  @Test
  public void testGroupByWithMultiValueMvToArrayWithoutGroupByEnable()
  {
    Map<String, Object> localContext = enableMultiValueUnnesting(context, false);

    testSelectQuery()
        .setSql("select MV_TO_ARRAY(dim3), count(*) as cnt1 from foo group by dim3")
        .setQueryContext(localContext)
        .setExpectedExecutionErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(ISE.class),
            ThrowableMessageMatcher.hasMessage(
                CoreMatchers.containsString(
                    "Encountered multi-value dimension [dim3] that cannot be processed with 'groupByEnableMultiValueUnnesting' set to false.")
            )
        ))
        .verifyExecutionError();
  }

  @Test
  public void testGroupByWithComplexColumnThrowsUnsupportedException()
  {
    testSelectQuery()
        .setSql("select unique_dim1 from foo2 group by unique_dim1")
        .setQueryContext(context)
        .setExpectedExecutionErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(UnsupportedSQLQueryException.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                "SQL requires a group-by on a column of type COMPLEX<hyperUnique> that is unsupported"))
        ))
        .verifyExecutionError();
  }

  @Test
  public void testGroupByMultiValueMeasureQuery()
  {
    final RowSignature rowSignature = RowSignature.builder()
                                                  .add("__time", ColumnType.LONG)
                                                  .add("cnt1", ColumnType.LONG)
                                                  .build();

    final GroupByQuery expectedQuery =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setDimensions(dimensions(new DefaultDimensionSpec("__time", "d0", ColumnType.LONG)))
                    .setAggregatorSpecs(
                        aggregators(
                            new FilteredAggregatorFactory(
                                new CountAggregatorFactory("a0"),
                                new NotDimFilter(new SelectorDimFilter("dim3", null, null)),
                                "a0"
                            )
                        )
                    )
                    .setContext(context)
                    .build();

    testSelectQuery()
        .setSql("select __time, count(dim3) as cnt1 from foo group by __time")
        .setQueryContext(context)
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(expectedQuery)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("d0", "__time"),
                                           new ColumnMapping("a0", "cnt1")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{946684800000L, 1L},
                new Object[]{946771200000L, 1L},
                new Object[]{946857600000L, 1L},
                new Object[]{978307200000L, !useDefault ? 1L : 0L},
                new Object[]{978393600000L, 0L},
                new Object[]{978480000000L, 0L}
            )
        )
        .verifyResults();
  }

  @Test
  public void testGroupByOnFooWithDurableStoragePathAssertions() throws IOException
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("cnt", ColumnType.LONG)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();


    testSelectQuery()
        .setSql("select cnt,count(*) as cnt1 from foo group by cnt")
        .setQueryContext(context)
        .setExpectedMSQSpec(MSQSpec.builder()
                                   .query(GroupByQuery.builder()
                                                      .setDataSource(CalciteTests.DATASOURCE1)
                                                      .setInterval(querySegmentSpec(Filtration
                                                                                        .eternity()))
                                                      .setGranularity(Granularities.ALL)
                                                      .setDimensions(dimensions(
                                                          new DefaultDimensionSpec(
                                                              "cnt",
                                                              "d0",
                                                              ColumnType.LONG
                                                          )
                                                      ))
                                                      .setAggregatorSpecs(aggregators(new CountAggregatorFactory(
                                                          "a0")))
                                                      .setContext(context)
                                                      .build())
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("d0", "cnt"),
                                           new ColumnMapping("a0", "cnt1")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(new Object[]{1L, 6L}))
        .verifyResults();
    if (DURABLE_STORAGE.equals(contextName) || FAULT_TOLERANCE.equals(contextName)) {
      new File(
          localFileStorageDir,
          DurableStorageUtils.getSuccessFilePath("query-test-query", 0, 0)
      );

      Mockito.verify(localFileStorageConnector, Mockito.times(2))
             .write(ArgumentMatchers.endsWith("__success"));
    }
  }

  @Test
  public void testMultiValueStringWithIncorrectType() throws IOException
  {
    final File toRead = MSQTestFileUtils.getResourceAsTemporaryFile(
        temporaryFolder,
        this,
        "/unparseable-mv-string-array.json"
    );
    final String toReadAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("language", ColumnType.STRING_ARRAY)
                                            .build();

    final GroupByQuery expectedQuery =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setDimensions(dimensions(new DefaultDimensionSpec("__time", "d0", ColumnType.LONG)))
                    .build();


    testSelectQuery()
        .setSql("WITH\n"
                + "kttm_data AS (\n"
                + "SELECT * FROM TABLE(\n"
                + "  EXTERN(\n"
                + "    '{ \"files\": [" + toReadAsJson + "],\"type\":\"local\"}',\n"
                + "    '{\"type\":\"json\"}',\n"
                + "    '[{\"name\":\"timestamp\",\"type\":\"string\"},{\"name\":\"agent_category\",\"type\":\"string\"},{\"name\":\"agent_type\",\"type\":\"string\"},{\"name\":\"browser\",\"type\":\"string\"},{\"name\":\"browser_version\",\"type\":\"string\"},{\"name\":\"city\",\"type\":\"string\"},{\"name\":\"continent\",\"type\":\"string\"},{\"name\":\"country\",\"type\":\"string\"},{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"event_subtype\",\"type\":\"string\"},{\"name\":\"loaded_image\",\"type\":\"string\"},{\"name\":\"adblock_list\",\"type\":\"string\"},{\"name\":\"forwarded_for\",\"type\":\"string\"},{\"name\":\"language\",\"type\":\"string\"},{\"name\":\"number\",\"type\":\"long\"},{\"name\":\"os\",\"type\":\"string\"},{\"name\":\"path\",\"type\":\"string\"},{\"name\":\"platform\",\"type\":\"string\"},{\"name\":\"referrer\",\"type\":\"string\"},{\"name\":\"referrer_host\",\"type\":\"string\"},{\"name\":\"region\",\"type\":\"string\"},{\"name\":\"remote_address\",\"type\":\"string\"},{\"name\":\"screen\",\"type\":\"string\"},{\"name\":\"session\",\"type\":\"string\"},{\"name\":\"session_length\",\"type\":\"long\"},{\"name\":\"timezone\",\"type\":\"string\"},{\"name\":\"timezone_offset\",\"type\":\"long\"},{\"name\":\"window\",\"type\":\"string\"}]'\n"
                + "  )\n"
                + "))\n"
                + "\n"
                + "SELECT\n"
                + "  FLOOR(TIME_PARSE(\"timestamp\") TO MINUTE) AS __time,\n"
                + "  MV_TO_ARRAY(\"language\") AS \"language\"\n"
                + "FROM kttm_data")
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1566691200000L, ImmutableList.of("en")},
            new Object[]{1566691200000L, ImmutableList.of("en", "es", "es-419", "es-MX")},
            new Object[]{1566691200000L, ImmutableList.of("en", "es", "es-419", "es-US")}
        ))
        .setExpectedMSQSpec(
            MSQSpec
                .builder()
                .query(expectedQuery)
                .columnMappings(new ColumnMappings(
                    ImmutableList.of(
                        new ColumnMapping("d0", "__time"),
                        new ColumnMapping("a0", "cnt")
                    )
                ))
                .tuningConfig(MSQTuningConfig.defaultConfig())
                .build())
        .setExpectedMSQFault(new CannotParseExternalDataFault(
            "Unable to add the row to the frame. Type conversion might be required."))
        .setQueryContext(context)
        .verifyResults();
  }

  @Nonnull
  private List<Object[]> expectedMultiValueFooRowsGroup()
  {
    ArrayList<Object[]> expected = new ArrayList<>();
    if (useDefault) {
      expected.add(new Object[]{"", 3L});
    } else {
      expected.add(new Object[]{null, 2L});
      expected.add(new Object[]{"", 1L});
    }
    expected.addAll(ImmutableList.of(
        new Object[]{"a", 1L},
        new Object[]{"b", 2L},
        new Object[]{"c", 1L},
        new Object[]{"d", 1L}
    ));
    return expected;
  }

  @Nonnull
  private List<Object[]> expectedMultiValueFooRowsGroupByList()
  {
    ArrayList<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{Collections.singletonList(null), !useDefault ? 2L : 3L});
    if (!useDefault) {
      expected.add(new Object[]{Collections.singletonList(""), 1L});
    }
    expected.addAll(ImmutableList.of(
        new Object[]{Collections.singletonList("a"), 1L},
        new Object[]{Collections.singletonList("b"), 2L},
        new Object[]{Collections.singletonList("c"), 1L},
        new Object[]{Collections.singletonList("d"), 1L}
    ));
    return expected;
  }

  private static Map<String, Object> enableMultiValueUnnesting(Map<String, Object> context, boolean value)
  {
    Map<String, Object> localContext = ImmutableMap.<String, Object>builder()
                                                   .putAll(context)
                                                   .put("groupByEnableMultiValueUnnesting", value)
                                                   .build();
    return localContext;
  }
}
