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
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.frame.util.DurableStorageUtils;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.msq.indexing.LegacyMSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.DurableStorageMSQDestination;
import org.apache.druid.msq.indexing.destination.MSQSelectDestination;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.indexing.error.CanceledFault;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.sql.MSQTaskQueryMaker;
import org.apache.druid.msq.test.CounterSnapshotMatcher;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.policy.Policy;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.server.lookup.cache.LookupLoadingSpec;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MSQSelectTest extends MSQTestBase
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

  public static Collection<Object[]> data()
  {
    Object[][] data = new Object[][]{
        {DEFAULT, DEFAULT_MSQ_CONTEXT},
        {DURABLE_STORAGE, DURABLE_STORAGE_MSQ_CONTEXT},
        {FAULT_TOLERANCE, FAULT_TOLERANCE_MSQ_CONTEXT},
        {PARALLEL_MERGE, PARALLEL_MERGE_MSQ_CONTEXT},
        {QUERY_RESULTS_WITH_DURABLE_STORAGE, QUERY_RESULTS_WITH_DURABLE_STORAGE_CONTEXT},
        {QUERY_RESULTS_WITH_DEFAULT, QUERY_RESULTS_WITH_DEFAULT_CONTEXT},
        {SUPERUSER, SUPERUSER_MSQ_CONTEXT}
    };

    return Arrays.asList(data);
  }
  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testCalculator(String contextName, Map<String, Object> context)
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("EXPR$0", ColumnType.LONG)
                                               .build();

    testSelectQuery()
        .setSql("select 1 + 1")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
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
                           .columnTypes(ColumnType.LONG)
                           .context(context)
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
                   .build()
        )
        .setExpectedRowSignature(resultSignature)
        .setQueryContext(context)
        .setExpectedResultRows(ImmutableList.of(new Object[]{2})).verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectOnFoo(String contextName, Map<String, Object> context)
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();

    testSelectQuery()
        .setSql("select cnt,dim1 from foo")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE1)
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .columns("cnt", "dim1")
                           .columnTypes(ColumnType.LONG, ColumnType.STRING)
                           .context(context)
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
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
                .with()
                .rows(isPageSizeLimited(contextName) ? new long[]{2, 2, 2} : new long[]{6})
                .frames(isPageSizeLimited(contextName) ? new long[]{1, 1, 1} : new long[]{1}),
            0, 0, "shuffle"
        )
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1L, ""},
            new Object[]{1L, "10.1"},
            new Object[]{1L, "2"},
            new Object[]{1L, "1"},
            new Object[]{1L, "def"},
            new Object[]{1L, "abc"}
        ))
        .setExpectedLookupLoadingSpec(LookupLoadingSpec.NONE)
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectOnFoo2(String contextName, Map<String, Object> context)
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("m1", ColumnType.LONG)
                                               .add("dim2", ColumnType.STRING)
                                               .build();

    testSelectQuery()
        .setSql("select m1,dim2 from foo2")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(newScanQueryBuilder()
                              .dataSource(CalciteTests.DATASOURCE2)
                              .intervals(querySegmentSpec(Filtration.eternity()))
                              .columns("m1", "dim2")
                              .columnTypes(ColumnType.LONG, ColumnType.STRING)
                              .context(context)
                              .build())
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
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
                .with()
                .rows(isPageSizeLimited(contextName) ? new long[]{1L, 2L} : new long[]{3L})
                .frames(isPageSizeLimited(contextName) ? new long[]{1L, 1L} : new long[]{1L}),
            0, 0, "shuffle"
        )
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectOnFooDuplicateColumnNames(String contextName, Map<String, Object> context)
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
            LegacyMSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE1)
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .columns("cnt", "dim1")
                           .columnTypes(ColumnType.LONG, ColumnType.STRING)
                           .context(context)
                           .build()
                   )
                   .columnMappings(expectedColumnMappings)
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
                   .build()
        )
        .setQueryContext(context)
        .setExpectedRowSignature(expectedOutputSignature)
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().totalFiles(1),
            0, 0, "input0"
        ).setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(6).frames(1),
            0, 0, "output"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with()
                .rows(isPageSizeLimited(contextName) ? new long[]{2, 2, 2} : new long[]{6})
                .frames(isPageSizeLimited(contextName) ? new long[]{1, 1, 1} : new long[]{1}),
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectOnFooWhereMatchesNoSegments(String contextName, Map<String, Object> context)
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();

    // Filter [__time >= timestamp '3000-01-01 00:00:00'] matches no segments at all.
    testSelectQuery()
        .setSql("select cnt,dim1 from foo where __time >= timestamp '3000-01-01 00:00:00'")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
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
                           .columnTypes(ColumnType.LONG, ColumnType.STRING)
                           .context(context)
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
                   .build()
        )
        .setQueryContext(context)
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of())
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectWithDynamicParameters(String contextName, Map<String, Object> context)
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();

    // Filter [__time >= timestamp '3000-01-01 00:00:00'] matches no segments at all.
    testSelectQuery()
        .setSql("select cnt,dim1 from foo where __time >= ?")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
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
                           .columnTypes(ColumnType.LONG, ColumnType.STRING)
                           .context(context)
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
                   .build()
        )
        .setDynamicParameters(
            ImmutableList.of(
                TypedValue.ofLocal(ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, DateTimes.of("3000-01-01").getMillis())
            )
        )
        .setQueryContext(context)
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of())
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectOnFooWhereMatchesNoData(String contextName, Map<String, Object> context)
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();

    testSelectQuery()
        .setSql("select cnt,dim1 from foo where dim2 = 'nonexistent'")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE1)
                           .intervals(querySegmentSpec(Intervals.ETERNITY))
                           .columns("cnt", "dim1")
                           .columnTypes(ColumnType.LONG, ColumnType.STRING)
                           .filters(equality("dim2", "nonexistent", ColumnType.STRING))
                           .context(context)
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
                   .build()
        )
        .setQueryContext(context)
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of())
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectAndOrderByOnFooWhereMatchesNoData(String contextName, Map<String, Object> context)
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();

    testSelectQuery()
        .setSql("select cnt,dim1 from foo where dim2 = 'nonexistent' order by dim1")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE1)
                           .intervals(querySegmentSpec(Intervals.ETERNITY))
                           .columns("cnt", "dim1")
                           .columnTypes(ColumnType.LONG, ColumnType.STRING)
                           .filters(equality("dim2", "nonexistent", ColumnType.STRING))
                           .context(context)
                           .orderBy(ImmutableList.of(OrderBy.ascending("dim1")))
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
                   .build()
        )
        .setQueryContext(context)
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(ImmutableList.of())
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testGroupByOnFoo(String contextName, Map<String, Object> context)
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("cnt", ColumnType.LONG)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    testSelectQuery()
        .setSql("select cnt,count(*) as cnt1 from foo group by cnt")
        .setExpectedMSQSpec(LegacyMSQSpec.builder()
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
                                   .destination(isDurableStorageDestination(contextName, context)
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testGroupByOrderByDimension(String contextName, Map<String, Object> context)
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
            LegacyMSQSpec.builder()
                   .query(query)
                   .columnMappings(
                       new ColumnMappings(ImmutableList.of(
                           new ColumnMapping("d0", "m1"),
                           new ColumnMapping("a0", "cnt")
                       )
                       ))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectWithLimit(String contextName, Map<String, Object> context)
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("cnt", ColumnType.LONG)
                                               .add("dim1", ColumnType.STRING)
                                               .build();

    final ImmutableList<Object[]> expectedResults = ImmutableList.of(
        new Object[]{1L, ""},
        new Object[]{1L, "10.1"},
        new Object[]{1L, "2"},
        new Object[]{1L, "1"},
        new Object[]{1L, "def"},
        new Object[]{1L, "abc"}
    );

    testSelectQuery()
        .setSql("select cnt, dim1 from foo limit 10")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE1)
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .columns("cnt", "dim1")
                           .columnTypes(ColumnType.LONG, ColumnType.STRING)
                           .context(context)
                           .limit(10)
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
                   .build()
        )
        .setQueryContext(context)
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(expectedResults)
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().totalFiles(1),
            0, 0, "input0"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(6),
            0, 0, "output"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(6),
            0, 0, "shuffle"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(6),
            1, 0, "input0"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(6),
            1, 0, "output"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(!context.containsKey(MultiStageQueryContext.CTX_ROWS_PER_PAGE) ? new long[] {6} : new long[] {2, 2, 2}),
            1, 0, "shuffle"
        )
        .setExpectedResultRows(expectedResults)
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectWithGroupByLimit(String contextName, Map<String, Object> context)
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("cnt", ColumnType.LONG)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();


    testSelectQuery()
        .setSql("select cnt,count(*) as cnt1 from foo group by cnt limit 10")
        .setQueryContext(context)
        .setExpectedMSQSpec(LegacyMSQSpec.builder()
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
                                   .destination(isDurableStorageDestination(contextName, context)
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(new Object[]{1L, 6L}))
        .verifyResults();

  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectRestricted(String contextName, Map<String, Object> context)
  {
    boolean isSuperUser = context.get(MSQTaskQueryMaker.USER_KEY).equals(CalciteTests.TEST_SUPERUSER_NAME);
    Policy policy = isSuperUser ? CalciteTests.POLICY_NO_RESTRICTION_SUPERUSER : CalciteTests.POLICY_RESTRICTION;
    long expectedResultRows = isSuperUser ? 6L : 1L;

    final RowSignature rowSignature = RowSignature.builder().add("EXPR$0", ColumnType.LONG).build();

    testSelectQuery()
        .setSql("select count(*) from druid.restrictedDatasource_m1_is_6")
        .setQueryContext(context)
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(
                       GroupByQuery.builder()
                                   .setDataSource(RestrictedDataSource.create(
                                       TableDataSource.create(CalciteTests.RESTRICTED_DATASOURCE),
                                       policy
                                   ))
                                   .setInterval(querySegmentSpec(Filtration.eternity()))
                                   .setGranularity(Granularities.ALL)
                                   .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                                   .setContext(context)
                                   .build())
                   .columnMappings(new ColumnMappings(ImmutableList.of(new ColumnMapping("a0", "EXPR$0"))))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(new Object[]{expectedResultRows}))
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectLookup(String contextName, Map<String, Object> context)
  {
    final RowSignature rowSignature = RowSignature.builder().add("EXPR$0", ColumnType.LONG).build();

    testSelectQuery()
        .setSql("select count(*) from lookup.lookyloo")
        .setQueryContext(context)
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
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
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(new Object[]{4L}))
        .setExpectedLookupLoadingSpec(LookupLoadingSpec.loadOnly(ImmutableSet.of("lookyloo")))
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testJoinWithLookup(String contextName, Map<String, Object> context)
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
            LegacyMSQSpec.builder()
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
                                           JoinType.INNER
                                       )
                                   )
                                   .setInterval(querySegmentSpec(Filtration.eternity()))
                                   .setDimFilter(not(equality("j0.v", "xa", ColumnType.STRING)))
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
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{"xabc", 1L}
            )
        )
        .setExpectedLookupLoadingSpec(LookupLoadingSpec.loadOnly(ImmutableSet.of("lookyloo")))
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testJoinRestrictedWithLookup(String contextName, Map<String, Object> context)
  {
    boolean isSuperUser = context.get(MSQTaskQueryMaker.USER_KEY).equals(CalciteTests.TEST_SUPERUSER_NAME);
    Policy policy = isSuperUser ? CalciteTests.POLICY_NO_RESTRICTION_SUPERUSER : CalciteTests.POLICY_RESTRICTION;
    ImmutableList<Object[]> expectedResult = isSuperUser ? ImmutableList.of(new Object[]{"xabc", 1L}) : ImmutableList.of();

    final RowSignature rowSignature =
        RowSignature.builder()
                    .add("v", ColumnType.STRING)
                    .add("cnt", ColumnType.LONG)
                    .build();

    testSelectQuery()
        .setSql("SELECT lookyloo.v, COUNT(*) AS cnt\n"
                + "FROM druid.restrictedDatasource_m1_is_6 as restricted LEFT JOIN lookup.lookyloo ON restricted.dim2 = lookyloo.k\n"
                + "WHERE lookyloo.v <> 'xa'\n"
                + "GROUP BY lookyloo.v")
        .setQueryContext(context)
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(
                       GroupByQuery.builder()
                                   .setDataSource(
                                       join(
                                           RestrictedDataSource.create(
                                               TableDataSource.create(CalciteTests.RESTRICTED_DATASOURCE),
                                               policy
                                           ),
                                           new LookupDataSource("lookyloo"),
                                           "j0.",
                                           equalsCondition(
                                               DruidExpression.ofColumn(ColumnType.STRING, "dim2"),
                                               DruidExpression.ofColumn(ColumnType.STRING, "j0.k")
                                           ),
                                           JoinType.INNER
                                       )
                                   )
                                   .setInterval(querySegmentSpec(Filtration.eternity()))
                                   .setDimFilter(not(equality("j0.v", "xa", ColumnType.STRING)))
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
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(expectedResult)
        .setExpectedLookupLoadingSpec(LookupLoadingSpec.loadOnly(ImmutableSet.of("lookyloo")))
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSubquery(String contextName, Map<String, Object> context)
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
            LegacyMSQSpec.builder()
                   .query(query)
                   .columnMappings(new ColumnMappings(ImmutableList.of(new ColumnMapping("a0", "cnt"))))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testBroadcastJoin(String contextName, Map<String, Object> context)
  {
    testJoin(contextName, context, JoinAlgorithm.BROADCAST);
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSortMergeJoin(String contextName, Map<String, Object> context)
  {
    testJoin(contextName, context, JoinAlgorithm.SORT_MERGE);
  }

  private void testJoin(String contextName, Map<String, Object> context, final JoinAlgorithm joinAlgorithm)
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

    final ImmutableList<Object[]> expectedResults = ImmutableList.of(
        new Object[]{null, 4.0},
        new Object[]{"", 3.0},
        new Object[]{"a", 2.5},
        new Object[]{"abc", 5.0}
    );

    final GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(
                        join(
                            new QueryDataSource(
                                newScanQueryBuilder()
                                    .dataSource(CalciteTests.DATASOURCE1)
                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                    .columns("dim2", "m1", "m2")
                                    .columnTypes(ColumnType.STRING, ColumnType.FLOAT, ColumnType.DOUBLE)
                                    .context(queryContext)
                                    .limit(10)
                                    .build()
                                    .withOverriddenContext(queryContext)
                            ),
                            new QueryDataSource(
                                newScanQueryBuilder()
                                    .dataSource(CalciteTests.DATASOURCE1)
                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                    .columns("m1")
                                    .columnTypes(ColumnType.FLOAT)
                                    .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                    .context(queryContext)
                                    .build()
                                    .withOverriddenContext(queryContext)
                            ),
                            "j0.",
                            equalsCondition(
                                DruidExpression.ofColumn(ColumnType.FLOAT, "m1"),
                                DruidExpression.ofColumn(ColumnType.FLOAT, "j0.m1")
                            ),
                            JoinType.INNER,
                            null,
                            joinAlgorithm
                        )
                    )
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setDimensions(new DefaultDimensionSpec("dim2", "d0", ColumnType.STRING))
                    .setGranularity(Granularities.ALL)
                    .setAggregatorSpecs(
                        aggregators(
                            new DoubleSumAggregatorFactory("a0:sum", "m2"),
                            new FilteredAggregatorFactory(
                                new CountAggregatorFactory("a0:count"),
                                notNull("m2"),
                                "a0:count"
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
            LegacyMSQSpec.builder()
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
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testGroupByOrderByAggregation(String contextName, Map<String, Object> context)
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
            LegacyMSQSpec.builder()
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
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testGroupByOrderByAggregationWithLimit(String contextName, Map<String, Object> context)
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
            LegacyMSQSpec.builder()
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
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testGroupByOrderByAggregationWithLimitAndOffset(String contextName, Map<String, Object> context)
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
            LegacyMSQSpec.builder()
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
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testExternGroupBy(String contextName, Map<String, Object> context) throws IOException
  {
    final File toRead = getResourceAsTemporaryFile("/wikipedia-sampled.json");
    final String toReadAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("cnt", ColumnType.LONG)
                                            .build();

    final GroupByQuery expectedQuery =
        GroupByQuery.builder()
                    .setDataSource(
                        new ExternalDataSource(
                            new LocalInputSource(
                                null,
                                null,
                                ImmutableList.of(toRead.getAbsoluteFile()),
                                SystemFields.none()
                            ),
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
            LegacyMSQSpec
                .builder()
                .query(expectedQuery)
                .columnMappings(new ColumnMappings(
                    ImmutableList.of(
                        new ColumnMapping("d0", "__time"),
                        new ColumnMapping("a0", "cnt")
                    )
                ))
                .tuningConfig(MSQTuningConfig.defaultConfig())
                .destination(isDurableStorageDestination(contextName, context)
                             ? DurableStorageMSQDestination.INSTANCE
                             : TaskReportMSQDestination.INSTANCE)
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


  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testExternSelectWithMultipleWorkers(String contextName, Map<String, Object> context) throws IOException
  {
    Map<String, Object> multipleWorkerContext = new HashMap<>(context);
    multipleWorkerContext.put(MultiStageQueryContext.CTX_MAX_NUM_TASKS, 3);

    final File toRead = getResourceAsTemporaryFile("/wikipedia-sampled.json");
    final String toReadAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    RowSignature rowSignature = RowSignature.builder()
                                            .add("__time", ColumnType.LONG)
                                            .add("user", ColumnType.STRING)
                                            .build();

    final ScanQuery expectedQuery =
        newScanQueryBuilder().dataSource(
                                 new ExternalDataSource(
                                     new LocalInputSource(
                                         null,
                                         null,
                                         ImmutableList.of(
                                             toRead.getAbsoluteFile(),
                                             toRead.getAbsoluteFile()
                                         ),
                                         SystemFields.none()
                                     ),
                                     new JsonInputFormat(null, null, null, null, null),
                                     RowSignature.builder()
                                                 .add("timestamp", ColumnType.STRING)
                                                 .add("page", ColumnType.STRING)
                                                 .add("user", ColumnType.STRING)
                                                 .build()
                                 )
                             ).eternityInterval().virtualColumns(
                                 new ExpressionVirtualColumn(
                                     "v0",
                                     "timestamp_floor(timestamp_parse(\"timestamp\",null,'UTC'),'P1D',null,'UTC')",
                                     ColumnType.LONG,
                                     CalciteTests.createExprMacroTable()
                                 )
                             )
                             .columns("v0", "user")
                             .columnTypes(ColumnType.LONG, ColumnType.STRING)
                             .filters(new LikeDimFilter("user", "%ot%", null, null))
                             .context(multipleWorkerContext)
                             .build();

    SelectTester selectTester = testSelectQuery()
        .setSql("SELECT\n"
                + "  floor(TIME_PARSE(\"timestamp\") to day) AS __time,\n"
                + "  user\n"
                + "FROM TABLE(\n"
                + "  EXTERN(\n"
                + "    '{ \"files\": [" + toReadAsJson + "," + toReadAsJson + "],\"type\":\"local\"}',\n"
                + "    '{\"type\": \"json\"}',\n"
                + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}, {\"name\": \"page\", \"type\": \"string\"}, {\"name\": \"user\", \"type\": \"string\"}]'\n"
                + "  )\n"
                + ") where user like '%ot%'")
        .setExpectedRowSignature(rowSignature)
        .setQueryContext(multipleWorkerContext)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1466985600000L, "Lsjbot"},
            new Object[]{1466985600000L, "Lsjbot"},
            new Object[]{1466985600000L, "Beau.bot"},
            new Object[]{1466985600000L, "Beau.bot"},
            new Object[]{1466985600000L, "Lsjbot"},
            new Object[]{1466985600000L, "Lsjbot"},
            new Object[]{1466985600000L, "TaxonBot"},
            new Object[]{1466985600000L, "TaxonBot"},
            new Object[]{1466985600000L, "GiftBot"},
            new Object[]{1466985600000L, "GiftBot"}
        ))
        .setExpectedMSQSpec(
            LegacyMSQSpec
                .builder()
                .query(expectedQuery)
                .columnMappings(new ColumnMappings(
                    ImmutableList.of(
                        new ColumnMapping("v0", "__time"),
                        new ColumnMapping("user", "user")
                    )
                ))
                .tuningConfig(MSQTuningConfig.defaultConfig())
                .destination(isDurableStorageDestination(contextName, context)
                             ? DurableStorageMSQDestination.INSTANCE
                             : TaskReportMSQDestination.INSTANCE)
                .build()
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(20).bytes(toRead.length()).files(1).totalFiles(1),
            0, 0, "input0"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(5).frames(1),
            0, 0, "output"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with()
                .rows(isPageSizeLimited(contextName) ? new long[]{1L, 1L, 1L, 1L, 1L} : new long[]{5L})
                .frames(isPageSizeLimited(contextName) ? new long[]{1L, 1L, 1L, 1L, 1L} : new long[]{1L}),
            0, 0, "shuffle"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(20).bytes(toRead.length()).files(1).totalFiles(1),
            0, 1, "input0"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(5).frames(1),
            0, 1, "output"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with()
                .rows(isPageSizeLimited(contextName) ? new long[]{1L, 1L, 1L, 1L, 1L} : new long[]{5L})
                .frames(isPageSizeLimited(contextName) ? new long[]{1L, 1L, 1L, 1L, 1L} : new long[]{1L}),
            0, 1, "shuffle"
        );
    // adding result stage counter checks
    if (isPageSizeLimited(contextName)) {
      selectTester.setExpectedCountersForStageWorkerChannel(
          CounterSnapshotMatcher
              .with().rows(2, 0, 2, 0, 2),
          1, 0, "input0"
      ).setExpectedCountersForStageWorkerChannel(
          CounterSnapshotMatcher
              .with().rows(2, 0, 2, 0, 2),
          1, 0, "output"
      ).setExpectedCountersForStageWorkerChannel(
          CounterSnapshotMatcher
              .with().rows(0, 2, 0, 2),
          1, 1, "input0"
      ).setExpectedCountersForStageWorkerChannel(
          CounterSnapshotMatcher
              .with().rows(0, 2, 0, 2),
          1, 1, "output"
      );
    }
    selectTester.verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testIncorrectSelectQuery(String contextName, Map<String, Object> context)
  {
    testSelectQuery()
        .setSql("select a from ")
        .setExpectedValidationErrorMatcher(
            invalidSqlContains("Received an unexpected token [from <EOF>] (line [1], column [10]), acceptable options")
        )
        .setQueryContext(context)
        .verifyPlanningErrors();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectOnInformationSchemaSource(String contextName, Map<String, Object> context)
  {
    testSelectQuery()
        .setSql("SELECT * FROM INFORMATION_SCHEMA.SCHEMATA")
        .setQueryContext(context)
        .setExpectedValidationErrorMatcher(
            invalidSqlIs("Cannot query table(s) [INFORMATION_SCHEMA.SCHEMATA] with SQL engine [msq-task]")
        )
        .verifyPlanningErrors();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectOnSysSource(String contextName, Map<String, Object> context)
  {
    testSelectQuery()
        .setSql("SELECT * FROM sys.segments")
        .setQueryContext(context)
        .setExpectedValidationErrorMatcher(
            invalidSqlIs("Cannot query table(s) [sys.segments] with SQL engine [msq-task]")
        )
        .verifyPlanningErrors();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectOnSysSourceWithJoin(String contextName, Map<String, Object> context)
  {
    testSelectQuery()
        .setSql("select s.segment_id, s.num_rows, f.dim1 from sys.segments as s, foo as f")
        .setQueryContext(context)
        .setExpectedValidationErrorMatcher(
            invalidSqlIs("Cannot query table(s) [sys.segments] with SQL engine [msq-task]")
        )
        .verifyPlanningErrors();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectOnSysSourceContainingWith(String contextName, Map<String, Object> context)
  {
    testSelectQuery()
        .setSql("with segment_source as (SELECT * FROM sys.segments) "
                + "select segment_source.segment_id, segment_source.num_rows from segment_source")
        .setQueryContext(context)
        .setExpectedValidationErrorMatcher(
            invalidSqlIs("Cannot query table(s) [sys.segments] with SQL engine [msq-task]")
        )
        .verifyPlanningErrors();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectOnUserDefinedSourceContainingWith(String contextName, Map<String, Object> context)
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("m1", ColumnType.LONG)
                                               .add("dim2", ColumnType.STRING)
                                               .build();

    testSelectQuery()
        .setSql("with sys as (SELECT * FROM foo2) "
                + "select m1, dim2 from sys")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(
                       newScanQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE2)
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .columns("m1", "dim2")
                           .columnTypes(ColumnType.LONG, ColumnType.STRING)
                           .context(context)
                           .build()
                   )
                   .columnMappings(ColumnMappings.identity(resultSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
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
                .with()
                .rows(isPageSizeLimited(contextName) ? new long[]{1, 2} : new long[]{3})
                .frames(isPageSizeLimited(contextName) ? new long[]{1, 1} : new long[]{1}),
            0, 0, "shuffle"
        )
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testScanWithMultiValueSelectQuery(String contextName, Map<String, Object> context)
  {
    RowSignature expectedResultSignature = RowSignature.builder()
                                                       .add("dim3", ColumnType.STRING)
                                                       .add("dim3_array", ColumnType.STRING_ARRAY)
                                                       .build();

    testSelectQuery()
        .setSql("select dim3, MV_TO_ARRAY(dim3) AS dim3_array from foo")
        .setExpectedMSQSpec(LegacyMSQSpec.builder()
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
                                              .columnTypes(ColumnType.STRING, ColumnType.STRING_ARRAY)
                                              .context(context)
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
                                   .destination(isDurableStorageDestination(contextName, context)
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(expectedResultSignature)
        .setQueryContext(context)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{"[\"a\",\"b\"]", ImmutableList.of("a", "b")},
            new Object[]{"[\"b\",\"c\"]", ImmutableList.of("b", "c")},
            new Object[]{"d", ImmutableList.of("d")},
            new Object[]{"", Collections.singletonList("")},
            new Object[]{null, null},
            new Object[]{null, null}
        )).verifyResults();
  }

  @Test
  public void testMultiValueStringFromJsonObjectArray() throws IOException
  {
    // In this test, "language" includes a JSON array like [{},{}] (containing objects rather than strings).
    final File toRead = getResourceAsTemporaryFile("/nonstring-mv-string-array.json");
    final String toReadAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    final RowSignature rowSignature = RowSignature.builder()
                                                  .add("__time", ColumnType.LONG)
                                                  .add("language", ColumnType.STRING_ARRAY)
                                                  .build();

    final ScanQuery expectedQuery =
        newScanQueryBuilder()
            .dataSource(
                new ExternalDataSource(
                    new LocalInputSource(null, null, ImmutableList.of(toRead), SystemFields.none()),
                    new JsonInputFormat(null, null, null, null, null),
                    RowSignature.builder()
                                .add("timestamp", ColumnType.STRING)
                                .add("language", ColumnType.STRING)
                                .build()
                )
            )
            .intervals(querySegmentSpec(Filtration.eternity()))
            .virtualColumns(
                expressionVirtualColumn("v0", "timestamp_parse(\"timestamp\",null,'UTC')", ColumnType.LONG),
                expressionVirtualColumn("v1", "mv_to_array(\"language\")", ColumnType.STRING_ARRAY)
            )
            .columns("v0", "v1")
            .columnTypes(ColumnType.LONG, ColumnType.STRING_ARRAY)
            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
            .context(DEFAULT_MSQ_CONTEXT)
            .build();

    testSelectQuery()
        .setSql("WITH\n"
                + "kttm_data AS (\n"
                + "SELECT * FROM TABLE(\n"
                + "  EXTERN(\n"
                + "    '{ \"files\": [" + toReadAsJson + "],\"type\":\"local\"}',\n"
                + "    '{\"type\":\"json\"}',\n"
                + "    '[{\"name\":\"timestamp\",\"type\":\"string\"},{\"name\":\"language\",\"type\":\"string\"}]'\n"
                + "  )\n"
                + "))\n"
                + "\n"
                + "SELECT\n"
                + "  TIME_PARSE(\"timestamp\") AS __time,\n"
                + "  MV_TO_ARRAY(\"language\") AS \"language\"\n"
                + "FROM kttm_data")
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1566691200031L, List.of("{}", "{}")},
            new Object[]{1566691200059L, List.of("en", "es", "es-419", "es-MX")},
            new Object[]{1566691200178L, List.of("en", "es", "es-419", "es-US")}
        ))
        .setExpectedMSQSpec(
            LegacyMSQSpec
                .builder()
                .query(expectedQuery)
                .columnMappings(new ColumnMappings(
                    ImmutableList.of(
                        new ColumnMapping("v0", "__time"),
                        new ColumnMapping("v1", "language")
                    )
                ))
                .tuningConfig(MSQTuningConfig.defaultConfig())
                .build())
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testGroupByWithLimit(String contextName, Map<String, Object> context)
  {
    RowSignature expectedResultSignature = RowSignature.builder()
                                                       .add("dim1", ColumnType.STRING)
                                                       .add("cnt", ColumnType.LONG)
                                                       .build();

    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(CalciteTests.DATASOURCE1)
                                     .setInterval(querySegmentSpec(Filtration.eternity()))
                                     .setGranularity(Granularities.ALL)
                                     .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                                     .setAggregatorSpecs(
                                         aggregators(
                                             new CountAggregatorFactory(
                                                 "a0"
                                             )
                                         )
                                     )
                                     .setDimFilter(not(equality("dim1", "", ColumnType.STRING)))
                                     .setLimit(1)
                                     .setContext(context)
                                     .build();

    testSelectQuery()
        .setSql("SELECT dim1, cnt FROM (SELECT dim1, COUNT(*) AS cnt FROM foo GROUP BY dim1 HAVING dim1 != '' LIMIT 1) LIMIT 20")
        .setExpectedMSQSpec(LegacyMSQSpec.builder()
                                   .query(query)
                                   .columnMappings(new ColumnMappings(ImmutableList.of(
                                       new ColumnMapping("d0", "dim1"),
                                       new ColumnMapping("a0", "cnt")
                                   )))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(isDurableStorageDestination(contextName, context)
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(expectedResultSignature)
        .setQueryContext(context)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{"1", 1L}
        )).verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testGroupByWithLimitAndOrdering(String contextName, Map<String, Object> context)
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("dim1", ColumnType.STRING)
                                            .add("count", ColumnType.LONG)
                                            .build();

    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(
                                         new ExternalDataSource(
                                             new InlineInputSource("dim1\nabc\nxyz\ndef\nxyz\nabc\nxyz\nabc\nxyz\ndef\nbbb\naaa"),
                                             new CsvInputFormat(null, null, null, true, 0, null),
                                             RowSignature.builder().add("dim1", ColumnType.STRING).build()
                                         )
                                     )
                                     .setInterval(querySegmentSpec(Filtration.eternity()))
                                     .setGranularity(Granularities.ALL)
                                     .addOrderByColumn(new OrderByColumnSpec("a0", OrderByColumnSpec.Direction.DESCENDING, StringComparators.NUMERIC))
                                     .addOrderByColumn(new OrderByColumnSpec("d0", OrderByColumnSpec.Direction.ASCENDING, StringComparators.LEXICOGRAPHIC))
                                     .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                                     .setAggregatorSpecs(
                                         aggregators(
                                             new CountAggregatorFactory(
                                                 "a0"
                                             )
                                         )
                                     )
                                     .setLimit(4)
                                     .setContext(context)
                                     .build();

    List<Object[]> expectedRows = ImmutableList.of(
        new Object[]{"xyz", 4L},
        new Object[]{"abc", 3L},
        new Object[]{"def", 2L},
        new Object[]{"aaa", 1L}
    );

    testSelectQuery()
        .setSql("WITH \"ext\" AS (\n"
                + "  SELECT *\n"
                + "  FROM TABLE(\n"
                + "    EXTERN(\n"
                + "      '{\"type\":\"inline\",\"data\":\"dim1\\nabc\\nxyz\\ndef\\nxyz\\nabc\\nxyz\\nabc\\nxyz\\ndef\\nbbb\\naaa\"}',\n"
                + "      '{\"type\":\"csv\",\"findColumnsFromHeader\":true}'\n"
                + "    )\n"
                + "  ) EXTEND (\"dim1\" VARCHAR)\n"
                + ")\n"
                + "SELECT\n"
                + "  \"dim1\",\n"
                + "  COUNT(*) AS \"count\"\n"
                + "FROM \"ext\"\n"
                + "GROUP BY 1\n"
                + "ORDER BY 2 DESC, 1\n"
                + "LIMIT 4\n")
        .setExpectedMSQSpec(LegacyMSQSpec.builder()
                                   .query(query)
                                   .columnMappings(new ColumnMappings(ImmutableList.of(
                                       new ColumnMapping("d0", "dim1"),
                                       new ColumnMapping("a0", "count")
                                   )))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(isDurableStorageDestination(contextName, context)
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().totalFiles(1),
            0, 0, "input0"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(5),
            0, 0, "output"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(4),
            1, 0, "shuffle"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(5),
            1, 0, "input0"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(5),
            1, 0, "output"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(4),
            2, 0, "input0"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(4),
            2, 0, "output"
        )
        .setExpectedCountersForStageWorkerChannel(
            CounterSnapshotMatcher
                .with().rows(!context.containsKey(MultiStageQueryContext.CTX_ROWS_PER_PAGE) ? new long[] {4} : new long[] {2, 2}),
            2, 0, "shuffle"
        )
        .setQueryContext(context)
        .setExpectedResultRows(expectedRows)
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testHavingOnApproximateCountDistinct(String contextName, Map<String, Object> context)
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
                                             range(
                                                 "a0",
                                                 ColumnType.LONG,
                                                 1L,
                                                 null,
                                                 true,
                                                 false
                                             )
                                         )
                                     )
                                     .setContext(context)
                                     .build();

    testSelectQuery()
        .setSql("SELECT dim2, COUNT(DISTINCT m1) as col FROM foo GROUP BY dim2 HAVING COUNT(DISTINCT m1) > 1")
        .setExpectedMSQSpec(LegacyMSQSpec.builder()
                                   .query(query)
                                   .columnMappings(new ColumnMappings(ImmutableList.of(
                                       new ColumnMapping("d0", "dim2"),
                                       new ColumnMapping("a0", "col")
                                   )))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(isDurableStorageDestination(contextName, context)
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setQueryContext(context)
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(
            ImmutableList.of(new Object[]{null, 2L}, new Object[]{"a", 2L})

        )
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testGroupByWithMultiValue(String contextName, Map<String, Object> context)
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
            LegacyMSQSpec.builder()
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
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(expectedMultiValueFooRowsGroup())
        .verifyResults();
  }


  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testGroupByWithMultiValueWithoutGroupByEnable(String contextName, Map<String, Object> context)
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

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testGroupByWithMultiValueMvToArray(String contextName, Map<String, Object> context)
  {
    Map<String, Object> localContext = enableMultiValueUnnesting(context, true);

    RowSignature rowSignature = RowSignature.builder()
                                            .add("EXPR$0", ColumnType.STRING_ARRAY)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    testSelectQuery()
        .setSql("select MV_TO_ARRAY(dim3), count(*) as cnt1 from foo group by dim3")
        .setQueryContext(localContext)
        .setExpectedMSQSpec(LegacyMSQSpec.builder()
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
                                                      .setAggregatorSpecs(
                                                          aggregators(new CountAggregatorFactory("a0"))
                                                      )
                                                      .setPostAggregatorSpecs(
                                                          expressionPostAgg(
                                                              "p0",
                                                              "mv_to_array(\"d0\")",
                                                              ColumnType.STRING_ARRAY
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
                                   .destination(isDurableStorageDestination(contextName, context)
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            expectedMultiValueFooRowsGroupByList()
        )
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testGroupByArrayWithMultiValueMvToArray(String contextName, Map<String, Object> context)
  {
    Map<String, Object> localContext = enableMultiValueUnnesting(context, true);

    RowSignature rowSignature = RowSignature.builder()
                                            .add("EXPR$0", ColumnType.STRING_ARRAY)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();

    List<Object[]> expected = ImmutableList.of(
        new Object[]{null, 2L},
        new Object[]{Collections.singletonList(""), 1L},
        new Object[]{Arrays.asList("a", "b"), 1L},
        new Object[]{Arrays.asList("b", "c"), 1L},
        new Object[]{Collections.singletonList("d"), 1L}
    );

    testSelectQuery()
        .setSql("select MV_TO_ARRAY(dim3), count(*) as cnt1 from foo group by MV_TO_ARRAY(dim3)")
        .setQueryContext(localContext)
        .setExpectedMSQSpec(LegacyMSQSpec.builder()
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
                                   .destination(isDurableStorageDestination(contextName, context)
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(expected)
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testTimeColumnAggregationFromExtern(String contextName, Map<String, Object> context) throws IOException
  {
    final File toRead = getResourceAsTemporaryFile("/wikipedia-sampled.json");
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
            new DruidExceptionMatcher(DruidException.Persona.ADMIN, DruidException.Category.INVALID_INPUT, "general")
                .expectMessageIs(
                    "Query could not be planned. A possible reason is "
                    + "[LATEST and EARLIEST aggregators implicitly depend on the __time column, "
                    + "but the table queried doesn't contain a __time column.  "
                    + "Please use LATEST_BY or EARLIEST_BY and specify the column explicitly.]"
                )
        )
        .setExpectedRowSignature(rowSignature)
        .verifyPlanningErrors();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testGroupByWithMultiValueMvToArrayWithoutGroupByEnable(String contextName, Map<String, Object> context)
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
        .setExpectedMetricDimensions(
            Map.of(
                DruidMetrics.DATASOURCE, "foo",
                DruidMetrics.INTERVAL, List.of(Intervals.ETERNITY.toString()),
                DruidMetrics.SUCCESS, false
            )
        )
        .verifyExecutionError();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testGroupByWithComplexColumnThrowsUnsupportedException(String contextName, Map<String, Object> context)
  {
    testSelectQuery()
        .setSql("select unique_dim1 from foo2 group by unique_dim1")
        .setQueryContext(context)
        .setExpectedExecutionErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(DruidException.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                "SQL requires a group-by on a column with type [COMPLEX<hyperUnique>] that is unsupported."))
        ))
        .verifyExecutionError();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testGroupByMultiValueMeasureQuery(String contextName, Map<String, Object> context)
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
                                notNull("dim3"),
                                "a0"
                            )
                        )
                    )
                    .setContext(context)
                    .build();

    testSelectQuery()
        .setSql("select __time, count(dim3) as cnt1 from foo group by __time")
        .setQueryContext(context)
        .setExpectedMSQSpec(LegacyMSQSpec.builder()
                                   .query(expectedQuery)
                                   .columnMappings(
                                       new ColumnMappings(ImmutableList.of(
                                           new ColumnMapping("d0", "__time"),
                                           new ColumnMapping("a0", "cnt1")
                                       )
                                       ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(isDurableStorageDestination(contextName, context)
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{946684800000L, 1L},
                new Object[]{946771200000L, 1L},
                new Object[]{946857600000L, 1L},
                new Object[]{978307200000L, 1L},
                new Object[]{978393600000L, 0L},
                new Object[]{978480000000L, 0L}
            )
        )
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testGroupByOnFooWithDurableStoragePathAssertions(String contextName, Map<String, Object> context) throws IOException
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("cnt", ColumnType.LONG)
                                            .add("cnt1", ColumnType.LONG)
                                            .build();


    testSelectQuery()
        .setSql("select cnt,count(*) as cnt1 from foo group by cnt")
        .setQueryContext(context)
        .setExpectedMSQSpec(LegacyMSQSpec.builder()
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
                                       ))
                                   )
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(isDurableStorageDestination(contextName, context)
                                                ? DurableStorageMSQDestination.INSTANCE
                                                : TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setExpectedRowSignature(rowSignature)
        .setExpectedResultRows(ImmutableList.of(new Object[]{1L, 6L}))
        .verifyResults();
    if (DURABLE_STORAGE.equals(contextName) || FAULT_TOLERANCE.equals(contextName)) {
      new File(
          localFileStorageDir,
          DurableStorageUtils.getWorkerOutputSuccessFilePath(TEST_CONTROLLER_TASK_ID, 0, 0)
      );

      Mockito.verify(localFileStorageConnector, Mockito.times(2))
             .write(ArgumentMatchers.endsWith("__success"));
    }
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectRowsGetUntruncatedByDefault(String contextName, Map<String, Object> context) throws IOException
  {
    RowSignature dummyRowSignature = RowSignature.builder().add("timestamp", ColumnType.LONG).build();

    final int numFiles = 200;

    final File toRead = getResourceAsTemporaryFile("/wikipedia-sampled.json");
    final String toReadFileNameAsJson = queryFramework().queryJsonMapper().writeValueAsString(toRead.getAbsolutePath());

    String externalFiles = String.join(", ", Collections.nCopies(numFiles, toReadFileNameAsJson));

    List<Object[]> result = new ArrayList<>();
    for (int i = 0; i < 3800; ++i) {
      result.add(new Object[]{1});
    }

    Assert.assertTrue(result.size() > Limits.MAX_SELECT_RESULT_ROWS);

    testSelectQuery()
        .setSql(StringUtils.format(
            " SELECT 1 as \"timestamp\"\n"
            + "FROM TABLE(\n"
            + "  EXTERN(\n"
            + "    '{ \"files\": [%s],\"type\":\"local\"}',\n"
            + "    '{\"type\": \"csv\", \"hasHeaderRow\": true}',\n"
            + "    '[{\"name\": \"timestamp\", \"type\": \"string\"}]'\n"
            + "  )\n"
            + ")",
            externalFiles
        ))
        .setExpectedRowSignature(dummyRowSignature)
        .setExpectedMSQSpec(
            LegacyMSQSpec
                .builder()
                .query(newScanQueryBuilder()
                           .dataSource(new ExternalDataSource(
                               new LocalInputSource(
                                   null,
                                   null,
                                   Collections.nCopies(numFiles, toRead),
                                   SystemFields.none()
                               ),
                               new CsvInputFormat(null, null, null, true, 0, null),
                               RowSignature.builder().add("timestamp", ColumnType.STRING).build()
                           ))
                           .intervals(querySegmentSpec(Filtration.eternity()))
                           .columns("v0")
                           .columnTypes(ColumnType.LONG)
                           .virtualColumns(new ExpressionVirtualColumn("v0", ExprEval.of(1L).toExpr(), ColumnType.LONG))
                           .context(context)
                           .build()
                )
                .columnMappings(new ColumnMappings(
                    ImmutableList.of(
                        new ColumnMapping("v0", "timestamp")
                    )
                ))
                .tuningConfig(MSQTuningConfig.defaultConfig())
                .destination(isDurableStorageDestination(contextName, context)
                             ? DurableStorageMSQDestination.INSTANCE
                             : TaskReportMSQDestination.INSTANCE)
                .build())
        .setQueryContext(context)
        .setExpectedResultRows(result)
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectUnnestOnInlineFoo(String contextName, Map<String, Object> context)
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("EXPR$0", ColumnType.LONG)
                                               .build();
    RowSignature outputSignature = RowSignature.builder()
                                               .add("d", ColumnType.LONG)
                                               .build();

    final ColumnMappings expectedColumnMappings = new ColumnMappings(
        ImmutableList.of(
            new ColumnMapping("EXPR$0", "d")
        )
    );

    testSelectQuery()
        .setSql("select d from UNNEST(ARRAY[1,2,3]) as unnested(d)")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(newScanQueryBuilder()
                              .dataSource(
                                  InlineDataSource.fromIterable(
                                      ImmutableList.of(
                                          new Object[]{1L},
                                          new Object[]{2L},
                                          new Object[]{3L}
                                      ),
                                      resultSignature
                                  )
                              )
                              .intervals(querySegmentSpec(Filtration.eternity()))
                              .columns("EXPR$0")
                              .columnTypes(ColumnType.LONG)
                              .context(context)
                              .build())
                   .columnMappings(expectedColumnMappings)
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
                   .build()
        )
        .setExpectedRowSignature(outputSignature)
        .setQueryContext(context)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1},
            new Object[]{2},
            new Object[]{3}
        ))
        .verifyResults();
  }


  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectUnnestOnFoo(String contextName, Map<String, Object> context)
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("j0.unnest", ColumnType.STRING)
                                               .build();

    RowSignature outputSignature = RowSignature.builder()
                                               .add("d3", ColumnType.STRING)
                                               .build();

    final ColumnMappings expectedColumnMappings = new ColumnMappings(
        ImmutableList.of(
            new ColumnMapping("j0.unnest", "d3")
        )
    );

    testSelectQuery()
        .setSql("SELECT d3 FROM foo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3)")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(newScanQueryBuilder()
                              .dataSource(UnnestDataSource.create(
                                  new TableDataSource(CalciteTests.DATASOURCE1),
                                  expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                                  null
                              ))
                              .intervals(querySegmentSpec(Filtration.eternity()))
                              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                              .context(context)
                              .columns("j0.unnest")
                              .columnTypes(ColumnType.STRING)
                              .build())
                   .columnMappings(expectedColumnMappings)
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
                   .build()
        )
        .setExpectedRowSignature(outputSignature)
        .setQueryContext(context)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{"a"},
                new Object[]{"b"},
                new Object[]{"b"},
                new Object[]{"c"},
                new Object[]{"d"},
                new Object[]{""},
                new Object[]{null},
                new Object[]{null}
            ))
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectUnnestOnQueryFoo(String contextName, Map<String, Object> context)
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("j0.unnest", ColumnType.STRING)
                                               .build();

    RowSignature resultSignature1 = RowSignature.builder()
                                                .add("dim3", ColumnType.STRING)
                                                .build();

    RowSignature outputSignature = RowSignature.builder()
                                               .add("d3", ColumnType.STRING)
                                               .build();

    final ColumnMappings expectedColumnMappings = new ColumnMappings(
        ImmutableList.of(
            new ColumnMapping("j0.unnest", "d3")
        )
    );

    testSelectQuery()
        .setSql("SELECT d3 FROM (select * from druid.foo where dim2='a' LIMIT 10), UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3)")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(newScanQueryBuilder()
                              .dataSource(UnnestDataSource.create(
                                  new QueryDataSource(
                                      newScanQueryBuilder()
                                          .dataSource(
                                              new TableDataSource(CalciteTests.DATASOURCE1)
                                          )
                                          .intervals(querySegmentSpec(Filtration.eternity()))
                                          .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                          .filters(equality("dim2", "a", ColumnType.STRING))
                                          .columns("dim3")
                                          .columnTypes(ColumnType.STRING)
                                          .context(context)
                                          .limit(10)
                                          .build()
                                  ),
                                  expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                                  null
                              ))
                              .intervals(querySegmentSpec(Filtration.eternity()))
                              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                              .context(context)
                              .columns("j0.unnest")
                              .columnTypes(ColumnType.STRING)
                              .build())
                   .columnMappings(expectedColumnMappings)
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
                   .build()
        )
        .setExpectedRowSignature(outputSignature)
        .setQueryContext(context)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{"a"},
                new Object[]{"b"},
                new Object[]{""}
            ))
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testQueryTimeout(String contextName, Map<String, Object> context)
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("m1", ColumnType.LONG)
                                               .add("dim2", ColumnType.STRING)
                                               .build();

    ImmutableMap<String, Object> timeoutContext = ImmutableMap.<String, Object>builder()
                                                              .putAll(context)
                                                              .put(QueryContexts.TIMEOUT_KEY, 1) // Trigger timeout
                                                              .build();

    testSelectQuery()
        .setSql("select m1,dim2 from foo2")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                         .query(newScanQueryBuilder()
                                    .dataSource(CalciteTests.DATASOURCE2)
                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                    .columns("m1", "dim2")
                                    .columnTypes(ColumnType.LONG, ColumnType.STRING)
                                    .context(timeoutContext)
                                    .build())
                         .columnMappings(ColumnMappings.identity(resultSignature))
                         .tuningConfig(MSQTuningConfig.defaultConfig())
                         .destination(isDurableStorageDestination(contextName, context)
                                      ? DurableStorageMSQDestination.INSTANCE
                                      : TaskReportMSQDestination.INSTANCE)
                         .build()
        )
        .setExpectedRowSignature(resultSignature)
        .setQueryContext(timeoutContext)
        .setExpectedMSQFault(CanceledFault.timeout())
        .setExpectedExecutionErrorMatcher(CoreMatchers.allOf(
            CoreMatchers.instanceOf(ISE.class),
            ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
                " Query canceled due to [Configured query timeout].")
            )
        ))
        .verifyExecutionError();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testSelectMetrics(String contextName, Map<String, Object> context)
  {
    RowSignature resultSignature = RowSignature.builder()
                                               .add("m1", ColumnType.LONG)
                                               .add("dim2", ColumnType.STRING)
                                               .build();

    testSelectQuery()
        .setSql("select m1,dim2 from foo2")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                         .query(newScanQueryBuilder()
                                    .dataSource(CalciteTests.DATASOURCE2)
                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                    .columns("m1", "dim2")
                                    .columnTypes(ColumnType.LONG, ColumnType.STRING)
                                    .context(context)
                                    .build())
                         .columnMappings(ColumnMappings.identity(resultSignature))
                         .tuningConfig(MSQTuningConfig.defaultConfig())
                         .destination(isDurableStorageDestination(contextName, context)
                                      ? DurableStorageMSQDestination.INSTANCE
                                      : TaskReportMSQDestination.INSTANCE)
                         .build()
        )
        .setExpectedRowSignature(resultSignature)
        .setQueryContext(context)
        .setExpectedResultRows(ImmutableList.of(
            new Object[]{1L, "en"},
            new Object[]{1L, "ru"},
            new Object[]{1L, "he"}
        ))
        .setExpectedMetricDimensions(
            Map.of(
                DruidMetrics.DATASOURCE, "foo2",
                DruidMetrics.INTERVAL, List.of(Intervals.ETERNITY.toString()),
                DruidMetrics.SUCCESS, true
            )
        )
        .verifyResults();
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testUnionAllUsingUnionDataSource(String contextName, Map<String, Object> context)
  {

    final RowSignature rowSignature = RowSignature.builder()
                                                  .add("__time", ColumnType.LONG)
                                                  .add("dim1", ColumnType.STRING)
                                                  .build();

    final List<Object[]> results = ImmutableList.of(
        new Object[]{946684800000L, ""},
        new Object[]{946684800000L, ""},
        new Object[]{946771200000L, "10.1"},
        new Object[]{946771200000L, "10.1"},
        new Object[]{946857600000L, "2"},
        new Object[]{946857600000L, "2"},
        new Object[]{978307200000L, "1"},
        new Object[]{978307200000L, "1"},
        new Object[]{978393600000L, "def"},
        new Object[]{978393600000L, "def"},
        new Object[]{978480000000L, "abc"},
        new Object[]{978480000000L, "abc"}
    );
    // This plans the query using DruidUnionDataSourceRule since the DruidUnionDataSourceRule#isCompatible
    // returns true (column names, types match, and it is a union on the table data sources).
    // It gets planned correctly, however MSQ engine cannot plan the query correctly
    testSelectQuery()
        .setSql("SELECT __time, dim1 FROM foo\n"
                + "UNION ALL\n"
                + "SELECT __time, dim1 FROM foo\n")
        .setExpectedRowSignature(rowSignature)
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(newScanQueryBuilder()
                              .dataSource(new UnionDataSource(
                                  ImmutableList.of(new TableDataSource("foo"), new TableDataSource("foo"))
                              ))
                              .intervals(querySegmentSpec(Filtration.eternity()))
                              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                              .context(context)
                              .columns("__time", "dim1")
                              .columnTypes(ColumnType.LONG, ColumnType.STRING)
                              .build())
                   .columnMappings(ColumnMappings.identity(rowSignature))
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
                   .build()
        )
        .setQueryContext(context)
        .setExpectedResultRows(results)
        .verifyResults();
  }

  private List<Object[]> expectedMultiValueFooRowsGroup()
  {
    ArrayList<Object[]> expected = new ArrayList<>();
    expected.add(new Object[]{null, 2L});
    expected.add(new Object[]{"", 1L});
    expected.addAll(ImmutableList.of(
        new Object[]{"a", 1L},
        new Object[]{"b", 2L},
        new Object[]{"c", 1L},
        new Object[]{"d", 1L}
    ));
    return expected;
  }

  private List<Object[]> expectedMultiValueFooRowsGroupByList()
  {
    return ImmutableList.of(
        new Object[]{null, 2L},
        new Object[]{Collections.singletonList(""), 1L},
        new Object[]{Collections.singletonList("a"), 1L},
        new Object[]{Collections.singletonList("b"), 2L},
        new Object[]{Collections.singletonList("c"), 1L},
        new Object[]{Collections.singletonList("d"), 1L}
    );
  }

  private static Map<String, Object> enableMultiValueUnnesting(Map<String, Object> context, boolean value)
  {
    Map<String, Object> localContext = ImmutableMap.<String, Object>builder()
                                                   .putAll(context)
                                                   .put("groupByEnableMultiValueUnnesting", value)
                                                   .build();
    return localContext;
  }

  private boolean isDurableStorageDestination(String contextName, Map<String, Object> context)
  {
    return QUERY_RESULTS_WITH_DURABLE_STORAGE.equals(contextName) || QUERY_RESULTS_WITH_DEFAULT_CONTEXT.equals(context);
  }

  public boolean isPageSizeLimited(String contextName)
  {
    return QUERY_RESULTS_WITH_DURABLE_STORAGE.equals(contextName);
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}:with context {0}")
  public void testTimeBoundaryGroupBy(String contextName, Map<String, Object> context)
  {
    final RowSignature rowSignature = RowSignature.builder()
                                                  .add("EXPR$0", ColumnType.LONG)
                                                  .add("EXPR$1", ColumnType.LONG)
                                                  .build();

    testSelectQuery()
        .setSql("SELECT MIN(__time), MAX(__time) FROM foo")
        .setExpectedMSQSpec(
            LegacyMSQSpec.builder()
                   .query(
                       GroupByQuery.builder()
                                  .setDataSource(CalciteTests.DATASOURCE1)
                                  .setInterval(querySegmentSpec(Filtration.eternity()))
                                  .setGranularity(Granularities.ALL)
                                  .setAggregatorSpecs(
                                      aggregators(
                                          new LongMinAggregatorFactory("a0", ColumnHolder.TIME_COLUMN_NAME),
                                          new LongMaxAggregatorFactory("a1", ColumnHolder.TIME_COLUMN_NAME)
                                      )
                                  )
                                  .setContext(context)
                                  .build()
                   )
                   .columnMappings(
                       new ColumnMappings(
                           ImmutableList.of(
                               new ColumnMapping("a0", "EXPR$0"),
                               new ColumnMapping("a1", "EXPR$1")
                           )
                       )
                   )
                   .tuningConfig(MSQTuningConfig.defaultConfig())
                   .destination(isDurableStorageDestination(contextName, context)
                                ? DurableStorageMSQDestination.INSTANCE
                                : TaskReportMSQDestination.INSTANCE)
                   .build()
        )
        .setExpectedRowSignature(rowSignature)
        .setQueryContext(context)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{
                    DateTimes.of("2000-01-01").getMillis(),
                    DateTimes.of("2001-01-03").getMillis()
                }
            )
        )
        .verifyResults();
  }
}
