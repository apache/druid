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

package org.apache.druid.query.aggregation.datasketches.hll.sql;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.java.util.common.StringEncoding;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.datasketches.SketchQueryContext;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchBuildAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchMergeAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchModule;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchToEstimatePostAggregator;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchToEstimateWithBoundsPostAggregator;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchToStringPostAggregator;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchUnionPostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.InvertedTopNMetricSpec;
import org.apache.druid.query.topn.NumericTopNMetricSpec;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.util.CacheTestHelperModule.ResultCacheMode;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.sql.guice.SqlModule;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class HllSketchSqlAggregatorTest extends BaseCalciteQueryTest
{
  private static final boolean ROUND = true;

  // For testHllSketchPostAggsGroupBy, testHllSketchPostAggsTimeseries
  private static final Object[] EXPECTED_PA_RESULT =
      new Object[]{
          "\"AgEHDAMIAgDhUv8P63iABQ==\"",
          "\"AgEHDAMIBgALpZ0PjpTfBY5ElQo+C7UE4jA+DKfcYQQ=\"",
          "\"AgEHDAMIAQAr8vsG\"",
          2.000000004967054d,
          3.000000004967054d,
          3.000000014901161d,
          2.000000004967054d,
          "[2.000000004967054,2.0,2.0001997319422404]",
          "[2.000000004967054,2.0,2.000099863468538]",
          "\"AgEHDAMIBgC1EYgH1mlHBwsKPwu5SK8MIiUxB7iZVwU=\"",
          2L,
          Joiner.on("\n").join(
              new Object[]{
                  "### HLL SKETCH SUMMARY: ",
                  "  Log Config K   : 12",
                  "  Hll Target     : HLL_4",
                  "  Current Mode   : LIST",
                  "  Memory         : false",
                  "  LB             : 2.0",
                  "  Estimate       : 2.000000004967054",
                  "  UB             : 2.000099863468538",
                  "  OutOfOrder Flag: false",
                  "  Coupon Count   : 2",
                  ""
              }
          ),
          Joiner.on("\n").join(
              new Object[]{
                  "### HLL SKETCH SUMMARY: ",
                  "  LOG CONFIG K   : 12",
                  "  HLL TARGET     : HLL_4",
                  "  CURRENT MODE   : LIST",
                  "  MEMORY         : FALSE",
                  "  LB             : 2.0",
                  "  ESTIMATE       : 2.000000004967054",
                  "  UB             : 2.000099863468538",
                  "  OUTOFORDER FLAG: FALSE",
                  "  COUPON COUNT   : 2",
                  ""
              }
          ),
          2.0,
          2L
      };

  /**
   * Expected virtual columns for {@link #testHllSketchPostAggsTimeseries()},
   * {@link #testHllSketchPostAggsGroupBy()}, {@link #testHllSketchFilteredAggregatorsTimeseries()}, and
   * {@link #testHllSketchFilteredAggregatorsGroupBy()}.
   */
  private static final List<VirtualColumn> EXPECTED_PA_VIRTUAL_COLUMNS =
      ImmutableList.of(
          new ExpressionVirtualColumn(
              "v0",
              "concat(\"dim2\",'hello')",
              ColumnType.STRING,
              TestExprMacroTable.INSTANCE
          ),
          new ExpressionVirtualColumn(
              "v1",
              "pow(abs((\"m1\" + 100)),2)",
              ColumnType.DOUBLE,
              TestExprMacroTable.INSTANCE
          )
      );

  /**
   * Expected aggregators for {@link #testHllSketchPostAggsTimeseries()} and {@link #testHllSketchPostAggsGroupBy()}.
   */
  private static final List<AggregatorFactory> EXPECTED_PA_AGGREGATORS =
      ImmutableList.of(
          new HllSketchBuildAggregatorFactory("a0", "dim2", null, null, null, false, true),
          new HllSketchBuildAggregatorFactory("a1", "m1", null, null, null, false, true),
          new HllSketchBuildAggregatorFactory("a2", "cnt", null, null, null, false, true),
          new HllSketchBuildAggregatorFactory("a3", "v0", null, null, null, false, true),
          new HllSketchBuildAggregatorFactory("a4", "v1", null, null, null, false, true),
          new HllSketchBuildAggregatorFactory("a5", "dim2", null, null, null, true, true),
          new HllSketchBuildAggregatorFactory("a6", "dim2", null, null, StringEncoding.UTF8, true, true)
      );

  /**
   * Expected aggregators for {@link #testHllSketchFilteredAggregatorsTimeseries()} and
   * {@link #testHllSketchFilteredAggregatorsGroupBy()}.
   */
  private static final List<AggregatorFactory> EXPECTED_FILTERED_AGGREGATORS =
      EXPECTED_PA_AGGREGATORS.stream()
                             .limit(5)
                             .map(factory -> new FilteredAggregatorFactory(
                                 factory,
                                 equality("dim2", "a", ColumnType.STRING)
                             ))
                             .collect(Collectors.toList());

  /**
   * Expected post-aggregators for {@link #testHllSketchPostAggsTimeseries()} and
   * {@link #testHllSketchPostAggsGroupBy()}.
   */
  private static final List<PostAggregator> EXPECTED_PA_POST_AGGREGATORS =
      ImmutableList.of(
          new HllSketchToEstimatePostAggregator("p1", new FieldAccessPostAggregator("p0", "a0"), false),
          new HllSketchToEstimatePostAggregator("p3", new FieldAccessPostAggregator("p2", "a0"), false),
          expressionPostAgg("p4", "(\"p3\" + 1)", ColumnType.DOUBLE),
          new HllSketchToEstimatePostAggregator("p6", new FieldAccessPostAggregator("p5", "a3"), false),
          new HllSketchToEstimatePostAggregator("p8", new FieldAccessPostAggregator("p7", "a0"), false),
          expressionPostAgg("p9", "abs(\"p8\")", ColumnType.DOUBLE),
          new HllSketchToEstimateWithBoundsPostAggregator("p11", new FieldAccessPostAggregator("p10", "a0"), 2),
          new HllSketchToEstimateWithBoundsPostAggregator("p13", new FieldAccessPostAggregator("p12", "a0"), 1),
          new HllSketchToStringPostAggregator("p15", new FieldAccessPostAggregator("p14", "a0")),
          new HllSketchToStringPostAggregator("p17", new FieldAccessPostAggregator("p16", "a0")),
          expressionPostAgg("p18", "upper(\"p17\")", ColumnType.STRING),
          new HllSketchToEstimatePostAggregator("p20", new FieldAccessPostAggregator("p19", "a0"), true)
      );

  /**
   * Expected post-aggregators for {@link #testHllSketchFilteredAggregatorsTimeseries()} and
   * {@link #testHllSketchFilteredAggregatorsGroupBy()}.
   */
  private static final List<PostAggregator> EXPECTED_FILTERED_POST_AGGREGATORS =
      ImmutableList.of(
          new HllSketchToEstimatePostAggregator("p1", new FieldAccessPostAggregator("p0", "a0"), false),
          new HllSketchToEstimatePostAggregator("p3", new FieldAccessPostAggregator("p2", "a1"), false),
          new HllSketchToEstimatePostAggregator("p5", new FieldAccessPostAggregator("p4", "a2"), false),
          new HllSketchToEstimatePostAggregator("p7", new FieldAccessPostAggregator("p6", "a3"), false),
          new HllSketchToEstimatePostAggregator("p9", new FieldAccessPostAggregator("p8", "a4"), false)
      );

  private static final ExprMacroTable MACRO_TABLE = new ExprMacroTable(
      ImmutableList.of(
          new HllPostAggExprMacros.HLLSketchEstimateExprMacro()
      )
  );


  @Override
  public void gatherProperties(Properties properties)
  {
    super.gatherProperties(properties);

    // Use APPROX_COUNT_DISTINCT_DS_HLL as APPROX_COUNT_DISTINCT impl for these tests.
    properties.put(SqlModule.PROPERTY_SQL_APPROX_COUNT_DISTINCT_CHOICE, HllSketchApproxCountDistinctSqlAggregator.NAME);
  }

  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
    builder.addModule(new HllSketchModule());
  }

  @SuppressWarnings("resource")
  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final JoinableFactoryWrapper joinableFactory,
      final Injector injector
  ) throws IOException
  {
    HllSketchModule.registerSerde();
    final QueryableIndex index = IndexBuilder
        .create()
        .tmpDir(temporaryFolder.newFolder())
        .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
        .schema(
            new IncrementalIndexSchema.Builder()
                .withMetrics(
                    new CountAggregatorFactory("cnt"),
                    new DoubleSumAggregatorFactory("m1", "m1"),
                    new HllSketchBuildAggregatorFactory("hllsketch_dim1", "dim1", null, null, null, false, ROUND),
                    new HllSketchBuildAggregatorFactory("hllsketch_dim3", "dim3", null, null, null, false, false),
                    new HllSketchBuildAggregatorFactory("hllsketch_m1", "m1", null, null, null, false, ROUND),
                    new HllSketchBuildAggregatorFactory("hllsketch_f1", "f1", null, null, null, false, ROUND),
                    new HllSketchBuildAggregatorFactory("hllsketch_l1", "l1", null, null, null, false, ROUND),
                    new HllSketchBuildAggregatorFactory("hllsketch_d1", "d1", null, null, null, false, ROUND)
                )
                .withRollup(false)
                .build()
        )
        .rows(TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS)
        .buildMMappedIndex();

    return SpecificSegmentsQuerySegmentWalker.createWalker(injector, conglomerate).add(
        DataSegment.builder()
                   .dataSource(CalciteTests.DATASOURCE1)
                   .interval(index.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        index
    );
  }

  @Test
  public void testApproxCountDistinctHllSketch()
  {
    // Can't vectorize due to SUBSTRING expression.
    cannotVectorize();

    final String sql = "SELECT\n"
                       + "  SUM(cnt),\n"
                       + "  APPROX_COUNT_DISTINCT_DS_HLL(dim2),\n" // uppercase
                       + "  APPROX_COUNT_DISTINCT_DS_HLL(dim2) FILTER(WHERE dim2 <> ''),\n" // lowercase; also, filtered
                       + "  APPROX_COUNT_DISTINCT(SUBSTRING(dim2, 1, 1)),\n" // on extractionFn, using generic A.C.D.
                       + "  COUNT(DISTINCT SUBSTRING(dim2, 1, 1) || 'x'),\n" // on expression, using COUNT DISTINCT
                       + "  APPROX_COUNT_DISTINCT_DS_HLL(hllsketch_dim1, 21, 'HLL_8'),\n" // on native HllSketch column
                       + "  APPROX_COUNT_DISTINCT_DS_HLL(hllsketch_dim1),\n" // on native HllSketch column
                       + "  APPROX_COUNT_DISTINCT_DS_HLL(hllsketch_dim1, CAST(21 AS BIGINT))\n" // also native column
                       + "FROM druid.foo";

    final List<Object[]> expectedResults;

    if (NullHandling.replaceWithDefault()) {
      expectedResults = ImmutableList.of(
          new Object[]{6L, 2L, 2L, 1L, 2L, 5L, 5L, 5L}
      );
    } else {
      expectedResults = ImmutableList.of(
          new Object[]{6L, 2L, 2L, 1L, 1L, 5L, 5L, 5L}
      );
    }

    testQuery(
        sql,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "substring(\"dim2\", 0, 1)",
                          ColumnType.STRING,
                          TestExprMacroTable.INSTANCE
                      ),
                      new ExpressionVirtualColumn(
                          "v1",
                          "concat(substring(\"dim2\", 0, 1),'x')",
                          ColumnType.STRING,
                          TestExprMacroTable.INSTANCE
                      )
                  )
                  .aggregators(
                      ImmutableList.of(
                          new LongSumAggregatorFactory("a0", "cnt"),
                          new HllSketchBuildAggregatorFactory("a1", "dim2", null, null, null, null, ROUND),
                          new FilteredAggregatorFactory(
                              new HllSketchBuildAggregatorFactory("a2", "dim2", null, null, null, null, ROUND),
                              not(equality("dim2", "", ColumnType.STRING))
                          ),
                          new HllSketchBuildAggregatorFactory("a3", "v0", null, null, null, null, ROUND),
                          new HllSketchBuildAggregatorFactory("a4", "v1", null, null, null, null, ROUND),
                          new HllSketchMergeAggregatorFactory("a5", "hllsketch_dim1", 21, "HLL_8", null, null, ROUND),
                          new HllSketchMergeAggregatorFactory("a6", "hllsketch_dim1", null, null, null, null, ROUND),
                          new HllSketchMergeAggregatorFactory("a7", "hllsketch_dim1", 21, "HLL_4", null, null, ROUND)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        expectedResults
    );
  }


  @Test
  public void testAvgDailyCountDistinctHllSketch()
  {
    // Can't vectorize due to outer query, which runs on an inline datasource.
    cannotVectorize();

    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            1.0
        }
    );

    testQuery(
        "SELECT\n"
        + "  AVG(u)\n"
        + "FROM ("
        + "  SELECT FLOOR(__time TO DAY), APPROX_COUNT_DISTINCT_DS_HLL(cnt) AS u\n"
        + "  FROM druid.foo\n"
        + "  GROUP BY 1\n"
        + ")",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                Druids.newTimeseriesQueryBuilder()
                                      .dataSource(CalciteTests.DATASOURCE1)
                                      .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(
                                          Filtration.eternity()
                                      )))
                                      .granularity(new PeriodGranularity(Period.days(1), null, DateTimeZone.UTC))
                                      .aggregators(
                                          Collections.singletonList(
                                              new HllSketchBuildAggregatorFactory(
                                                  "a0:a",
                                                  "cnt",
                                                  null,
                                                  null,
                                                  null,
                                                  null,
                                                  ROUND
                                              )
                                          )
                                      )
                                      .postAggregators(
                                          ImmutableList.of(
                                              new FinalizingFieldAccessPostAggregator("a0", "a0:a")
                                          )
                                      )
                                      .context(QUERY_CONTEXT_DEFAULT)
                                      .build()
                                      .withOverriddenContext(
                                          BaseCalciteQueryTest.getTimeseriesContextWithFloorTime(
                                              ImmutableMap.of(
                                                  TimeseriesQuery.SKIP_EMPTY_BUCKETS,
                                                  true,
                                                  BaseQuery.SQL_QUERY_ID,
                                                  "dummy"
                                              ),
                                              "d0"
                                          )
                                      )
                            )
                        )
                        .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(
                            NullHandling.replaceWithDefault()
                            ? Arrays.asList(
                                new DoubleSumAggregatorFactory("_a0:sum", "a0"),
                                new CountAggregatorFactory("_a0:count")
                            )
                            : Arrays.asList(
                                new DoubleSumAggregatorFactory("_a0:sum", "a0"),
                                new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("_a0:count"),
                                    notNull("a0")
                                )
                            )
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new ArithmeticPostAggregator(
                                    "_a0",
                                    "quotient",
                                    ImmutableList.of(
                                        new FieldAccessPostAggregator(null, "_a0:sum"),
                                        new FieldAccessPostAggregator(null, "_a0:count")
                                    )
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        expectedResults
    );
  }

  @Test
  public void testApproxCountDistinctHllSketchIsRounded()
  {
    testQuery(
        "SELECT"
        + "   dim2,"
        + "   APPROX_COUNT_DISTINCT_DS_HLL(m1)"
        + " FROM druid.foo"
        + " GROUP BY dim2"
        + " HAVING APPROX_COUNT_DISTINCT_DS_HLL(m1) = 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "_d0")))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(
                            aggregators(
                                new HllSketchBuildAggregatorFactory("a0", "m1", null, null, null, true, true)
                            )
                        )
                        .setHavingSpec(having(equality("a0", 2L, ColumnType.LONG)))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of(
            new Object[]{null, 2L},
            new Object[]{"a", 2L}
        )
        : ImmutableList.of(
            new Object[]{"a", 2L}
        )
    );
  }

  @Test
  public void testHllSketchFilteredAggregatorsGroupBy()
  {
    testQuery(
        "SELECT\n"
        + "  DS_HLL(dim2) FILTER(WHERE MV_CONTAINS(dim2, 'a')),\n"
        + "  DS_HLL(m1) FILTER(WHERE MV_CONTAINS(dim2, 'a')),\n"
        + "  DS_HLL(cnt) FILTER(WHERE MV_CONTAINS(dim2, 'a')),\n"
        + "  DS_HLL(CONCAT(dim2, 'hello')) FILTER(WHERE MV_CONTAINS(dim2, 'a')),\n"
        + "  DS_HLL(POWER(ABS(m1 + 100), 2)) FILTER(WHERE MV_CONTAINS(dim2, 'a')),\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(dim2) FILTER(WHERE MV_CONTAINS(dim2, 'a'))),\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(m1) FILTER(WHERE MV_CONTAINS(dim2, 'a'))),\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(cnt) FILTER(WHERE MV_CONTAINS(dim2, 'a'))),\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(CONCAT(dim2, 'hello')) FILTER(WHERE MV_CONTAINS(dim2, 'a'))),\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(POWER(ABS(m1 + 100), 2)) FILTER(WHERE MV_CONTAINS(dim2, 'a')))\n"
        + "FROM druid.foo\n"
        + "GROUP BY cnt",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(new MultipleIntervalSegmentSpec(Collections.singletonList(Filtration.eternity())))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(VirtualColumns.create(EXPECTED_PA_VIRTUAL_COLUMNS))
                        .setDimensions(new DefaultDimensionSpec("cnt", "_d0", ColumnType.LONG))
                        .setAggregatorSpecs(EXPECTED_FILTERED_AGGREGATORS)
                        .setPostAggregatorSpecs(EXPECTED_FILTERED_POST_AGGREGATORS)
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{
                "\"AgEHDAMIAQDhUv8P\"",
                "\"AgEHDAMIAgALpZ0PPgu1BA==\"",
                "\"AgEHDAMIAQAr8vsG\"",
                "\"AgEHDAMIAQCba0kG\"",
                "\"AgEHDAMIAgC1EYgHuUivDA==\"",
                1.0,
                2.000000004967054,
                1.0,
                1.0,
                2.000000004967054
            }
        )
    );
  }

  @Test
  public void testHllSketchFilteredAggregatorsTimeseries()
  {
    testQuery(
        "SELECT\n"
        + "  DS_HLL(dim2) FILTER(WHERE MV_CONTAINS(dim2, 'a')),\n"
        + "  DS_HLL(m1) FILTER(WHERE MV_CONTAINS(dim2, 'a')),\n"
        + "  DS_HLL(cnt) FILTER(WHERE MV_CONTAINS(dim2, 'a')),\n"
        + "  DS_HLL(CONCAT(dim2, 'hello')) FILTER(WHERE MV_CONTAINS(dim2, 'a')),\n"
        + "  DS_HLL(POWER(ABS(m1 + 100), 2)) FILTER(WHERE MV_CONTAINS(dim2, 'a')),\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(dim2) FILTER(WHERE MV_CONTAINS(dim2, 'a'))),\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(m1) FILTER(WHERE MV_CONTAINS(dim2, 'a'))),\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(cnt) FILTER(WHERE MV_CONTAINS(dim2, 'a'))),\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(CONCAT(dim2, 'hello')) FILTER(WHERE MV_CONTAINS(dim2, 'a'))),\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(POWER(ABS(m1 + 100), 2)) FILTER(WHERE MV_CONTAINS(dim2, 'a')))\n"
        + "FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .virtualColumns(VirtualColumns.create(EXPECTED_PA_VIRTUAL_COLUMNS))
                  .aggregators(EXPECTED_FILTERED_AGGREGATORS)
                  .postAggregators(EXPECTED_FILTERED_POST_AGGREGATORS)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                "\"AgEHDAMIAQDhUv8P\"",
                "\"AgEHDAMIAgALpZ0PPgu1BA==\"",
                "\"AgEHDAMIAQAr8vsG\"",
                "\"AgEHDAMIAQCba0kG\"",
                "\"AgEHDAMIAgC1EYgHuUivDA==\"",
                1.0,
                2.000000004967054,
                1.0,
                1.0,
                2.000000004967054
            }
        )
    );
  }

  @Test
  public void testHllSketchPostAggsGroupBy()
  {
    testQuery(
        "SELECT\n"
        + "  DS_HLL(dim2),\n"
        + "  DS_HLL(m1),\n"
        + "  DS_HLL(cnt),\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(dim2)),\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(dim2)) + 1,\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(CONCAT(dim2, 'hello'))),\n"
        + "  ABS(HLL_SKETCH_ESTIMATE(DS_HLL(dim2))),\n"
        + "  HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(DS_HLL(dim2), 2),\n"
        + "  HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(DS_HLL(dim2)),\n"
        + "  DS_HLL(POWER(ABS(m1 + 100), 2)),\n"
        + "  APPROX_COUNT_DISTINCT_DS_HLL(dim2),\n"
        + "  HLL_SKETCH_TO_STRING(DS_HLL(dim2)),\n"
        + "  UPPER(HLL_SKETCH_TO_STRING(DS_HLL(dim2))),\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(dim2), true)\n,"
        + "  APPROX_COUNT_DISTINCT_DS_HLL_UTF8(dim2)\n"
        + "FROM druid.foo\n"
        + "GROUP BY cnt",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(new MultipleIntervalSegmentSpec(Collections.singletonList(Filtration.eternity())))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(VirtualColumns.create(EXPECTED_PA_VIRTUAL_COLUMNS))
                        .setDimensions(new DefaultDimensionSpec("cnt", "_d0", ColumnType.LONG))
                        .setAggregatorSpecs(EXPECTED_PA_AGGREGATORS)
                        .setPostAggregatorSpecs(EXPECTED_PA_POST_AGGREGATORS)
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(EXPECTED_PA_RESULT)
    );
  }

  @Test
  public void testHllSketchPostAggsTimeseries()
  {
    testQuery(
        "SELECT\n"
        + "  DS_HLL(dim2),\n"
        + "  DS_HLL(m1),\n"
        + "  DS_HLL(cnt),\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(dim2)),\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(dim2)) + 1,\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(CONCAT(dim2, 'hello'))),\n"
        + "  ABS(HLL_SKETCH_ESTIMATE(DS_HLL(dim2))),\n"
        + "  HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(DS_HLL(dim2), 2),\n"
        + "  HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(DS_HLL(dim2)),\n"
        + "  DS_HLL(POWER(ABS(m1 + 100), 2)),\n"
        + "  APPROX_COUNT_DISTINCT_DS_HLL(dim2),\n"
        + "  HLL_SKETCH_TO_STRING(DS_HLL(dim2)),\n"
        + "  UPPER(HLL_SKETCH_TO_STRING(DS_HLL(dim2))),\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(dim2), true),\n"
        + "  APPROX_COUNT_DISTINCT_DS_HLL_UTF8(dim2)\n"
        + "FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .virtualColumns(VirtualColumns.create(EXPECTED_PA_VIRTUAL_COLUMNS))
                  .aggregators(EXPECTED_PA_AGGREGATORS)
                  .postAggregators(EXPECTED_PA_POST_AGGREGATORS)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(EXPECTED_PA_RESULT)
    );
  }

  @Test
  public void testHllSketchPostAggsFinalizeOuterSketches()
  {
    final ImmutableMap<String, Object> queryContext =
        ImmutableMap.<String, Object>builder()
                    .putAll(QUERY_CONTEXT_DEFAULT)
                    .put(SketchQueryContext.CTX_FINALIZE_OUTER_SKETCHES, true)
                    .build();

    final String sketchSummary = "### HLL SKETCH SUMMARY: \n"
                                 + "  Log Config K   : 12\n"
                                 + "  Hll Target     : HLL_4\n"
                                 + "  Current Mode   : LIST\n"
                                 + "  Memory         : false\n"
                                 + "  LB             : 2.0\n"
                                 + "  Estimate       : 2.000000004967054\n"
                                 + "  UB             : 2.000099863468538\n"
                                 + "  OutOfOrder Flag: false\n"
                                 + "  Coupon Count   : 2\n";

    final String otherSketchSummary = "### HLL SKETCH SUMMARY: \n"
                                      + "  LOG CONFIG K   : 12\n"
                                      + "  HLL TARGET     : HLL_4\n"
                                      + "  CURRENT MODE   : LIST\n"
                                      + "  MEMORY         : FALSE\n"
                                      + "  LB             : 2.0\n"
                                      + "  ESTIMATE       : 2.000000004967054\n"
                                      + "  UB             : 2.000099863468538\n"
                                      + "  OUTOFORDER FLAG: FALSE\n"
                                      + "  COUPON COUNT   : 2\n";
    testQuery(
        "SELECT\n"
        + "  DS_HLL(dim2),\n"
        + "  DS_HLL(m1),\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(dim2)),\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(dim2)) + 1,\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(CONCAT(dim2, 'hello'))),\n"
        + "  ABS(HLL_SKETCH_ESTIMATE(DS_HLL(dim2))),\n"
        + "  HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(DS_HLL(dim2), 2),\n"
        + "  HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(DS_HLL(dim2)),\n"
        + "  DS_HLL(POWER(ABS(m1 + 100), 2)),\n"
        + "  APPROX_COUNT_DISTINCT_DS_HLL(dim2),\n"
        + "  HLL_SKETCH_TO_STRING(DS_HLL(dim2)),\n"
        + "  UPPER(HLL_SKETCH_TO_STRING(DS_HLL(dim2))),\n"
        + "  HLL_SKETCH_ESTIMATE(DS_HLL(dim2), true)\n"
        + "FROM druid.foo",
        queryContext,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "concat(\"dim2\",'hello')",
                          ColumnType.STRING,
                          TestExprMacroTable.INSTANCE
                      ),
                      new ExpressionVirtualColumn(
                          "v1",
                          "pow(abs((\"m1\" + 100)),2)",
                          ColumnType.DOUBLE,
                          TestExprMacroTable.INSTANCE
                      )
                  )
                  .aggregators(
                      new HllSketchBuildAggregatorFactory("a0", "dim2", null, null, null, true, true),
                      new HllSketchBuildAggregatorFactory("a1", "m1", null, null, null, true, true),
                      new HllSketchBuildAggregatorFactory("a2", "v0", null, null, null, true, true),
                      new HllSketchBuildAggregatorFactory("a3", "v1", null, null, null, true, true),
                      new HllSketchBuildAggregatorFactory("a4", "dim2", null, null, null, true, true)
                  )
                  .postAggregators(
                      new HllSketchToEstimatePostAggregator("p1", new FieldAccessPostAggregator("p0", "a0"), false),
                      new HllSketchToEstimatePostAggregator("p3", new FieldAccessPostAggregator("p2", "a0"), false),
                      expressionPostAgg("p4", "(\"p3\" + 1)", ColumnType.DOUBLE),
                      new HllSketchToEstimatePostAggregator("p6", new FieldAccessPostAggregator("p5", "a2"), false),
                      new HllSketchToEstimatePostAggregator(
                          "p8",
                          new FieldAccessPostAggregator("p7", "a0"),
                          false
                      ),
                      expressionPostAgg("p9", "abs(\"p8\")", ColumnType.DOUBLE),
                      new HllSketchToEstimateWithBoundsPostAggregator(
                          "p11",
                          new FieldAccessPostAggregator("p10", "a0"),
                          2
                      ),
                      new HllSketchToEstimateWithBoundsPostAggregator(
                          "p13",
                          new FieldAccessPostAggregator("p12", "a0"),
                          1
                      ),
                      new HllSketchToStringPostAggregator("p15", new FieldAccessPostAggregator("p14", "a0")),
                      new HllSketchToStringPostAggregator("p17", new FieldAccessPostAggregator("p16", "a0")),
                      expressionPostAgg("p18", "upper(\"p17\")", ColumnType.STRING),
                      new HllSketchToEstimatePostAggregator("p20", new FieldAccessPostAggregator("p19", "a0"), true)
                  )
                  .context(queryContext)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                "2",
                "6",
                2.000000004967054d,
                3.000000004967054d,
                3.000000014901161d,
                2.000000004967054d,
                "[2.000000004967054,2.0,2.0001997319422404]",
                "[2.000000004967054,2.0,2.000099863468538]",
                "6",
                2L,
                sketchSummary,
                otherSketchSummary,
                2.0
            }
        )
    );
  }

  @Test
  public void testtHllSketchPostAggsPostSort()
  {
    final String sketchSummary = "### HLL SKETCH SUMMARY: \n"
                                 + "  Log Config K   : 12\n"
                                 + "  Hll Target     : HLL_4\n"
                                 + "  Current Mode   : LIST\n"
                                 + "  Memory         : false\n"
                                 + "  LB             : 2.0\n"
                                 + "  Estimate       : 2.000000004967054\n"
                                 + "  UB             : 2.000099863468538\n"
                                 + "  OutOfOrder Flag: false\n"
                                 + "  Coupon Count   : 2\n";

    final String sql = "SELECT DS_HLL(dim2) as y FROM druid.foo ORDER BY HLL_SKETCH_ESTIMATE(DS_HLL(dim2)) DESC LIMIT 10";

    testQuery(
        StringUtils.format("SELECT HLL_SKETCH_ESTIMATE(y), HLL_SKETCH_TO_STRING(y) from (%s)", sql),
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      ImmutableList.of(
                          new HllSketchBuildAggregatorFactory(
                              "a0",
                              "dim2",
                              null,
                              null,
                              null,
                              false,
                              true
                          )
                      )
                  )
                  .postAggregators(
                      ImmutableList.of(
                          new HllSketchToEstimatePostAggregator("p1", new FieldAccessPostAggregator("p0", "a0"), false),
                          new HllSketchToEstimatePostAggregator("s1", new FieldAccessPostAggregator("s0", "a0"), false),
                          new HllSketchToStringPostAggregator("s3", new FieldAccessPostAggregator("s2", "a0"))
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                2.000000004967054d,
                sketchSummary
            }
        )
    );
  }

  @Test
  public void testEmptyTimeseriesResults()
  {
    // timeseries with all granularity have a single group, so should return default results for given aggregators
    testQuery(
        "SELECT\n"
        + " APPROX_COUNT_DISTINCT_DS_HLL(dim2),\n"
        + " DS_HLL(dim2)\n"
        + "FROM druid.foo WHERE dim2 = 0",
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(CalciteTests.DATASOURCE1)
                               .intervals(querySegmentSpec(Filtration.eternity()))
                               .filters(numericEquality("dim2", 0L, ColumnType.LONG))
                               .granularity(Granularities.ALL)
                               .aggregators(
                                   aggregators(
                                       new HllSketchBuildAggregatorFactory(
                                           "a0",
                                           "dim2",
                                           null,
                                           null,
                                           null,
                                           null,
                                           true
                                       ),
                                       new HllSketchBuildAggregatorFactory(
                                           "a1",
                                           "dim2",
                                           null,
                                           null,
                                           null,
                                           false,
                                           true
                                       )
                                   )
                               )
                               .context(QUERY_CONTEXT_DEFAULT)
                               .build()),
        ImmutableList.of(new Object[]{0L, "\"AgEHDAMMAAA=\""})
    );
  }

  @Test
  public void testGroupByAggregatorDefaultValues()
  {
    testQuery(
        "SELECT\n"
        + "dim2,\n"
        + "APPROX_COUNT_DISTINCT_DS_HLL(dim2) FILTER(WHERE dim1 = 'nonexistent'),"
        + "DS_HLL(dim2) FILTER(WHERE dim1 = 'nonexistent')"
        + "FROM foo WHERE dim2 = 'a' GROUP BY dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(equality("dim2", "a", ColumnType.STRING))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "'a'", ColumnType.STRING))
                        .setDimensions(new DefaultDimensionSpec("v0", "_d0", ColumnType.STRING))
                        .setAggregatorSpecs(
                            aggregators(
                                new FilteredAggregatorFactory(
                                    new HllSketchBuildAggregatorFactory(
                                        "a0",
                                        "v0",
                                        null,
                                        null,
                                        null,
                                        null,
                                        true
                                    ),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new HllSketchBuildAggregatorFactory(
                                        "a1",
                                        "v0",
                                        null,
                                        null,
                                        null,
                                        false,
                                        true
                                    ),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(new Object[]{"a", 0L, "\"AgEHDAMMAAA=\""})
    );
  }

  @Test
  public void testGroupByAggregatorDefaultValuesFinalizeOuterSketches()
  {
    final ImmutableMap<String, Object> queryContext =
        ImmutableMap.<String, Object>builder()
                    .putAll(QUERY_CONTEXT_DEFAULT)
                    .put(SketchQueryContext.CTX_FINALIZE_OUTER_SKETCHES, true)
                    .build();

    testQuery(
        "SELECT\n"
        + "dim2,\n"
        + "APPROX_COUNT_DISTINCT_DS_HLL(dim2) FILTER(WHERE dim1 = 'nonexistent'),"
        + "DS_HLL(dim2) FILTER(WHERE dim1 = 'nonexistent')"
        + "FROM foo WHERE dim2 = 'a' GROUP BY dim2",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(equality("dim2", "a", ColumnType.STRING))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "'a'", ColumnType.STRING))
                        .setDimensions(new DefaultDimensionSpec("v0", "_d0", ColumnType.STRING))
                        .setAggregatorSpecs(
                            aggregators(
                                new FilteredAggregatorFactory(
                                    new HllSketchBuildAggregatorFactory("a0", "v0", null, null, null, null, true),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new HllSketchBuildAggregatorFactory("a1", "v0", null, null, null, null, true),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                )
                            )
                        )
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(new Object[]{"a", 0L, "0"})
    );
  }

  @Test
  public void testHllEstimateAsVirtualColumn()
  {
    testQuery(
        "SELECT"
        + " HLL_SKETCH_ESTIMATE(hllsketch_dim1),"
        + " HLL_SKETCH_ESTIMATE(hllsketch_d1),"
        + " HLL_SKETCH_ESTIMATE(hllsketch_l1),"
        + " HLL_SKETCH_ESTIMATE(hllsketch_f1)"
        + " FROM druid.foo",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    makeSketchEstimateExpression("v0", "hllsketch_dim1"),
                    makeSketchEstimateExpression("v1", "hllsketch_d1"),
                    makeSketchEstimateExpression("v2", "hllsketch_l1"),
                    makeSketchEstimateExpression("v3", "hllsketch_f1")
                )
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .columns("v0", "v1", "v2", "v3")
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{0.0D, 1.0D, 1.0D, 1.0D},
            new Object[]{1.0D, 1.0D, 1.0D, 1.0D},
            new Object[]{1.0D, 1.0D, 1.0D, 1.0D},
            new Object[]{1.0D, 0.0D, 0.0D, 0.0D},
            new Object[]{1.0D, 0.0D, 0.0D, 0.0D},
            new Object[]{1.0D, 0.0D, 0.0D, 0.0D}
        )
    );
  }

  @Test
  public void testHllEstimateAsVirtualColumnWithRound()
  {
    testQuery(
        "SELECT"
        + " HLL_SKETCH_ESTIMATE(hllsketch_dim3, FALSE), HLL_SKETCH_ESTIMATE(hllsketch_dim3, TRUE)"
        + " FROM druid.foo",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(new ExpressionVirtualColumn(
                    "v0",
                    "hll_sketch_estimate(\"hllsketch_dim3\",0)",
                    ColumnType.DOUBLE,
                    MACRO_TABLE
                ), new ExpressionVirtualColumn(
                    "v1",
                    "hll_sketch_estimate(\"hllsketch_dim3\",1)",
                    ColumnType.DOUBLE,
                    MACRO_TABLE
                ))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .columns("v0", "v1")
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{2.000000004967054D, 2.0D},
            new Object[]{2.000000004967054D, 2.0D},
            new Object[]{1.0D, 1.0D},
            new Object[]{0.0D, 0.0D},
            new Object[]{0.0D, 0.0D},
            new Object[]{0.0D, 0.0D}
        )
    );
  }

  @Test
  public void testHllEstimateAsVirtualColumnOnNonHllCol()
  {
    try {
      testQuery(
          "SELECT"
          + " HLL_SKETCH_ESTIMATE(dim2)"
          + " FROM druid.foo",
          ImmutableList.of(
              newScanQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(new ExpressionVirtualColumn(
                      "v0",
                      "hll_sketch_estimate(\"dim2\")",
                      ColumnType.DOUBLE,
                      MACRO_TABLE
                  ))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .columns("v0")
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
          ),
          ImmutableList.of()
      );
    }
    catch (IllegalArgumentException e) {
      Assert.assertTrue(
          e.getMessage().contains("Input byte[] should at least have 2 bytes for base64 bytes")
      );
    }
  }

  @Test
  public void testHllEstimateAsVirtualColumnWithGroupByOrderBy()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT"
        + " HLL_SKETCH_ESTIMATE(hllsketch_dim1), count(*)"
        + " FROM druid.foo"
        + " GROUP BY 1"
        + " ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(makeSketchEstimateExpression("v0", "hllsketch_dim1"))
                        .setDimensions(
                            new DefaultDimensionSpec("v0", "_d0", ColumnType.DOUBLE))
                        .setAggregatorSpecs(
                            aggregators(
                                new CountAggregatorFactory("a0")
                            )
                        )
                        .setLimitSpec(
                            DefaultLimitSpec
                                .builder()
                                .orderBy(
                                    new OrderByColumnSpec(
                                        "a0",
                                        OrderByColumnSpec.Direction.DESCENDING,
                                        StringComparators.NUMERIC
                                    )
                                )
                                .build()
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1.0D, 5L},
            new Object[]{0.0D, 1L}
        )
    );
  }

  @Test
  public void testHllEstimateAsVirtualColumnWithTopN()
  {
    testQuery(
        "SELECT"
        + " HLL_SKETCH_ESTIMATE(hllsketch_dim1), COUNT(*)"
        + " FROM druid.foo"
        + " GROUP BY 1 ORDER BY 2"
        + " LIMIT 2",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("v0", "_d0", ColumnType.DOUBLE))
                .virtualColumns(makeSketchEstimateExpression("v0", "hllsketch_dim1"))
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .threshold(2)
                .aggregators(new CountAggregatorFactory("a0"))
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{0.0D, 1L},
            new Object[]{1.0D, 5L}
        )
    );
  }

  @Test
  public void testHllWithOrderedWindowing()
  {
    testBuilder()
        .queryContext(ImmutableMap.of(PlannerContext.CTX_ENABLE_WINDOW_FNS, true))
        .sql(
            "SELECT dim1,coalesce(cast(l1 as integer),-999),"
                + " HLL_SKETCH_ESTIMATE( DS_HLL(dim1) OVER ( ORDER BY l1 ), true)"
                + " FROM druid.foo"
                + " WHERE length(dim1)>0"
        )
        .expectedResults(
            ImmutableList.of(
                new Object[] {"1", -999, 3.0D},
                new Object[] {"def", -999, 3.0D},
                new Object[] {"abc", -999, 3.0D},
                new Object[] {"2", 0, 4.0D},
                new Object[] {"10.1", 325323, 5.0D}
            )
        )
        .run();
  }

  @SqlTestFrameworkConfig(resultCache = ResultCacheMode.ENABLED)
  @Test
  public void testResultCacheWithWindowing()
  {
    cannotVectorize();
    skipVectorize();
    for (int i = 0; i < 2; i++) {
      testBuilder()
          .queryContext(ImmutableMap.of(PlannerContext.CTX_ENABLE_WINDOW_FNS, true))
          .sql(
              "SELECT "
                  + " TIME_FLOOR(__time, 'P1D') as dayLvl,\n"
                  + "  dim1,\n"
                  + "  HLL_SKETCH_ESTIMATE(DS_HLL(hllsketch_dim1,18,'HLL_4'), true),\n"
                  + "  HLL_SKETCH_ESTIMATE(DS_HLL(DS_HLL(hllsketch_dim1,18,'HLL_4'),18,'HLL_4') OVER (PARTITION BY dim1), true),"
                  + "  1\n"
                  + "FROM\n"
                  + "  (select * from  druid.foo ) ttt\n"
                  + "  WHERE  __time >= '1903-08-02' AND __time <= '2033-08-07'\n"
                  + "  and dim1 not like '%ikipedia' and l1 > -4\n"
                  + "  group by 1,2"
          )
          .expectedResults(
              ImmutableList.of(
                  new Object[] {946684800000L, "", 0.0D, 0.0D, 1},
                  new Object[] {946771200000L, "10.1", 1.0D, 1.0D, 1},
                  new Object[] {946857600000L, "2", 1.0D, 1.0D, 1}
              )
          )
          .run();
    }
  }

  /**
   * This is an extremely subtle test, so we explain with a comment.  The `m1` column in the input data looks like
   * `["1.0", "2.0", "3.0", "4.0", "5.0", "6.0"]` while the `d1` column looks like
   * `[1.0, 1.7, 0.0]`.  That is, "m1" is numbers-as-strings, while d1 is numbers-as-numbers.  If you take the
   * uniques across both columns, you expect no overlap, so 9 entries.  However, if the `1.0` from `d1` gets
   * converted into `"1.0"` or vice-versa, the result can become 8 because then the sketch will hash the same
   * value multiple times considering them duplicates.  This test validates that the aggregator properly builds
   * the sketches preserving the initial type of the data as it came in.  Specifically, the test was added when
   * a code change caused the 1.0 to get converted to a String such that the resulting value of the query was 8
   * instead of 9.
   */
  @Test
  public void testEstimateStringAndDoubleAreDifferent()
  {
    testQuery(
        "SELECT"
        + " HLL_SKETCH_ESTIMATE(HLL_SKETCH_UNION(DS_HLL(hllsketch_d1), DS_HLL(hllsketch_m1)), true)"
        + " FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      new HllSketchMergeAggregatorFactory("a0", "hllsketch_d1", null, null, null, false, true),
                      new HllSketchMergeAggregatorFactory("a1", "hllsketch_m1", null, null, null, false, true)
                  )
                  .postAggregators(
                      new HllSketchToEstimatePostAggregator(
                          "p3",
                          new HllSketchUnionPostAggregator(
                              "p2",
                              Arrays.asList(
                                  new FieldAccessPostAggregator("p0", "a0"),
                                  new FieldAccessPostAggregator("p1", "a1")
                              ),
                              null,
                              null
                          ),
                          true
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{9.0D}
        )
    );
  }

  /**
   * This is a test in a similar vein to {@link #testEstimateStringAndDoubleAreDifferent()} except here we are
   * ensuring that float values and doubles values are considered equivalent.  The expected initial inputs were
   * <p>
   * 1. d1 -> [1.0, 1.7, 0.0]
   * 2. f1 -> [1.0f, 0.1f, 0.0f]
   * <p>
   * If we assume that doubles and floats are the same, that means that there are 4 unique values, not 6
   */
  @Test
  public void testFloatAndDoubleAreConsideredTheSame()
  {
    // This is a test in a similar vein to testEstimateStringAndDoubleAreDifferent above
    testQuery(
        "SELECT"
        + " HLL_SKETCH_ESTIMATE(HLL_SKETCH_UNION(DS_HLL(hllsketch_d1), DS_HLL(hllsketch_f1)), true)"
        + " FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      new HllSketchMergeAggregatorFactory("a0", "hllsketch_d1", null, null, null, false, true),
                      new HllSketchMergeAggregatorFactory("a1", "hllsketch_f1", null, null, null, false, true)
                  )
                  .postAggregators(
                      new HllSketchToEstimatePostAggregator(
                          "p3",
                          new HllSketchUnionPostAggregator(
                              "p2",
                              Arrays.asList(
                                  new FieldAccessPostAggregator("p0", "a0"),
                                  new FieldAccessPostAggregator("p1", "a1")
                              ),
                              null,
                              null
                          ),
                          true
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{4.0D}
        )
    );
  }

  private ExpressionVirtualColumn makeSketchEstimateExpression(String outputName, String field)
  {
    return new ExpressionVirtualColumn(
        outputName,
        StringUtils.format("hll_sketch_estimate(\"%s\")", field),
        ColumnType.DOUBLE,
        MACRO_TABLE
    );
  }
}
