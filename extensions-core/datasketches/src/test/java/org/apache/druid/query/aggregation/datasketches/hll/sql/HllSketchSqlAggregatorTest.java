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

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchBuildAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchMergeAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchModule;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchToEstimatePostAggregator;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchToEstimateWithBoundsPostAggregator;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchToStringPostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.aggregation.ApproxCountDistinctSqlAggregator;
import org.apache.druid.sql.calcite.aggregation.builtin.CountSqlAggregator;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class HllSketchSqlAggregatorTest extends BaseCalciteQueryTest
{
  private static final boolean ROUND = true;

  @Override
  public Iterable<? extends Module> getJacksonModules()
  {
    return Iterables.concat(super.getJacksonModules(), new HllSketchModule().getJacksonModules());
  }

  @SuppressWarnings("resource")
  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
      QueryRunnerFactoryConglomerate conglomerate
  ) throws IOException
  {
    HllSketchModule.registerSerde();
    final QueryableIndex index = IndexBuilder.create()
                                             .tmpDir(temporaryFolder.newFolder())
                                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                             .schema(
                                                 new IncrementalIndexSchema.Builder()
                                                     .withMetrics(
                                                         new CountAggregatorFactory("cnt"),
                                                         new DoubleSumAggregatorFactory("m1", "m1"),
                                                         new HllSketchBuildAggregatorFactory(
                                                             "hllsketch_dim1",
                                                             "dim1",
                                                             null,
                                                             null,
                                                             ROUND
                                                         )
                                                     )
                                                     .withRollup(false)
                                                     .build()
                                             )
                                             .rows(TestDataBuilder.ROWS1)
                                             .buildMMappedIndex();

    return new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
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

  @Override
  public DruidOperatorTable createOperatorTable()
  {
    final HllSketchApproxCountDistinctSqlAggregator approxCountDistinctSqlAggregator =
        new HllSketchApproxCountDistinctSqlAggregator();

    return new DruidOperatorTable(
        ImmutableSet.of(
            approxCountDistinctSqlAggregator,
            new HllSketchObjectSqlAggregator(),

            // Use APPROX_COUNT_DISTINCT_DS_HLL as APPROX_COUNT_DISTINCT impl for these tests.
            new CountSqlAggregator(new ApproxCountDistinctSqlAggregator(approxCountDistinctSqlAggregator)),
            new ApproxCountDistinctSqlAggregator(approxCountDistinctSqlAggregator)
        ),
        ImmutableSet.of(
            new HllSketchSetUnionOperatorConversion(),
            new HllSketchEstimateOperatorConversion(),
            new HllSketchToStringOperatorConversion(),
            new HllSketchEstimateWithErrorBoundsOperatorConversion()
        )
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
                       + "  APPROX_COUNT_DISTINCT_DS_HLL(hllsketch_dim1)\n" // on native HllSketch column
                       + "FROM druid.foo";

    final List<Object[]> expectedResults;

    if (NullHandling.replaceWithDefault()) {
      expectedResults = ImmutableList.of(
          new Object[]{6L, 2L, 2L, 1L, 2L, 5L, 5L}
      );
    } else {
      expectedResults = ImmutableList.of(
          new Object[]{6L, 2L, 2L, 1L, 1L, 5L, 5L}
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
                          new HllSketchBuildAggregatorFactory(
                              "a1",
                              "dim2",
                              null,
                              null,
                              ROUND
                          ),
                          new FilteredAggregatorFactory(
                              new HllSketchBuildAggregatorFactory(
                                  "a2",
                                  "dim2",
                                  null,
                                  null,
                                  ROUND
                              ),
                              BaseCalciteQueryTest.not(BaseCalciteQueryTest.selector("dim2", "", null))
                          ),
                          new HllSketchBuildAggregatorFactory(
                              "a3",
                              "v0",
                              null,
                              null,
                              ROUND
                          ),
                          new HllSketchBuildAggregatorFactory(
                              "a4",
                              "v1",
                              null,
                              null,
                              ROUND
                          ),
                          new HllSketchMergeAggregatorFactory("a5", "hllsketch_dim1", 21, "HLL_8", ROUND),
                          new HllSketchMergeAggregatorFactory("a6", "hllsketch_dim1", null, null, ROUND)
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
            1L
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
                                new LongSumAggregatorFactory("_a0:sum", "a0"),
                                new CountAggregatorFactory("_a0:count")
                            )
                            : Arrays.asList(
                                new LongSumAggregatorFactory("_a0:sum", "a0"),
                                new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("_a0:count"),
                                    BaseCalciteQueryTest.not(BaseCalciteQueryTest.selector("a0", null, null))
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
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(
                            aggregators(
                                new HllSketchBuildAggregatorFactory("a0", "m1", null, null, true)
                            )
                        )
                        .setHavingSpec(having(selector("a0", "2", null)))
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
  public void testHllSketchPostAggs()
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
                      ImmutableList.of(
                          new HllSketchBuildAggregatorFactory(
                              "a0",
                              "dim2",
                              null,
                              null,
                              true
                          ),
                          new HllSketchBuildAggregatorFactory(
                              "a1",
                              "m1",
                              null,
                              null,
                              true
                          ),
                          new HllSketchBuildAggregatorFactory(
                              "a2",
                              "v0",
                              null,
                              null,
                              true
                          ),
                          new HllSketchBuildAggregatorFactory(
                              "a3",
                              "v1",
                              null,
                              null,
                              true
                          ),
                          new HllSketchBuildAggregatorFactory(
                              "a4",
                              "dim2",
                              null,
                              null,
                              true
                          )
                      )
                  )
                  .postAggregators(
                      ImmutableList.of(
                          new FieldAccessPostAggregator("p0", "a0"),
                          new FieldAccessPostAggregator("p1", "a1"),
                          new HllSketchToEstimatePostAggregator("p3", new FieldAccessPostAggregator("p2", "a0"), false),
                          new HllSketchToEstimatePostAggregator("p5", new FieldAccessPostAggregator("p4", "a0"), false),
                          new ExpressionPostAggregator("p6", "(\"p5\" + 1)", null, TestExprMacroTable.INSTANCE),
                          new HllSketchToEstimatePostAggregator("p8", new FieldAccessPostAggregator("p7", "a2"), false),
                          new HllSketchToEstimatePostAggregator(
                              "p10",
                              new FieldAccessPostAggregator("p9", "a0"),
                              false
                          ),
                          new ExpressionPostAggregator("p11", "abs(\"p10\")", null, TestExprMacroTable.INSTANCE),
                          new HllSketchToEstimateWithBoundsPostAggregator(
                              "p13",
                              new FieldAccessPostAggregator("p12", "a0"),
                              2
                          ),
                          new HllSketchToEstimateWithBoundsPostAggregator(
                              "p15",
                              new FieldAccessPostAggregator("p14", "a0"),
                              1
                          ),
                          new FieldAccessPostAggregator("p16", "a3"),
                          new HllSketchToStringPostAggregator("p18", new FieldAccessPostAggregator("p17", "a0")),
                          new HllSketchToStringPostAggregator("p20", new FieldAccessPostAggregator("p19", "a0")),
                          new ExpressionPostAggregator("p21", "upper(\"p20\")", null, TestExprMacroTable.INSTANCE),
                          new HllSketchToEstimatePostAggregator("p23", new FieldAccessPostAggregator("p22", "a0"), true)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                "\"AgEHDAMIAgDhUv8P63iABQ==\"",
                "\"AgEHDAMIBgALpZ0PjpTfBY5ElQo+C7UE4jA+DKfcYQQ=\"",
                2.000000004967054d,
                3.000000004967054d,
                3.000000014901161d,
                2.000000004967054d,
                "[2.000000004967054,2.0,2.0001997319422404]",
                "[2.000000004967054,2.0,2.000099863468538]",
                "\"AgEHDAMIBgC1EYgH1mlHBwsKPwu5SK8MIiUxB7iZVwU=\"",
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
                              true
                          )
                      )
                  )
                  .postAggregators(
                      ImmutableList.of(
                          new FieldAccessPostAggregator("p0", "a0"),
                          new HllSketchToEstimatePostAggregator("p2", new FieldAccessPostAggregator("p1", "a0"), false),
                          new HllSketchToEstimatePostAggregator("s1", new FieldAccessPostAggregator("s0", "p0"), false),
                          new HllSketchToStringPostAggregator("s3", new FieldAccessPostAggregator("s2", "p0"))
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
                               .filters(bound("dim2", "0", "0", false, false, null, StringComparators.NUMERIC))
                               .granularity(Granularities.ALL)
                               .aggregators(
                                   aggregators(
                                       new HllSketchBuildAggregatorFactory(
                                           "a0",
                                           "dim2",
                                           null,
                                           null,
                                           true
                                       ),
                                       new HllSketchBuildAggregatorFactory(
                                           "a1",
                                           "dim2",
                                           null,
                                           null,
                                           true
                                       )
                                   )
                               )
                               .context(QUERY_CONTEXT_DEFAULT)
                               .build()),
        ImmutableList.of(new Object[]{0L, "0"})
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
                        .setDimFilter(selector("dim2", "a", null))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "'a'", ColumnType.STRING))
                        .setDimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.STRING))
                        .setAggregatorSpecs(
                            aggregators(
                                new FilteredAggregatorFactory(
                                    new HllSketchBuildAggregatorFactory(
                                        "a0",
                                        "v0",
                                        null,
                                        null,
                                        true
                                    ),
                                    selector("dim1", "nonexistent", null)
                                ),
                                new FilteredAggregatorFactory(
                                    new HllSketchBuildAggregatorFactory(
                                        "a1",
                                        "v0",
                                        null,
                                        null,
                                        true
                                    ),
                                    selector("dim1", "nonexistent", null)
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(new Object[]{"a", 0L, "0"})
    );
  }
}
