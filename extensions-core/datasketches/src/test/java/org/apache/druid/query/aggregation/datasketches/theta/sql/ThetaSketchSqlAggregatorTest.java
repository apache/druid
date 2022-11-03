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

package org.apache.druid.query.aggregation.datasketches.theta.sql;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.theta.SketchEstimatePostAggregator;
import org.apache.druid.query.aggregation.datasketches.theta.SketchMergeAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.theta.SketchModule;
import org.apache.druid.query.aggregation.datasketches.theta.SketchSetPostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
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

public class ThetaSketchSqlAggregatorTest extends BaseCalciteQueryTest
{
  private static final String DATA_SOURCE = "foo";

  @Override
  public Iterable<? extends Module> getJacksonModules()
  {
    return Iterables.concat(super.getJacksonModules(), new SketchModule().getJacksonModules());
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
      QueryRunnerFactoryConglomerate conglomerate
  ) throws IOException
  {
    SketchModule.registerSerde();

    final QueryableIndex index = IndexBuilder.create()
                                             .tmpDir(temporaryFolder.newFolder())
                                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                             .schema(
                                                 new IncrementalIndexSchema.Builder()
                                                     .withMetrics(
                                                         new CountAggregatorFactory("cnt"),
                                                         new DoubleSumAggregatorFactory("m1", "m1"),
                                                         new SketchMergeAggregatorFactory(
                                                             "thetasketch_dim1",
                                                             "dim1",
                                                             null,
                                                             false,
                                                             false,
                                                             null
                                                         )
                                                     )
                                                     .withRollup(false)
                                                     .build()
                                             )
                                             .rows(TestDataBuilder.ROWS1)
                                             .buildMMappedIndex();

    return new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
        DataSegment.builder()
                   .dataSource(DATA_SOURCE)
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
    final ThetaSketchApproxCountDistinctSqlAggregator approxCountDistinctSqlAggregator =
        new ThetaSketchApproxCountDistinctSqlAggregator();

    return new DruidOperatorTable(
        ImmutableSet.of(
            new ThetaSketchApproxCountDistinctSqlAggregator(),
            new ThetaSketchObjectSqlAggregator(),

            // Use APPROX_COUNT_DISTINCT_DS_THETA as APPROX_COUNT_DISTINCT impl for these tests.
            new CountSqlAggregator(new ApproxCountDistinctSqlAggregator(approxCountDistinctSqlAggregator)),
            new ApproxCountDistinctSqlAggregator(approxCountDistinctSqlAggregator)
        ),
        ImmutableSet.of(
            new ThetaSketchEstimateOperatorConversion(),
            new ThetaSketchEstimateWithErrorBoundsOperatorConversion(),
            new ThetaSketchSetIntersectOperatorConversion(),
            new ThetaSketchSetUnionOperatorConversion(),
            new ThetaSketchSetNotOperatorConversion()
        )
    );
  }

  @Test
  public void testApproxCountDistinctThetaSketch()
  {
    // Cannot vectorize due to SUBSTRING.
    cannotVectorize();

    final String sql = "SELECT\n"
                       + "  SUM(cnt),\n"
                       + "  APPROX_COUNT_DISTINCT_DS_THETA(dim2),\n"
                       // uppercase
                       + "  APPROX_COUNT_DISTINCT_DS_THETA(dim2) FILTER(WHERE dim2 <> ''),\n"
                       // lowercase; also, filtered
                       + "  APPROX_COUNT_DISTINCT(SUBSTRING(dim2, 1, 1)),\n"
                       // on extractionFn, using A.C.D.
                       + "  COUNT(DISTINCT SUBSTRING(dim2, 1, 1) || 'x'),\n"
                       // on expression, using COUNT DISTINCT
                       + "  APPROX_COUNT_DISTINCT_DS_THETA(thetasketch_dim1, 32768),\n"
                       // on native theta sketch column
                       + "  APPROX_COUNT_DISTINCT_DS_THETA(thetasketch_dim1)\n"
                       // on native theta sketch column
                       + "FROM druid.foo";

    final List<Object[]> expectedResults;

    if (NullHandling.replaceWithDefault()) {
      expectedResults = ImmutableList.of(
          new Object[]{
              6L,
              2L,
              2L,
              1L,
              2L,
              5L,
              5L
          }
      );
    } else {
      expectedResults = ImmutableList.of(
          new Object[]{
              6L,
              2L,
              2L,
              1L,
              1L,
              5L,
              5L
          }
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
                          new SketchMergeAggregatorFactory(
                              "a1",
                              "dim2",
                              null,
                              null,
                              null,
                              null
                          ),
                          new FilteredAggregatorFactory(
                              new SketchMergeAggregatorFactory(
                                  "a2",
                                  "dim2",
                                  null,
                                  null,
                                  null,
                                  null
                              ),
                              BaseCalciteQueryTest.not(BaseCalciteQueryTest.selector("dim2", "", null))
                          ),
                          new SketchMergeAggregatorFactory(
                              "a3",
                              "v0",
                              null,
                              null,
                              null,
                              null
                          ),
                          new SketchMergeAggregatorFactory(
                              "a4",
                              "v1",
                              null,
                              null,
                              null,
                              null
                          ),
                          new SketchMergeAggregatorFactory("a5", "thetasketch_dim1", 32768, null, null, null),
                          new SketchMergeAggregatorFactory("a6", "thetasketch_dim1", null, null, null, null)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        expectedResults
    );
  }

  @Test
  public void testAvgDailyCountDistinctThetaSketch()
  {
    // Can't vectorize due to outer query (it operates on an inlined data source, which cannot be vectorized).
    cannotVectorize();

    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            1L
        }
    );

    testQuery(
        "SELECT\n"
        + "  AVG(u)\n"
        + "FROM (SELECT FLOOR(__time TO DAY), APPROX_COUNT_DISTINCT_DS_THETA(cnt) AS u FROM druid.foo GROUP BY 1)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(Druids.newTimeseriesQueryBuilder()
                                                      .dataSource(CalciteTests.DATASOURCE1)
                                                      .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(
                                                          Filtration.eternity()
                                                      )))
                                                      .granularity(new PeriodGranularity(
                                                          Period.days(1),
                                                          null,
                                                          DateTimeZone.UTC
                                                      ))
                                                      .aggregators(
                                                          Collections.singletonList(
                                                              new SketchMergeAggregatorFactory(
                                                                  "a0:a",
                                                                  "cnt",
                                                                  null,
                                                                  null,
                                                                  null,
                                                                  null
                                                              )
                                                          )
                                                      )
                                                      .postAggregators(
                                                          ImmutableList.of(
                                                              new FinalizingFieldAccessPostAggregator(
                                                                  "a0",
                                                                  "a0:a"
                                                              )
                                                          )
                                                      )
                                                      .context(TIMESERIES_CONTEXT_BY_GRAN)
                                                      .build()
                                                      .withOverriddenContext(
                                                          BaseCalciteQueryTest.getTimeseriesContextWithFloorTime(
                                                              TIMESERIES_CONTEXT_BY_GRAN,
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
  public void testThetaSketchPostAggs()
  {
    final List<Object[]> expectedResults;

    if (NullHandling.replaceWithDefault()) {
      expectedResults = ImmutableList.of(
          new Object[]{
              6L,
              2.0d,
              3.0d,
              "{\"estimate\":2.0,\"highBound\":2.0,\"lowBound\":2.0,\"numStdDev\":10}",
              "\"AQMDAAA6zJOQxkPsNomrZQ==\"",
              "\"AgMDAAAazJMGAAAAAACAP1XTBztMIcMJ+HOoBBne1zKQxkPsNomrZUeWbJt3n+VpF8EdUoUHAXvxsLkOSE0lfQ==\"",
              "\"AQMDAAA6zJMXwR1ShQcBew==\"",
              "\"AQMDAAA6zJOQxkPsNomrZQ==\"",
              1.0d
          }
      );
    } else {
      expectedResults = ImmutableList.of(
          new Object[]{
              6L,
              2.0d,
              3.0d,
              "{\"estimate\":2.0,\"highBound\":2.0,\"lowBound\":2.0,\"numStdDev\":10}",
              "\"AQMDAAA6zJOQxkPsNomrZQ==\"",
              "\"AgMDAAAazJMGAAAAAACAP1XTBztMIcMJ+HOoBBne1zKQxkPsNomrZUeWbJt3n+VpF8EdUoUHAXvxsLkOSE0lfQ==\"",
              "\"AQMDAAA6zJMXwR1ShQcBew==\"",
              "\"AQMDAAA6zJOQxkPsNomrZQ==\"",
              1.0d
          }
      );
    }

    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  theta_sketch_estimate(DS_THETA(dim2)),\n"
        + "  theta_sketch_estimate(DS_THETA(CONCAT(dim2, 'hello'))),\n"
        + "  theta_sketch_estimate_with_error_bounds(DS_THETA(dim2), 10),\n"
        + "  THETA_SKETCH_INTERSECT(DS_THETA(dim2), DS_THETA(dim1)),\n"
        + "  THETA_SKETCH_UNION(DS_THETA(dim2), DS_THETA(dim1)),\n"
        + "  THETA_SKETCH_NOT(DS_THETA(dim2), DS_THETA(dim1)),\n"
        + "  THETA_SKETCH_INTERSECT(32768, DS_THETA(dim2), DS_THETA(dim1)),\n"
        + "  theta_sketch_estimate(THETA_SKETCH_INTERSECT(THETA_SKETCH_INTERSECT(DS_THETA(dim2), DS_THETA(dim1)), DS_THETA(dim2)))\n"
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
                      )
                  )
                  .aggregators(
                      ImmutableList.of(
                          new LongSumAggregatorFactory("a0", "cnt"),
                          new SketchMergeAggregatorFactory(
                              "a1",
                              "dim2",
                              null,
                              null,
                              null,
                              null
                          ),
                          new SketchMergeAggregatorFactory(
                              "a2",
                              "v0",
                              null,
                              null,
                              null,
                              null
                          ),
                          new SketchMergeAggregatorFactory(
                              "a3",
                              "dim1",
                              null,
                              null,
                              null,
                              null
                          )
                      )
                  )
                  .postAggregators(
                      new SketchEstimatePostAggregator(
                          "p1",
                          new FieldAccessPostAggregator("p0", "a1"),
                          null
                      ),
                      new SketchEstimatePostAggregator(
                          "p3",
                          new FieldAccessPostAggregator("p2", "a2"),
                          null
                      ),
                      new SketchEstimatePostAggregator(
                          "p5",
                          new FieldAccessPostAggregator("p4", "a1"),
                          10
                      ),
                      new SketchSetPostAggregator(
                          "p8",
                          "INTERSECT",
                          null,
                          ImmutableList.of(
                              new FieldAccessPostAggregator("p6", "a1"),
                              new FieldAccessPostAggregator("p7", "a3")
                          )
                      ),
                      new SketchSetPostAggregator(
                          "p11",
                          "UNION",
                          null,
                          ImmutableList.of(
                              new FieldAccessPostAggregator("p9", "a1"),
                              new FieldAccessPostAggregator("p10", "a3")
                          )
                      ),
                      new SketchSetPostAggregator(
                          "p14",
                          "NOT",
                          null,
                          ImmutableList.of(
                              new FieldAccessPostAggregator("p12", "a1"),
                              new FieldAccessPostAggregator("p13", "a3")
                          )
                      ),
                      new SketchSetPostAggregator(
                          "p17",
                          "INTERSECT",
                          32768,
                          ImmutableList.of(
                              new FieldAccessPostAggregator("p15", "a1"),
                              new FieldAccessPostAggregator("p16", "a3")
                          )
                      ),
                      new SketchEstimatePostAggregator(
                          "p23",
                          new SketchSetPostAggregator(
                              "p22",
                              "INTERSECT",
                              null,
                              ImmutableList.of(
                                  new SketchSetPostAggregator(
                                      "p20",
                                      "INTERSECT",
                                      null,
                                      ImmutableList.of(
                                          new FieldAccessPostAggregator("p18", "a1"),
                                          new FieldAccessPostAggregator("p19", "a3")
                                      )
                                  ),
                                  new FieldAccessPostAggregator("p21", "a1")
                              )
                          ),
                          null
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        expectedResults
    );
  }

  @Test
  public void testThetaSketchPostAggsPostSort()
  {
    final String sql = "SELECT DS_THETA(dim2) as y FROM druid.foo ORDER BY THETA_SKETCH_ESTIMATE(DS_THETA(dim2)) DESC LIMIT 10";

    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            2.0d
        }
    );

    testQuery(
        StringUtils.format("SELECT THETA_SKETCH_ESTIMATE(y) from (%s)", sql),
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(CalciteTests.DATASOURCE1)
                               .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                               .granularity(Granularities.ALL)
                               .aggregators(
                                   ImmutableList.of(
                                       new SketchMergeAggregatorFactory(
                                           "a0",
                                           "dim2",
                                           null,
                                           null,
                                           null,
                                           null
                                       )
                                   )
                               )
                               .postAggregators(
                                   new FieldAccessPostAggregator("p0", "a0"),
                                   new SketchEstimatePostAggregator(
                                       "p2",
                                       new FieldAccessPostAggregator("p1", "a0"),
                                       null
                                   ),
                                   new SketchEstimatePostAggregator(
                                       "s1",
                                       new FieldAccessPostAggregator("s0", "p0"),
                                       null
                                   )
                               )
                               .context(QUERY_CONTEXT_DEFAULT)
                               .build()

        ),
        expectedResults
    );
  }

  @Test
  public void testEmptyTimeseriesResults()
  {
    testQuery(
        "SELECT\n"
        + "  APPROX_COUNT_DISTINCT_DS_THETA(dim2),\n"
        + "  APPROX_COUNT_DISTINCT_DS_THETA(thetasketch_dim1),\n"
        + "  DS_THETA(dim2, 1024),\n"
        + "  DS_THETA(thetasketch_dim1, 1024)\n"
        + "FROM druid.foo WHERE dim2 = 0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .filters(bound("dim2", "0", "0", false, false, null, StringComparators.NUMERIC))
                  .aggregators(
                      ImmutableList.of(
                          new SketchMergeAggregatorFactory(
                              "a0",
                              "dim2",
                              null,
                              null,
                              null,
                              null
                          ),
                          new SketchMergeAggregatorFactory(
                              "a1",
                              "thetasketch_dim1",
                              null,
                              null,
                              null,
                              null
                          ),
                          new SketchMergeAggregatorFactory(
                              "a2",
                              "dim2",
                              1024,
                              null,
                              null,
                              null
                          ),
                          new SketchMergeAggregatorFactory(
                              "a3",
                              "thetasketch_dim1",
                              1024,
                              null,
                              null,
                              null
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{0L, 0L, "0.0", "0.0"})
    );
  }

  @Test
  public void testGroupByAggregatorDefaultValues()
  {
    testQuery(
        "SELECT\n"
        + "dim2,\n"
        + "  APPROX_COUNT_DISTINCT_DS_THETA(dim2) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + "  APPROX_COUNT_DISTINCT_DS_THETA(thetasketch_dim1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + "  DS_THETA(dim2, 1024) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + "  DS_THETA(thetasketch_dim1, 1024) FILTER(WHERE dim1 = 'nonexistent')\n"
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
                                    new SketchMergeAggregatorFactory(
                                        "a0",
                                        "v0",
                                        null,
                                        null,
                                        null,
                                        null
                                    ),
                                    selector("dim1", "nonexistent", null)
                                ),
                                new FilteredAggregatorFactory(
                                    new SketchMergeAggregatorFactory(
                                        "a1",
                                        "thetasketch_dim1",
                                        null,
                                        null,
                                        null,
                                        null
                                    ),
                                    selector("dim1", "nonexistent", null)
                                ),
                                new FilteredAggregatorFactory(
                                    new SketchMergeAggregatorFactory(
                                        "a2",
                                        "v0",
                                        1024,
                                        null,
                                        null,
                                        null
                                    ),
                                    selector("dim1", "nonexistent", null)
                                ),
                                new FilteredAggregatorFactory(
                                    new SketchMergeAggregatorFactory(
                                        "a3",
                                        "thetasketch_dim1",
                                        1024,
                                        null,
                                        null,
                                        null
                                    ),
                                    selector("dim1", "nonexistent", null)
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(new Object[]{"a", 0L, 0L, "0.0", "0.0"})
    );
  }
}
