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

package org.apache.druid.query.aggregation.histogram.sql;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.histogram.ApproximateHistogramAggregatorFactory;
import org.apache.druid.query.aggregation.histogram.ApproximateHistogramDruidModule;
import org.apache.druid.query.aggregation.histogram.ApproximateHistogramFoldingAggregatorFactory;
import org.apache.druid.query.aggregation.histogram.QuantilePostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
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
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class QuantileSqlAggregatorTest extends BaseCalciteQueryTest
{
  private static final DruidOperatorTable OPERATOR_TABLE = new DruidOperatorTable(
      ImmutableSet.of(new QuantileSqlAggregator()),
      ImmutableSet.of()
  );

  @Override
  public Iterable<? extends Module> getJacksonModules()
  {
    return Iterables.concat(super.getJacksonModules(), new ApproximateHistogramDruidModule().getJacksonModules());
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
      QueryRunnerFactoryConglomerate conglomerate
  ) throws IOException
  {
    ApproximateHistogramDruidModule.registerSerde();

    final QueryableIndex index = IndexBuilder.create(CalciteTests.getJsonMapper())
                                             .tmpDir(temporaryFolder.newFolder())
                                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                             .schema(
                                                 new IncrementalIndexSchema.Builder()
                                                     .withMetrics(
                                                         new CountAggregatorFactory("cnt"),
                                                         new DoubleSumAggregatorFactory("m1", "m1"),
                                                         new ApproximateHistogramAggregatorFactory(
                                                             "hist_m1",
                                                             "m1",
                                                             null,
                                                             null,
                                                             null,
                                                             null,
                                                             false
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
    return OPERATOR_TABLE;
  }

  @Test
  public void testQuantileOnFloatAndLongs()
  {
    testQuery(
        "SELECT\n"
        + "APPROX_QUANTILE(m1, 0.01),\n"
        + "APPROX_QUANTILE(m1, 0.5, 50),\n"
        + "APPROX_QUANTILE(m1, 0.98, 200),\n"
        + "APPROX_QUANTILE(m1, 0.99),\n"
        + "APPROX_QUANTILE(m1 * 2, 0.97),\n"
        + "APPROX_QUANTILE(m1, 0.99) FILTER(WHERE dim1 = 'abc'),\n"
        + "APPROX_QUANTILE(m1, 0.999) FILTER(WHERE dim1 <> 'abc'),\n"
        + "APPROX_QUANTILE(m1, 0.999) FILTER(WHERE dim1 = 'abc'),\n"
        + "APPROX_QUANTILE(cnt, 0.5)\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "(\"m1\" * 2)",
                          ColumnType.FLOAT,
                          TestExprMacroTable.INSTANCE
                      )
                  )
                  .aggregators(ImmutableList.of(
                      new ApproximateHistogramAggregatorFactory("a0:agg", "m1", null, null, null, null, false),
                      new ApproximateHistogramAggregatorFactory("a2:agg", "m1", 200, null, null, null, false),
                      new ApproximateHistogramAggregatorFactory("a4:agg", "v0", null, null, null, null, false),
                      new FilteredAggregatorFactory(
                          new ApproximateHistogramAggregatorFactory("a5:agg", "m1", null, null, null, null, false),
                          new SelectorDimFilter("dim1", "abc", null)
                      ),
                      new FilteredAggregatorFactory(
                          new ApproximateHistogramAggregatorFactory("a6:agg", "m1", null, null, null, null, false),
                          new NotDimFilter(new SelectorDimFilter("dim1", "abc", null))
                      ),
                      new ApproximateHistogramAggregatorFactory("a8:agg", "cnt", null, null, null, null, false)
                  ))
                  .postAggregators(
                      new QuantilePostAggregator("a0", "a0:agg", 0.01f),
                      new QuantilePostAggregator("a1", "a0:agg", 0.50f),
                      new QuantilePostAggregator("a2", "a2:agg", 0.98f),
                      new QuantilePostAggregator("a3", "a0:agg", 0.99f),
                      new QuantilePostAggregator("a4", "a4:agg", 0.97f),
                      new QuantilePostAggregator("a5", "a5:agg", 0.99f),
                      new QuantilePostAggregator("a6", "a6:agg", 0.999f),
                      new QuantilePostAggregator("a7", "a5:agg", 0.999f),
                      new QuantilePostAggregator("a8", "a8:agg", 0.50f)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                1.0,
                3.0,
                5.880000114440918,
                5.940000057220459,
                11.640000343322754,
                6.0,
                4.994999885559082,
                6.0,
                1.0
            }
        )
    );
  }

  @Test
  public void testQuantileOnComplexColumn()
  {
    testQuery(
        "SELECT\n"
        + "APPROX_QUANTILE(hist_m1, 0.01),\n"
        + "APPROX_QUANTILE(hist_m1, 0.5, 50),\n"
        + "APPROX_QUANTILE(hist_m1, 0.98, 200),\n"
        + "APPROX_QUANTILE(hist_m1, 0.99),\n"
        + "APPROX_QUANTILE(hist_m1, 0.99) FILTER(WHERE dim1 = 'abc'),\n"
        + "APPROX_QUANTILE(hist_m1, 0.999) FILTER(WHERE dim1 <> 'abc'),\n"
        + "APPROX_QUANTILE(hist_m1, 0.999) FILTER(WHERE dim1 = 'abc')\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      new ApproximateHistogramFoldingAggregatorFactory("a0:agg", "hist_m1", null, null, null, null, false),
                      new ApproximateHistogramFoldingAggregatorFactory("a2:agg", "hist_m1", 200, null, null, null, false),
                      new FilteredAggregatorFactory(
                          new ApproximateHistogramFoldingAggregatorFactory("a4:agg", "hist_m1", null, null, null, null, false),
                          new SelectorDimFilter("dim1", "abc", null)
                      ),
                      new FilteredAggregatorFactory(
                          new ApproximateHistogramFoldingAggregatorFactory("a5:agg", "hist_m1", null, null, null, null, false),
                          new NotDimFilter(new SelectorDimFilter("dim1", "abc", null))
                      )
                  ))
                  .postAggregators(
                      new QuantilePostAggregator("a0", "a0:agg", 0.01f),
                      new QuantilePostAggregator("a1", "a0:agg", 0.50f),
                      new QuantilePostAggregator("a2", "a2:agg", 0.98f),
                      new QuantilePostAggregator("a3", "a0:agg", 0.99f),
                      new QuantilePostAggregator("a4", "a4:agg", 0.99f),
                      new QuantilePostAggregator("a5", "a5:agg", 0.999f),
                      new QuantilePostAggregator("a6", "a4:agg", 0.999f)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1.0, 3.0, 5.880000114440918, 5.940000057220459, 6.0, 4.994999885559082, 6.0}
        )
    );
  }

  @Test
  public void testQuantileOnInnerQuery()
  {
    final List<Object[]> expectedResults;
    if (NullHandling.replaceWithDefault()) {
      expectedResults = ImmutableList.of(new Object[]{7.0, 8.26386833190918});
    } else {
      expectedResults = ImmutableList.of(new Object[]{5.25, 6.59091854095459});
    }

    testQuery(
        "SELECT AVG(x), APPROX_QUANTILE(x, 0.98)\n"
        + "FROM (SELECT dim2, SUM(m1) AS x FROM foo GROUP BY dim2)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(new DefaultDimensionSpec("dim2", "d0"))
                                            .setAggregatorSpecs(
                                                ImmutableList.of(
                                                    new DoubleSumAggregatorFactory("a0", "m1")
                                                )
                                            )
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(
                            new DoubleSumAggregatorFactory("_a0:sum", "a0"),
                            new CountAggregatorFactory("_a0:count"),
                            new ApproximateHistogramAggregatorFactory(
                                "_a1:agg",
                                "a0",
                                null,
                                null,
                                null,
                                null,
                                false
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
                                ),
                                new QuantilePostAggregator("_a1", "_a1:agg", 0.98f)
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        expectedResults
    );
  }

  @Test
  public void testQuantileOnCastedString()
  {
    cannotVectorize();

    final List<Object[]> expectedResults;
    if (NullHandling.replaceWithDefault()) {
      expectedResults = ImmutableList.of(
          new Object[]{"", 0.0d},
          new Object[]{"a", 0.0d},
          new Object[]{"b", 0.0d},
          new Object[]{"c", 10.100000381469727d},
          new Object[]{"d", 2.0d}
      );
    } else {
      expectedResults = ImmutableList.of(
          new Object[]{null, Double.NaN},
          new Object[]{"", 1.0d},
          new Object[]{"a", Double.NaN},
          new Object[]{"b", 10.100000381469727d},
          new Object[]{"c", 10.100000381469727d},
          new Object[]{"d", 2.0d}
      );
    }
    testQuery(
        "SELECT dim3, APPROX_QUANTILE(CAST(dim1 as DOUBLE), 0.5) from foo group by dim3",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new ExpressionVirtualColumn(
                                "v0",
                                "CAST(\"dim1\", 'DOUBLE')",
                                ColumnType.FLOAT,
                                ExprMacroTable.nil()
                            )
                        )
                        .setDimensions(new DefaultDimensionSpec("dim3", "d0"))
                        .setAggregatorSpecs(
                            new ApproximateHistogramAggregatorFactory(
                                "a0:agg",
                                "v0",
                                50,
                                7,
                                Float.NEGATIVE_INFINITY,
                                Float.POSITIVE_INFINITY,
                                false
                            )
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new QuantilePostAggregator("a0", "a0:agg", 0.5f)
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
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
        + "APPROX_QUANTILE(hist_m1, 0.01),\n"
        + "APPROX_QUANTILE(m1, 0.01)\n"
        + "FROM foo WHERE dim2 = 0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .filters(bound("dim2", "0", "0", false, false, null, StringComparators.NUMERIC))
                  .aggregators(ImmutableList.of(
                      new ApproximateHistogramFoldingAggregatorFactory("a0:agg", "hist_m1", null, null, null, null, false),
                      new ApproximateHistogramAggregatorFactory("a1:agg", "m1", null, null, null, null, false)

                  ))
                  .postAggregators(
                      new QuantilePostAggregator("a0", "a0:agg", 0.01f),
                      new QuantilePostAggregator("a1", "a1:agg", 0.01f)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{Double.NaN, Double.NaN}
        )
    );
  }

  @Test
  public void testGroupByAggregatorDefaultValues()
  {
    testQuery(
        "SELECT\n"
        + "dim2,\n"
        + "APPROX_QUANTILE(hist_m1, 0.01) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + "APPROX_QUANTILE(m1, 0.01) FILTER(WHERE dim1 = 'nonexistent')\n"
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
                                    new ApproximateHistogramFoldingAggregatorFactory("a0:agg", "hist_m1", null, null, null, null, false),
                                    selector("dim1", "nonexistent", null)
                                ),
                                new FilteredAggregatorFactory(
                                    new ApproximateHistogramAggregatorFactory("a1:agg", "m1", null, null, null, null, false),
                                    selector("dim1", "nonexistent", null)
                                )
                            )
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new QuantilePostAggregator("a0", "a0:agg", 0.01f),
                                new QuantilePostAggregator("a1", "a1:agg", 0.01f)
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"a", Double.NaN, Double.NaN}
        )
    );
  }
}
