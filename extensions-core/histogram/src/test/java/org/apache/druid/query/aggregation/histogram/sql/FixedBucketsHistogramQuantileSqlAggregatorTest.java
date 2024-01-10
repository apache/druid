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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.histogram.ApproximateHistogramDruidModule;
import org.apache.druid.query.aggregation.histogram.FixedBucketsHistogram;
import org.apache.druid.query.aggregation.histogram.FixedBucketsHistogramAggregatorFactory;
import org.apache.druid.query.aggregation.histogram.QuantilePostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class FixedBucketsHistogramQuantileSqlAggregatorTest extends BaseCalciteQueryTest
{
  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
    builder.addModule(new ApproximateHistogramDruidModule());
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final JoinableFactoryWrapper joinableFactory,
      final Injector injector
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
                                                         new FixedBucketsHistogramAggregatorFactory(
                                                             "fbhist_m1",
                                                             "m1",
                                                             20,
                                                             0,
                                                             10,
                                                             FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                                                             false
                                                         )
                                                     )
                                                     .withRollup(false)
                                                     .build()
                                             )
                                             .rows(TestDataBuilder.ROWS1)
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
  public void testQuantileOnFloatAndLongs()
  {
    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            1.0299999713897705,
            3.5,
            6.440000057220459,
            6.470000267028809,
            12.40999984741211,
            6.494999885559082,
            5.497499942779541,
            6.499499797821045,
            6.499499797821045,
            6.499499797821045,
            6.499499797821045,
            1.25
        }
    );

    testQuery(
        "SELECT\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.01, 20, 0.0, 10.0),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.5, 20, 0.0, 10.0),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.98, 20, 0.0, 10.0),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.99, 20, 0.0, 10.0),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(m1 * 2, 0.97, 40, 0.0, 20.0),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.99, 20, 0.0, 10.0) FILTER(WHERE dim1 = 'abc'),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.999, 20, 0.0, 10.0) FILTER(WHERE dim1 <> 'abc'),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.999, 20, 0.0, 10.0) FILTER(WHERE dim1 = 'abc'),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.999, 20, 0.0, 10.0, 'ignore') FILTER(WHERE dim1 = 'abc'),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.999, 20, 0.0, 10.0, 'clip') FILTER(WHERE dim1 = 'abc'),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.999, 20, 0.0, 10.0, 'overflow') FILTER(WHERE dim1 = 'abc'),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(cnt, 0.5, 20, 0.0, 10.0)\n"
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
                      new FixedBucketsHistogramAggregatorFactory(
                          "a0:agg",
                          "m1",
                          20,
                          0.0d,
                          10.0d,
                          FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                          false
                      ),
                      new FixedBucketsHistogramAggregatorFactory(
                          "a4:agg",
                          "v0",
                          40,
                          0.0d,
                          20.0d,
                          FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                          false
                      ),
                      new FilteredAggregatorFactory(
                          new FixedBucketsHistogramAggregatorFactory(
                              "a5:agg",
                              "m1",
                              20,
                              0.0d,
                              10.0d,
                              FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                              false
                          ),
                          equality("dim1", "abc", ColumnType.STRING)
                      ),
                      new FilteredAggregatorFactory(
                          new FixedBucketsHistogramAggregatorFactory(
                              "a6:agg",
                              "m1",
                              20,
                              0.0d,
                              10.0d,
                              FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                              false
                          ),
                          not(equality("dim1", "abc", ColumnType.STRING))
                      ),
                      new FilteredAggregatorFactory(
                          new FixedBucketsHistogramAggregatorFactory(
                              "a9:agg",
                              "m1",
                              20,
                              0.0d,
                              10.0d,
                              FixedBucketsHistogram.OutlierHandlingMode.CLIP,
                              false
                          ),
                          equality("dim1", "abc", ColumnType.STRING)
                      ),
                      new FilteredAggregatorFactory(
                          new FixedBucketsHistogramAggregatorFactory(
                              "a10:agg",
                              "m1",
                              20,
                              0.0d,
                              10.0d,
                              FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
                              false
                          ),
                          equality("dim1", "abc", ColumnType.STRING)
                      ),
                      new FixedBucketsHistogramAggregatorFactory(
                          "a11:agg",
                          "cnt",
                          20,
                          0.0d,
                          10.0d,
                          FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                          false
                      )
                  ))
                  .postAggregators(
                      new QuantilePostAggregator("a0", "a0:agg", 0.01f),
                      new QuantilePostAggregator("a1", "a0:agg", 0.50f),
                      new QuantilePostAggregator("a2", "a0:agg", 0.98f),
                      new QuantilePostAggregator("a3", "a0:agg", 0.99f),
                      new QuantilePostAggregator("a4", "a4:agg", 0.97f),
                      new QuantilePostAggregator("a5", "a5:agg", 0.99f),
                      new QuantilePostAggregator("a6", "a6:agg", 0.999f),
                      new QuantilePostAggregator("a7", "a5:agg", 0.999f),
                      new QuantilePostAggregator("a8", "a5:agg", 0.999f),
                      new QuantilePostAggregator("a9", "a9:agg", 0.999f),
                      new QuantilePostAggregator("a10", "a10:agg", 0.999f),
                      new QuantilePostAggregator("a11", "a11:agg", 0.50f)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        expectedResults
    );
  }

  @Test
  public void testQuantileWithCastedLiteralArguments()
  {
    final List<Object[]> expectedResults = ImmutableList.of(new Object[]{6.499499797821045});
    testQuery(
        "SELECT\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS("
        + "m1, "
        + "CAST(0.999 AS DOUBLE), "
        + "CAST(20 AS INTEGER), "
        + "CAST(0.0 AS DOUBLE), "
        + "CAST(10.0 AS DOUBLE), "
        + "CAST('overflow' AS VARCHAR)"
        + ") "
        + "FILTER(WHERE dim1 = 'abc')\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      new FilteredAggregatorFactory(
                          new FixedBucketsHistogramAggregatorFactory(
                              "a0:agg",
                              "m1",
                              20,
                              0.0d,
                              10.0d,
                              FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
                              false
                          ),
                          equality("dim1", "abc", ColumnType.STRING)
                      )
                  ))
                  .postAggregators(
                      new QuantilePostAggregator("a0", "a0:agg", 0.999f)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        expectedResults
    );
  }

  @Test
  public void testQuantileOnCastedString()
  {
    testQuery(
        "SELECT\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(CAST(dim1 AS DOUBLE), 0.01, 20, 0.0, 10.0),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(CAST(dim1 AS DOUBLE), 0.5, 20, 0.0, 10.0),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(CAST(dim1 AS DOUBLE), 0.98, 20, 0.0, 10.0),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(CAST(dim1 AS DOUBLE), 0.99, 20, 0.0, 10.0),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(CAST(dim1 AS DOUBLE) * 2, 0.97, 40, 0.0, 20.0),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(CAST(dim1 AS DOUBLE), 0.99, 20, 0.0, 10.0) FILTER(WHERE dim1 = 'abc'),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(CAST(dim1 AS DOUBLE), 0.999, 20, 0.0, 10.0) FILTER(WHERE dim1 <> 'abc'),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(CAST(dim1 AS DOUBLE), 0.999, 20, 0.0, 10.0) FILTER(WHERE dim1 = 'abc')\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "CAST(\"dim1\", 'DOUBLE')",
                          ColumnType.FLOAT,
                          TestExprMacroTable.INSTANCE
                      ),
                      new ExpressionVirtualColumn(
                          "v1",
                          "(CAST(\"dim1\", 'DOUBLE') * 2)",
                          ColumnType.FLOAT,
                          TestExprMacroTable.INSTANCE
                      )
                  )
                  .aggregators(ImmutableList.of(
                      new FixedBucketsHistogramAggregatorFactory(
                          "a0:agg",
                          "v0",
                          20,
                          0.0d,
                          10.0d,
                          FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                          false
                      ),
                      new FixedBucketsHistogramAggregatorFactory(
                          "a4:agg",
                          "v1",
                          40,
                          0.0d,
                          20.0d,
                          FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                          false
                      ),
                      new FilteredAggregatorFactory(
                          new FixedBucketsHistogramAggregatorFactory(
                              "a5:agg",
                              "v0",
                              20,
                              0.0d,
                              10.0d,
                              FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                              false
                          ),
                          equality("dim1", "abc", ColumnType.STRING)
                      ),
                      new FilteredAggregatorFactory(
                          new FixedBucketsHistogramAggregatorFactory(
                              "a6:agg",
                              "v0",
                              20,
                              0.0d,
                              10.0d,
                              FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                              false
                          ),
                          not(equality("dim1", "abc", ColumnType.STRING))
                      )
                  ))
                  .postAggregators(
                      new QuantilePostAggregator("a0", "a0:agg", 0.01f),
                      new QuantilePostAggregator("a1", "a0:agg", 0.50f),
                      new QuantilePostAggregator("a2", "a0:agg", 0.98f),
                      new QuantilePostAggregator("a3", "a0:agg", 0.99f),
                      new QuantilePostAggregator("a4", "a4:agg", 0.97f),
                      new QuantilePostAggregator("a5", "a5:agg", 0.99f),
                      new QuantilePostAggregator("a6", "a6:agg", 0.999f),
                      new QuantilePostAggregator("a7", "a5:agg", 0.999f)
                  )
                  .context(ImmutableMap.of(QueryContexts.CTX_SQL_QUERY_ID, "dummy"))
                  .build()
        ),
        ImmutableList.of(
            NullHandling.replaceWithDefault()
            ? new Object[]{
                0.00833333283662796,
                0.4166666567325592,
                2.450000047683716,
                2.4749999046325684,
                4.425000190734863,
                0.4950000047683716,
                2.498000144958496,
                0.49950000643730164
            }
            : new Object[]{
                1.0099999904632568,
                1.5,
                2.4800000190734863,
                2.490000009536743,
                4.470000267028809,
                0.0,
                2.499000072479248,
                0.0
            }
        )
    );
  }

  @Test
  public void testQuantileOnComplexColumn()
  {
    cannotVectorize();

    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            1.0299999713897705,
            3.5,
            6.293333530426025,
            6.470000267028809,
            6.494999885559082,
            5.497499942779541,
            6.499499797821045
        }
    );

    testQuery(
        "SELECT\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.01, 20, 0.0, 10.0),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.5, 20, 0.0, 10.0),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.98, 30, 0.0, 10.0),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.99, 20, 0.0, 10.0),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.99, 20, 0.0, 10.0) FILTER(WHERE dim1 = 'abc'),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.999, 20, 0.0, 10.0) FILTER(WHERE dim1 <> 'abc'),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.999, 20, 0.0, 10.0) FILTER(WHERE dim1 = 'abc')\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      new FixedBucketsHistogramAggregatorFactory(
                          "a0:agg",
                          "fbhist_m1",
                          20,
                          0.0,
                          10.0,
                          FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                          false
                      ),
                      new FixedBucketsHistogramAggregatorFactory(
                          "a2:agg",
                          "fbhist_m1",
                          30,
                          0.0,
                          10.0,
                          FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                          false
                      ),
                      new FilteredAggregatorFactory(
                          new FixedBucketsHistogramAggregatorFactory(
                              "a4:agg",
                              "fbhist_m1",
                              20,
                              0.0,
                              10.0,
                              FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                              false
                          ),
                          equality("dim1", "abc", ColumnType.STRING)
                      ),
                      new FilteredAggregatorFactory(
                          new FixedBucketsHistogramAggregatorFactory(
                              "a5:agg",
                              "fbhist_m1",
                              20,
                              0.0,
                              10.0,
                              FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                              false
                          ),
                          not(equality("dim1", "abc", ColumnType.STRING))
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
        expectedResults
    );
  }

  @Test
  public void testQuantileOnInnerQuery()
  {
    final List<Object[]> expectedResults;
    if (NullHandling.replaceWithDefault()) {
      expectedResults = ImmutableList.of(new Object[]{7.0, 11.940000534057617});
    } else {
      expectedResults = ImmutableList.of(new Object[]{5.25, 8.920000076293945});
    }

    testQuery(
        "SELECT AVG(x), APPROX_QUANTILE_FIXED_BUCKETS(x, 0.98, 100, 0.0, 100.0)\n"
        + "FROM (SELECT dim2, SUM(m1) AS x FROM foo GROUP BY dim2)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(
                                                Filtration.eternity())))
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
                            NullHandling.replaceWithDefault() ?
                            new CountAggregatorFactory("_a0:count") :
                            new FilteredAggregatorFactory(
                                new CountAggregatorFactory("_a0:count"),
                                notNull("a0")
                            ),
                            new FixedBucketsHistogramAggregatorFactory(
                                "_a1:agg",
                                "a0",
                                100,
                                0,
                                100.0d,
                                FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
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
  public void testEmptyTimeseriesResults()
  {
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.01, 20, 0.0, 10.0),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.01, 20, 0.0, 10.0)\n"
        + "FROM foo WHERE dim2 = 0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .filters(numericEquality("dim2", 0L, ColumnType.LONG))
                  .aggregators(ImmutableList.of(
                      new FixedBucketsHistogramAggregatorFactory(
                          "a0:agg",
                          "fbhist_m1",
                          20,
                          0.0,
                          10.0,
                          FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                          false
                      ),
                      new FixedBucketsHistogramAggregatorFactory(
                          "a1:agg",
                          "m1",
                          20,
                          0.0,
                          10.0,
                          FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                          false
                      )

                  ))
                  .postAggregators(
                      new QuantilePostAggregator("a0", "a0:agg", 0.01f),
                      new QuantilePostAggregator("a1", "a1:agg", 0.01f)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{0.0, 0.0}
        )
    );
  }

  @Test
  public void testGroupByAggregatorDefaultValues()
  {
    cannotVectorize();
    testQuery(
        "SELECT\n"
        + "dim2,\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.01, 20, 0.0, 10.0) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.01, 20, 0.0, 10.0) FILTER(WHERE dim1 = 'nonexistent')\n"
        + "FROM foo WHERE dim2 = 'a' GROUP BY dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(equality("dim2", "a", ColumnType.STRING))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "'a'", ColumnType.STRING))
                        .setDimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.STRING))
                        .setAggregatorSpecs(
                            aggregators(
                                new FilteredAggregatorFactory(
                                    new FixedBucketsHistogramAggregatorFactory(
                                        "a0:agg",
                                        "fbhist_m1",
                                        20,
                                        0.0,
                                        10.0,
                                        FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                                        false
                                    ),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new FixedBucketsHistogramAggregatorFactory(
                                        "a1:agg",
                                        "m1",
                                        20,
                                        0.0,
                                        10.0,
                                        FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                                        false
                                    ),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
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
            new Object[]{"a", 0.0, 0.0}
        )
    );
  }
}
