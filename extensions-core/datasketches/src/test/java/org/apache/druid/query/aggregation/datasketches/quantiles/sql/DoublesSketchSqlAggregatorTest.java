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

package org.apache.druid.query.aggregation.datasketches.quantiles.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.datasketches.SketchQueryContext;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchModule;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchToCDFPostAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchToHistogramPostAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchToQuantilePostAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchToQuantilesPostAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchToRankPostAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchToStringPostAggregator;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DoublesSketchSqlAggregatorTest extends BaseCalciteQueryTest
{
  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
    builder.addModule(new DoublesSketchModule());
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final JoinableFactoryWrapper joinableFactory,
      final Injector injector
  ) throws IOException
  {
    DoublesSketchModule.registerSerde();

    final QueryableIndex index =
        IndexBuilder.create(CalciteTests.getJsonMapper())
                    .tmpDir(temporaryFolder.newFolder())
                    .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                    .schema(
                        new IncrementalIndexSchema.Builder()
                            .withMetrics(
                                new CountAggregatorFactory("cnt"),
                                new DoubleSumAggregatorFactory("m1", "m1"),
                                new DoublesSketchAggregatorFactory(
                                    "qsketch_m1",
                                    "m1",
                                    128
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
    testQuery(
        "SELECT\n"
        + "APPROX_QUANTILE_DS(m1, 0.01),\n"
        + "APPROX_QUANTILE_DS(m1, 0.5, 64),\n"
        + "APPROX_QUANTILE_DS(m1, 0.98, 256),\n"
        + "APPROX_QUANTILE_DS(m1, 0.99),\n"
        + "APPROX_QUANTILE_DS(m1 * 2, 0.97),\n"
        + "APPROX_QUANTILE_DS(m1, 0.99) FILTER(WHERE dim1 = 'abc'),\n"
        + "APPROX_QUANTILE_DS(m1, 0.999) FILTER(WHERE dim1 <> 'abc'),\n"
        + "APPROX_QUANTILE_DS(m1, 0.999) FILTER(WHERE dim1 = 'abc'),\n"
        + "APPROX_QUANTILE_DS(cnt, 0.5)\n"
        + "FROM foo",
        Collections.singletonList(
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
                      new DoublesSketchAggregatorFactory("a0:agg", "m1", null),
                      new DoublesSketchAggregatorFactory("a1:agg", "m1", 64),
                      new DoublesSketchAggregatorFactory("a2:agg", "m1", 256),
                      new DoublesSketchAggregatorFactory("a4:agg", "v0", null),
                      new FilteredAggregatorFactory(
                          new DoublesSketchAggregatorFactory("a5:agg", "m1", null),
                          equality("dim1", "abc", ColumnType.STRING)
                      ),
                      new FilteredAggregatorFactory(
                          new DoublesSketchAggregatorFactory("a6:agg", "m1", null),
                          not(equality("dim1", "abc", ColumnType.STRING))
                      ),
                      new DoublesSketchAggregatorFactory("a8:agg", "cnt", null)
                  ))
                  .postAggregators(
                      new DoublesSketchToQuantilePostAggregator("a0", makeFieldAccessPostAgg("a0:agg"), 0.01f),
                      new DoublesSketchToQuantilePostAggregator("a1", makeFieldAccessPostAgg("a1:agg"), 0.50f),
                      new DoublesSketchToQuantilePostAggregator("a2", makeFieldAccessPostAgg("a2:agg"), 0.98f),
                      new DoublesSketchToQuantilePostAggregator("a3", makeFieldAccessPostAgg("a0:agg"), 0.99f),
                      new DoublesSketchToQuantilePostAggregator("a4", makeFieldAccessPostAgg("a4:agg"), 0.97f),
                      new DoublesSketchToQuantilePostAggregator("a5", makeFieldAccessPostAgg("a5:agg"), 0.99f),
                      new DoublesSketchToQuantilePostAggregator("a6", makeFieldAccessPostAgg("a6:agg"), 0.999f),
                      new DoublesSketchToQuantilePostAggregator("a7", makeFieldAccessPostAgg("a5:agg"), 0.999f),
                      new DoublesSketchToQuantilePostAggregator("a8", makeFieldAccessPostAgg("a8:agg"), 0.50f)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                1.0,
                3.0,
                6.0,
                6.0,
                12.0,
                6.0,
                5.0,
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
        + "APPROX_QUANTILE_DS(qsketch_m1, 0.01),\n"
        + "APPROX_QUANTILE_DS(qsketch_m1, 0.5, 64),\n"
        + "APPROX_QUANTILE_DS(qsketch_m1, 0.98, 256),\n"
        + "APPROX_QUANTILE_DS(qsketch_m1, CAST(0.99 AS DOUBLE)),\n"
        + "APPROX_QUANTILE_DS(qsketch_m1, 0.99) FILTER(WHERE dim1 = 'abc'),\n"
        + "APPROX_QUANTILE_DS(qsketch_m1, 0.999) FILTER(WHERE dim1 <> 'abc'),\n"
        + "APPROX_QUANTILE_DS(qsketch_m1, 0.999) FILTER(WHERE dim1 = 'abc')\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      new DoublesSketchAggregatorFactory("a0:agg", "qsketch_m1", null),
                      new DoublesSketchAggregatorFactory("a1:agg", "qsketch_m1", 64),
                      new DoublesSketchAggregatorFactory("a2:agg", "qsketch_m1", 256),
                      new FilteredAggregatorFactory(
                          new DoublesSketchAggregatorFactory("a4:agg", "qsketch_m1", null),
                          equality("dim1", "abc", ColumnType.STRING)
                      ),
                      new FilteredAggregatorFactory(
                          new DoublesSketchAggregatorFactory("a5:agg", "qsketch_m1", null),
                          not(equality("dim1", "abc", ColumnType.STRING))
                      )
                  ))
                  .postAggregators(
                      new DoublesSketchToQuantilePostAggregator("a0", makeFieldAccessPostAgg("a0:agg"), 0.01f),
                      new DoublesSketchToQuantilePostAggregator("a1", makeFieldAccessPostAgg("a1:agg"), 0.50f),
                      new DoublesSketchToQuantilePostAggregator("a2", makeFieldAccessPostAgg("a2:agg"), 0.98f),
                      new DoublesSketchToQuantilePostAggregator("a3", makeFieldAccessPostAgg("a0:agg"), 0.99f),
                      new DoublesSketchToQuantilePostAggregator("a4", makeFieldAccessPostAgg("a4:agg"), 0.99f),
                      new DoublesSketchToQuantilePostAggregator("a5", makeFieldAccessPostAgg("a5:agg"), 0.999f),
                      new DoublesSketchToQuantilePostAggregator("a6", makeFieldAccessPostAgg("a4:agg"), 0.999f)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                1.0,
                3.0,
                6.0,
                6.0,
                6.0,
                5.0,
                6.0
            }
        )
    );
  }

  @Test
  public void testQuantileOnCastedString()
  {
    final List<Object[]> expectedResults;
    if (NullHandling.replaceWithDefault()) {
      expectedResults = ImmutableList.of(
          new Object[]{
              0.0,
              0.0,
              10.1,
              10.1,
              20.2,
              0.0,
              10.1,
              0.0
          }
      );
    } else {
      expectedResults = ImmutableList.of(
          new Object[]{
              1.0,
              2.0,
              10.1,
              10.1,
              20.2,
              Double.NaN,
              2.0,
              Double.NaN
          }
      );
    }

    testQuery(
        "SELECT\n"
        + "APPROX_QUANTILE_DS(CAST(dim1 as DOUBLE), 0.01),\n"
        + "APPROX_QUANTILE_DS(CAST(dim1 as DOUBLE), 0.5, 64),\n"
        + "APPROX_QUANTILE_DS(CAST(dim1 as DOUBLE), 0.98, 256),\n"
        + "APPROX_QUANTILE_DS(CAST(dim1 as DOUBLE), 0.99),\n"
        + "APPROX_QUANTILE_DS(CAST(dim1 as DOUBLE) * 2, 0.97),\n"
        + "APPROX_QUANTILE_DS(CAST(dim1 as DOUBLE), 0.99) FILTER(WHERE dim2 = 'abc'),\n"
        + "APPROX_QUANTILE_DS(CAST(dim1 as DOUBLE), 0.999) FILTER(WHERE dim2 <> 'abc'),\n"
        + "APPROX_QUANTILE_DS(CAST(dim1 as DOUBLE), 0.999) FILTER(WHERE dim2 = 'abc')\n"
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
                      new DoublesSketchAggregatorFactory("a0:agg", "v0", 128),
                      new DoublesSketchAggregatorFactory("a1:agg", "v0", 64),
                      new DoublesSketchAggregatorFactory("a2:agg", "v0", 256),
                      new DoublesSketchAggregatorFactory("a4:agg", "v1", 128),
                      new FilteredAggregatorFactory(
                          new DoublesSketchAggregatorFactory("a5:agg", "v0", 128),
                          equality("dim2", "abc", ColumnType.STRING)
                      ),
                      new FilteredAggregatorFactory(
                          new DoublesSketchAggregatorFactory("a6:agg", "v0", 128),
                          not(equality("dim2", "abc", ColumnType.STRING))
                      )
                  ))
                  .postAggregators(
                      new DoublesSketchToQuantilePostAggregator("a0", makeFieldAccessPostAgg("a0:agg"), 0.01f),
                      new DoublesSketchToQuantilePostAggregator("a1", makeFieldAccessPostAgg("a1:agg"), 0.50f),
                      new DoublesSketchToQuantilePostAggregator("a2", makeFieldAccessPostAgg("a2:agg"), 0.98f),
                      new DoublesSketchToQuantilePostAggregator("a3", makeFieldAccessPostAgg("a0:agg"), 0.99f),
                      new DoublesSketchToQuantilePostAggregator("a4", makeFieldAccessPostAgg("a4:agg"), 0.97f),
                      new DoublesSketchToQuantilePostAggregator("a5", makeFieldAccessPostAgg("a5:agg"), 0.99f),
                      new DoublesSketchToQuantilePostAggregator("a6", makeFieldAccessPostAgg("a6:agg"), 0.999f),
                      new DoublesSketchToQuantilePostAggregator("a7", makeFieldAccessPostAgg("a5:agg"), 0.999f)
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
      expectedResults = ImmutableList.of(new Object[]{7.0, 11.0});
    } else {
      expectedResults = ImmutableList.of(new Object[]{5.25, 8.0});
    }

    testQuery(
        "SELECT AVG(x), APPROX_QUANTILE_DS(x, 0.98)\n"
        + "FROM (SELECT dim2, SUM(m1) AS x FROM foo GROUP BY dim2)",
        Collections.singletonList(
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
                            NullHandling.replaceWithDefault() ?
                            new CountAggregatorFactory("_a0:count") :
                            new FilteredAggregatorFactory(
                                new CountAggregatorFactory("_a0:count"),
                                notNull("a0")
                            ),
                            new DoublesSketchAggregatorFactory(
                                "_a1:agg",
                                "a0",
                                null
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
                                new DoublesSketchToQuantilePostAggregator(
                                    "_a1",
                                    makeFieldAccessPostAgg("_a1:agg"),
                                    0.98f
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
  public void testQuantileOnInnerQuantileQuery()
  {
    ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
    builder.add(new Object[]{"", 1.0});
    builder.add(new Object[]{"1", 4.0});
    builder.add(new Object[]{"10.1", 2.0});
    builder.add(new Object[]{"2", 3.0});
    builder.add(new Object[]{"abc", 6.0});
    builder.add(new Object[]{"def", 5.0});
    final List<Object[]> expectedResults = builder.build();

    testQuery(
        "SELECT dim1, APPROX_QUANTILE_DS(x, 0.5)\n"
        + "FROM (SELECT dim1, dim2, APPROX_QUANTILE_DS(m1, 0.5) AS x FROM foo GROUP BY dim1, dim2) GROUP BY dim1",
        Collections.singletonList(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(
                                                new DefaultDimensionSpec("dim1", "d0"),
                                                new DefaultDimensionSpec("dim2", "d1")
                                            )
                                            .setAggregatorSpecs(
                                                ImmutableList.of(
                                                    new DoublesSketchAggregatorFactory("a0:agg", "m1", 128)
                                                )
                                            )
                                            .setPostAggregatorSpecs(
                                                ImmutableList.of(
                                                    new DoublesSketchToQuantilePostAggregator(
                                                        "a0",
                                                        makeFieldAccessPostAgg("a0:agg"),
                                                        0.5f
                                                    )
                                                )
                                            )
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(new DefaultDimensionSpec("d0", "_d0", ColumnType.STRING))
                        .setAggregatorSpecs(
                            new DoublesSketchAggregatorFactory("_a0:agg", "a0", 128)
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new DoublesSketchToQuantilePostAggregator(
                                    "_a0",
                                    makeFieldAccessPostAgg("_a0:agg"),
                                    0.5f
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
  public void testDoublesSketchPostAggs()
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  APPROX_QUANTILE_DS(cnt, 0.5) + 1,\n"
        + "  DS_GET_QUANTILE(DS_QUANTILES_SKETCH(cnt), 0.5) + 1000,\n"
        + "  DS_GET_QUANTILE(DS_QUANTILES_SKETCH(cnt + 123), 0.5) + 1000,\n"
        + "  ABS(DS_GET_QUANTILE(DS_QUANTILES_SKETCH(cnt), 0.5)),\n"
        + "  DS_GET_QUANTILES(DS_QUANTILES_SKETCH(cnt), 0.5, 0.8),\n"
        + "  DS_GET_QUANTILES(DS_QUANTILES_SKETCH(cnt), CAST(0.5 AS DOUBLE), CAST(0.8 AS DOUBLE)),\n"
        + "  DS_HISTOGRAM(DS_QUANTILES_SKETCH(cnt), 0.2, 0.6),\n"
        + "  DS_RANK(DS_QUANTILES_SKETCH(cnt), 3),\n"
        + "  DS_CDF(DS_QUANTILES_SKETCH(cnt), 0.2, 0.6),\n"
        + "  -- The nonvectorized query uses a regular Aggregator, and the vectorized query uses a buffer-based\n"
        + "  -- VectorAggregator. The buffer-based aggregators return HeapCompactDoublesSketch instead of\n"
        + "  -- HeapUpdateDoublesSketch since they must make a copy out of the buffer before returning something.\n"
        + "  -- Use REPLACE to normalize summaries.\n"
        + "  REPLACE("
        + "    REPLACE("
        + "      DS_QUANTILE_SUMMARY(DS_QUANTILES_SKETCH(cnt)),"
        + "      'HeapCompactDoublesSketch',"
        + "      'HeapUpdateDoublesSketch'"
        + "    ),"
        + "    'Combined Buffer Capacity     : 6',"
        + "    'Combined Buffer Capacity     : 8'"
        + "  )\n"
        + "FROM foo",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "(\"cnt\" + 123)",
                          ColumnType.FLOAT,
                          TestExprMacroTable.INSTANCE
                      )
                  )
                  .aggregators(ImmutableList.of(
                      new LongSumAggregatorFactory("a0", "cnt"),
                      new DoublesSketchAggregatorFactory("a1:agg", "cnt", 128),
                      new DoublesSketchAggregatorFactory("a2:agg", "cnt", 128, null, false),
                      new DoublesSketchAggregatorFactory("a3:agg", "v0", 128, null, false)
                  ))
                  .postAggregators(
                      new DoublesSketchToQuantilePostAggregator(
                          "a1",
                          makeFieldAccessPostAgg("a1:agg"),
                          0.5f
                      ),
                      expressionPostAgg(
                          "p0",
                          "(\"a1\" + 1)",
                          ColumnType.DOUBLE
                      ),
                      new DoublesSketchToQuantilePostAggregator(
                          "p2",
                          new FieldAccessPostAggregator(
                              "p1",
                              "a2:agg"
                          ),
                          0.5f
                      ),
                      expressionPostAgg(
                          "p3",
                          "(\"p2\" + 1000)",
                          ColumnType.DOUBLE
                      ),
                      new DoublesSketchToQuantilePostAggregator(
                          "p5",
                          new FieldAccessPostAggregator(
                              "p4",
                              "a3:agg"
                          ),
                          0.5f
                      ),
                      expressionPostAgg(
                          "p6",
                          "(\"p5\" + 1000)",
                          ColumnType.DOUBLE
                      ),
                      new DoublesSketchToQuantilePostAggregator(
                          "p8",
                          new FieldAccessPostAggregator(
                              "p7",
                              "a2:agg"
                          ),
                          0.5f
                      ),
                      expressionPostAgg("p9", "abs(\"p8\")", ColumnType.DOUBLE),
                      new DoublesSketchToQuantilesPostAggregator(
                          "p11",
                          new FieldAccessPostAggregator(
                              "p10",
                              "a2:agg"
                          ),
                          new double[]{0.5d, 0.8d}
                      ),
                      new DoublesSketchToQuantilesPostAggregator(
                          "p13",
                          new FieldAccessPostAggregator(
                              "p12",
                              "a2:agg"
                          ),
                          new double[]{0.5d, 0.8d}
                      ),
                      new DoublesSketchToHistogramPostAggregator(
                          "p15",
                          new FieldAccessPostAggregator(
                              "p14",
                              "a2:agg"
                          ),
                          new double[]{0.2d, 0.6d},
                          null
                      ),
                      new DoublesSketchToRankPostAggregator(
                          "p17",
                          new FieldAccessPostAggregator(
                              "p16",
                              "a2:agg"
                          ),
                          3.0d
                      ),
                      new DoublesSketchToCDFPostAggregator(
                          "p19",
                          new FieldAccessPostAggregator(
                              "p18",
                              "a2:agg"
                          ),
                          new double[]{0.2d, 0.6d}
                      ),
                      new DoublesSketchToStringPostAggregator(
                          "p21",
                          new FieldAccessPostAggregator(
                              "p20",
                              "a2:agg"
                          )
                      ),
                      expressionPostAgg(
                          "p22",
                          "replace(replace(\"p21\",'HeapCompactDoublesSketch','HeapUpdateDoublesSketch'),"
                          + "'Combined Buffer Capacity     : 6',"
                          + "'Combined Buffer Capacity     : 8')",
                          ColumnType.STRING
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                6L,
                2.0d,
                1001.0d,
                1124.0d,
                1.0d,
                "[1.0,1.0]",
                "[1.0,1.0]",
                "[0.0,0.0,6.0]",
                1.0d,
                "[0.0,0.0,1.0]",
                "\n"
                  + "### Quantiles HeapUpdateDoublesSketch SUMMARY: \n"
                  + "   Empty                        : false\n"
                  + "   Memory, Capacity bytes       : false, \n"
                  + "   Estimation Mode              : false\n"
                  + "   K                            : 128\n"
                  + "   N                            : 6\n"
                  + "   Levels (Needed, Total, Valid): 0, 0, 0\n"
                  + "   Level Bit Pattern            : 0\n"
                  + "   BaseBufferCount              : 6\n"
                  + "   Combined Buffer Capacity     : 8\n"
                  + "   Retained Items               : 6\n"
                  + "   Compact Storage Bytes        : 80\n"
                  + "   Updatable Storage Bytes      : 96\n"
                  + "   Normalized Rank Error        : 1.406%\n"
                  + "   Normalized Rank Error (PMF)  : 1.711%\n"
                  + "   Min Item                     : 1.000000e+00\n"
                  + "   Max Item                     : 1.000000e+00\n"
                  + "### END SKETCH SUMMARY\n"
            }
        )
    );
  }

  @Test
  public void testDoublesSketchPostAggsPostSort()
  {
    testQuery(
        "SELECT DS_GET_QUANTILE(y, 0.5), DS_GET_QUANTILE(y, 0.98) from ("
        + "SELECT DS_QUANTILES_SKETCH(m1) as y FROM druid.foo ORDER BY DS_GET_QUANTILE(DS_QUANTILES_SKETCH(m1), 0.5) DESC LIMIT 10"
        + ")",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      ImmutableList.of(
                          new DoublesSketchAggregatorFactory("a0:agg", "m1", 128, null, false)
                      )
                  )
                  .postAggregators(
                      ImmutableList.of(
                          new DoublesSketchToQuantilePostAggregator(
                              "p1",
                              new FieldAccessPostAggregator("p0", "a0:agg"),
                              0.5
                          ),
                          new DoublesSketchToQuantilePostAggregator(
                              "s1",
                              new FieldAccessPostAggregator("s0", "a0:agg"),
                              0.5
                          ),
                          new DoublesSketchToQuantilePostAggregator(
                              "s3",
                              new FieldAccessPostAggregator("s2", "a0:agg"),
                              0.9800000190734863
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                3.0d,
                6.0d
            }
        )
    );
  }

  @Test
  public void testEmptyTimeseriesResults()
  {
    testQuery(
        "SELECT\n"
        + "APPROX_QUANTILE_DS(m1, 0.01),\n"
        + "APPROX_QUANTILE_DS(qsketch_m1, 0.01),\n"
        + "DS_QUANTILES_SKETCH(m1),\n"
        + "DS_QUANTILES_SKETCH(qsketch_m1)\n"
        + "FROM foo WHERE dim2 = 0",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .filters(numericEquality("dim2", 0L, ColumnType.LONG))
                  .aggregators(ImmutableList.of(
                      new DoublesSketchAggregatorFactory("a0:agg", "m1", null),
                      new DoublesSketchAggregatorFactory("a1:agg", "qsketch_m1", null),
                      new DoublesSketchAggregatorFactory("a2:agg", "m1", null, null, false),
                      new DoublesSketchAggregatorFactory("a3:agg", "qsketch_m1", null, null, false)
                  ))
                  .postAggregators(
                      new DoublesSketchToQuantilePostAggregator("a0", makeFieldAccessPostAgg("a0:agg"), 0.01f),
                      new DoublesSketchToQuantilePostAggregator("a1", makeFieldAccessPostAgg("a1:agg"), 0.01f)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                Double.NaN,
                Double.NaN,
                "\"AQMIHoAAAAA=\"",
                "\"AQMIHoAAAAA=\""
            }
        )
    );
  }

  @Test
  public void testEmptyTimeseriesResultsWithFinalizeSketches()
  {
    final ImmutableMap<String, Object> queryContext =
        ImmutableMap.<String, Object>builder()
                    .putAll(QUERY_CONTEXT_DEFAULT)
                    .put(SketchQueryContext.CTX_FINALIZE_OUTER_SKETCHES, true)
                    .build();

    testQuery(
        "SELECT\n"
        + "APPROX_QUANTILE_DS(m1, 0.01),\n"
        + "APPROX_QUANTILE_DS(qsketch_m1, 0.01),\n"
        + "DS_QUANTILES_SKETCH(m1),\n"
        + "DS_QUANTILES_SKETCH(qsketch_m1)\n"
        + "FROM foo WHERE dim2 = 0",
        queryContext,
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .filters(numericEquality("dim2", 0L, ColumnType.LONG))
                  .aggregators(ImmutableList.of(
                      new DoublesSketchAggregatorFactory("a0:agg", "m1", null),
                      new DoublesSketchAggregatorFactory("a1:agg", "qsketch_m1", null),
                      new DoublesSketchAggregatorFactory("a2:agg", "m1", null, null, true),
                      new DoublesSketchAggregatorFactory("a3:agg", "qsketch_m1", null, null, true)
                  ))
                  .postAggregators(
                      new DoublesSketchToQuantilePostAggregator("a0", makeFieldAccessPostAgg("a0:agg"), 0.01f),
                      new DoublesSketchToQuantilePostAggregator("a1", makeFieldAccessPostAgg("a1:agg"), 0.01f)
                  )
                  .context(queryContext)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                Double.NaN,
                Double.NaN,
                "0",
                "0"
            }
        )
    );
  }

  @Test
  public void testGroupByAggregatorDefaultValues()
  {
    testQuery(
        "SELECT\n"
        + "dim2,\n"
        + "APPROX_QUANTILE_DS(m1, 0.01) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + "APPROX_QUANTILE_DS(qsketch_m1, 0.01) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + "DS_QUANTILES_SKETCH(m1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + "DS_QUANTILES_SKETCH(qsketch_m1) FILTER(WHERE dim1 = 'nonexistent')\n"
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
                                    new DoublesSketchAggregatorFactory("a0:agg", "m1", null),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new DoublesSketchAggregatorFactory("a1:agg", "qsketch_m1", null),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new DoublesSketchAggregatorFactory("a2:agg", "m1", null, null, false),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new DoublesSketchAggregatorFactory("a3:agg", "qsketch_m1", null, null, false),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                )
                            )
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new DoublesSketchToQuantilePostAggregator(
                                    "a0",
                                    makeFieldAccessPostAgg("a0:agg"),
                                    0.01f
                                ),
                                new DoublesSketchToQuantilePostAggregator("a1", makeFieldAccessPostAgg("a1:agg"), 0.01f)
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{
                "a",
                Double.NaN,
                Double.NaN,
                "\"AQMIHoAAAAA=\"",
                "\"AQMIHoAAAAA=\""
            }
        )
    );
  }

  @Test
  public void testGroupByAggregatorDefaultValuesWithFinalizeSketches()
  {
    final ImmutableMap<String, Object> queryContext =
        ImmutableMap.<String, Object>builder()
                    .putAll(QUERY_CONTEXT_DEFAULT)
                    .put(SketchQueryContext.CTX_FINALIZE_OUTER_SKETCHES, true)
                    .build();

    testQuery(
        "SELECT\n"
        + "dim2,\n"
        + "APPROX_QUANTILE_DS(m1, 0.01) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + "APPROX_QUANTILE_DS(qsketch_m1, 0.01) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + "DS_QUANTILES_SKETCH(m1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + "DS_QUANTILES_SKETCH(qsketch_m1) FILTER(WHERE dim1 = 'nonexistent')\n"
        + "FROM foo WHERE dim2 = 'a' GROUP BY dim2",
        queryContext,
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
                                    new DoublesSketchAggregatorFactory("a0:agg", "m1", null),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new DoublesSketchAggregatorFactory("a1:agg", "qsketch_m1", null),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new DoublesSketchAggregatorFactory("a2:agg", "m1", null, null, true),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new DoublesSketchAggregatorFactory("a3:agg", "qsketch_m1", null, null, true),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                )
                            )
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new DoublesSketchToQuantilePostAggregator(
                                    "a0",
                                    makeFieldAccessPostAgg("a0:agg"),
                                    0.01f
                                ),
                                new DoublesSketchToQuantilePostAggregator("a1", makeFieldAccessPostAgg("a1:agg"), 0.01f)
                            )
                        )
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{
                "a",
                Double.NaN,
                Double.NaN,
                "0",
                "0"
            }
        )
    );
  }

  @Test
  public void testSuccessWithSmallMaxStreamLength()
  {
    final Map<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    context.put(
        DoublesSketchApproxQuantileSqlAggregator.CTX_APPROX_QUANTILE_DS_MAX_STREAM_LENGTH,
        1
    );
    testQuery(
        "SELECT\n"
        + "APPROX_QUANTILE_DS(m1, 0.01),\n"
        + "APPROX_QUANTILE_DS(cnt, 0.5)\n"
        + "FROM foo",
        context,
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      new DoublesSketchAggregatorFactory("a0:agg", "m1", null, 1L, null),
                      new DoublesSketchAggregatorFactory("a1:agg", "cnt", null, 1L, null)
                  ))
                  .postAggregators(
                      new DoublesSketchToQuantilePostAggregator("a0", makeFieldAccessPostAgg("a0:agg"), 0.01f),
                      new DoublesSketchToQuantilePostAggregator("a1", makeFieldAccessPostAgg("a1:agg"), 0.50f)
                  )
                  .context(context)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                1.0,
                1.0
            }
        )
    );
  }

  private static PostAggregator makeFieldAccessPostAgg(String name)
  {
    return new FieldAccessPostAggregator(name, name);
  }
}
