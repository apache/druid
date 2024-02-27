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

package org.apache.druid.query.aggregation.tdigestsketch.sql;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.tdigestsketch.TDigestSketchAggregatorFactory;
import org.apache.druid.query.aggregation.tdigestsketch.TDigestSketchModule;
import org.apache.druid.query.aggregation.tdigestsketch.TDigestSketchToQuantilePostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
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

public class TDigestSketchSqlAggregatorTest extends BaseCalciteQueryTest
{
  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
    builder.addModule(new TDigestSketchModule());
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final JoinableFactoryWrapper joinableFactory,
      final Injector injector
  ) throws IOException
  {
    TDigestSketchModule.registerSerde();

    final QueryableIndex index =
        IndexBuilder.create(CalciteTests.getJsonMapper())
                    .tmpDir(temporaryFolder.newFolder())
                    .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                    .schema(
                        new IncrementalIndexSchema.Builder()
                            .withMetrics(
                                new CountAggregatorFactory("cnt"),
                                new DoubleSumAggregatorFactory("m1", "m1"),
                                new TDigestSketchAggregatorFactory(
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
  public void testComputingSketchOnNumericValues()
  {
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "TDIGEST_GENERATE_SKETCH(m1, 200)"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      new TDigestSketchAggregatorFactory("a0:agg", "m1", 200)
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        ImmutableList.of(
            new String[]{
                "\"AAAAAT/wAAAAAAAAQBgAAAAAAABAaQAAAAAAAAAAAAY/8AAAAAAAAD/wAAAAAAAAP/AAAAAAAABAAAAAAAAAAD/wAAAAAAAAQAgAAAAAAAA/8AAAAAAAAEAQAAAAAAAAP/AAAAAAAABAFAAAAAAAAD/wAAAAAAAAQBgAAAAAAAA=\""
            }
        )
    );
  }

  @Test
  public void testCastedQuantileAndCompressionParamForTDigestQuantileAgg()
  {
    cannotVectorize();
    testQuery(
        "SELECT\n"
        + "TDIGEST_QUANTILE(m1, CAST(0.0 AS DOUBLE)), "
        + "TDIGEST_QUANTILE(m1, CAST(0.5 AS FLOAT), CAST(200 AS INTEGER)), "
        + "TDIGEST_QUANTILE(m1, CAST(1.0 AS DOUBLE), 300)\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      new TDigestSketchAggregatorFactory("a0:agg", "m1",
                                                         TDigestSketchAggregatorFactory.DEFAULT_COMPRESSION
                      ),
                      new TDigestSketchAggregatorFactory("a1:agg", "m1",
                                                         200
                      ),
                      new TDigestSketchAggregatorFactory("a2:agg", "m1",
                                                         300
                      )
                  ))
                  .postAggregators(
                      new TDigestSketchToQuantilePostAggregator("a0", makeFieldAccessPostAgg("a0:agg"), 0.0f),
                      new TDigestSketchToQuantilePostAggregator("a1", makeFieldAccessPostAgg("a1:agg"), 0.5f),
                      new TDigestSketchToQuantilePostAggregator("a2", makeFieldAccessPostAgg("a2:agg"), 1.0f)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        ImmutableList.of(
            new Object[]{1.0, 3.5, 6.0}
        )
    );
  }

  @Test
  public void testComputingSketchOnNumericValuesWithCastedCompressionParameter()
  {
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "TDIGEST_GENERATE_SKETCH(m1, CAST(200 AS INTEGER))"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      new TDigestSketchAggregatorFactory("a0:agg", "m1", 200)
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        ImmutableList.of(
            new String[]{
                "\"AAAAAT/wAAAAAAAAQBgAAAAAAABAaQAAAAAAAAAAAAY/8AAAAAAAAD/wAAAAAAAAP/AAAAAAAABAAAAAAAAAAD/wAAAAAAAAQAgAAAAAAAA/8AAAAAAAAEAQAAAAAAAAP/AAAAAAAABAFAAAAAAAAD/wAAAAAAAAQBgAAAAAAAA=\""
            }
        )
    );
  }

  @Test
  public void testComputingSketchOnCastedString()
  {
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "TDIGEST_GENERATE_SKETCH(CAST(dim1 AS DOUBLE), 200)"
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
                          ExprMacroTable.nil()
                      )
                  )
                  .aggregators(ImmutableList.of(
                      new TDigestSketchAggregatorFactory("a0:agg", "v0", 200)
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        ImmutableList.of(
            NullHandling.replaceWithDefault()
            ? new String[]{
                "\"AAAAAQAAAAAAAAAAQCQzMzMzMzNAaQAAAAAAAAAAAAY/8AAAAAAAAAAAAAAAAAAAP/AAAAAAAAAAAAAAAAAAAD/wAAAAAAAAAAAAAAAAAAA/8AAAAAAAAD/wAAAAAAAAP/AAAAAAAABAAAAAAAAAAD/wAAAAAAAAQCQzMzMzMzM=\""
            }
            : new String[]{
                "\"AAAAAT/wAAAAAAAAQCQzMzMzMzNAaQAAAAAAAAAAAAM/8AAAAAAAAD/wAAAAAAAAP/AAAAAAAABAAAAAAAAAAD/wAAAAAAAAQCQzMzMzMzM=\""
            }
        )
    );
  }

  @Test
  public void testDefaultCompressionForTDigestGenerateSketchAgg()
  {
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "TDIGEST_GENERATE_SKETCH(m1)"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      new TDigestSketchAggregatorFactory("a0:agg", "m1", TDigestSketchAggregatorFactory.DEFAULT_COMPRESSION)
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        ImmutableList.of(
            new Object[]{"\"AAAAAT/wAAAAAAAAQBgAAAAAAABAWQAAAAAAAAAAAAY/8AAAAAAAAD/wAAAAAAAAP/AAAAAAAABAAAAAAAAAAD/wAAAAAAAAQAgAAAAAAAA/8AAAAAAAAEAQAAAAAAAAP/AAAAAAAABAFAAAAAAAAD/wAAAAAAAAQBgAAAAAAAA=\""}
        )
    );
  }

  @Test
  public void testComputingQuantileOnPreAggregatedSketch()
  {
    cannotVectorize();

    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            1.1,
            2.9,
            5.3,
            6.0
        }
    );

    testQuery(
        "SELECT\n"
        + "TDIGEST_QUANTILE(qsketch_m1, 0.1),\n"
        + "TDIGEST_QUANTILE(qsketch_m1, 0.4),\n"
        + "TDIGEST_QUANTILE(qsketch_m1, 0.8),\n"
        + "TDIGEST_QUANTILE(qsketch_m1, 1.0)\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      new TDigestSketchAggregatorFactory("a0:agg", "qsketch_m1", 100)
                  ))
                  .postAggregators(
                      new TDigestSketchToQuantilePostAggregator("a0", makeFieldAccessPostAgg("a0:agg"), 0.1f),
                      new TDigestSketchToQuantilePostAggregator("a1", makeFieldAccessPostAgg("a0:agg"), 0.4f),
                      new TDigestSketchToQuantilePostAggregator("a2", makeFieldAccessPostAgg("a0:agg"), 0.8f),
                      new TDigestSketchToQuantilePostAggregator("a3", makeFieldAccessPostAgg("a0:agg"), 1.0f)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        expectedResults
    );
  }

  @Test
  public void testGeneratingSketchAndComputingQuantileOnFly()
  {
    cannotVectorize();

    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            1.0,
            3.5,
            6.0
        }
    );

    testQuery(
        "SELECT TDIGEST_QUANTILE(x, 0.0), TDIGEST_QUANTILE(x, 0.5), TDIGEST_QUANTILE(x, 1.0)\n"
        + "FROM (SELECT dim1, TDIGEST_GENERATE_SKETCH(m1, 200) AS x FROM foo group by dim1)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(new DefaultDimensionSpec("dim1", "d0"))
                                            .setAggregatorSpecs(
                                                ImmutableList.of(
                                                    new TDigestSketchAggregatorFactory("a0:agg", "m1", 200)
                                                )
                                            )
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(
                            ImmutableList.of(
                                new TDigestSketchAggregatorFactory("_a0:agg", "a0:agg", 100)
                            )
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new TDigestSketchToQuantilePostAggregator("_a0", makeFieldAccessPostAgg("_a0:agg"), 0.0f),
                                new TDigestSketchToQuantilePostAggregator("_a1", makeFieldAccessPostAgg("_a0:agg"), 0.5f),
                                new TDigestSketchToQuantilePostAggregator("_a2", makeFieldAccessPostAgg("_a0:agg"), 1.0f)
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        expectedResults
    );
  }

  @Test
  public void testQuantileOnNumericValues()
  {
    cannotVectorize();

    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            1.0,
            3.5,
            6.0
        }
    );

    testQuery(
        "SELECT\n"
        + "TDIGEST_QUANTILE(m1, 0.0), TDIGEST_QUANTILE(m1, 0.5), TDIGEST_QUANTILE(m1, 1.0)\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      new TDigestSketchAggregatorFactory("a0:agg", "m1", null)
                  ))
                  .postAggregators(
                      new TDigestSketchToQuantilePostAggregator("a0", makeFieldAccessPostAgg("a0:agg"), 0.0f),
                      new TDigestSketchToQuantilePostAggregator("a1", makeFieldAccessPostAgg("a0:agg"), 0.5f),
                      new TDigestSketchToQuantilePostAggregator("a2", makeFieldAccessPostAgg("a0:agg"), 1.0f)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        expectedResults
    );
  }

  @Test
  public void testCompressionParamForTDigestQuantileAgg()
  {
    cannotVectorize();
    testQuery(
        "SELECT\n"
        + "TDIGEST_QUANTILE(m1, 0.0), TDIGEST_QUANTILE(m1, 0.5, 200), TDIGEST_QUANTILE(m1, 1.0, 300)\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      new TDigestSketchAggregatorFactory("a0:agg", "m1",
                                                         TDigestSketchAggregatorFactory.DEFAULT_COMPRESSION
                      ),
                      new TDigestSketchAggregatorFactory("a1:agg", "m1",
                                                         200
                      ),
                      new TDigestSketchAggregatorFactory("a2:agg", "m1",
                                                         300
                      )
                  ))
                  .postAggregators(
                      new TDigestSketchToQuantilePostAggregator("a0", makeFieldAccessPostAgg("a0:agg"), 0.0f),
                      new TDigestSketchToQuantilePostAggregator("a1", makeFieldAccessPostAgg("a1:agg"), 0.5f),
                      new TDigestSketchToQuantilePostAggregator("a2", makeFieldAccessPostAgg("a2:agg"), 1.0f)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        ImmutableList.of(
            new Object[]{1.0, 3.5, 6.0}
        )
    );
  }

  @Test
  public void testQuantileOnCastedString()
  {
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  TDIGEST_QUANTILE(CAST(dim1 AS DOUBLE), 0.0),\n"
        + "  TDIGEST_QUANTILE(CAST(dim1 AS DOUBLE), 0.5),\n"
        + "  TDIGEST_QUANTILE(CAST(dim1 AS DOUBLE), 1.0)\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "CAST(\"dim1\", 'DOUBLE')",
                          ColumnType.FLOAT,
                          ExprMacroTable.nil()
                      )
                  )
                  .aggregators(new TDigestSketchAggregatorFactory("a0:agg", "v0", 100))
                  .postAggregators(
                      new TDigestSketchToQuantilePostAggregator(
                          "a0",
                          new FieldAccessPostAggregator("a0:agg", "a0:agg"),
                          0.0
                      ),
                      new TDigestSketchToQuantilePostAggregator(
                          "a1",
                          new FieldAccessPostAggregator("a0:agg", "a0:agg"),
                          0.5
                      ),
                      new TDigestSketchToQuantilePostAggregator(
                          "a2",
                          new FieldAccessPostAggregator("a0:agg", "a0:agg"),
                          1.0
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        ImmutableList.of(
            NullHandling.replaceWithDefault()
            ? new Object[]{0.0, 0.5, 10.1}
            : new Object[]{1.0, 2.0, 10.1}
        )
    );
  }

  @Test
  public void testEmptyTimeseriesResults()
  {
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "TDIGEST_GENERATE_SKETCH(m1),"
        + "TDIGEST_QUANTILE(qsketch_m1, 0.1)"
        + "FROM foo WHERE dim2 = 0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .filters(numericEquality("dim2", 0L, ColumnType.LONG))
                  .granularity(Granularities.ALL)
                  .aggregators(ImmutableList.of(
                      new TDigestSketchAggregatorFactory("a0:agg", "m1", TDigestSketchAggregatorFactory.DEFAULT_COMPRESSION),
                      new TDigestSketchAggregatorFactory("a1:agg", "qsketch_m1", 100)
                  ))
                  .postAggregators(
                      new TDigestSketchToQuantilePostAggregator("a1", makeFieldAccessPostAgg("a1:agg"), 0.1f)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        ImmutableList.of(
            new Object[]{"\"AAAAAX/wAAAAAAAA//AAAAAAAABAWQAAAAAAAAAAAAA=\"", Double.NaN}
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
        + "TDIGEST_GENERATE_SKETCH(m1) FILTER(WHERE dim1 = 'nonexistent'),"
        + "TDIGEST_QUANTILE(qsketch_m1, 0.1) FILTER(WHERE dim1 = 'nonexistent')"
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
                                    new TDigestSketchAggregatorFactory("a0:agg", "m1", TDigestSketchAggregatorFactory.DEFAULT_COMPRESSION),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new TDigestSketchAggregatorFactory("a1:agg", "qsketch_m1", 100),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                )
                            )
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new TDigestSketchToQuantilePostAggregator("a1", makeFieldAccessPostAgg("a1:agg"), 0.1f)
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        ImmutableList.of(
            new Object[]{"a", "\"AAAAAX/wAAAAAAAA//AAAAAAAABAWQAAAAAAAAAAAAA=\"", Double.NaN}
        )
    );
  }

  private static PostAggregator makeFieldAccessPostAgg(String name)
  {
    return new FieldAccessPostAggregator(name, name);
  }
}
