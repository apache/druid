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

package org.apache.druid.query.aggregation.variance.sql;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.stats.DruidStatsModule;
import org.apache.druid.query.aggregation.variance.StandardDeviationPostAggregator;
import org.apache.druid.query.aggregation.variance.VarianceAggregatorCollector;
import org.apache.druid.query.aggregation.variance.VarianceAggregatorFactory;
import org.apache.druid.query.aggregation.variance.VarianceSerde;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.serde.ComplexMetrics;
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

public class VarianceSqlAggregatorTest extends BaseCalciteQueryTest
{
  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
    builder.addModule(new DruidStatsModule());
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final JoinableFactoryWrapper joinableFactory,
      final Injector injector
  ) throws IOException
  {
    ComplexMetrics.registerSerde(VarianceSerde.TYPE_NAME, new VarianceSerde());

    final QueryableIndex index =
        IndexBuilder.create(CalciteTests.getJsonMapper().registerModules(new DruidStatsModule().getJacksonModules()))
                    .tmpDir(temporaryFolder.newFolder())
                    .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                    .schema(
                        new IncrementalIndexSchema.Builder()
                            .withDimensionsSpec(
                                new DimensionsSpec(
                                    ImmutableList.<DimensionSchema>builder()
                                                 .addAll(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2", "dim3")))
                                                 .add(new DoubleDimensionSchema("d1"))
                                                 .add(new FloatDimensionSchema("f1"))
                                                 .add(new LongDimensionSchema("l1"))
                                                 .build()
                                )
                            )
                            .withMetrics(
                                new CountAggregatorFactory("cnt"),
                                new DoubleSumAggregatorFactory("m1", "m1"),
                                new VarianceAggregatorFactory("var1", "m1", null, null)
                            )
                            .withRollup(false)
                            .build()
                    )
                    .rows(TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS)
                    .buildMMappedIndex();

    return SpecificSegmentsQuerySegmentWalker.createWalker(injector, conglomerate).add(
        DataSegment.builder()
                   .dataSource(CalciteTests.DATASOURCE3)
                   .interval(index.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        index
    );
  }

  public void addToHolder(VarianceAggregatorCollector holder, Object raw)
  {
    addToHolder(holder, raw, 1);
  }

  public void addToHolder(VarianceAggregatorCollector holder, Object raw, int multiply)
  {
    if (raw != null) {
      if (raw instanceof Double) {
        double v = ((Double) raw).doubleValue() * multiply;
        holder.add(v);
      } else if (raw instanceof Float) {
        float v = ((Float) raw).floatValue() * multiply;
        holder.add(v);
      } else if (raw instanceof Long) {
        long v = ((Long) raw).longValue() * multiply;
        holder.add(v);
      } else if (raw instanceof Integer) {
        int v = ((Integer) raw).intValue() * multiply;
        holder.add(v);
      }
    } else {
      if (NullHandling.replaceWithDefault()) {
        holder.add(0.0f);
      }
    }
  }

  @Test
  public void testVarPop()
  {
    VarianceAggregatorCollector holder1 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder2 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder3 = new VarianceAggregatorCollector();
    for (InputRow row : TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS) {
      Object raw1 = row.getRaw("d1");
      Object raw2 = row.getRaw("f1");
      Object raw3 = row.getRaw("l1");
      addToHolder(holder1, raw1);
      addToHolder(holder2, raw2);
      addToHolder(holder3, raw3);
    }

    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            holder1.getVariance(true),
            holder2.getVariance(true),
            holder3.getVariance(true)
        }
    );
    testQuery(
        "SELECT\n"
        + "VAR_POP(d1),\n"
        + "VAR_POP(f1),\n"
        + "VAR_POP(l1)\n"
        + "FROM numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      ImmutableList.of(
                          new VarianceAggregatorFactory("a0:agg", "d1", "population", "double"),
                          new VarianceAggregatorFactory("a1:agg", "f1", "population", "float"),
                          new VarianceAggregatorFactory("a2:agg", "l1", "population", "long")
                      )
                  )
                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        expectedResults
    );
  }

  @Test
  public void testVarSamp()
  {
    VarianceAggregatorCollector holder1 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder2 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder3 = new VarianceAggregatorCollector();
    for (InputRow row : TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS) {
      Object raw1 = row.getRaw("d1");
      Object raw2 = row.getRaw("f1");
      Object raw3 = row.getRaw("l1");
      addToHolder(holder1, raw1);
      addToHolder(holder2, raw2);
      addToHolder(holder3, raw3);
    }

    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[] {
            holder1.getVariance(false),
            holder2.getVariance(false).doubleValue(),
            holder3.getVariance(false),
        }
    );
    testQuery(
        "SELECT\n"
        + "VAR_SAMP(d1),\n"
        + "VAR_SAMP(f1),\n"
        + "VAR_SAMP(l1)\n"
        + "FROM numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      ImmutableList.of(
                          new VarianceAggregatorFactory("a0:agg", "d1", "sample", "double"),
                          new VarianceAggregatorFactory("a1:agg", "f1", "sample", "float"),
                          new VarianceAggregatorFactory("a2:agg", "l1", "sample", "long")
                      )
                  )
                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        expectedResults
    );
  }

  @Test
  public void testStdDevPop()
  {
    VarianceAggregatorCollector holder1 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder2 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder3 = new VarianceAggregatorCollector();
    for (InputRow row : TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS) {
      Object raw1 = row.getRaw("d1");
      Object raw2 = row.getRaw("f1");
      Object raw3 = row.getRaw("l1");
      addToHolder(holder1, raw1);
      addToHolder(holder2, raw2);
      addToHolder(holder3, raw3);
    }

    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[] {
            Math.sqrt(holder1.getVariance(true)),
            Math.sqrt(holder2.getVariance(true)),
            Math.sqrt(holder3.getVariance(true)),
        }
    );

    testQuery(
        "SELECT\n"
        + "STDDEV_POP(d1),\n"
        + "STDDEV_POP(f1),\n"
        + "STDDEV_POP(l1)\n"
        + "FROM numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      ImmutableList.of(
                          new VarianceAggregatorFactory("a0:agg", "d1", "population", "double"),
                          new VarianceAggregatorFactory("a1:agg", "f1", "population", "float"),
                          new VarianceAggregatorFactory("a2:agg", "l1", "population", "long")
                      )
                  )
                  .postAggregators(
                      ImmutableList.of(
                          new StandardDeviationPostAggregator("a0", "a0:agg", "population"),
                          new StandardDeviationPostAggregator("a1", "a1:agg", "population"),
                          new StandardDeviationPostAggregator("a2", "a2:agg", "population")
                      )
                  )
                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        expectedResults
    );
  }

  @Test
  public void testStdDevSamp()
  {
    VarianceAggregatorCollector holder1 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder2 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder3 = new VarianceAggregatorCollector();
    for (InputRow row : TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS) {
      Object raw1 = row.getRaw("d1");
      Object raw2 = row.getRaw("f1");
      Object raw3 = row.getRaw("l1");
      addToHolder(holder1, raw1);
      addToHolder(holder2, raw2);
      addToHolder(holder3, raw3);
    }

    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            Math.sqrt(holder1.getVariance(false)),
            Math.sqrt(holder2.getVariance(false)),
            Math.sqrt(holder3.getVariance(false)),
        }
    );

    testQuery(
        "SELECT\n"
        + "STDDEV_SAMP(d1),\n"
        + "STDDEV_SAMP(f1),\n"
        + "STDDEV_SAMP(l1)\n"
        + "FROM numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      ImmutableList.of(
                          new VarianceAggregatorFactory("a0:agg", "d1", "sample", "double"),
                          new VarianceAggregatorFactory("a1:agg", "f1", "sample", "float"),
                          new VarianceAggregatorFactory("a2:agg", "l1", "sample", "long")
                      )
                  )
                  .postAggregators(
                      new StandardDeviationPostAggregator("a0", "a0:agg", "sample"),
                      new StandardDeviationPostAggregator("a1", "a1:agg", "sample"),
                      new StandardDeviationPostAggregator("a2", "a2:agg", "sample")
                  )
                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        expectedResults
    );
  }

  @Test
  public void testStdDevWithVirtualColumns()
  {
    VarianceAggregatorCollector holder1 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder2 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder3 = new VarianceAggregatorCollector();
    for (InputRow row : TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS) {
      Object raw1 = row.getRaw("d1");
      Object raw2 = row.getRaw("f1");
      Object raw3 = row.getRaw("l1");
      addToHolder(holder1, raw1, 7);
      addToHolder(holder2, raw2, 7);
      addToHolder(holder3, raw3, 7);
    }

    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            Math.sqrt(holder1.getVariance(false)),
            Math.sqrt(holder2.getVariance(false)),
            Math.sqrt(holder3.getVariance(false)),
        }
    );

    testQuery(
        "SELECT\n"
        + "STDDEV(d1*7),\n"
        + "STDDEV(f1*7),\n"
        + "STDDEV(l1*7)\n"
        + "FROM numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      BaseCalciteQueryTest.expressionVirtualColumn("v0", "(\"d1\" * 7)", ColumnType.DOUBLE),
                      BaseCalciteQueryTest.expressionVirtualColumn("v1", "(\"f1\" * 7)", ColumnType.FLOAT),
                      BaseCalciteQueryTest.expressionVirtualColumn("v2", "(\"l1\" * 7)", ColumnType.LONG)
                  )
                  .aggregators(
                      ImmutableList.of(
                          new VarianceAggregatorFactory("a0:agg", "v0", "sample", "double"),
                          new VarianceAggregatorFactory("a1:agg", "v1", "sample", "float"),
                          new VarianceAggregatorFactory("a2:agg", "v2", "sample", "long")
                      )
                  )
                  .postAggregators(
                      new StandardDeviationPostAggregator("a0", "a0:agg", "sample"),
                      new StandardDeviationPostAggregator("a1", "a1:agg", "sample"),
                      new StandardDeviationPostAggregator("a2", "a2:agg", "sample")
                  )
                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        expectedResults
    );
  }


  @Test
  public void testVarianceOrderBy()
  {
    List<Object[]> expectedResults = NullHandling.sqlCompatible()
                                     ? ImmutableList.of(
        new Object[]{"a", 0.0},
        new Object[]{null, 0.0},
        new Object[]{"", 0.0},
        new Object[]{"abc", null}
    ) : ImmutableList.of(
        new Object[]{"a", 0.5},
        new Object[]{"", 0.003333333432674409},
        new Object[]{"abc", 0.0}
    );

    testQuery(
        "select dim2, VARIANCE(f1) from druid.numfoo group by 1 order by 2 desc",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(new DefaultDimensionSpec("dim2", "_d0"))
                        .setAggregatorSpecs(
                            new VarianceAggregatorFactory("a0:agg", "f1", "sample", "float")
                        )
                        .setLimitSpec(
                            DefaultLimitSpec
                                .builder()
                                .orderBy(
                                    new OrderByColumnSpec(
                                        "a0:agg",
                                        OrderByColumnSpec.Direction.DESCENDING,
                                        StringComparators.NUMERIC
                                    )
                                )
                                .build()
                        )
                        .setContext(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        expectedResults
    );
  }

  @Test
  public void testVariancesOnCastedString()
  {
    testQuery(
        "SELECT\n"
        + "STDDEV_POP(CAST(dim1 AS DOUBLE)),\n"
        + "STDDEV_SAMP(CAST(dim1 AS DOUBLE)),\n"
        + "STDDEV(CAST(dim1 AS DOUBLE)),\n"
        + "VARIANCE(CAST(dim1 AS DOUBLE))\n"
        + "FROM numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
            .dataSource(CalciteTests.DATASOURCE3)
            .intervals(querySegmentSpec(Filtration.eternity()))
            .virtualColumns(
                new ExpressionVirtualColumn("v0", "CAST(\"dim1\", 'DOUBLE')", ColumnType.DOUBLE, ExprMacroTable.nil())
            )
            .granularity(Granularities.ALL)
            .aggregators(
                new VarianceAggregatorFactory("a0:agg", "v0", "population", "double"),
                new VarianceAggregatorFactory("a1:agg", "v0", "sample", "double"),
                new VarianceAggregatorFactory("a2:agg", "v0", "sample", "double"),
                new VarianceAggregatorFactory("a3:agg", "v0", "sample", "double")
            )
            .postAggregators(
                new StandardDeviationPostAggregator("a0", "a0:agg", "population"),
                new StandardDeviationPostAggregator("a1", "a1:agg", "sample"),
                new StandardDeviationPostAggregator("a2", "a2:agg", "sample")
            )
            .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
            .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        ImmutableList.of(
            NullHandling.replaceWithDefault()
            ? new Object[]{3.61497656362466, 3.960008417499471, 3.960008417499471, 15.681666666666667}
            : new Object[]{4.074582459862878, 4.990323970779185, 4.990323970779185, 24.903333333333332}
        )
    );
  }

  @Test
  public void testEmptyTimeseriesResults()
  {
    testQuery(
        "SELECT\n"
        + "STDDEV_POP(d1),\n"
        + "STDDEV_SAMP(d1),\n"
        + "STDDEV(d1),\n"
        + "VARIANCE(d1),\n"
        + "STDDEV_POP(l1),\n"
        + "STDDEV_SAMP(l1),\n"
        + "STDDEV(l1),\n"
        + "VARIANCE(l1)\n"
        + "FROM numfoo WHERE dim2 = 0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(numericEquality("dim2", 0L, ColumnType.LONG))
                  .aggregators(
                      new VarianceAggregatorFactory("a0:agg", "d1", "population", "double"),
                      new VarianceAggregatorFactory("a1:agg", "d1", "sample", "double"),
                      new VarianceAggregatorFactory("a2:agg", "d1", "sample", "double"),
                      new VarianceAggregatorFactory("a3:agg", "d1", "sample", "double"),
                      new VarianceAggregatorFactory("a4:agg", "l1", "population", "long"),
                      new VarianceAggregatorFactory("a5:agg", "l1", "sample", "long"),
                      new VarianceAggregatorFactory("a6:agg", "l1", "sample", "long"),
                      new VarianceAggregatorFactory("a7:agg", "l1", "sample", "long")

                  )
                  .postAggregators(
                      new StandardDeviationPostAggregator("a0", "a0:agg", "population"),
                      new StandardDeviationPostAggregator("a1", "a1:agg", "sample"),
                      new StandardDeviationPostAggregator("a2", "a2:agg", "sample"),
                      new StandardDeviationPostAggregator("a4", "a4:agg", "population"),
                      new StandardDeviationPostAggregator("a5", "a5:agg", "sample"),
                      new StandardDeviationPostAggregator("a6", "a6:agg", "sample")
                  )
                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        ImmutableList.of(
            NullHandling.replaceWithDefault()
            ? new Object[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}
            : new Object[]{null, null, null, null, null, null, null, null}
        )
    );
  }

  @Test
  public void testGroupByAggregatorDefaultValues()
  {
    testQuery(
        "SELECT\n"
        + "dim2,\n"
        + "STDDEV_POP(d1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + "STDDEV_SAMP(d1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + "STDDEV(d1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + "VARIANCE(d1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + "STDDEV_POP(l1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + "STDDEV_SAMP(l1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + "STDDEV(l1) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + "VARIANCE(l1) FILTER(WHERE dim1 = 'nonexistent')\n"
        + "FROM numfoo WHERE dim2 = 'a' GROUP BY dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(equality("dim2", "a", ColumnType.STRING))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "'a'", ColumnType.STRING))
                        .setDimensions(new DefaultDimensionSpec("v0", "_d0", ColumnType.STRING))
                        .setAggregatorSpecs(
                            aggregators(
                                new FilteredAggregatorFactory(
                                    new VarianceAggregatorFactory("a0:agg", "d1", "population", "double"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new VarianceAggregatorFactory("a1:agg", "d1", "sample", "double"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new VarianceAggregatorFactory("a2:agg", "d1", "sample", "double"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new VarianceAggregatorFactory("a3:agg", "d1", "sample", "double"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new VarianceAggregatorFactory("a4:agg", "l1", "population", "long"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new VarianceAggregatorFactory("a5:agg", "l1", "sample", "long"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new VarianceAggregatorFactory("a6:agg", "l1", "sample", "long"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new VarianceAggregatorFactory("a7:agg", "l1", "sample", "long"),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                )
                            )
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new StandardDeviationPostAggregator("a0", "a0:agg", "population"),
                                new StandardDeviationPostAggregator("a1", "a1:agg", "sample"),
                                new StandardDeviationPostAggregator("a2", "a2:agg", "sample"),
                                new StandardDeviationPostAggregator("a4", "a4:agg", "population"),
                                new StandardDeviationPostAggregator("a5", "a5:agg", "sample"),
                                new StandardDeviationPostAggregator("a6", "a6:agg", "sample")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        ImmutableList.of(
            NullHandling.replaceWithDefault()
            ? new Object[]{"a", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}
            : new Object[]{"a", null, null, null, null, null, null, null, null}
        )
    );
  }

  @Test
  public void testVarianceAggAsInput()
  {
    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            3.5D,
            2.9166666666666665D,
            3.5D,
            1.8708286933869707D,
            1.707825127659933D,
            1.8708286933869707D
        }
    );
    testQuery(
        "SELECT\n"
        + "VARIANCE(var1),\n"
        + "VAR_POP(var1),\n"
        + "VAR_SAMP(var1),\n"
        + "STDDEV(var1),\n"
        + "STDDEV_POP(var1),\n"
        + "STDDEV_SAMP(var1)\n"
        + "FROM numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      ImmutableList.of(
                          new VarianceAggregatorFactory("a0:agg", "var1", "sample", "variance"),
                          new VarianceAggregatorFactory("a1:agg", "var1", "population", "variance"),
                          new VarianceAggregatorFactory("a2:agg", "var1", "sample", "variance"),
                          new VarianceAggregatorFactory("a3:agg", "var1", "sample", "variance"),
                          new VarianceAggregatorFactory("a4:agg", "var1", "population", "variance"),
                          new VarianceAggregatorFactory("a5:agg", "var1", "sample", "variance")
                      )
                  )
                  .postAggregators(
                      new StandardDeviationPostAggregator("a3", "a3:agg", "sample"),
                      new StandardDeviationPostAggregator("a4", "a4:agg", "population"),
                      new StandardDeviationPostAggregator("a5", "a5:agg", "sample")
                  )
                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ResultMatchMode.EQUALS_EPS,
        expectedResults
    );
  }
}
