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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.variance.StandardDeviationPostAggregator;
import org.apache.druid.query.aggregation.variance.VarianceAggregatorCollector;
import org.apache.druid.query.aggregation.variance.VarianceAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(JUnitParamsRunner.class)
public class VarianceSqlAggregatorTest extends InitializedNullHandlingTest
{
  private static AuthenticationResult authenticationResult = CalciteTests.REGULAR_USER_AUTH_RESULT;
  private static final String DATA_SOURCE = "numfoo";

  private static QueryRunnerFactoryConglomerate conglomerate;
  private static Closer resourceCloser;

  private SqlLifecycle sqlLifecycle;

  @BeforeClass
  public static void setUpClass()
  {
    resourceCloser = Closer.create();
    conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(resourceCloser);
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    resourceCloser.close();
  }

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create();

  private SpecificSegmentsQuerySegmentWalker walker;
  private SqlLifecycleFactory sqlLifecycleFactory;

  @Before
  public void setUp() throws Exception
  {
    InputRowParser parser = new MapInputRowParser(
        new TimeAndDimsParseSpec(
            new TimestampSpec("t", "iso", null),
            new DimensionsSpec(
                ImmutableList.<DimensionSchema>builder()
                    .addAll(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2", "dim3")))
                    .add(new DoubleDimensionSchema("d1"))
                    .add(new FloatDimensionSchema("f1"))
                    .add(new LongDimensionSchema("l1"))
                    .build(),
                null,
                null
            )
        ));

    final QueryableIndex index =
        IndexBuilder.create()
                    .tmpDir(temporaryFolder.newFolder())
                    .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                    .schema(
                        new IncrementalIndexSchema.Builder()
                            .withMetrics(
                                new CountAggregatorFactory("cnt"),
                                new DoubleSumAggregatorFactory("m1", "m1")
                            )
                            .withDimensionsSpec(parser)
                            .withRollup(false)
                            .build()
                    )
                    .rows(CalciteTests.ROWS1_WITH_NUMERIC_DIMS)
                    .buildMMappedIndex();

    walker = new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
        DataSegment.builder()
                   .dataSource(DATA_SOURCE)
                   .interval(index.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        index
    );

    final PlannerConfig plannerConfig = new PlannerConfig();
    final DruidOperatorTable operatorTable = new DruidOperatorTable(
        ImmutableSet.of(
            new BaseVarianceSqlAggregator.VarPopSqlAggregator(),
            new BaseVarianceSqlAggregator.VarSampSqlAggregator(),
            new BaseVarianceSqlAggregator.VarianceSqlAggregator(),
            new BaseVarianceSqlAggregator.StdDevPopSqlAggregator(),
            new BaseVarianceSqlAggregator.StdDevSampSqlAggregator(),
            new BaseVarianceSqlAggregator.StdDevSqlAggregator()
        ),
        ImmutableSet.of()
    );
    SchemaPlus rootSchema =
        CalciteTests.createMockRootSchema(conglomerate, walker, plannerConfig, AuthTestUtils.TEST_AUTHORIZER_MAPPER);

    sqlLifecycleFactory = CalciteTests.createSqlLifecycleFactory(
        new PlannerFactory(
            rootSchema,
            CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
            operatorTable,
            CalciteTests.createExprMacroTable(),
            plannerConfig,
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            CalciteTests.getJsonMapper(),
            CalciteTests.DRUID_SCHEMA_NAME
        )
    );
    queryLogHook.clearRecordedQueries();
    sqlLifecycle = sqlLifecycleFactory.factorize();
  }

  @After
  public void tearDown() throws Exception
  {
    walker.close();
    walker = null;
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
  public void testVarPop() throws Exception
  {
    final String sql = "SELECT\n"
                       + "VAR_POP(d1),\n"
                       + "VAR_POP(f1),\n"
                       + "VAR_POP(l1)\n"
                       + "FROM numfoo";

    final List<Object[]> results =
        sqlLifecycle.runSimple(
            sql,
            BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT,
            CalciteTestBase.DEFAULT_PARAMETERS,
            authenticationResult
        ).toList();

    VarianceAggregatorCollector holder1 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder2 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder3 = new VarianceAggregatorCollector();
    for (InputRow row : CalciteTests.ROWS1_WITH_NUMERIC_DIMS) {
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
            holder2.getVariance(true).floatValue(),
            holder3.getVariance(true).longValue()
        }
    );
    assertResultsEquals(expectedResults, results);

    Assert.assertEquals(
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
              .context(BaseCalciteQueryTest.TIMESERIES_CONTEXT_DEFAULT)
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testVarSamp() throws Exception
  {
    final String sql = "SELECT\n"
                       + "VAR_SAMP(d1),\n"
                       + "VAR_SAMP(f1),\n"
                       + "VAR_SAMP(l1)\n"
                       + "FROM numfoo";

    final List<Object[]> results =
        sqlLifecycle.runSimple(
            sql,
            BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT,
            CalciteTestBase.DEFAULT_PARAMETERS,
            authenticationResult
        ).toList();

    VarianceAggregatorCollector holder1 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder2 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder3 = new VarianceAggregatorCollector();
    for (InputRow row : CalciteTests.ROWS1_WITH_NUMERIC_DIMS) {
      Object raw1 = row.getRaw("d1");
      Object raw2 = row.getRaw("f1");
      Object raw3 = row.getRaw("l1");
      addToHolder(holder1, raw1);
      addToHolder(holder2, raw2);
      addToHolder(holder3, raw3);
    }

    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            holder1.getVariance(false),
            holder2.getVariance(false).floatValue(),
            holder3.getVariance(false).longValue(),
            }
    );
    assertResultsEquals(expectedResults, results);

    Assert.assertEquals(
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
              .context(BaseCalciteQueryTest.TIMESERIES_CONTEXT_DEFAULT)
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testStdDevPop() throws Exception
  {
    final String sql = "SELECT\n"
                       + "STDDEV_POP(d1),\n"
                       + "STDDEV_POP(f1),\n"
                       + "STDDEV_POP(l1)\n"
                       + "FROM numfoo";

    final List<Object[]> results =
        sqlLifecycle.runSimple(
            sql,
            BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT,
            CalciteTestBase.DEFAULT_PARAMETERS,
            authenticationResult
        ).toList();

    VarianceAggregatorCollector holder1 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder2 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder3 = new VarianceAggregatorCollector();
    for (InputRow row : CalciteTests.ROWS1_WITH_NUMERIC_DIMS) {
      Object raw1 = row.getRaw("d1");
      Object raw2 = row.getRaw("f1");
      Object raw3 = row.getRaw("l1");
      addToHolder(holder1, raw1);
      addToHolder(holder2, raw2);
      addToHolder(holder3, raw3);
    }

    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            Math.sqrt(holder1.getVariance(true)),
            (float) Math.sqrt(holder2.getVariance(true)),
            (long) Math.sqrt(holder3.getVariance(true)),
            }
    );
    assertResultsEquals(expectedResults, results);

    Assert.assertEquals(
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
              .context(BaseCalciteQueryTest.TIMESERIES_CONTEXT_DEFAULT)
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testStdDevSamp() throws Exception
  {
    final String sql = "SELECT\n"
                       + "STDDEV_SAMP(d1),\n"
                       + "STDDEV_SAMP(f1),\n"
                       + "STDDEV_SAMP(l1)\n"
                       + "FROM numfoo";

    final List<Object[]> results =
        sqlLifecycle.runSimple(
            sql,
            BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT,
            CalciteTestBase.DEFAULT_PARAMETERS,
            authenticationResult
        ).toList();

    VarianceAggregatorCollector holder1 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder2 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder3 = new VarianceAggregatorCollector();
    for (InputRow row : CalciteTests.ROWS1_WITH_NUMERIC_DIMS) {
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
            (float) Math.sqrt(holder2.getVariance(false)),
            (long) Math.sqrt(holder3.getVariance(false)),
            }
    );
    assertResultsEquals(expectedResults, results);

    Assert.assertEquals(
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
              .context(BaseCalciteQueryTest.TIMESERIES_CONTEXT_DEFAULT)
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testStdDevWithVirtualColumns() throws Exception
  {
    final String sql = "SELECT\n"
                       + "STDDEV(d1*7),\n"
                       + "STDDEV(f1*7),\n"
                       + "STDDEV(l1*7)\n"
                       + "FROM numfoo";

    final List<Object[]> results =
        sqlLifecycle.runSimple(
            sql,
            BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT,
            CalciteTestBase.DEFAULT_PARAMETERS,
            authenticationResult
        ).toList();

    VarianceAggregatorCollector holder1 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder2 = new VarianceAggregatorCollector();
    VarianceAggregatorCollector holder3 = new VarianceAggregatorCollector();
    for (InputRow row : CalciteTests.ROWS1_WITH_NUMERIC_DIMS) {
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
            (float) Math.sqrt(holder2.getVariance(false)),
            (long) Math.sqrt(holder3.getVariance(false)),
            }
    );
    assertResultsEquals(expectedResults, results);

    Assert.assertEquals(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE3)
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
              .granularity(Granularities.ALL)
              .virtualColumns(
                  BaseCalciteQueryTest.expressionVirtualColumn("v0", "(\"d1\" * 7)", ValueType.DOUBLE),
                  BaseCalciteQueryTest.expressionVirtualColumn("v1", "(\"f1\" * 7)", ValueType.FLOAT),
                  BaseCalciteQueryTest.expressionVirtualColumn("v2", "(\"l1\" * 7)", ValueType.LONG)
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
              .context(BaseCalciteQueryTest.TIMESERIES_CONTEXT_DEFAULT)
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }


  @Test
  public void testVarianceOrderBy() throws Exception
  {
    queryLogHook.clearRecordedQueries();
    final String sql = "select dim2, VARIANCE(f1) from druid.numfoo group by 1 order by 2 desc";
    final List<Object[]> results =
        sqlLifecycle.runSimple(
            sql,
            BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT,
            CalciteTestBase.DEFAULT_PARAMETERS,
            authenticationResult
        ).toList();
    List<Object[]> expectedResults = NullHandling.sqlCompatible()
                                     ? ImmutableList.of(
        new Object[]{"a", 0f},
        new Object[]{null, 0f},
        new Object[]{"", 0f},
        new Object[]{"abc", null}
    ) : ImmutableList.of(
        new Object[]{"a", 0.5f},
        new Object[]{"", 0.0033333334f},
        new Object[]{"abc", 0f}
    );
    assertResultsEquals(expectedResults, results);

    Assert.assertEquals(
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
                    .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  public Object[] timeseriesQueryRunners()
  {
    return QueryRunnerTestHelper.makeQueryRunners(
        new TimeseriesQueryRunnerFactory(
            new TimeseriesQueryQueryToolChest(),
            new TimeseriesQueryEngine(),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        )
    ).toArray();
  }

  @Test
  @Parameters(method = "timeseriesQueryRunners")
  public void testEmptyTimeseries(QueryRunner<Result<TimeseriesResultValue>> runner)
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                  .intervals(QueryRunnerTestHelper.EMPTY_INTERVAL)
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.ROWS_COUNT,
                                          QueryRunnerTestHelper.INDEX_DOUBLE_SUM,
                                          new VarianceAggregatorFactory("variance", "index", null, null)
                                      )
                                  )
                                  .descending(true)
                                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                                  .build();
    Map<String, Object> resultMap = new HashMap<>();
    resultMap.put("rows", 0L);
    resultMap.put("index", NullHandling.defaultDoubleValue());
    resultMap.put("variance", NullHandling.defaultDoubleValue());
    List<Result<TimeseriesResultValue>> expectedResults = ImmutableList.of(
        new Result<>(
            DateTimes.of("2020-04-02"),
            new TimeseriesResultValue(
                resultMap
            )
        )
    );
    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query)).toList();
    TestHelper.assertExpectedResults(expectedResults, actualResults);
  }

  private static void assertResultsEquals(List<Object[]> expectedResults, List<Object[]> results)
  {
    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Object[] expectedResult = expectedResults.get(i);
      Object[] result = results.get(i);
      Assert.assertEquals(expectedResult.length, result.length);
      for (int j = 0; j < expectedResult.length; j++) {
        if (expectedResult[j] instanceof Float) {
          Assert.assertEquals((Float) expectedResult[j], (Float) result[j], 1e-10);
        } else if (expectedResult[j] instanceof Double) {
          Assert.assertEquals((Double) expectedResult[j], (Double) result[j], 1e-10);
        } else {
          Assert.assertEquals(expectedResult[j], result[j]);
        }
      }
    }
  }
}
