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

package org.apache.druid.query.aggregation.bloom.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
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
import org.apache.druid.guice.BloomFilterSerializersModule;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.bloom.BloomFilterAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.SystemSchema;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class BloomFilterSqlAggregatorTest
{
  private static final int TEST_NUM_ENTRIES = 1000;
  private static AuthenticationResult authenticationResult = CalciteTests.REGULAR_USER_AUTH_RESULT;
  private static final Injector INJECTOR = Guice.createInjector(
      binder -> {
        binder.bind(Key.get(ObjectMapper.class, Json.class)).toInstance(TestHelper.makeJsonMapper());
        binder.bind(LookupExtractorFactoryContainerProvider.class).toInstance(
            LookupEnabledTestExprMacroTable.createTestLookupReferencesManager(
                ImmutableMap.of(
                    "a", "xa",
                    "abc", "xabc"
                )
            )
        );
      }
  );

  private static ObjectMapper jsonMapper =
      INJECTOR
          .getInstance(Key.get(ObjectMapper.class, Json.class))
          .registerModules(Collections.singletonList(new BloomFilterSerializersModule()));

  private static final String DATA_SOURCE = "numfoo";

  private static QueryRunnerFactoryConglomerate conglomerate;
  private static Closer resourceCloser;

  @BeforeClass
  public static void setUpClass()
  {
    final Pair<QueryRunnerFactoryConglomerate, Closer> conglomerateCloserPair = CalciteTests
        .createQueryRunnerFactoryConglomerate();
    conglomerate = conglomerateCloserPair.lhs;
    resourceCloser = conglomerateCloserPair.rhs;
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    resourceCloser.close();
  }

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create(jsonMapper);

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
                   .build(),
        index
    );

    final PlannerConfig plannerConfig = new PlannerConfig();
    final DruidSchema druidSchema = CalciteTests.createMockSchema(conglomerate, walker, plannerConfig);
    final SystemSchema systemSchema = CalciteTests.createMockSystemSchema(druidSchema, walker, plannerConfig);
    final DruidOperatorTable operatorTable = new DruidOperatorTable(
        ImmutableSet.of(new BloomFilterSqlAggregator()),
        ImmutableSet.of()
    );

    sqlLifecycleFactory = CalciteTests.createSqlLifecycleFactory(
        new PlannerFactory(
            druidSchema,
            systemSchema,
            CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
            operatorTable,
            CalciteTests.createExprMacroTable(),
            plannerConfig,
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            jsonMapper
        )
    );
  }

  @After
  public void tearDown() throws Exception
  {
    walker.close();
    walker = null;
  }

  @Test
  public void testBloomFilterAgg() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT\n"
                       + "BLOOM_FILTER(dim1, 1000)\n"
                       + "FROM numfoo";

    final List<Object[]> results =
        sqlLifecycle.runSimple(sql, BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT, authenticationResult).toList();

    BloomKFilter expected1 = new BloomKFilter(TEST_NUM_ENTRIES);
    for (InputRow row : CalciteTests.ROWS1_WITH_NUMERIC_DIMS) {
      String raw = NullHandling.emptyToNullIfNeeded((String) row.getRaw("dim1"));
      if (raw == null) {
        expected1.addBytes(null, 0, 0);
      } else {
        expected1.addString(raw);
      }
    }

    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            jsonMapper.writeValueAsString(expected1)
        }
    );
    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertArrayEquals(expectedResults.get(i), results.get(i));
    }

    Assert.assertEquals(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE3)
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
              .granularity(Granularities.ALL)
              .aggregators(
                  ImmutableList.of(
                    new BloomFilterAggregatorFactory(
                        "a0:agg",
                        new DefaultDimensionSpec("dim1", "a0:dim1"),
                        TEST_NUM_ENTRIES
                    )
                  )
              )
              .context(BaseCalciteQueryTest.TIMESERIES_CONTEXT_DEFAULT)
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testBloomFilterTwoAggs() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT\n"
                       + "BLOOM_FILTER(dim1, 1000),\n"
                       + "BLOOM_FILTER(dim2, 1000)\n"
                       + "FROM numfoo";

    final List<Object[]> results =
        sqlLifecycle.runSimple(sql, BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT, authenticationResult).toList();

    BloomKFilter expected1 = new BloomKFilter(TEST_NUM_ENTRIES);
    BloomKFilter expected2 = new BloomKFilter(TEST_NUM_ENTRIES);
    for (InputRow row : CalciteTests.ROWS1_WITH_NUMERIC_DIMS) {
      String raw = NullHandling.emptyToNullIfNeeded((String) row.getRaw("dim1"));
      if (raw == null) {
        expected1.addBytes(null, 0, 0);
      } else {
        expected1.addString(raw);
      }
      List<String> lst = row.getDimension("dim2");
      if (lst.size() == 0) {
        expected2.addBytes(null, 0, 0);
      }
      for (String s : lst) {
        String val = NullHandling.emptyToNullIfNeeded(s);
        if (val == null) {
          expected2.addBytes(null, 0, 0);
        } else {
          expected2.addString(val);
        }
      }
    }

    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            jsonMapper.writeValueAsString(expected1),
            jsonMapper.writeValueAsString(expected2)
        }
    );
    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertArrayEquals(expectedResults.get(i), results.get(i));
    }

    Assert.assertEquals(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE3)
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
              .granularity(Granularities.ALL)
              .aggregators(
                  ImmutableList.of(
                      new BloomFilterAggregatorFactory(
                          "a0:agg",
                          new DefaultDimensionSpec("dim1", "a0:dim1"),
                          TEST_NUM_ENTRIES
                      ),
                      new BloomFilterAggregatorFactory(
                          "a1:agg",
                          new DefaultDimensionSpec("dim2", "a1:dim2"),
                          TEST_NUM_ENTRIES
                      )
                  )
              )
              .context(BaseCalciteQueryTest.TIMESERIES_CONTEXT_DEFAULT)
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testBloomFilterAggExtractionFn() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT\n"
                       + "BLOOM_FILTER(SUBSTRING(dim1, 1, 1), 1000)\n"
                       + "FROM numfoo";

    final List<Object[]> results =
        sqlLifecycle.runSimple(sql, BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT, authenticationResult).toList();

    BloomKFilter expected1 = new BloomKFilter(TEST_NUM_ENTRIES);
    for (InputRow row : CalciteTests.ROWS1_WITH_NUMERIC_DIMS) {
      String raw = NullHandling.emptyToNullIfNeeded((String) row.getRaw("dim1"));
      // empty string extractionFn produces null
      if (raw == null || "".equals(raw)) {
        expected1.addBytes(null, 0, 0);
      } else {
        expected1.addString(raw.substring(0, 1));
      }
    }
    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            jsonMapper.writeValueAsString(expected1)
        }
    );
    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertArrayEquals(expectedResults.get(i), results.get(i));
    }

    Assert.assertEquals(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE3)
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
              .granularity(Granularities.ALL)
              .aggregators(
                  ImmutableList.of(
                      new BloomFilterAggregatorFactory(
                          "a0:agg",
                          new ExtractionDimensionSpec(
                              "dim1",
                              "a0:dim1",
                              new SubstringDimExtractionFn(0, 1)
                          ),
                          TEST_NUM_ENTRIES
                      )
                  )
              )
              .context(BaseCalciteQueryTest.TIMESERIES_CONTEXT_DEFAULT)
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testBloomFilterAggLong() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();

    final String sql = "SELECT\n"
                       + "BLOOM_FILTER(l1, 1000)\n"
                       + "FROM numfoo";

    final List<Object[]> results =
        sqlLifecycle.runSimple(sql, BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT, authenticationResult).toList();


    BloomKFilter expected3 = new BloomKFilter(TEST_NUM_ENTRIES);
    for (InputRow row : CalciteTests.ROWS1_WITH_NUMERIC_DIMS) {
      Object raw = row.getRaw("l1");
      if (raw == null) {
        if (NullHandling.replaceWithDefault()) {
          expected3.addLong(NullHandling.defaultLongValue());
        } else {
          expected3.addBytes(null, 0, 0);
        }
      } else {
        expected3.addLong(((Number) raw).longValue());
      }
    }
    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            jsonMapper.writeValueAsString(expected3)
        }
    );
    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertArrayEquals(expectedResults.get(i), results.get(i));
    }

    Assert.assertEquals(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE3)
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
              .granularity(Granularities.ALL)
              .aggregators(
                  ImmutableList.of(
                    new BloomFilterAggregatorFactory(
                        "a0:agg",
                        new DefaultDimensionSpec("l1", "a0:l1", ValueType.LONG),
                        TEST_NUM_ENTRIES
                    )
                )
              )
              .context(BaseCalciteQueryTest.TIMESERIES_CONTEXT_DEFAULT)
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testBloomFilterAggLongVirtualColumn() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT\n"
                       + "BLOOM_FILTER(l1 * 2, 1000)\n"
                       + "FROM numfoo";

    final List<Object[]> results =
        sqlLifecycle.runSimple(sql, BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT, authenticationResult).toList();

    BloomKFilter expected1 = new BloomKFilter(TEST_NUM_ENTRIES);
    for (InputRow row : CalciteTests.ROWS1_WITH_NUMERIC_DIMS) {
      Object raw = row.getRaw("l1");
      if (raw == null) {
        if (NullHandling.replaceWithDefault()) {
          expected1.addLong(NullHandling.defaultLongValue());
        } else {
          expected1.addBytes(null, 0, 0);
        }
      } else {
        expected1.addLong(2 * ((Number) raw).longValue());
      }
    }
    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            jsonMapper.writeValueAsString(expected1)
        }
    );
    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertArrayEquals(expectedResults.get(i), results.get(i));
    }

    Assert.assertEquals(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE3)
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
              .granularity(Granularities.ALL)
              .virtualColumns(
                  new ExpressionVirtualColumn(
                    "v0",
                    "(\"l1\" * 2)",
                    ValueType.LONG,
                    TestExprMacroTable.INSTANCE
                )
              )
              .aggregators(
                  ImmutableList.of(
                    new BloomFilterAggregatorFactory(
                        "a0:agg",
                        new DefaultDimensionSpec("v0", "v0"),
                        TEST_NUM_ENTRIES
                    )
                )
              )
              .context(BaseCalciteQueryTest.TIMESERIES_CONTEXT_DEFAULT)
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testBloomFilterAggFloatVirtualColumn() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT\n"
                       + "BLOOM_FILTER(f1 * 2, 1000)\n"
                       + "FROM numfoo";

    final List<Object[]> results =
        sqlLifecycle.runSimple(sql, BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT, authenticationResult).toList();

    BloomKFilter expected1 = new BloomKFilter(TEST_NUM_ENTRIES);
    for (InputRow row : CalciteTests.ROWS1_WITH_NUMERIC_DIMS) {
      Object raw = row.getRaw("f1");
      if (raw == null) {
        if (NullHandling.replaceWithDefault()) {
          expected1.addFloat(NullHandling.defaultFloatValue());
        } else {
          expected1.addBytes(null, 0, 0);
        }
      } else {
        expected1.addFloat(2 * ((Number) raw).floatValue());
      }
    }
    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            jsonMapper.writeValueAsString(expected1)
        }
    );
    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertArrayEquals(expectedResults.get(i), results.get(i));
    }

    // Verify query
    Assert.assertEquals(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE3)
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
              .granularity(Granularities.ALL)
              .virtualColumns(
                  new ExpressionVirtualColumn(
                    "v0",
                    "(\"f1\" * 2)",
                    ValueType.FLOAT,
                    TestExprMacroTable.INSTANCE
                )
              )
              .aggregators(
                  ImmutableList.of(
                    new BloomFilterAggregatorFactory(
                        "a0:agg",
                        new DefaultDimensionSpec("v0", "v0"),
                        TEST_NUM_ENTRIES
                    )
                )
              )
              .context(BaseCalciteQueryTest.TIMESERIES_CONTEXT_DEFAULT)
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testBloomFilterAggDoubleVirtualColumn() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT\n"
                       + "BLOOM_FILTER(d1 * 2, 1000)\n"
                       + "FROM numfoo";

    final List<Object[]> results =
        sqlLifecycle.runSimple(sql, BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT, authenticationResult).toList();

    BloomKFilter expected1 = new BloomKFilter(TEST_NUM_ENTRIES);
    for (InputRow row : CalciteTests.ROWS1_WITH_NUMERIC_DIMS) {
      Object raw = row.getRaw("d1");
      if (raw == null) {
        if (NullHandling.replaceWithDefault()) {
          expected1.addDouble(NullHandling.defaultDoubleValue());
        } else {
          expected1.addBytes(null, 0, 0);
        }
      } else {
        expected1.addDouble(2 * ((Number) raw).doubleValue());
      }
    }
    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            jsonMapper.writeValueAsString(expected1)
        }
    );
    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertArrayEquals(expectedResults.get(i), results.get(i));
    }

    // Verify query
    Assert.assertEquals(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE3)
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
              .granularity(Granularities.ALL)
              .virtualColumns(
                  new ExpressionVirtualColumn(
                    "v0",
                    "(\"d1\" * 2)",
                    ValueType.DOUBLE,
                    TestExprMacroTable.INSTANCE
                )
              )
              .aggregators(
                  ImmutableList.of(
                    new BloomFilterAggregatorFactory(
                        "a0:agg",
                        new DefaultDimensionSpec("v0", "v0"),
                        TEST_NUM_ENTRIES
                    )
                )
              )
              .context(BaseCalciteQueryTest.TIMESERIES_CONTEXT_DEFAULT)
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }
}
