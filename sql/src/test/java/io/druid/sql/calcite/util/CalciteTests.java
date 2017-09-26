/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.metamx.emitter.core.NoopEmitter;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.collections.StupidPool;
import io.druid.data.input.Firehose;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.guice.ExpressionModule;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.Pair;
import io.druid.math.expr.ExprMacroTable;
import io.druid.query.DefaultGenericQueryMetricsFactory;
import io.druid.query.DefaultQueryRunnerFactoryConglomerate;
import io.druid.query.DruidProcessingConfig;
import io.druid.query.Query;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FloatSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.expression.LookupExprMacro;
import io.druid.query.expression.TestExprMacroTable;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import io.druid.query.groupby.strategy.GroupByStrategySelector;
import io.druid.query.lookup.LookupReferencesManager;
import io.druid.query.metadata.SegmentMetadataQueryConfig;
import io.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import io.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.scan.ScanQuery;
import io.druid.query.scan.ScanQueryConfig;
import io.druid.query.scan.ScanQueryEngine;
import io.druid.query.scan.ScanQueryQueryToolChest;
import io.druid.query.scan.ScanQueryRunnerFactory;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectQueryConfig;
import io.druid.query.select.SelectQueryEngine;
import io.druid.query.select.SelectQueryQueryToolChest;
import io.druid.query.select.SelectQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryConfig;
import io.druid.query.topn.TopNQueryQueryToolChest;
import io.druid.query.topn.TopNQueryRunnerFactory;
import io.druid.segment.IndexBuilder;
import io.druid.segment.QueryableIndex;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.realtime.firehose.LocalFirehoseFactory;
import io.druid.server.QueryLifecycleFactory;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.log.NoopRequestLogger;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthTestUtils;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.expression.SqlOperatorConversion;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.schema.DruidSchema;
import io.druid.sql.calcite.view.NoopViewManager;
import io.druid.sql.calcite.view.ViewManager;
import io.druid.sql.guice.SqlModule;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Utility functions for Calcite tests.
 */
public class CalciteTests
{
  public static final String DATASOURCE1 = "foo";
  public static final String DATASOURCE2 = "foo2";
  public static final String LINEITEM_DATASOURCE = "lineitem";

  private static final String FOO_TIMESTAMP_COLUMN = "t";
  private static final String LINEITEM_TIMESTAMP_COLUMN = "l_shipdate";

  private static final IncrementalIndexSchema INDEX_SCHEMA = new IncrementalIndexSchema.Builder()
      .withMetrics(
          new CountAggregatorFactory("cnt"),
          new FloatSumAggregatorFactory("m1", "m1"),
          new DoubleSumAggregatorFactory("m2", "m2"),
          new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
      )
      .withRollup(false)
      .build();

  private static final IncrementalIndexSchema LINEITEM_INDEX_SCHEMA = new IncrementalIndexSchema.Builder()
      .withMetrics(
          new CountAggregatorFactory("cnt"),
          new LongSumAggregatorFactory("l_quantity", "l_quantity"),
          new FloatSumAggregatorFactory("l_extendedprice", "l_extendedprice"),
          new DoubleSumAggregatorFactory("l_discount", "l_discount"),
          new DoubleSumAggregatorFactory("l_tax", "l_tax"),
          new HyperUniquesAggregatorFactory("unique_suppkey", "l_suppkey")
      )
      .withRollup(false)
      .build();

  private static final InputRowParser<Map<String, Object>> FOO_PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(FOO_TIMESTAMP_COLUMN, "iso", null),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2")),
              null,
              null
          )
      )
  );

  private static final StringInputRowParser LINEITEM_PARSER = new StringInputRowParser(
      new DelimitedParseSpec(
          new TimestampSpec(LINEITEM_TIMESTAMP_COLUMN, "auto", null),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of(
                  "l_orderkey",
                  "l_partkey",
                  "l_suppkey",
                  "l_linenumber",
                  "l_returnflag",
                  "l_linestatus",
                  "l_shipdate",
                  "l_commitdate",
                  "l_receiptdate",
                  "l_shipinstruct",
                  "l_shipmode",
                  "l_comment"
              )),
              null,
              null
          ),
          "|",
          null,
          Lists.newArrayList(
              "l_orderkey",
              "l_partkey",
              "l_suppkey",
              "l_linenumber",
              "l_quantity",
              "l_extendedprice",
              "l_discount",
              "l_tax",
              "l_returnflag",
              "l_linestatus",
              "l_shipdate",
              "l_commitdate",
              "l_receiptdate",
              "l_shipinstruct",
              "l_shipmode",
              "l_comment"
          ),
          false,
          0
      ),
      null
  );

  public static final List<InputRow> ROWS1 = ImmutableList.of(
      createRow(
          ImmutableMap.of("t", "2000-01-01", "m1", "1.0", "m2", "1.0", "dim1", "", "dim2", ImmutableList.of("a"))
      ),
      createRow(
          ImmutableMap.of("t", "2000-01-02", "m1", "2.0", "m2", "2.0", "dim1", "10.1", "dim2", ImmutableList.of())
      ),
      createRow(
          ImmutableMap.of("t", "2000-01-03", "m1", "3.0", "m2", "3.0", "dim1", "2", "dim2", ImmutableList.of(""))
      ),
      createRow(
          ImmutableMap.of("t", "2001-01-01", "m1", "4.0", "m2", "4.0", "dim1", "1", "dim2", ImmutableList.of("a"))
      ),
      createRow(
          ImmutableMap.of("t", "2001-01-02", "m1", "5.0", "m2", "5.0", "dim1", "def", "dim2", ImmutableList.of("abc"))
      ),
      createRow(
          ImmutableMap.of("t", "2001-01-03", "m1", "6.0", "m2", "6.0", "dim1", "abc")
      )
  );

  public static final List<InputRow> ROWS2 = ImmutableList.of(
      createRow("2000-01-01", "דרואיד", "he", 1.0),
      createRow("2000-01-01", "druid", "en", 1.0),
      createRow("2000-01-01", "друид", "ru", 1.0)
  );

  private static final Map<String, TestDataSource> TEST_DATA_SOURCES = initializeTestSources();

  private static class TestDataSource
  {
    private final String name;
    private final String timestampColumnName;
    private final InputRowParser<?> parser;
    private final IncrementalIndexSchema indexSchema;
    private final Function<File, List<InputRow>> inputRowProvider;

    TestDataSource(String name, String timestampColumnName, InputRowParser<?> parser, IncrementalIndexSchema indexSchema, Function<File, List<InputRow>> inputRowProvider)
    {
      this.name = name;
      this.timestampColumnName = timestampColumnName;
      this.parser = parser;
      this.indexSchema = indexSchema;
      this.inputRowProvider = inputRowProvider;
    }
  }

  private static final Map<String, TestDataSource> initializeTestSources()
  {
    return ImmutableMap.of(
        DATASOURCE1,
        new TestDataSource(DATASOURCE1, FOO_TIMESTAMP_COLUMN, FOO_PARSER, INDEX_SCHEMA, notUsed -> ROWS1),
        DATASOURCE2,
        new TestDataSource(DATASOURCE2, FOO_TIMESTAMP_COLUMN, FOO_PARSER, INDEX_SCHEMA, notUsed -> ROWS2),
        LINEITEM_DATASOURCE,
        new TestDataSource(
            LINEITEM_DATASOURCE,
            LINEITEM_TIMESTAMP_COLUMN,
            LINEITEM_PARSER,
            LINEITEM_INDEX_SCHEMA,
            tmpDir -> {
              try {
                return loadTestData(tmpDir);
              }
              catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
        )
    );
  }

  private static final Supplier<SelectQueryConfig> SELECT_CONFIG_SUPPLIER = Suppliers.ofInstance(
      new SelectQueryConfig(true)
  );

  private static final Injector INJECTOR = Guice.createInjector(
      new Module()
      {
        @Override
        public void configure(final Binder binder)
        {
          binder.bind(Key.get(ObjectMapper.class, Json.class)).toInstance(TestHelper.getJsonMapper());

          // This Module is just to get a LookupReferencesManager with a usable "lookyloo" lookup.

          LookupReferencesManager testLookupReferencesManager = TestExprMacroTable.createTestLookupReferencesManager(
              ImmutableMap.of(
                  "a", "xa",
                  "abc", "xabc"
              )
          );
          binder.bind(LookupReferencesManager.class).toInstance(testLookupReferencesManager);
        }
      }
  );

  private static final QueryRunnerFactoryConglomerate CONGLOMERATE = new DefaultQueryRunnerFactoryConglomerate(
      ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>builder()
          .put(
              SegmentMetadataQuery.class,
              new SegmentMetadataQueryRunnerFactory(
                  new SegmentMetadataQueryQueryToolChest(
                      new SegmentMetadataQueryConfig("P1W")
                  ),
                  QueryRunnerTestHelper.NOOP_QUERYWATCHER
              )
          )
          .put(
              ScanQuery.class,
              new ScanQueryRunnerFactory(
                  new ScanQueryQueryToolChest(
                      new ScanQueryConfig(),
                      new DefaultGenericQueryMetricsFactory(TestHelper.getJsonMapper())
                  ),
                  new ScanQueryEngine()
              )
          )
          .put(
              SelectQuery.class,
              new SelectQueryRunnerFactory(
                  new SelectQueryQueryToolChest(
                      TestHelper.getJsonMapper(),
                      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator(),
                      SELECT_CONFIG_SUPPLIER
                  ),
                  new SelectQueryEngine(SELECT_CONFIG_SUPPLIER),
                  QueryRunnerTestHelper.NOOP_QUERYWATCHER
              )
          )
          .put(
              TimeseriesQuery.class,
              new TimeseriesQueryRunnerFactory(
                  new TimeseriesQueryQueryToolChest(
                      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                  ),
                  new TimeseriesQueryEngine(),
                  QueryRunnerTestHelper.NOOP_QUERYWATCHER
              )
          )
          .put(
              TopNQuery.class,
              new TopNQueryRunnerFactory(
                  new StupidPool<>(
                      "TopNQueryRunnerFactory-bufferPool",
                      new Supplier<ByteBuffer>()
                      {
                        @Override
                        public ByteBuffer get()
                        {
                          return ByteBuffer.allocate(10 * 1024 * 1024);
                        }
                      }
                  ),
                  new TopNQueryQueryToolChest(
                      new TopNQueryConfig(),
                      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                  ),
                  QueryRunnerTestHelper.NOOP_QUERYWATCHER
              )
          )
          .put(
              GroupByQuery.class,
              GroupByQueryRunnerTest.makeQueryRunnerFactory(
                  GroupByQueryRunnerTest.DEFAULT_MAPPER,
                  new GroupByQueryConfig()
                  {
                    @Override
                    public String getDefaultStrategy()
                    {
                      return GroupByStrategySelector.STRATEGY_V2;
                    }
                  },
                  new DruidProcessingConfig()
                  {
                    @Override
                    public String getFormatString()
                    {
                      return null;
                    }

                    @Override
                    public int intermediateComputeSizeBytes()
                    {
                      return 10 * 1024 * 1024;
                    }

                    @Override
                    public int getNumMergeBuffers()
                    {
                      // Need 3 buffers for CalciteQueryTest.testDoubleNestedGroupby.
                      // Two buffers for the broker and one for the queryable
                      return 3;
                    }
                  }
              )
          )
          .build()
  );

  private static List<InputRow> loadTestData(final File tmpDir) throws IOException
  {
    final URL testDataUrl = CalciteTests.class.getClassLoader().getResource("lineitem");
    Preconditions.checkNotNull(testDataUrl, "missing data directory for lineitem");
    try (
        final Firehose firehose = new LocalFirehoseFactory(
            new File(testDataUrl.getFile()), "*.tbl", null
        ).connect(LINEITEM_PARSER, tmpDir)
    ) {
      final List<InputRow> rows = new ArrayList<>();
      while (firehose.hasMore()) {
        rows.add(firehose.nextRow());
      }
      return rows;
    }
  }

  private CalciteTests()
  {
    // No instantiation.
  }

  public static QueryRunnerFactoryConglomerate queryRunnerFactoryConglomerate()
  {
    return CONGLOMERATE;
  }

  public static QueryLifecycleFactory createMockQueryLifecycleFactory(final QuerySegmentWalker walker)
  {
    return new QueryLifecycleFactory(
        new QueryToolChestWarehouse()
        {
          @Override
          public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(final QueryType query)
          {
            return CONGLOMERATE.findFactory(query).getToolchest();
          }
        },
        walker,
        new DefaultGenericQueryMetricsFactory(INJECTOR.getInstance(Key.get(ObjectMapper.class, Json.class))),
        new ServiceEmitter("dummy", "dummy", new NoopEmitter()),
        new NoopRequestLogger(),
        new ServerConfig(),
        new AuthConfig(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER
    );
  }

  public static ObjectMapper getJsonMapper()
  {
    return INJECTOR.getInstance(Key.get(ObjectMapper.class, Json.class));
  }

  private static String[] getInputSourceNameForCurrentMethod(Method method) throws NoSuchMethodException
  {
    final InputDataSource annotation = method.getAnnotation(InputDataSource.class);
    return annotation == null || annotation.names().length == 0 ? new String[]{"foo"} : annotation.names();
  }

  public static SpecificSegmentsQuerySegmentWalker createMockWalker(Method method, final File tmpDir)
      throws IOException, NoSuchMethodException
  {
    return createMockWalker(getInputSourceNameForCurrentMethod(method), tmpDir);
  }

  private static Pair<QueryableIndex, DataSegment> createIndexAndSegmentDesc(String dataSourceName, File tmpDir, int seq)
  {
    final TestDataSource testDataSource = TEST_DATA_SOURCES.get(dataSourceName);
    Preconditions.checkNotNull(testDataSource, "Unknown data source name: [%s]", dataSourceName);
    final QueryableIndex index = IndexBuilder.create()
                                             .tmpDir(new File(tmpDir, Integer.toString(seq)))
                                             .indexMerger(TestHelper.getTestIndexMergerV9())
                                             .schema(testDataSource.indexSchema)
                                             .rows(testDataSource.inputRowProvider.apply(tmpDir))
                                             .buildMMappedIndex();
    final DataSegment segmentDescriptor = DataSegment.builder()
                                                     .dataSource(testDataSource.name)
                                                     .interval(index.getDataInterval())
                                                     .version("1")
                                                     .shardSpec(new LinearShardSpec(0))
                                                     .build();
    return new Pair<>(index, segmentDescriptor);
  }

  public static SpecificSegmentsQuerySegmentWalker createMockWalker(final String[] dataSourceNames, final File tmpDir)
      throws IOException
  {
    final SpecificSegmentsQuerySegmentWalker walker = new SpecificSegmentsQuerySegmentWalker(
        queryRunnerFactoryConglomerate()
    );

    int i = 0;
    for (String dataSourceName : dataSourceNames) {
      final Pair<QueryableIndex, DataSegment> pair = createIndexAndSegmentDesc(dataSourceName, tmpDir, i++);
      walker.add(pair.rhs, pair.lhs);
    }
    return walker;
  }

  public static ExprMacroTable createExprMacroTable()
  {
    final List<ExprMacroTable.ExprMacro> exprMacros = new ArrayList<>();
    for (Class<? extends ExprMacroTable.ExprMacro> clazz : ExpressionModule.EXPR_MACROS) {
      exprMacros.add(INJECTOR.getInstance(clazz));
    }
    exprMacros.add(INJECTOR.getInstance(LookupExprMacro.class));
    return new ExprMacroTable(exprMacros);
  }

  public static DruidOperatorTable createOperatorTable()
  {
    try {
      final Set<SqlAggregator> aggregators = new HashSet<>();
      final Set<SqlOperatorConversion> extractionOperators = new HashSet<>();

      for (Class<? extends SqlAggregator> clazz : SqlModule.DEFAULT_AGGREGATOR_CLASSES) {
        aggregators.add(INJECTOR.getInstance(clazz));
      }

      for (Class<? extends SqlOperatorConversion> clazz : SqlModule.DEFAULT_OPERATOR_CONVERSION_CLASSES) {
        extractionOperators.add(INJECTOR.getInstance(clazz));
      }

      return new DruidOperatorTable(aggregators, extractionOperators);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static DruidSchema createMockSchema(
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig
  )
  {
    return createMockSchema(walker, plannerConfig, new NoopViewManager());
  }

  public static DruidSchema createMockSchema(
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig,
      final ViewManager viewManager
  )
  {
    final DruidSchema schema = new DruidSchema(
        CalciteTests.createMockQueryLifecycleFactory(walker),
        new TestServerInventoryView(walker.getSegments()),
        plannerConfig,
        viewManager,
        AuthTestUtils.TEST_AUTHENTICATOR_MAPPER
    );

    schema.start();
    try {
      schema.awaitInitialization();
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }

    schema.stop();
    return schema;
  }

  public static InputRow createRow(final ImmutableMap<String, ?> map)
  {
    return FOO_PARSER.parse((Map<String, Object>) map);
  }

  public static InputRow createRow(final Object t, final String dim1, final String dim2, final double m1)
  {
    return FOO_PARSER.parse(
        ImmutableMap.<String, Object>of(
            "t", new DateTime(t, ISOChronology.getInstanceUTC()).getMillis(),
            "dim1", dim1,
            "dim2", dim2,
            "m1", m1
        )
    );
  }
}
