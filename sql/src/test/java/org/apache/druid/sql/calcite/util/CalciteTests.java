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

package org.apache.druid.sql.calcite.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.curator.discovery.ServerDiscoverySelector;
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
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeType;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.query.expression.LookupExprMacro;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerFactory;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.metadata.SegmentMetadataQueryConfig;
import org.apache.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import org.apache.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.select.SelectQuery;
import org.apache.druid.query.select.SelectQueryEngine;
import org.apache.druid.query.select.SelectQueryQueryToolChest;
import org.apache.druid.query.select.SelectQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;
import org.apache.druid.query.topn.TopNQueryRunnerFactory;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.coordinator.BytesAccumulatingResponseHandler;
import org.apache.druid.server.log.NoopRequestLogger;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AllowAllAuthenticator;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.QueryLookupOperatorConversion;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.MetadataSegmentView;
import org.apache.druid.sql.calcite.schema.SystemSchema;
import org.apache.druid.sql.calcite.view.NoopViewManager;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility functions for Calcite tests.
 */
public class CalciteTests
{
  public static final String DATASOURCE1 = "foo";
  public static final String DATASOURCE2 = "foo2";
  public static final String DATASOURCE3 = "numfoo";
  public static final String DATASOURCE4 = "foo4";
  public static final String FORBIDDEN_DATASOURCE = "forbiddenDatasource";

  public static final String TEST_SUPERUSER_NAME = "testSuperuser";
  public static final AuthorizerMapper TEST_AUTHORIZER_MAPPER = new AuthorizerMapper(null)
  {
    @Override
    public Authorizer getAuthorizer(String name)
    {
      return (authenticationResult, resource, action) -> {
        if (authenticationResult.getIdentity().equals(TEST_SUPERUSER_NAME)) {
          return Access.OK;
        }

        if (resource.getType() == ResourceType.DATASOURCE && resource.getName().equals(FORBIDDEN_DATASOURCE)) {
          return new Access(false);
        } else {
          return Access.OK;
        }
      };
    }
  };
  public static final AuthenticatorMapper TEST_AUTHENTICATOR_MAPPER;

  static {
    final Map<String, Authenticator> defaultMap = new HashMap<>();
    defaultMap.put(
        AuthConfig.ALLOW_ALL_NAME,
        new AllowAllAuthenticator()
        {
          @Override
          public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
          {
            return new AuthenticationResult((String) context.get("user"), AuthConfig.ALLOW_ALL_NAME, null, null);
          }
        }
    );
    TEST_AUTHENTICATOR_MAPPER = new AuthenticatorMapper(defaultMap);
  }

  public static final Escalator TEST_AUTHENTICATOR_ESCALATOR;

  static {
    TEST_AUTHENTICATOR_ESCALATOR = new NoopEscalator()
    {

      @Override
      public AuthenticationResult createEscalatedAuthenticationResult()
      {
        return SUPER_USER_AUTH_RESULT;
      }
    };
  }

  public static final AuthenticationResult REGULAR_USER_AUTH_RESULT = new AuthenticationResult(
      AuthConfig.ALLOW_ALL_NAME,
      AuthConfig.ALLOW_ALL_NAME,
      null, null
  );

  public static final AuthenticationResult SUPER_USER_AUTH_RESULT = new AuthenticationResult(
      TEST_SUPERUSER_NAME,
      AuthConfig.ALLOW_ALL_NAME,
      null, null
  );

  private static final String TIMESTAMP_COLUMN = "t";

  private static final Injector INJECTOR = Guice.createInjector(
      (Module) binder -> {
        binder.bind(Key.get(ObjectMapper.class, Json.class)).toInstance(TestHelper.makeJsonMapper());

        // This Module is just to get a LookupExtractorFactoryContainerProvider with a usable "lookyloo" lookup.

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

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", null),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2", "dim3")),
              null,
              null
          )
      )
  );

  private static final InputRowParser<Map<String, Object>> PARSER_NUMERIC_DIMS = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", null),
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
      )
  );

  private static final IncrementalIndexSchema INDEX_SCHEMA = new IncrementalIndexSchema.Builder()
      .withMetrics(
          new CountAggregatorFactory("cnt"),
          new FloatSumAggregatorFactory("m1", "m1"),
          new DoubleSumAggregatorFactory("m2", "m2"),
          new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
      )
      .withRollup(false)
      .build();

  private static final IncrementalIndexSchema INDEX_SCHEMA_NUMERIC_DIMS = new IncrementalIndexSchema.Builder()
      .withMetrics(
          new CountAggregatorFactory("cnt"),
          new FloatSumAggregatorFactory("m1", "m1"),
          new DoubleSumAggregatorFactory("m2", "m2"),
          new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
      )
      .withDimensionsSpec(PARSER_NUMERIC_DIMS)
      .withRollup(false)
      .build();

  public static final List<InputRow> ROWS1 = ImmutableList.of(
      createRow(
          ImmutableMap.<String, Object>builder()
              .put("t", "2000-01-01")
              .put("m1", "1.0")
              .put("m2", "1.0")
              .put("dim1", "")
              .put("dim2", ImmutableList.of("a"))
              .put("dim3", ImmutableList.of("a", "b"))
              .build()
      ),
      createRow(
          ImmutableMap.<String, Object>builder()
              .put("t", "2000-01-02")
              .put("m1", "2.0")
              .put("m2", "2.0")
              .put("dim1", "10.1")
              .put("dim2", ImmutableList.of())
              .put("dim3", ImmutableList.of("b", "c"))
              .build()
      ),
      createRow(
          ImmutableMap.<String, Object>builder()
              .put("t", "2000-01-03")
              .put("m1", "3.0")
              .put("m2", "3.0")
              .put("dim1", "2")
              .put("dim2", ImmutableList.of(""))
              .put("dim3", ImmutableList.of("d"))
              .build()
      ),
      createRow(
          ImmutableMap.<String, Object>builder()
              .put("t", "2001-01-01")
              .put("m1", "4.0")
              .put("m2", "4.0")
              .put("dim1", "1")
              .put("dim2", ImmutableList.of("a"))
              .put("dim3", ImmutableList.of(""))
              .build()
      ),
      createRow(
          ImmutableMap.<String, Object>builder()
              .put("t", "2001-01-02")
              .put("m1", "5.0")
              .put("m2", "5.0")
              .put("dim1", "def")
              .put("dim2", ImmutableList.of("abc"))
              .put("dim3", ImmutableList.of())
              .build()
      ),
      createRow(
          ImmutableMap.<String, Object>builder()
              .put("t", "2001-01-03")
              .put("m1", "6.0")
              .put("m2", "6.0")
              .put("dim1", "abc")
              .build()
      )
  );

  public static final List<InputRow> ROWS1_WITH_NUMERIC_DIMS = ImmutableList.of(
      createRow(
          ImmutableMap.<String, Object>builder()
              .put("t", "2000-01-01")
              .put("m1", "1.0")
              .put("m2", "1.0")
              .put("d1", 1.0)
              .put("f1", 1.0f)
              .put("l1", 7L)
              .put("dim1", "")
              .put("dim2", ImmutableList.of("a"))
              .put("dim3", ImmutableList.of("a", "b"))
              .build(),
          PARSER_NUMERIC_DIMS
      ),
      createRow(
          ImmutableMap.<String, Object>builder()
              .put("t", "2000-01-02")
              .put("m1", "2.0")
              .put("m2", "2.0")
              .put("d1", 1.7)
              .put("f1", 0.1f)
              .put("l1", 325323L)
              .put("dim1", "10.1")
              .put("dim2", ImmutableList.of())
              .put("dim3", ImmutableList.of("b", "c"))
              .build(),
          PARSER_NUMERIC_DIMS
      ),
      createRow(
          ImmutableMap.<String, Object>builder()
              .put("t", "2000-01-03")
              .put("m1", "3.0")
              .put("m2", "3.0")
              .put("d1", 0.0)
              .put("f1", 0.0)
              .put("l1", 0)
              .put("dim1", "2")
              .put("dim2", ImmutableList.of(""))
              .put("dim3", ImmutableList.of("d"))
              .build(),
          PARSER_NUMERIC_DIMS
      ),
      createRow(
          ImmutableMap.<String, Object>builder()
              .put("t", "2001-01-01")
              .put("m1", "4.0")
              .put("m2", "4.0")
              .put("dim1", "1")
              .put("dim2", ImmutableList.of("a"))
              .put("dim3", ImmutableList.of(""))
              .build(),
          PARSER_NUMERIC_DIMS
      ),
      createRow(
          ImmutableMap.<String, Object>builder()
              .put("t", "2001-01-02")
              .put("m1", "5.0")
              .put("m2", "5.0")
              .put("dim1", "def")
              .put("dim2", ImmutableList.of("abc"))
              .put("dim3", ImmutableList.of())
              .build(),
          PARSER_NUMERIC_DIMS
      ),
      createRow(
          ImmutableMap.<String, Object>builder()
              .put("t", "2001-01-03")
              .put("m1", "6.0")
              .put("m2", "6.0")
              .put("dim1", "abc")
              .build(),
          PARSER_NUMERIC_DIMS
      )
  );

  public static final List<InputRow> ROWS2 = ImmutableList.of(
      createRow("2000-01-01", "דרואיד", "he", 1.0),
      createRow("2000-01-01", "druid", "en", 1.0),
      createRow("2000-01-01", "друид", "ru", 1.0)
  );

  public static final List<InputRow> ROWS1_WITH_FULL_TIMESTAMP = ImmutableList.of(
      createRow(
          ImmutableMap.<String, Object>builder()
              .put("t", "2000-01-01T10:51:45.695Z")
              .put("m1", "1.0")
              .put("m2", "1.0")
              .put("dim1", "")
              .put("dim2", ImmutableList.of("a"))
              .put("dim3", ImmutableList.of("a", "b"))
              .build()
      ),
      createRow(
          ImmutableMap.<String, Object>builder()
              .put("t", "2000-01-18T10:51:45.695Z")
              .put("m1", "2.0")
              .put("m2", "2.0")
              .put("dim1", "10.1")
              .put("dim2", ImmutableList.of())
              .put("dim3", ImmutableList.of("b", "c"))
              .build()
      )
  );


  public static final List<InputRow> FORBIDDEN_ROWS = ImmutableList.of(
      createRow("2000-01-01", "forbidden", "abcd", 9999.0)
  );

  private CalciteTests()
  {
    // No instantiation.
  }

  /**
   * Returns a new {@link QueryRunnerFactoryConglomerate} and a {@link Closer} which should be closed at the end of the
   * test.
   */
  public static Pair<QueryRunnerFactoryConglomerate, Closer> createQueryRunnerFactoryConglomerate()
  {
    final Closer resourceCloser = Closer.create();
    final CloseableStupidPool<ByteBuffer> stupidPool = new CloseableStupidPool<>(
        "TopNQueryRunnerFactory-bufferPool",
        () -> ByteBuffer.allocate(10 * 1024 * 1024)
    );
    resourceCloser.register(stupidPool);
    final Pair<GroupByQueryRunnerFactory, Closer> factoryCloserPair = GroupByQueryRunnerTest
        .makeQueryRunnerFactory(
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
              public int getNumThreads()
              {
                // Only use 1 thread for tests.
                return 1;
              }

              @Override
              public int getNumMergeBuffers()
              {
                // Need 3 buffers for CalciteQueryTest.testDoubleNestedGroupby.
                // Two buffers for the broker and one for the queryable
                return 3;
              }
            }
        );
    final GroupByQueryRunnerFactory factory = factoryCloserPair.lhs;
    resourceCloser.register(factoryCloserPair.rhs);

    final QueryRunnerFactoryConglomerate conglomerate = new DefaultQueryRunnerFactoryConglomerate(
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
                        new DefaultGenericQueryMetricsFactory()
                    ),
                    new ScanQueryEngine(),
                    new ScanQueryConfig()
                )
            )
            .put(
                SelectQuery.class,
                new SelectQueryRunnerFactory(
                    new SelectQueryQueryToolChest(
                        TestHelper.makeJsonMapper(),
                        QueryRunnerTestHelper.noopIntervalChunkingQueryRunnerDecorator()
                    ),
                    new SelectQueryEngine(),
                    QueryRunnerTestHelper.NOOP_QUERYWATCHER
                )
            )
            .put(
                TimeseriesQuery.class,
                new TimeseriesQueryRunnerFactory(
                    new TimeseriesQueryQueryToolChest(QueryRunnerTestHelper.noopIntervalChunkingQueryRunnerDecorator()),
                    new TimeseriesQueryEngine(),
                    QueryRunnerTestHelper.NOOP_QUERYWATCHER
                )
            )
            .put(
                TopNQuery.class,
                new TopNQueryRunnerFactory(
                    stupidPool,
                    new TopNQueryQueryToolChest(
                        new TopNQueryConfig(),
                        QueryRunnerTestHelper.noopIntervalChunkingQueryRunnerDecorator()
                    ),
                    QueryRunnerTestHelper.NOOP_QUERYWATCHER
                )
            )
            .put(GroupByQuery.class, factory)
            .build()
    );
    return Pair.of(conglomerate, resourceCloser);
  }

  public static QueryLifecycleFactory createMockQueryLifecycleFactory(
      final QuerySegmentWalker walker,
      final QueryRunnerFactoryConglomerate conglomerate
  )
  {
    return new QueryLifecycleFactory(
        new QueryToolChestWarehouse()
        {
          @Override
          public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(final QueryType query)
          {
            return conglomerate.findFactory(query).getToolchest();
          }
        },
        walker,
        new DefaultGenericQueryMetricsFactory(),
        new ServiceEmitter("dummy", "dummy", new NoopEmitter()),
        new NoopRequestLogger(),
        new AuthConfig(),
        TEST_AUTHORIZER_MAPPER
    );
  }

  public static SqlLifecycleFactory createSqlLifecycleFactory(final PlannerFactory plannerFactory)
  {
    return new SqlLifecycleFactory(
        plannerFactory,
        new ServiceEmitter("dummy", "dummy", new NoopEmitter()),
        new NoopRequestLogger()
    );
  }

  public static ObjectMapper getJsonMapper()
  {
    return INJECTOR.getInstance(Key.get(ObjectMapper.class, Json.class));
  }

  public static SpecificSegmentsQuerySegmentWalker createMockWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final File tmpDir
  )
  {
    final QueryableIndex index1 = IndexBuilder
        .create()
        .tmpDir(new File(tmpDir, "1"))
        .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
        .schema(INDEX_SCHEMA)
        .rows(ROWS1)
        .buildMMappedIndex();

    final QueryableIndex index2 = IndexBuilder
        .create()
        .tmpDir(new File(tmpDir, "2"))
        .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
        .schema(INDEX_SCHEMA)
        .rows(ROWS2)
        .buildMMappedIndex();

    final QueryableIndex forbiddenIndex = IndexBuilder
        .create()
        .tmpDir(new File(tmpDir, "forbidden"))
        .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
        .schema(INDEX_SCHEMA)
        .rows(FORBIDDEN_ROWS)
        .buildMMappedIndex();

    final QueryableIndex indexNumericDims = IndexBuilder
        .create()
        .tmpDir(new File(tmpDir, "3"))
        .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
        .schema(INDEX_SCHEMA_NUMERIC_DIMS)
        .rows(ROWS1_WITH_NUMERIC_DIMS)
        .buildMMappedIndex();

    final QueryableIndex index4 = IndexBuilder
        .create()
        .tmpDir(new File(tmpDir, "4"))
        .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
        .schema(INDEX_SCHEMA)
        .rows(ROWS1_WITH_FULL_TIMESTAMP)
        .buildMMappedIndex();


    return new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
        DataSegment.builder()
                   .dataSource(DATASOURCE1)
                   .interval(index1.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        index1
    ).add(
        DataSegment.builder()
                   .dataSource(DATASOURCE2)
                   .interval(index2.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        index2
    ).add(
        DataSegment.builder()
                   .dataSource(FORBIDDEN_DATASOURCE)
                   .interval(forbiddenIndex.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        forbiddenIndex
    ).add(
        DataSegment.builder()
                   .dataSource(DATASOURCE3)
                   .interval(indexNumericDims.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        indexNumericDims
    ).add(
        DataSegment.builder()
                   .dataSource(DATASOURCE4)
                   .interval(index4.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        index4
    );
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
      final Set<SqlOperatorConversion> extractionOperators = new HashSet<>();
      extractionOperators.add(INJECTOR.getInstance(QueryLookupOperatorConversion.class));
      return new DruidOperatorTable(ImmutableSet.of(), extractionOperators);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static DruidSchema createMockSchema(
      final QueryRunnerFactoryConglomerate conglomerate,
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig
  )
  {
    return createMockSchema(conglomerate, walker, plannerConfig, new NoopViewManager());
  }

  public static DruidSchema createMockSchema(
      final QueryRunnerFactoryConglomerate conglomerate,
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig,
      final ViewManager viewManager
  )
  {
    final DruidSchema schema = new DruidSchema(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        new TestServerInventoryView(walker.getSegments()),
        plannerConfig,
        viewManager,
        TEST_AUTHENTICATOR_ESCALATOR
    );

    try {
      schema.start();
      schema.awaitInitialization();
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    schema.stop();
    return schema;
  }

  public static InputRow createRow(final ImmutableMap<String, ?> map)
  {
    return PARSER.parseBatch((Map<String, Object>) map).get(0);
  }

  public static InputRow createRow(final ImmutableMap<String, ?> map, InputRowParser<Map<String, Object>> parser)
  {
    return parser.parseBatch((Map<String, Object>) map).get(0);
  }

  public static InputRow createRow(final Object t, final String dim1, final String dim2, final double m1)
  {
    return PARSER.parseBatch(
        ImmutableMap.of(
            "t", new DateTime(t, ISOChronology.getInstanceUTC()).getMillis(),
            "dim1", dim1,
            "dim2", dim2,
            "m1", m1
        )
    ).get(0);
  }


  public static SystemSchema createMockSystemSchema(
      final DruidSchema druidSchema,
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig
  )
  {
    final DruidLeaderClient druidLeaderClient = new DruidLeaderClient(
        EasyMock.createMock(HttpClient.class),
        EasyMock.createMock(DruidNodeDiscoveryProvider.class),
        NodeType.COORDINATOR,
        "/simple/leader",
        new ServerDiscoverySelector(EasyMock.createMock(ServiceProvider.class), "test")
    )
    {
    };
    final SystemSchema schema = new SystemSchema(
        druidSchema,
        new MetadataSegmentView(
            druidLeaderClient,
            getJsonMapper(),
            new BytesAccumulatingResponseHandler(),
            new BrokerSegmentWatcherConfig(),
            plannerConfig
        ),
        new TestServerInventoryView(walker.getSegments()),
        EasyMock.createMock(ServerInventoryView.class),
        TEST_AUTHORIZER_MAPPER,
        druidLeaderClient,
        druidLeaderClient,
        EasyMock.createMock(DruidNodeDiscoveryProvider.class),
        getJsonMapper()
    );
    return schema;
  }
}
