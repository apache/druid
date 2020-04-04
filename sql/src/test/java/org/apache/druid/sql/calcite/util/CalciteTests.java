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
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ServerInventoryView;
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
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.query.expression.LookupExprMacro;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QueryStackTests;
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
import org.apache.druid.sql.calcite.schema.InformationSchema;
import org.apache.druid.sql.calcite.schema.LookupSchema;
import org.apache.druid.sql.calcite.schema.MetadataSegmentView;
import org.apache.druid.sql.calcite.schema.SystemSchema;
import org.apache.druid.sql.calcite.view.DruidViewMacroFactory;
import org.apache.druid.sql.calcite.view.NoopViewManager;
import org.apache.druid.sql.calcite.view.ViewManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.chrono.ISOChronology;

import javax.annotation.Nullable;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;

/**
 * Utility functions for Calcite tests.
 */
public class CalciteTests
{
  public static final String DATASOURCE1 = "foo";
  public static final String DATASOURCE2 = "foo2";
  public static final String DATASOURCE3 = "numfoo";
  public static final String DATASOURCE4 = "foo4";
  public static final String DATASOURCE5 = "lotsocolumns";
  public static final String FORBIDDEN_DATASOURCE = "forbiddenDatasource";
  public static final String DRUID_SCHEMA_NAME = "druid";
  public static final String INFORMATION_SCHEMA_NAME = "INFORMATION_SCHEMA";
  public static final String SYSTEM_SCHEMA_NAME = "sys";
  public static final String LOOKUP_SCHEMA_NAME = "lookup";

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
      binder -> {
        binder.bind(Key.get(ObjectMapper.class, Json.class)).toInstance(TestHelper.makeJsonMapper());

        // This Module is just to get a LookupExtractorFactoryContainerProvider with a usable "lookyloo" lookup.

        final LookupExtractorFactoryContainerProvider lookupProvider =
            LookupEnabledTestExprMacroTable.createTestLookupProvider(
                ImmutableMap.of(
                    "a", "xa",
                    "abc", "xabc",
                    "nosuchkey", "mysteryvalue",
                    "6", "x6"
                )
            );
        binder.bind(LookupExtractorFactoryContainerProvider.class).toInstance(lookupProvider);
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
                  .add(new DoubleDimensionSchema("d2"))
                  .add(new FloatDimensionSchema("f1"))
                  .add(new FloatDimensionSchema("f2"))
                  .add(new LongDimensionSchema("l1"))
                  .add(new LongDimensionSchema("l2"))
                  .build(),
              null,
              null
          )
      )
  );

  private static final InputRowParser<Map<String, Object>> PARSER_LOTS_OF_COLUMNS = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec("timestamp", "millis", null),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(
                  ImmutableList.<String>builder().add("dimHyperUnique")
                                                 .add("dimMultivalEnumerated")
                                                 .add("dimMultivalEnumerated2")
                                                 .add("dimMultivalSequentialWithNulls")
                                                 .add("dimSequential")
                                                 .add("dimSequentialHalfNull")
                                                 .add("dimUniform")
                                                 .add("dimZipf")
                                                 .add("metFloatNormal")
                                                 .add("metFloatZipf")
                                                 .add("metLongSequential")
                                                 .add("metLongUniform")
                                                 .build()
              ),
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

  private static final IncrementalIndexSchema INDEX_SCHEMA_LOTS_O_COLUMNS = new IncrementalIndexSchema.Builder()
      .withMetrics(
          new CountAggregatorFactory("count")
      )
      .withDimensionsSpec(PARSER_LOTS_OF_COLUMNS)
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
              .put("d2", 1.7)
              .put("f1", 0.1f)
              .put("f2", 0.1f)
              .put("l1", 325323L)
              .put("l2", 325323L)
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
              .put("d2", 0.0)
              .put("f1", 0.0)
              .put("f2", 0.0)
              .put("l1", 0)
              .put("l2", 0)
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

  // Hi, I'm Troy McClure. You may remember these rows from such benchmarks generator schemas as basic and expression
  public static final List<InputRow> ROWS_LOTS_OF_COLUMNS = ImmutableList.of(
      createRow(
          ImmutableMap.<String, Object>builder()
              .put("timestamp", 1576306800000L)
              .put("metFloatZipf", 147.0)
              .put("dimMultivalSequentialWithNulls", Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8"))
              .put("dimMultivalEnumerated2", Arrays.asList(null, "Orange", "Apple"))
              .put("metLongUniform", 372)
              .put("metFloatNormal", 5000.0)
              .put("dimZipf", "27")
              .put("dimUniform", "74416")
              .put("dimMultivalEnumerated", Arrays.asList("Baz", "World", "Hello", "Baz"))
              .put("metLongSequential", 0)
              .put("dimHyperUnique", "0")
              .put("dimSequential", "0")
              .put("dimSequentialHalfNull", "0")
              .build(),
          PARSER_LOTS_OF_COLUMNS
      ),
      createRow(
          ImmutableMap.<String, Object>builder()
              .put("timestamp", 1576306800000L)
              .put("metFloatZipf", 25.0)
              .put("dimMultivalEnumerated2", Arrays.asList("Xylophone", null, "Corundum"))
              .put("metLongUniform", 252)
              .put("metFloatNormal", 4999.0)
              .put("dimZipf", "9")
              .put("dimUniform", "50515")
              .put("dimMultivalEnumerated", Arrays.asList("Baz", "World", "World", "World"))
              .put("metLongSequential", 8)
              .put("dimHyperUnique", "8")
              .put("dimSequential", "8")
              .build(),
          PARSER_LOTS_OF_COLUMNS
      )
  );

  private CalciteTests()
  {
    // No instantiation.
  }

  public static final DruidViewMacroFactory DRUID_VIEW_MACRO_FACTORY = new TestDruidViewMacroFactory();

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
    return createMockWalker(conglomerate, tmpDir, QueryStackTests.DEFAULT_NOOP_SCHEDULER);
  }

  public static SpecificSegmentsQuerySegmentWalker createMockWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final File tmpDir,
      final QueryScheduler scheduler
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

    final QueryableIndex indexLotsOfColumns = IndexBuilder
        .create()
        .tmpDir(new File(tmpDir, "5"))
        .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
        .schema(INDEX_SCHEMA_LOTS_O_COLUMNS)
        .rows(ROWS_LOTS_OF_COLUMNS)
        .buildMMappedIndex();


    return new SpecificSegmentsQuerySegmentWalker(
        conglomerate,
        INJECTOR.getInstance(LookupExtractorFactoryContainerProvider.class),
        null,
        scheduler
    ).add(
        DataSegment.builder()
                   .dataSource(DATASOURCE1)
                   .interval(index1.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        index1
    ).add(
        DataSegment.builder()
                   .dataSource(DATASOURCE2)
                   .interval(index2.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        index2
    ).add(
        DataSegment.builder()
                   .dataSource(FORBIDDEN_DATASOURCE)
                   .interval(forbiddenIndex.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        forbiddenIndex
    ).add(
        DataSegment.builder()
                   .dataSource(DATASOURCE3)
                   .interval(indexNumericDims.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        indexNumericDims
    ).add(
        DataSegment.builder()
                   .dataSource(DATASOURCE4)
                   .interval(index4.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        index4
    ).add(
        DataSegment.builder()
                   .dataSource(DATASOURCE5)
                   .interval(indexLotsOfColumns.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        indexLotsOfColumns
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

  public static LookupSchema createMockLookupSchema()
  {
    return new LookupSchema(INJECTOR.getInstance(LookupExtractorFactoryContainerProvider.class));
  }

  public static SystemSchema createMockSystemSchema(
      final DruidSchema druidSchema,
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig,
      final AuthorizerMapper authorizerMapper
  )
  {

    final DruidNode coordinatorNode = new DruidNode("test", "dummy", false, 8080, null, true, false);
    FakeDruidNodeDiscoveryProvider provider = new FakeDruidNodeDiscoveryProvider(
        ImmutableMap.of(
            NodeRole.COORDINATOR, new FakeDruidNodeDiscovery(ImmutableMap.of(NodeRole.COORDINATOR, coordinatorNode))
        )
    );

    final DruidLeaderClient druidLeaderClient = new DruidLeaderClient(
        new FakeHttpClient(),
        provider,
        NodeRole.COORDINATOR,
        "/simple/leader"
    );

    return new SystemSchema(
        druidSchema,
        new MetadataSegmentView(
            druidLeaderClient,
            getJsonMapper(),
            new BytesAccumulatingResponseHandler(),
            new BrokerSegmentWatcherConfig(),
            plannerConfig
        ),
        new TestServerInventoryView(walker.getSegments()),
        new FakeServerInventoryView(),
        authorizerMapper,
        druidLeaderClient,
        druidLeaderClient,
        provider,
        getJsonMapper()
    );
  }

  public static SchemaPlus createMockRootSchema(
      final QueryRunnerFactoryConglomerate conglomerate,
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig,
      final AuthorizerMapper authorizerMapper
  )
  {
    DruidSchema druidSchema = createMockSchema(conglomerate, walker, plannerConfig);
    SystemSchema systemSchema =
        CalciteTests.createMockSystemSchema(druidSchema, walker, plannerConfig, authorizerMapper);

    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();
    InformationSchema informationSchema =
        new InformationSchema(rootSchema, authorizerMapper, CalciteTests.DRUID_SCHEMA_NAME);
    LookupSchema lookupSchema = CalciteTests.createMockLookupSchema();
    rootSchema.add(CalciteTests.DRUID_SCHEMA_NAME, druidSchema);
    rootSchema.add(CalciteTests.INFORMATION_SCHEMA_NAME, informationSchema);
    rootSchema.add(CalciteTests.SYSTEM_SCHEMA_NAME, systemSchema);
    rootSchema.add(CalciteTests.LOOKUP_SCHEMA_NAME, lookupSchema);
    return rootSchema;
  }

  public static SchemaPlus createMockRootSchema(
      final QueryRunnerFactoryConglomerate conglomerate,
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig,
      final ViewManager viewManager,
      final AuthorizerMapper authorizerMapper
  )
  {
    DruidSchema druidSchema = createMockSchema(conglomerate, walker, plannerConfig, viewManager);
    SystemSchema systemSchema =
        CalciteTests.createMockSystemSchema(druidSchema, walker, plannerConfig, authorizerMapper);
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();
    InformationSchema informationSchema =
        new InformationSchema(rootSchema, authorizerMapper, CalciteTests.DRUID_SCHEMA_NAME);
    LookupSchema lookupSchema = CalciteTests.createMockLookupSchema();
    rootSchema.add(CalciteTests.DRUID_SCHEMA_NAME, druidSchema);
    rootSchema.add(CalciteTests.INFORMATION_SCHEMA_NAME, informationSchema);
    rootSchema.add(CalciteTests.SYSTEM_SCHEMA_NAME, systemSchema);
    rootSchema.add(CalciteTests.LOOKUP_SCHEMA_NAME, lookupSchema);
    return rootSchema;
  }

  /**
   * Some Calcite exceptions (such as that thrown by
   * {@link org.apache.druid.sql.calcite.CalciteQueryTest#testCountStarWithTimeFilterUsingStringLiteralsInvalid)},
   * are structured as a chain of RuntimeExceptions caused by InvocationTargetExceptions. To get the root exception
   * it is necessary to make getTargetException calls on the InvocationTargetExceptions.
   */
  public static Throwable getRootCauseFromInvocationTargetExceptionChain(Throwable t)
  {
    Throwable curThrowable = t;
    while (curThrowable.getCause() instanceof InvocationTargetException) {
      curThrowable = ((InvocationTargetException) curThrowable.getCause()).getTargetException();
    }
    return curThrowable;
  }

  private static DruidSchema createMockSchema(
      final QueryRunnerFactoryConglomerate conglomerate,
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig
  )
  {
    return createMockSchema(conglomerate, walker, plannerConfig, new NoopViewManager());
  }

  private static DruidSchema createMockSchema(
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

  /**
   * A fake {@link HttpClient} for {@link #createMockSystemSchema}.
   */
  private static class FakeHttpClient implements HttpClient
  {
    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(
        Request request,
        HttpResponseHandler<Intermediate, Final> handler
    )
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(
        Request request,
        HttpResponseHandler<Intermediate, Final> handler,
        Duration readTimeout
    )
    {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * A fake {@link DruidNodeDiscoveryProvider} for {@link #createMockSystemSchema}.
   */
  private static class FakeDruidNodeDiscoveryProvider extends DruidNodeDiscoveryProvider
  {
    private final Map<NodeRole, FakeDruidNodeDiscovery> nodeDiscoveries;

    public FakeDruidNodeDiscoveryProvider(Map<NodeRole, FakeDruidNodeDiscovery> nodeDiscoveries)
    {
      this.nodeDiscoveries = nodeDiscoveries;
    }

    @Override
    public BooleanSupplier getForNode(DruidNode node, NodeRole nodeRole)
    {
      boolean get = nodeDiscoveries.getOrDefault(nodeRole, new FakeDruidNodeDiscovery())
                                   .getAllNodes()
                                   .stream()
                                   .anyMatch(x -> x.getDruidNode().equals(node));
      return () -> get;
    }

    @Override
    public DruidNodeDiscovery getForNodeRole(NodeRole nodeRole)
    {
      return nodeDiscoveries.getOrDefault(nodeRole, new FakeDruidNodeDiscovery());
    }
  }

  private static class FakeDruidNodeDiscovery implements DruidNodeDiscovery
  {
    private final Set<DiscoveryDruidNode> nodes;

    FakeDruidNodeDiscovery()
    {
      this.nodes = new HashSet<>();
    }

    FakeDruidNodeDiscovery(Map<NodeRole, DruidNode> nodes)
    {
      this.nodes = Sets.newHashSetWithExpectedSize(nodes.size());
      nodes.forEach((k, v) -> {
        addNode(v, k);
      });
    }

    @Override
    public Collection<DiscoveryDruidNode> getAllNodes()
    {
      return nodes;
    }

    void addNode(DruidNode node, NodeRole role)
    {
      final DiscoveryDruidNode discoveryNode = new DiscoveryDruidNode(node, role, ImmutableMap.of());
      this.nodes.add(discoveryNode);
    }

    @Override
    public void registerListener(Listener listener)
    {

    }
  }


  /**
   * A fake {@link ServerInventoryView} for {@link #createMockSystemSchema}.
   */
  private static class FakeServerInventoryView implements ServerInventoryView
  {
    @Nullable
    @Override
    public DruidServer getInventoryValue(String serverKey)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<DruidServer> getInventory()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isStarted()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSegmentLoadedByServer(String serverKey, DataSegment segment)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void registerServerRemovedCallback(Executor exec, ServerRemovedCallback callback)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void registerSegmentCallback(Executor exec, SegmentCallback callback)
    {
      throw new UnsupportedOperationException();
    }
  }
}
