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

package org.apache.druid.sql.avatica;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import org.apache.calcite.avatica.AvaticaClientRuntimeException;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.server.AbstractAvaticaHandler;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QuerySchedulerProvider;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.RequestLogLine;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.DruidSchemaName;
import org.apache.druid.sql.calcite.schema.NamedSchema;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.eclipse.jetty.server.Server;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Tests the Avatica-based JDBC implementation using JSON serialization. See
 * {@link DruidAvaticaProtobufHandlerTest} for a subclass which runs
 * this same set of tests using Protobuf serialization.
 */
public class DruidAvaticaHandlerTest extends CalciteTestBase
{
  private static final AvaticaServerConfig AVATICA_CONFIG = new AvaticaServerConfig()
  {
    @Override
    public int getMaxConnections()
    {
      // This must match the number of Connection objects created in testTooManyStatements()
      return 4;
    }

    @Override
    public int getMaxStatementsPerConnection()
    {
      return 4;
    }
  };
  private static final String DUMMY_SQL_QUERY_ID = "dummy";

  private static QueryRunnerFactoryConglomerate conglomerate;
  private static Closer resourceCloser;

  private final boolean nullNumeric = !NullHandling.replaceWithDefault();

  @BeforeClass
  public static void setUpClass()
  {
    resourceCloser = Closer.create();
    conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(resourceCloser);
    System.setProperty("user.timezone", "UTC");
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    resourceCloser.close();
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create();

  private SpecificSegmentsQuerySegmentWalker walker;
  private Server server;
  private Connection client;
  private Connection clientNoTrailingSlash;
  private Connection superuserClient;
  private Connection clientLosAngeles;
  private DruidMeta druidMeta;
  private String url;
  private Injector injector;
  private TestRequestLogger testRequestLogger;

  @Before
  public void setUp() throws Exception
  {
    walker = CalciteTests.createMockWalker(conglomerate, temporaryFolder.newFolder());
    final PlannerConfig plannerConfig = new PlannerConfig();
    final DruidOperatorTable operatorTable = CalciteTests.createOperatorTable();
    final ExprMacroTable macroTable = CalciteTests.createExprMacroTable();
    final DruidSchemaCatalog rootSchema =
        CalciteTests.createMockRootSchema(conglomerate, walker, plannerConfig, CalciteTests.TEST_AUTHORIZER_MAPPER);
    testRequestLogger = new TestRequestLogger();

    injector = new CoreInjectorBuilder(new StartupInjectorBuilder().build())
        .addModule(binder -> {
            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
            binder.bind(AuthenticatorMapper.class).toInstance(CalciteTests.TEST_AUTHENTICATOR_MAPPER);
            binder.bind(AuthorizerMapper.class).toInstance(CalciteTests.TEST_AUTHORIZER_MAPPER);
            binder.bind(Escalator.class).toInstance(CalciteTests.TEST_AUTHENTICATOR_ESCALATOR);
            binder.bind(RequestLogger.class).toInstance(testRequestLogger);
            binder.bind(DruidSchemaCatalog.class).toInstance(rootSchema);
            for (NamedSchema schema : rootSchema.getNamedSchemas().values()) {
              Multibinder.newSetBinder(binder, NamedSchema.class).addBinding().toInstance(schema);
            }
            binder.bind(QueryLifecycleFactory.class)
                  .toInstance(CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate));
            binder.bind(DruidOperatorTable.class).toInstance(operatorTable);
            binder.bind(ExprMacroTable.class).toInstance(macroTable);
            binder.bind(PlannerConfig.class).toInstance(plannerConfig);
            binder.bind(String.class)
                  .annotatedWith(DruidSchemaName.class)
                  .toInstance(CalciteTests.DRUID_SCHEMA_NAME);
            binder.bind(AvaticaServerConfig.class).toInstance(AVATICA_CONFIG);
            binder.bind(ServiceEmitter.class).to(NoopServiceEmitter.class);
            binder.bind(QuerySchedulerProvider.class).in(LazySingleton.class);
            binder.bind(QueryScheduler.class)
                  .toProvider(QuerySchedulerProvider.class)
                  .in(LazySingleton.class);
            binder.bind(SqlEngine.class).to(NativeSqlEngine.class);
            binder.bind(new TypeLiteral<Supplier<DefaultQueryConfig>>(){}).toInstance(Suppliers.ofInstance(new DefaultQueryConfig(ImmutableMap.of())));
            binder.bind(CalciteRulesManager.class).toInstance(new CalciteRulesManager(ImmutableSet.of()));
          }
         )
        .build();

    druidMeta = injector.getInstance(DruidMeta.class);
    final AbstractAvaticaHandler handler = this.getAvaticaHandler(druidMeta);
    final int port = ThreadLocalRandom.current().nextInt(9999) + 10000;
    server = new Server(new InetSocketAddress("127.0.0.1", port));
    server.setHandler(handler);
    server.start();
    url = this.getJdbcConnectionString(port);
    client = DriverManager.getConnection(url, "regularUser", "druid");
    superuserClient = DriverManager.getConnection(url, CalciteTests.TEST_SUPERUSER_NAME, "druid");
    clientNoTrailingSlash = DriverManager.getConnection(StringUtils.maybeRemoveTrailingSlash(url), CalciteTests.TEST_SUPERUSER_NAME, "druid");

    final Properties propertiesLosAngeles = new Properties();
    propertiesLosAngeles.setProperty("sqlTimeZone", "America/Los_Angeles");
    propertiesLosAngeles.setProperty("user", "regularUserLA");
    propertiesLosAngeles.setProperty(BaseQuery.SQL_QUERY_ID, DUMMY_SQL_QUERY_ID);
    clientLosAngeles = DriverManager.getConnection(url, propertiesLosAngeles);
  }

  @After
  public void tearDown() throws Exception
  {
    client.close();
    clientLosAngeles.close();
    clientNoTrailingSlash.close();
    server.stop();
    walker.close();
    walker = null;
    client = null;
    clientLosAngeles = null;
    clientNoTrailingSlash = null;
    server = null;
  }

  @Test
  public void testSelectCount() throws SQLException
  {
    try (Statement stmt = client.createStatement()) {
      final ResultSet resultSet = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM druid.foo");
      final List<Map<String, Object>> rows = getRows(resultSet);
      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of("cnt", 6L)
          ),
          rows
      );
    }
  }

  @Test
  public void testSelectCountNoTrailingSlash() throws SQLException
  {
    try (Statement stmt = clientNoTrailingSlash.createStatement()) {
      final ResultSet resultSet = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM druid.foo");
      final List<Map<String, Object>> rows = getRows(resultSet);
      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of("cnt", 6L)
          ),
          rows
      );
    }
  }

  @Test
  public void testSelectCountAlternateStyle() throws SQLException
  {
    try (PreparedStatement stmt = client.prepareStatement("SELECT COUNT(*) AS cnt FROM druid.foo")) {
      final ResultSet resultSet = stmt.executeQuery();
      final List<Map<String, Object>> rows = getRows(resultSet);
      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of("cnt", 6L)
          ),
          rows
      );
    }
  }

  @Test
  public void testTimestampsInResponse() throws SQLException
  {
    try (Statement stmt = client.createStatement()) {
      final ResultSet resultSet = stmt.executeQuery(
          "SELECT __time, CAST(__time AS DATE) AS t2 FROM druid.foo LIMIT 1"
      );

      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of(
                  "__time", new Timestamp(DateTimes.of("2000-01-01T00:00:00.000Z").getMillis()),
                  "t2", new Date(DateTimes.of("2000-01-01").getMillis())
              )
          ),
          getRows(resultSet)
      );
    }
  }

  @Test
  public void testTimestampsInResponseLosAngelesTimeZone() throws SQLException
  {
    try (Statement stmt = clientLosAngeles.createStatement()) {
      final ResultSet resultSet = stmt.executeQuery(
          "SELECT __time, CAST(__time AS DATE) AS t2 FROM druid.foo LIMIT 1"
      );

      final DateTimeZone timeZone = DateTimes.inferTzFromString("America/Los_Angeles");
      final DateTime localDateTime = new DateTime("2000-01-01T00Z", timeZone);

      final List<Map<String, Object>> resultRows = getRows(resultSet);

      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of(
                  "__time", new Timestamp(Calcites.jodaToCalciteTimestamp(localDateTime, timeZone)),
                  "t2", new Date(Calcites.jodaToCalciteTimestamp(localDateTime.dayOfMonth().roundFloorCopy(), timeZone))
              )
          ),
          resultRows
      );
    }
  }

  @Test
  public void testFieldAliasingSelect() throws SQLException
  {
    try (Statement stmt = client.createStatement()) {
      final ResultSet resultSet = stmt.executeQuery(
          "SELECT dim2 AS \"x\", dim2 AS \"y\" FROM druid.foo LIMIT 1"
      );

      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of("x", "a", "y", "a")
          ),
          getRows(resultSet)
      );
    }
  }

  @Test
  public void testSelectBoolean() throws SQLException
  {
    try (Statement stmt = client.createStatement()) {
      final ResultSet resultSet = stmt.executeQuery(
          "SELECT dim2, dim2 IS NULL AS isnull FROM druid.foo LIMIT 1"
      );

      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of("dim2", "a", "isnull", false)
          ),
          getRows(resultSet)
      );
    }
  }

  @Test
  public void testExplainSelectCount() throws SQLException
  {
    try (Statement stmt = clientLosAngeles.createStatement()) {
      final ResultSet resultSet = stmt.executeQuery(
          "EXPLAIN PLAN FOR SELECT COUNT(*) AS cnt FROM druid.foo"
      );

      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of(
                  "PLAN",
                  StringUtils.format("DruidQueryRel(query=[{\"queryType\":\"timeseries\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"granularity\":{\"type\":\"all\"},\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],\"context\":{\"sqlQueryId\":\"%s\",\"sqlStringifyArrays\":false,\"sqlTimeZone\":\"America/Los_Angeles\"}}], signature=[{a0:LONG}])\n",
                                     DUMMY_SQL_QUERY_ID
                  ),
                  "RESOURCES",
                  "[{\"name\":\"foo\",\"type\":\"DATASOURCE\"}]"
              )
          ),
          getRows(resultSet)
      );
    }
  }

  @Test
  public void testDatabaseMetaDataCatalogs() throws SQLException
  {
    final DatabaseMetaData metaData = client.getMetaData();
    Assert.assertEquals(
        ImmutableList.of(
            row(Pair.of("TABLE_CAT", "druid"))
        ),
        getRows(metaData.getCatalogs())
    );
  }

  @Test
  public void testDatabaseMetaDataSchemas() throws SQLException
  {
    final DatabaseMetaData metaData = client.getMetaData();
    Assert.assertEquals(
        ImmutableList.of(
            row(Pair.of("TABLE_CATALOG", "druid"), Pair.of("TABLE_SCHEM", "druid"))
        ),
        getRows(metaData.getSchemas(null, "druid"))
    );
  }

  @Test
  public void testDatabaseMetaDataTables() throws SQLException
  {
    final DatabaseMetaData metaData = client.getMetaData();
    Assert.assertEquals(
        ImmutableList.of(
            row(
                Pair.of("TABLE_CAT", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.BROADCAST_DATASOURCE),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")
            ),
            row(
                Pair.of("TABLE_CAT", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.DATASOURCE1),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")
            ),
            row(
                Pair.of("TABLE_CAT", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.DATASOURCE2),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")
            ),
            row(
                Pair.of("TABLE_CAT", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.DATASOURCE4),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")

            ),
            row(
                Pair.of("TABLE_CAT", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.DATASOURCE5),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")
            ),
            row(
                Pair.of("TABLE_CAT", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.DATASOURCE3),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")
            ),
            row(
                Pair.of("TABLE_CAT", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.SOME_DATASOURCE),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")
            ),
            row(
                Pair.of("TABLE_CAT", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.SOMEXDATASOURCE),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")
            ),
            row(
                Pair.of("TABLE_CAT", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.USERVISITDATASOURCE),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")
            )
        ),
        getRows(
            metaData.getTables(null, "druid", "%", null),
            ImmutableSet.of("TABLE_CAT", "TABLE_NAME", "TABLE_SCHEM", "TABLE_TYPE")
        )
    );
  }

  @Test
  public void testDatabaseMetaDataTablesAsSuperuser() throws SQLException
  {
    final DatabaseMetaData metaData = superuserClient.getMetaData();
    Assert.assertEquals(
        ImmutableList.of(
            row(
                Pair.of("TABLE_CAT", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.BROADCAST_DATASOURCE),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")
            ),
            row(
                Pair.of("TABLE_CAT", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.DATASOURCE1),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")
            ),
            row(
                Pair.of("TABLE_CAT", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.DATASOURCE2),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")
            ),
            row(
                Pair.of("TABLE_CAT", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.DATASOURCE4),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")
            ),
            row(
                Pair.of("TABLE_CAT", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.FORBIDDEN_DATASOURCE),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")
            ),
            row(
                Pair.of("TABLE_CAT", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.DATASOURCE5),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")
            ),
            row(
                Pair.of("TABLE_CAT", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.DATASOURCE3),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")
            ),
            row(
                Pair.of("TABLE_CAT", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.SOME_DATASOURCE),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")
            ),
            row(
                Pair.of("TABLE_CAT", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.SOMEXDATASOURCE),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")
            ),
            row(
                Pair.of("TABLE_CAT", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.USERVISITDATASOURCE),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")
            )
        ),
        getRows(
            metaData.getTables(null, "druid", "%", null),
            ImmutableSet.of("TABLE_CAT", "TABLE_NAME", "TABLE_SCHEM", "TABLE_TYPE")
        )
    );
  }

  @Test
  public void testDatabaseMetaDataColumns() throws SQLException
  {
    final DatabaseMetaData metaData = client.getMetaData();
    Assert.assertEquals(
        ImmutableList.of(
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("COLUMN_NAME", "__time"),
                Pair.of("DATA_TYPE", Types.TIMESTAMP),
                Pair.of("TYPE_NAME", "TIMESTAMP"),
                Pair.of("IS_NULLABLE", "NO")
            ),
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("COLUMN_NAME", "dim1"),
                Pair.of("DATA_TYPE", Types.VARCHAR),
                Pair.of("TYPE_NAME", "VARCHAR"),
                Pair.of("IS_NULLABLE", "YES")
            ),
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("COLUMN_NAME", "dim2"),
                Pair.of("DATA_TYPE", Types.VARCHAR),
                Pair.of("TYPE_NAME", "VARCHAR"),
                Pair.of("IS_NULLABLE", "YES")
            ),
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("COLUMN_NAME", "dim3"),
                Pair.of("DATA_TYPE", Types.VARCHAR),
                Pair.of("TYPE_NAME", "VARCHAR"),
                Pair.of("IS_NULLABLE", "YES")
            ),
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("COLUMN_NAME", "cnt"),
                Pair.of("DATA_TYPE", Types.BIGINT),
                Pair.of("TYPE_NAME", "BIGINT"),
                Pair.of("IS_NULLABLE", nullNumeric ? "YES" : "NO")
            ),
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("COLUMN_NAME", "m1"),
                Pair.of("DATA_TYPE", Types.FLOAT),
                Pair.of("TYPE_NAME", "FLOAT"),
                Pair.of("IS_NULLABLE", nullNumeric ? "YES" : "NO")
            ),
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("COLUMN_NAME", "m2"),
                Pair.of("DATA_TYPE", Types.DOUBLE),
                Pair.of("TYPE_NAME", "DOUBLE"),
                Pair.of("IS_NULLABLE", nullNumeric ? "YES" : "NO")
            ),
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("COLUMN_NAME", "unique_dim1"),
                Pair.of("DATA_TYPE", Types.OTHER),
                Pair.of("TYPE_NAME", "COMPLEX<hyperUnique>"),
                Pair.of("IS_NULLABLE", "YES")
            )
        ),
        getRows(
            metaData.getColumns(null, "dr_id", "foo", null),
            ImmutableSet.of("IS_NULLABLE", "TABLE_NAME", "TABLE_SCHEM", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME")
        )
    );
  }

  @Test
  public void testDatabaseMetaDataColumnsOnForbiddenDatasource() throws SQLException
  {
    final DatabaseMetaData metaData = client.getMetaData();
    Assert.assertEquals(
        ImmutableList.of(),
        getRows(
            metaData.getColumns(null, "dr_id", CalciteTests.FORBIDDEN_DATASOURCE, null),
            ImmutableSet.of("IS_NULLABLE", "TABLE_NAME", "TABLE_SCHEM", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME")
        )
    );
  }

  @Test
  public void testDatabaseMetaDataColumnsWithSuperuser() throws SQLException
  {
    final DatabaseMetaData metaData = superuserClient.getMetaData();
    Assert.assertEquals(
        ImmutableList.of(
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.FORBIDDEN_DATASOURCE),
                Pair.of("COLUMN_NAME", "__time"),
                Pair.of("DATA_TYPE", Types.TIMESTAMP),
                Pair.of("TYPE_NAME", "TIMESTAMP"),
                Pair.of("IS_NULLABLE", "NO")
            ),
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.FORBIDDEN_DATASOURCE),
                Pair.of("COLUMN_NAME", "dim1"),
                Pair.of("DATA_TYPE", Types.VARCHAR),
                Pair.of("TYPE_NAME", "VARCHAR"),
                Pair.of("IS_NULLABLE", "YES")
            ),
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.FORBIDDEN_DATASOURCE),
                Pair.of("COLUMN_NAME", "dim2"),
                Pair.of("DATA_TYPE", Types.VARCHAR),
                Pair.of("TYPE_NAME", "VARCHAR"),
                Pair.of("IS_NULLABLE", "YES")
            ),
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.FORBIDDEN_DATASOURCE),
                Pair.of("COLUMN_NAME", "cnt"),
                Pair.of("DATA_TYPE", Types.BIGINT),
                Pair.of("TYPE_NAME", "BIGINT"),
                Pair.of("IS_NULLABLE", nullNumeric ? "YES" : "NO")
            ),
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.FORBIDDEN_DATASOURCE),
                Pair.of("COLUMN_NAME", "m1"),
                Pair.of("DATA_TYPE", Types.FLOAT),
                Pair.of("TYPE_NAME", "FLOAT"),
                Pair.of("IS_NULLABLE", nullNumeric ? "YES" : "NO")
            ),
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.FORBIDDEN_DATASOURCE),
                Pair.of("COLUMN_NAME", "m2"),
                Pair.of("DATA_TYPE", Types.DOUBLE),
                Pair.of("TYPE_NAME", "DOUBLE"),
                Pair.of("IS_NULLABLE", nullNumeric ? "YES" : "NO")
            ),
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.FORBIDDEN_DATASOURCE),
                Pair.of("COLUMN_NAME", "unique_dim1"),
                Pair.of("DATA_TYPE", Types.OTHER),
                Pair.of("TYPE_NAME", "COMPLEX<hyperUnique>"),
                Pair.of("IS_NULLABLE", "YES")
            )
        ),
        getRows(
            metaData.getColumns(null, "dr_id", CalciteTests.FORBIDDEN_DATASOURCE, null),
            ImmutableSet.of("IS_NULLABLE", "TABLE_NAME", "TABLE_SCHEM", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME")
        )
    );
  }

  @Test(timeout = 90_000L)
  public void testConcurrentQueries() throws InterruptedException, ExecutionException
  {
    final List<ListenableFuture<Integer>> futures = new ArrayList<>();
    final ListeningExecutorService exec = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(AVATICA_CONFIG.getMaxStatementsPerConnection())
    );
    for (int i = 0; i < 2000; i++) {
      final String query = StringUtils.format("SELECT COUNT(*) + %s AS ci FROM foo", i);
      futures.add(
          exec.submit(() -> {
            try (
                final Statement statement = client.createStatement();
                final ResultSet resultSet = statement.executeQuery(query)
            ) {
              final List<Map<String, Object>> rows = getRows(resultSet);
              return ((Number) Iterables.getOnlyElement(rows).get("ci")).intValue();
            }
            catch (SQLException e) {
              throw new RuntimeException(e);
            }
          })
      );
    }

    final List<Integer> integers = Futures.allAsList(futures).get();
    for (int i = 0; i < 2000; i++) {
      Assert.assertEquals(i + 6, (int) integers.get(i));
    }
    exec.shutdown();
  }

  @Test
  public void testTooManyStatements() throws SQLException
  {
    for (int i = 0; i < 4; i++) {
      client.createStatement();
    }

    expectedException.expect(AvaticaClientRuntimeException.class);
    expectedException.expectMessage("Too many open statements, limit is [4]");
    client.createStatement();
  }

  @Test
  public void testNotTooManyStatementsWhenYouCloseThem() throws SQLException
  {
    for (int i = 0; i < 10; i++) {
      client.createStatement().close();
    }
  }

  /**
   * JDBC allows sequential reuse of statements. A statement is not closed until
   * the application closes it (or the connection), but the statement's result set
   * is closed on each EOF.
   */
  @Test
  public void testManyUsesOfTheSameStatement() throws SQLException
  {
    try (Statement statement = client.createStatement()) {
      for (int i = 0; i < 50; i++) {
        final ResultSet resultSet = statement.executeQuery(
            "SELECT COUNT(*) AS cnt FROM druid.foo"
        );
        Assert.assertEquals(
            ImmutableList.of(
                ImmutableMap.of("cnt", 6L)
            ),
            getRows(resultSet)
        );
      }
    }
  }

  /**
   * Statements should not be closed if then encounter an error. The {@code ResultSet}
   * can be closed, but not the statement.
   */
  @Test
  public void tesErrorsDoNotCloseStatements() throws SQLException
  {
    try (Statement statement = client.createStatement()) {
      try {
        statement.executeQuery("SELECT SUM(nonexistent) FROM druid.foo");
        Assert.fail();
      }
      catch (Exception e) {
        // Expected
      }

      final ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) AS cnt FROM druid.foo");
      Assert.assertEquals(
          ImmutableList.of(ImmutableMap.of("cnt", 6L)),
          getRows(resultSet)
      );
    }
  }

  /**
   * Since errors do not close statements, they must be closed by the application,
   * preferably in a try-with-resources block.
   */
  @Test
  public void testNotTooManyStatementsWhenClosed()
  {
    for (int i = 0; i < 50; i++) {
      try (Statement statement = client.createStatement()) {
        statement.executeQuery("SELECT SUM(nonexistent) FROM druid.foo");
        Assert.fail();
      }
      catch (Exception e) {
        // Expected
      }
    }
  }

  @Test
  public void testAutoReconnectOnNoSuchConnection() throws SQLException
  {
    for (int i = 0; i < 50; i++) {
      final ResultSet resultSet = client.createStatement().executeQuery("SELECT COUNT(*) AS cnt FROM druid.foo");
      Assert.assertEquals(
          ImmutableList.of(ImmutableMap.of("cnt", 6L)),
          getRows(resultSet)
      );
      druidMeta.closeAllConnections();
    }
  }

  @Test
  public void testTooManyConnections() throws SQLException
  {
    client.createStatement();
    clientLosAngeles.createStatement();
    superuserClient.createStatement();
    clientNoTrailingSlash.createStatement();

    expectedException.expect(AvaticaClientRuntimeException.class);
    expectedException.expectMessage("Too many connections");

    DriverManager.getConnection(url);
  }

  @Test
  public void testNotTooManyConnectionsWhenTheyAreEmpty() throws SQLException
  {
    for (int i = 0; i < 4; i++) {
      try (Connection connection = DriverManager.getConnection(url)) {
      }
    }
  }

  @Test
  public void testMaxRowsPerFrame() throws Exception
  {
    final AvaticaServerConfig smallFrameConfig = new AvaticaServerConfig()
    {
      @Override
      public int getMaxConnections()
      {
        return 2;
      }

      @Override
      public int getMaxStatementsPerConnection()
      {
        return 4;
      }

      @Override
      public int getMaxRowsPerFrame()
      {
        return 2;
      }
    };

    final PlannerConfig plannerConfig = new PlannerConfig();
    final DruidOperatorTable operatorTable = CalciteTests.createOperatorTable();
    final ExprMacroTable macroTable = CalciteTests.createExprMacroTable();
    final List<Meta.Frame> frames = new ArrayList<>();
    final ScheduledExecutorService exec = Execs.scheduledSingleThreaded("testMaxRowsPerFrame");
    DruidSchemaCatalog rootSchema =
        CalciteTests.createMockRootSchema(conglomerate, walker, plannerConfig, AuthTestUtils.TEST_AUTHORIZER_MAPPER);
    DruidMeta smallFrameDruidMeta = new DruidMeta(
        CalciteTests.createSqlStatementFactory(
            CalciteTests.createMockSqlEngine(walker, conglomerate),
            new PlannerFactory(
                rootSchema,
                operatorTable,
                macroTable,
                plannerConfig,
                AuthTestUtils.TEST_AUTHORIZER_MAPPER,
                CalciteTests.getJsonMapper(),
                CalciteTests.DRUID_SCHEMA_NAME,
                new CalciteRulesManager(ImmutableSet.of())
            )
        ),
        smallFrameConfig,
        new ErrorHandler(new ServerConfig()),
        exec,
        injector.getInstance(AuthenticatorMapper.class).getAuthenticatorChain()
    )
    {
      @Override
      public Frame fetch(
          final StatementHandle statement,
          final long offset,
          final int fetchMaxRowCount
      ) throws NoSuchStatementException, MissingResultsException
      {
        // overriding fetch allows us to track how many frames are processed after the first frame
        Frame frame = super.fetch(statement, offset, fetchMaxRowCount);
        frames.add(frame);
        return frame;
      }
    };

    final AbstractAvaticaHandler handler = this.getAvaticaHandler(smallFrameDruidMeta);
    final int port = ThreadLocalRandom.current().nextInt(9999) + 20000;
    Server smallFrameServer = new Server(new InetSocketAddress("127.0.0.1", port));
    smallFrameServer.setHandler(handler);
    smallFrameServer.start();
    String smallFrameUrl = this.getJdbcConnectionString(port);
    Connection smallFrameClient = DriverManager.getConnection(smallFrameUrl, "regularUser", "druid");

    final ResultSet resultSet = smallFrameClient.createStatement().executeQuery(
        "SELECT dim1 FROM druid.foo"
    );
    List<Map<String, Object>> rows = getRows(resultSet);
    Assert.assertEquals(2, frames.size());
    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("dim1", ""),
            ImmutableMap.of("dim1", "10.1"),
            ImmutableMap.of("dim1", "2"),
            ImmutableMap.of("dim1", "1"),
            ImmutableMap.of("dim1", "def"),
            ImmutableMap.of("dim1", "abc")
        ),
        rows
    );

    exec.shutdown();
  }

  @Test
  public void testMinRowsPerFrame() throws Exception
  {
    final int minFetchSize = 1000;
    final AvaticaServerConfig smallFrameConfig = new AvaticaServerConfig()
    {
      @Override
      public int getMaxConnections()
      {
        return 2;
      }

      @Override
      public int getMaxStatementsPerConnection()
      {
        return 4;
      }

      @Override
      public int getMinRowsPerFrame()
      {
        return minFetchSize;
      }
    };

    final PlannerConfig plannerConfig = new PlannerConfig();
    final DruidOperatorTable operatorTable = CalciteTests.createOperatorTable();
    final ExprMacroTable macroTable = CalciteTests.createExprMacroTable();
    final List<Meta.Frame> frames = new ArrayList<>();
    final ScheduledExecutorService exec = Execs.scheduledSingleThreaded("testMaxRowsPerFrame");
    DruidSchemaCatalog rootSchema =
        CalciteTests.createMockRootSchema(conglomerate, walker, plannerConfig, AuthTestUtils.TEST_AUTHORIZER_MAPPER);
    DruidMeta smallFrameDruidMeta = new DruidMeta(
        CalciteTests.createSqlStatementFactory(
            CalciteTests.createMockSqlEngine(walker, conglomerate),
            new PlannerFactory(
                rootSchema,
                operatorTable,
                macroTable,
                plannerConfig,
                AuthTestUtils.TEST_AUTHORIZER_MAPPER,
                CalciteTests.getJsonMapper(),
                CalciteTests.DRUID_SCHEMA_NAME,
                new CalciteRulesManager(ImmutableSet.of())
            )
        ),
        smallFrameConfig,
        new ErrorHandler(new ServerConfig()),
        exec,
        injector.getInstance(AuthenticatorMapper.class).getAuthenticatorChain()
    )
    {
      @Override
      public Frame fetch(
          final StatementHandle statement,
          final long offset,
          final int fetchMaxRowCount
      ) throws NoSuchStatementException, MissingResultsException
      {
        // overriding fetch allows us to track how many frames are processed after the first frame, and also fetch size
        Assert.assertEquals(minFetchSize, fetchMaxRowCount);
        Frame frame = super.fetch(statement, offset, fetchMaxRowCount);
        frames.add(frame);
        return frame;
      }
    };

    final AbstractAvaticaHandler handler = this.getAvaticaHandler(smallFrameDruidMeta);
    final int port = ThreadLocalRandom.current().nextInt(9999) + 20000;
    Server smallFrameServer = new Server(new InetSocketAddress("127.0.0.1", port));
    smallFrameServer.setHandler(handler);
    smallFrameServer.start();
    String smallFrameUrl = this.getJdbcConnectionString(port);
    Connection smallFrameClient = DriverManager.getConnection(smallFrameUrl, "regularUser", "druid");

    // use a prepared statement because Avatica currently ignores fetchSize on the initial fetch of a Statement
    PreparedStatement statement = smallFrameClient.prepareStatement("SELECT dim1 FROM druid.foo");
    // set a fetch size below the minimum configured threshold
    statement.setFetchSize(2);
    final ResultSet resultSet = statement.executeQuery();
    List<Map<String, Object>> rows = getRows(resultSet);
    // expect minimum threshold to be used, which should be enough to do this all in first fetch
    Assert.assertEquals(0, frames.size());
    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("dim1", ""),
            ImmutableMap.of("dim1", "10.1"),
            ImmutableMap.of("dim1", "2"),
            ImmutableMap.of("dim1", "1"),
            ImmutableMap.of("dim1", "def"),
            ImmutableMap.of("dim1", "abc")
        ),
        rows
    );

    exec.shutdown();
  }

  @Test
  public void testSqlRequestLog() throws SQLException
  {
    // valid sql
    testRequestLogger.clear();
    for (int i = 0; i < 3; i++) {
      try (Statement stmt = client.createStatement()) {
        stmt.executeQuery("SELECT COUNT(*) AS cnt FROM druid.foo");
      }
    }
    Assert.assertEquals(3, testRequestLogger.getSqlQueryLogs().size());
    for (RequestLogLine logLine : testRequestLogger.getSqlQueryLogs()) {
      final Map<String, Object> stats = logLine.getQueryStats().getStats();
      Assert.assertEquals(true, stats.get("success"));
      Assert.assertEquals("regularUser", stats.get("identity"));
      Assert.assertTrue(stats.containsKey("sqlQuery/time"));
      Assert.assertTrue(stats.containsKey("sqlQuery/bytes"));
    }

    // invalid sql
    testRequestLogger.clear();
    try (Statement stmt = client.createStatement()) {
      stmt.executeQuery("SELECT notexist FROM druid.foo");
      Assert.fail("invalid SQL should throw SQLException");
    }
    catch (SQLException e) {
      // Expected
    }
    Assert.assertEquals(1, testRequestLogger.getSqlQueryLogs().size());
    {
      final Map<String, Object> stats = testRequestLogger.getSqlQueryLogs().get(0).getQueryStats().getStats();
      Assert.assertEquals(false, stats.get("success"));
      Assert.assertEquals("regularUser", stats.get("identity"));
      Assert.assertTrue(stats.containsKey("exception"));
    }

    // unauthorized sql
    testRequestLogger.clear();
    try (Statement stmt = client.createStatement()) {
      stmt.executeQuery("SELECT count(*) FROM druid.forbiddenDatasource");
      Assert.fail("unauthorzed SQL should throw SQLException");
    }
    catch (SQLException e) {
      // Expected
    }
    Assert.assertEquals(1, testRequestLogger.getSqlQueryLogs().size());
    {
      final Map<String, Object> stats = testRequestLogger.getSqlQueryLogs().get(0).getQueryStats().getStats();
      Assert.assertEquals(false, stats.get("success"));
      Assert.assertEquals("regularUser", stats.get("identity"));
      Assert.assertTrue(stats.containsKey("exception"));
    }
  }

  @Test
  public void testSqlRequestLogPrepared() throws SQLException
  {
    // valid sql
    testRequestLogger.clear();
    for (int i = 0; i < 3; i++) {
      try (PreparedStatement stmt = client.prepareStatement("SELECT COUNT(*) AS cnt FROM druid.foo")) {
        stmt.execute();
      }
    }
    Assert.assertEquals(6, testRequestLogger.getSqlQueryLogs().size());
    for (RequestLogLine logLine : testRequestLogger.getSqlQueryLogs()) {
      final Map<String, Object> stats = logLine.getQueryStats().getStats();
      Assert.assertEquals(true, stats.get("success"));
      Assert.assertEquals("regularUser", stats.get("identity"));
      Assert.assertTrue(stats.containsKey("sqlQuery/time"));
      Assert.assertTrue(stats.containsKey("sqlQuery/bytes"));
    }

    // invalid sql
    testRequestLogger.clear();
    try (PreparedStatement stmt = client.prepareStatement("SELECT notexist FROM druid.foo")) {
      Assert.fail("invalid SQL should throw SQLException");
    }
    catch (SQLException e) {
      // Expected
    }
    Assert.assertEquals(1, testRequestLogger.getSqlQueryLogs().size());
    {
      final Map<String, Object> stats = testRequestLogger.getSqlQueryLogs().get(0).getQueryStats().getStats();
      Assert.assertEquals(false, stats.get("success"));
      Assert.assertEquals("regularUser", stats.get("identity"));
      Assert.assertTrue(stats.containsKey("exception"));
    }

    // unauthorized sql
    testRequestLogger.clear();
    try (PreparedStatement stmt = client.prepareStatement("SELECT count(*) FROM druid.forbiddenDatasource")) {
      Assert.fail("unauthorzed SQL should throw SQLException");
    }
    catch (SQLException e) {
      // Expected
    }
    Assert.assertEquals(1, testRequestLogger.getSqlQueryLogs().size());
    {
      final Map<String, Object> stats = testRequestLogger.getSqlQueryLogs().get(0).getQueryStats().getStats();
      Assert.assertEquals(false, stats.get("success"));
      Assert.assertEquals("regularUser", stats.get("identity"));
      Assert.assertTrue(stats.containsKey("exception"));
    }
  }

  @Test
  public void testParameterBinding() throws SQLException
  {
    try (PreparedStatement statement = client.prepareStatement("SELECT COUNT(*) AS cnt FROM druid.foo WHERE dim1 = ? OR dim1 = ?")) {
      statement.setString(1, "abc");
      statement.setString(2, "def");
      final ResultSet resultSet = statement.executeQuery();
      final List<Map<String, Object>> rows = getRows(resultSet);
      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of("cnt", 2L)
          ),
          rows
      );
    }
  }

  @Test
  public void testSysTableParameterBindingRegularUser() throws SQLException
  {
    try (PreparedStatement statement =
        client.prepareStatement("SELECT COUNT(*) AS cnt FROM sys.servers WHERE servers.host = ?")) {
      statement.setString(1, "dummy");

      Assert.assertThrows(
          "Insufficient permission to view servers",
          AvaticaSqlException.class,
          statement::executeQuery
      );
    }
  }

  @Test
  public void testSysTableParameterBindingSuperUser() throws SQLException
  {
    try (PreparedStatement statement =
        superuserClient.prepareStatement("SELECT COUNT(*) AS cnt FROM sys.servers WHERE servers.host = ?")) {
      statement.setString(1, "dummy");
      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of("cnt", 1L)
          ),
          getRows(statement.executeQuery())
      );
    }
  }

  @Test
  public void testExecuteMany() throws SQLException
  {
    try (PreparedStatement statement =
        superuserClient.prepareStatement("SELECT COUNT(*) AS cnt FROM sys.servers WHERE servers.host = ?")) {
      statement.setString(1, "dummy");
      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of("cnt", 1L)
          ),
          getRows(statement.executeQuery())
      );
      statement.setString(1, "foo");
      Assert.assertEquals(
          Collections.emptyList(),
          getRows(statement.executeQuery())
      );
      statement.setString(1, "dummy");
      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of("cnt", 1L)
          ),
          getRows(statement.executeQuery())
      );
    }
  }

  @Test
  public void testExtendedCharacters() throws SQLException
  {
    try (Statement stmt = client.createStatement()) {
      final ResultSet resultSet = stmt.executeQuery(
          "SELECT COUNT(*) AS cnt FROM druid.lotsocolumns WHERE dimMultivalEnumerated = 'ㅑ ㅓ ㅕ ㅗ ㅛ ㅜ ㅠ ㅡ ㅣ'"
      );
      final List<Map<String, Object>> rows = getRows(resultSet);
      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of("cnt", 1L)
          ),
          rows
      );
    }

    try (PreparedStatement statement = client.prepareStatement(
        "SELECT COUNT(*) AS cnt FROM druid.lotsocolumns WHERE dimMultivalEnumerated = ?")) {
      statement.setString(1, "ㅑ ㅓ ㅕ ㅗ ㅛ ㅜ ㅠ ㅡ ㅣ");
      final ResultSet resultSet2 = statement.executeQuery();
      final List<Map<String, Object>> rows = getRows(resultSet2);
      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of("cnt", 1L)
          ),
          rows
      );
      Assert.assertEquals(rows, rows);
    }
  }

  @Test
  public void testEscapingForGetColumns() throws SQLException
  {
    final DatabaseMetaData metaData = client.getMetaData();

    ImmutableList<Map<String, Object>> someDatasourceColumns = ImmutableList.of(
        row(
            Pair.of("TABLE_SCHEM", "druid"),
            Pair.of("TABLE_NAME", CalciteTests.SOME_DATASOURCE),
            Pair.of("COLUMN_NAME", "__time")
        ),
        row(
            Pair.of("TABLE_SCHEM", "druid"),
            Pair.of("TABLE_NAME", CalciteTests.SOME_DATASOURCE),
            Pair.of("COLUMN_NAME", "dim1")
        ),
        row(
            Pair.of("TABLE_SCHEM", "druid"),
            Pair.of("TABLE_NAME", CalciteTests.SOME_DATASOURCE),
            Pair.of("COLUMN_NAME", "dim2")
        ),
        row(
            Pair.of("TABLE_SCHEM", "druid"),
            Pair.of("TABLE_NAME", CalciteTests.SOME_DATASOURCE),
            Pair.of("COLUMN_NAME", "dim3")
        ),
        row(
            Pair.of("TABLE_SCHEM", "druid"),
            Pair.of("TABLE_NAME", CalciteTests.SOME_DATASOURCE),
            Pair.of("COLUMN_NAME", "cnt")
        ),
        row(
            Pair.of("TABLE_SCHEM", "druid"),
            Pair.of("TABLE_NAME", CalciteTests.SOME_DATASOURCE),
            Pair.of("COLUMN_NAME", "m1")
        ),
        row(
            Pair.of("TABLE_SCHEM", "druid"),
            Pair.of("TABLE_NAME", CalciteTests.SOME_DATASOURCE),
            Pair.of("COLUMN_NAME", "m2")
        ),
        row(
            Pair.of("TABLE_SCHEM", "druid"),
            Pair.of("TABLE_NAME", CalciteTests.SOME_DATASOURCE),
            Pair.of("COLUMN_NAME", "unique_dim1")
        )
    );
    // If the escape clause wasn't correctly set, rows for potentially none or more than
    // one datasource (some_datasource and somexdatasource) would have been returned
    Assert.assertEquals(
        someDatasourceColumns,
        getRows(
            metaData.getColumns(null, "dr_id", CalciteTests.SOME_DATSOURCE_ESCAPED, null),
            ImmutableSet.of("TABLE_NAME", "TABLE_SCHEM", "COLUMN_NAME")
        )
    );

    ImmutableList<Map<String, Object>> someXDatasourceColumns = ImmutableList.of(
        row(
            Pair.of("TABLE_SCHEM", "druid"),
            Pair.of("TABLE_NAME", CalciteTests.SOMEXDATASOURCE),
            Pair.of("COLUMN_NAME", "__time")
        ),
        row(
            Pair.of("TABLE_SCHEM", "druid"),
            Pair.of("TABLE_NAME", CalciteTests.SOMEXDATASOURCE),
            Pair.of("COLUMN_NAME", "cnt_x")
        ),
        row(
            Pair.of("TABLE_SCHEM", "druid"),
            Pair.of("TABLE_NAME", CalciteTests.SOMEXDATASOURCE),
            Pair.of("COLUMN_NAME", "m1_x")
        ),
        row(
            Pair.of("TABLE_SCHEM", "druid"),
            Pair.of("TABLE_NAME", CalciteTests.SOMEXDATASOURCE),
            Pair.of("COLUMN_NAME", "m2_x")
        ),
        row(
            Pair.of("TABLE_SCHEM", "druid"),
            Pair.of("TABLE_NAME", CalciteTests.SOMEXDATASOURCE),
            Pair.of("COLUMN_NAME", "unique_dim1_x")
        )
    );
    Assert.assertEquals(
        someXDatasourceColumns,
        getRows(
            metaData.getColumns(null, "dr_id", "somexdatasource", null),
            ImmutableSet.of("TABLE_NAME", "TABLE_SCHEM", "COLUMN_NAME")
        )
    );

    List<Map<String, Object>> columnsOfBothTables = new ArrayList<>(someDatasourceColumns);
    columnsOfBothTables.addAll(someXDatasourceColumns);
    // Assert that the pattern matching still works when no escape string is provided
    Assert.assertEquals(
        columnsOfBothTables,
        getRows(
            metaData.getColumns(null, "dr_id", "some_datasource", null),
            ImmutableSet.of("TABLE_NAME", "TABLE_SCHEM", "COLUMN_NAME")
        )
    );

    // Assert column name pattern works correctly when _ is in the column names
    Assert.assertEquals(
        ImmutableList.of(
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.SOMEXDATASOURCE),
                Pair.of("COLUMN_NAME", "m1_x")
            ),
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.SOMEXDATASOURCE),
                Pair.of("COLUMN_NAME", "m2_x")
            )
        ),
        getRows(
            metaData.getColumns("druid", "dr_id", CalciteTests.SOMEXDATASOURCE, "m_\\_x"),
            ImmutableSet.of("TABLE_NAME", "TABLE_SCHEM", "COLUMN_NAME")
        )
    );

    // Assert column name pattern with % works correctly for column names starting with m
    Assert.assertEquals(
        ImmutableList.of(
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.SOME_DATASOURCE),
                Pair.of("COLUMN_NAME", "m1")
            ),
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.SOME_DATASOURCE),
                Pair.of("COLUMN_NAME", "m2")
            ),
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.SOMEXDATASOURCE),
                Pair.of("COLUMN_NAME", "m1_x")
            ),
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.SOMEXDATASOURCE),
                Pair.of("COLUMN_NAME", "m2_x")
            )
        ),
        getRows(
            metaData.getColumns("druid", "dr_id", CalciteTests.SOME_DATASOURCE, "m%"),
            ImmutableSet.of("TABLE_NAME", "TABLE_SCHEM", "COLUMN_NAME")
        )
    );
  }

  @Test
  public void testEscapingForGetTables() throws SQLException
  {
    final DatabaseMetaData metaData = client.getMetaData();

    Assert.assertEquals(
        ImmutableList.of(
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.SOME_DATASOURCE)
            )
        ),
        getRows(
            metaData.getTables("druid", "dr_id", CalciteTests.SOME_DATSOURCE_ESCAPED, null),
            ImmutableSet.of("TABLE_SCHEM", "TABLE_NAME")
        )
    );

    Assert.assertEquals(
        ImmutableList.of(
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.SOMEXDATASOURCE)
            )
        ),
        getRows(
            metaData.getTables("druid", "dr_id", CalciteTests.SOMEXDATASOURCE, null),
            ImmutableSet.of("TABLE_SCHEM", "TABLE_NAME")
        )
    );

    // Assert that some_datasource is treated as a pattern that matches some_datasource and somexdatasource
    Assert.assertEquals(
        ImmutableList.of(
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.SOME_DATASOURCE)
            ),
            row(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", CalciteTests.SOMEXDATASOURCE)
            )
        ),
        getRows(
            metaData.getTables("druid", "dr_id", CalciteTests.SOME_DATASOURCE, null),
            ImmutableSet.of("TABLE_SCHEM", "TABLE_NAME")
        )
    );
  }

  @Test
  public void testArrayStuff() throws SQLException
  {
    try (PreparedStatement statement = client.prepareStatement(
        "SELECT ARRAY_AGG(dim2) AS arr1, ARRAY_AGG(l1) AS arr2, ARRAY_AGG(d1)  AS arr3, ARRAY_AGG(f1) AS arr4 FROM druid.numfoo")) {
      final ResultSet resultSet = statement.executeQuery();
      final List<Map<String, Object>> rows = getRows(resultSet);
      Assert.assertEquals(1, rows.size());
      Assert.assertTrue(rows.get(0).containsKey("arr1"));
      Assert.assertTrue(rows.get(0).containsKey("arr2"));
      Assert.assertTrue(rows.get(0).containsKey("arr3"));
      Assert.assertTrue(rows.get(0).containsKey("arr4"));
      if (NullHandling.sqlCompatible()) {
        Assert.assertArrayEquals(new Object[]{"a", null, "", "a", "abc", null}, (Object[]) rows.get(0).get("arr1"));
        Assert.assertArrayEquals(new Object[]{7L, 325323L, 0L, null, null, null}, (Object[]) rows.get(0).get("arr2"));
        Assert.assertArrayEquals(new Object[]{1.0, 1.7, 0.0, null, null, null}, (Object[]) rows.get(0).get("arr3"));
        Assert.assertArrayEquals(new Object[]{1.0f, 0.1f, 0.0f, null, null, null}, (Object[]) rows.get(0).get("arr4"));
      } else {
        Assert.assertArrayEquals(new Object[]{"a", null, null, "a", "abc", null}, (Object[]) rows.get(0).get("arr1"));
        Assert.assertArrayEquals(new Object[]{7L, 325323L, 0L, 0L, 0L, 0L}, (Object[]) rows.get(0).get("arr2"));
        Assert.assertArrayEquals(new Object[]{1.0, 1.7, 0.0, 0.0, 0.0, 0.0}, (Object[]) rows.get(0).get("arr3"));
        Assert.assertArrayEquals(new Object[]{1.0f, 0.1f, 0.0f, 0.0f, 0.0f, 0.0f}, (Object[]) rows.get(0).get("arr4"));
      }
    }
  }

  /**
   * Verify that a security exception is mapped to the correct Avatica SQL error codes.
   */
  @Test
  public void testUnauthorizedTable()
  {
    final String query = "SELECT * FROM " + CalciteTests.FORBIDDEN_DATASOURCE;
    final String expectedError = "Error 2 (00002) : Error while executing SQL \"" +
            query + "\": Remote driver error: Unauthorized";
    try (Statement statement = client.createStatement()) {
      statement.executeQuery(query);
    }
    catch (SQLException e) {
      Assert.assertEquals(
          e.getMessage(),
          expectedError
      );
      return;
    }
    Assert.fail("Test failed, did not get SQLException");
  }

  // Default implementation is for JSON to allow debugging of tests.
  protected String getJdbcConnectionString(final int port)
  {
    return StringUtils.format(
            "jdbc:avatica:remote:url=http://127.0.0.1:%d%s",
            port,
            DruidAvaticaJsonHandler.AVATICA_PATH
    );
  }

  // Default implementation is for JSON to allow debugging of tests.
  protected AbstractAvaticaHandler getAvaticaHandler(final DruidMeta druidMeta)
  {
    return new DruidAvaticaJsonHandler(
            druidMeta,
            new DruidNode("dummy", "dummy", false, 1, null, true, false),
            new AvaticaMonitor()
    );
  }

  private static List<Map<String, Object>> getRows(final ResultSet resultSet) throws SQLException
  {
    return getRows(resultSet, null);
  }

  private static List<Map<String, Object>> getRows(final ResultSet resultSet, final Set<String> returnKeys)
      throws SQLException
  {
    try {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final List<Map<String, Object>> rows = new ArrayList<>();
      while (resultSet.next()) {
        final Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
          if (returnKeys == null || returnKeys.contains(metaData.getColumnLabel(i + 1))) {
            Object result = resultSet.getObject(i + 1);
            if (result instanceof Array) {
              row.put(metaData.getColumnLabel(i + 1), ((Array) result).getArray());
            } else {
              row.put(metaData.getColumnLabel(i + 1), result);
            }
          }
        }
        rows.add(row);
      }
      return rows;
    }
    finally {
      resultSet.close();
    }
  }

  @SafeVarargs
  private static Map<String, Object> row(final Pair<String, ?>... entries)
  {
    final Map<String, Object> m = new HashMap<>();
    for (Pair<String, ?> entry : entries) {
      m.put(entry.lhs, entry.rhs);
    }
    return m;
  }
}
