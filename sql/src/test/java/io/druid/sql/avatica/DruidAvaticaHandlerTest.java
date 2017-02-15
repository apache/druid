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

package io.druid.sql.avatica;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.java.util.common.Pair;
import io.druid.server.DruidNode;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.planner.PlannerFactory;
import io.druid.sql.calcite.util.CalciteTests;
import io.druid.sql.calcite.util.QueryLogHook;
import io.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.calcite.avatica.AvaticaClientRuntimeException;
import org.apache.calcite.schema.SchemaPlus;
import org.eclipse.jetty.server.Server;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

public class DruidAvaticaHandlerTest
{
  private static final AvaticaServerConfig AVATICA_CONFIG = new AvaticaServerConfig()
  {
    @Override
    public int getMaxConnections()
    {
      return 2;
    }

    @Override
    public int getMaxStatementsPerConnection()
    {
      return 2;
    }
  };

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create();

  private SpecificSegmentsQuerySegmentWalker walker;
  private Server server;
  private Connection client;
  private Connection clientLosAngeles;

  @Before
  public void setUp() throws Exception
  {
    Calcites.setSystemProperties();
    walker = CalciteTests.createMockWalker(temporaryFolder.newFolder());
    final PlannerConfig plannerConfig = new PlannerConfig();
    final SchemaPlus rootSchema = Calcites.createRootSchema(
        CalciteTests.createMockSchema(
            walker,
            plannerConfig
        )
    );
    final DruidOperatorTable operatorTable = CalciteTests.createOperatorTable();
    final DruidAvaticaHandler handler = new DruidAvaticaHandler(
        new DruidMeta(new PlannerFactory(rootSchema, walker, operatorTable, plannerConfig), AVATICA_CONFIG),
        new DruidNode("dummy", "dummy", 1),
        new AvaticaMonitor()
    );
    final int port = new Random().nextInt(9999) + 10000;
    server = new Server(new InetSocketAddress("127.0.0.1", port));
    server.setHandler(handler);
    server.start();
    final String url = String.format(
        "jdbc:avatica:remote:url=http://127.0.0.1:%d%s",
        port,
        DruidAvaticaHandler.AVATICA_PATH
    );
    client = DriverManager.getConnection(url);

    final Properties propertiesLosAngeles = new Properties();
    propertiesLosAngeles.setProperty("sqlTimeZone", "America/Los_Angeles");
    clientLosAngeles = DriverManager.getConnection(url, propertiesLosAngeles);
  }

  @After
  public void tearDown() throws Exception
  {
    client.close();
    clientLosAngeles.close();
    server.stop();
    walker.close();
    walker = null;
    client = null;
    clientLosAngeles = null;
    server = null;
  }

  @Test
  public void testSelectCount() throws Exception
  {
    final ResultSet resultSet = client.createStatement().executeQuery("SELECT COUNT(*) AS cnt FROM druid.foo");
    final List<Map<String, Object>> rows = getRows(resultSet);
    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("cnt", 6L)
        ),
        rows
    );
  }

  @Test
  public void testSelectCountAlternateStyle() throws Exception
  {
    final ResultSet resultSet = client.prepareStatement("SELECT COUNT(*) AS cnt FROM druid.foo").executeQuery();
    final List<Map<String, Object>> rows = getRows(resultSet);
    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("cnt", 6L)
        ),
        rows
    );
  }

  @Test
  public void testTimestampsInResponse() throws Exception
  {
    final ResultSet resultSet = client.createStatement().executeQuery(
        "SELECT __time, CAST(__time AS DATE) AS t2 FROM druid.foo LIMIT 1"
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of(
                "__time", new Timestamp(new DateTime("2000-01-01T00:00:00.000Z").getMillis()),
                "t2", new Date(new DateTime("2000-01-01").getMillis())
            )
        ),
        getRows(resultSet)
    );
  }

  @Test
  public void testTimestampsInResponseLosAngelesTimeZone() throws Exception
  {
    final ResultSet resultSet = clientLosAngeles.createStatement().executeQuery(
        "SELECT __time, CAST(__time AS DATE) AS t2 FROM druid.foo LIMIT 1"
    );

    final DateTimeZone timeZone = DateTimeZone.forID("America/Los_Angeles");
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

  @Test
  public void testFieldAliasingSelect() throws Exception
  {
    final ResultSet resultSet = client.createStatement().executeQuery(
        "SELECT dim2 AS \"x\", dim2 AS \"y\" FROM druid.foo LIMIT 1"
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("x", "a", "y", "a")
        ),
        getRows(resultSet)
    );
  }

  @Test
  public void testExplainSelectCount() throws Exception
  {
    final ResultSet resultSet = client.createStatement().executeQuery(
        "EXPLAIN PLAN FOR SELECT COUNT(*) AS cnt FROM druid.foo"
    );

    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of(
                "PLAN",
                "DruidQueryRel(dataSource=[foo], dimensions=[[]], aggregations=[[Aggregation{aggregatorFactories=[CountAggregatorFactory{name='a0'}], postAggregator=null, finalizingPostAggregatorFactory=null}]])\n"
            )
        ),
        getRows(resultSet)
    );
  }

  @Test
  public void testDatabaseMetaDataCatalogs() throws Exception
  {
    final DatabaseMetaData metaData = client.getMetaData();
    Assert.assertEquals(
        ImmutableList.of(
            ROW(Pair.of("TABLE_CAT", ""))
        ),
        getRows(metaData.getCatalogs())
    );
  }

  @Test
  public void testDatabaseMetaDataSchemas() throws Exception
  {
    final DatabaseMetaData metaData = client.getMetaData();
    Assert.assertEquals(
        ImmutableList.of(
            ROW(Pair.of("TABLE_CATALOG", ""), Pair.of("TABLE_SCHEM", "druid"))
        ),
        getRows(metaData.getSchemas(null, "druid"))
    );
  }

  @Test
  public void testDatabaseMetaDataTables() throws Exception
  {
    final DatabaseMetaData metaData = client.getMetaData();
    Assert.assertEquals(
        ImmutableList.of(
            ROW(
                Pair.of("TABLE_CAT", null),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_TYPE", "TABLE")
            ),
            ROW(
                Pair.of("TABLE_CAT", null),
                Pair.of("TABLE_NAME", "foo2"),
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
  public void testDatabaseMetaDataColumns() throws Exception
  {
    final DatabaseMetaData metaData = client.getMetaData();
    Assert.assertEquals(
        ImmutableList.of(
            ROW(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("COLUMN_NAME", "__time"),
                Pair.of("DATA_TYPE", Types.TIMESTAMP),
                Pair.of("TYPE_NAME", "TIMESTAMP"),
                Pair.of("IS_NULLABLE", "NO")
            ),
            ROW(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("COLUMN_NAME", "cnt"),
                Pair.of("DATA_TYPE", Types.BIGINT),
                Pair.of("TYPE_NAME", "BIGINT"),
                Pair.of("IS_NULLABLE", "NO")
            ),
            ROW(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("COLUMN_NAME", "dim1"),
                Pair.of("DATA_TYPE", Types.VARCHAR),
                Pair.of("TYPE_NAME", "VARCHAR"),
                Pair.of("IS_NULLABLE", "NO")
            ),
            ROW(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("COLUMN_NAME", "dim2"),
                Pair.of("DATA_TYPE", Types.VARCHAR),
                Pair.of("TYPE_NAME", "VARCHAR"),
                Pair.of("IS_NULLABLE", "NO")
            ),
            ROW(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("COLUMN_NAME", "m1"),
                Pair.of("DATA_TYPE", Types.FLOAT),
                Pair.of("TYPE_NAME", "FLOAT"),
                Pair.of("IS_NULLABLE", "NO")
            ),
            ROW(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("COLUMN_NAME", "unique_dim1"),
                Pair.of("DATA_TYPE", Types.OTHER),
                Pair.of("TYPE_NAME", "OTHER"),
                Pair.of("IS_NULLABLE", "NO")
            )
        ),
        getRows(
            metaData.getColumns(null, "dr_id", "foo", null),
            ImmutableSet.of("IS_NULLABLE", "TABLE_NAME", "TABLE_SCHEM", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME")
        )
    );
  }

  @Test
  public void testTooManyStatements() throws Exception
  {
    final Statement statement1 = client.createStatement();
    final Statement statement2 = client.createStatement();

    expectedException.expect(AvaticaClientRuntimeException.class);
    expectedException.expectMessage("Too many open statements, limit is[2]");
    final Statement statement3 = client.createStatement();
  }

  @Test
  public void testNotTooManyStatementsWhenYouCloseThem() throws Exception
  {
    client.createStatement().close();
    client.createStatement().close();
    client.createStatement().close();
    client.createStatement().close();
    client.createStatement().close();

    Assert.assertTrue(true);
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
      final List<Map<String, Object>> rows = Lists.newArrayList();
      while (resultSet.next()) {
        final Map<String, Object> row = Maps.newHashMap();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
          if (returnKeys == null || returnKeys.contains(metaData.getColumnLabel(i + 1))) {
            row.put(metaData.getColumnLabel(i + 1), resultSet.getObject(i + 1));
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

  private static Map<String, Object> ROW(final Pair<String, ?>... entries)
  {
    final Map<String, Object> m = Maps.newHashMap();
    for (Pair<String, ?> entry : entries) {
      m.put(entry.lhs, entry.rhs);
    }
    return m;
  }
}
