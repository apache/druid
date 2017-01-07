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
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.util.CalciteTests;
import org.apache.calcite.jdbc.CalciteConnection;
import org.eclipse.jetty.server.Server;
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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class DruidAvaticaHandlerTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private CalciteConnection serverConnection;
  private Server server;
  private Connection client;

  @Before
  public void setUp() throws Exception
  {
    final PlannerConfig plannerConfig = new PlannerConfig();
    serverConnection = Calcites.jdbc(
        CalciteTests.createMockSchema(
            CalciteTests.createWalker(temporaryFolder.newFolder()),
            plannerConfig
        ),
        plannerConfig
    );
    final ServerConfig serverConfig = new ServerConfig()
    {
      @Override
      public boolean isEnableAvatica()
      {
        return true;
      }
    };
    final DruidAvaticaHandler handler = new DruidAvaticaHandler(
        serverConnection,
        new DruidNode("dummy", "dummy", 1),
        new AvaticaMonitor(),
        serverConfig
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
  }

  @After
  public void tearDown() throws Exception
  {
    client.close();
    server.stop();
    serverConnection.close();
    client = null;
    server = null;
    serverConnection = null;
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
  public void testFieldAliasingSelect() throws Exception
  {
    final ResultSet resultSet = client.createStatement().executeQuery(
        "SELECT dim2 AS \"x\", dim2 AS \"y\" FROM druid.foo LIMIT 1"
    );
    final List<Map<String, Object>> rows = getRows(resultSet);
    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of("x", "a", "y", "a")
        ),
        rows
    );
  }

  @Test
  public void testExplainSelectCount() throws Exception
  {
    final ResultSet resultSet = client.createStatement().executeQuery(
        "EXPLAIN PLAN FOR SELECT COUNT(*) AS cnt FROM druid.foo"
    );
    final List<Map<String, Object>> rows = getRows(resultSet);
    Assert.assertEquals(
        ImmutableList.of(
            ImmutableMap.of(
                "PLAN",
                "EnumerableInterpreter\n"
                + "  DruidQueryRel(dataSource=[foo], dimensions=[[]], aggregations=[[Aggregation{aggregatorFactories=[CountAggregatorFactory{name='a0'}], postAggregator=null, finalizingPostAggregatorFactory=null}]])\n"
            )
        ),
        rows
    );
  }

  @Test
  public void testDatabaseMetaDataSchemas() throws Exception
  {
    final DatabaseMetaData metaData = client.getMetaData();
    Assert.assertEquals(
        ImmutableList.of(
            ROW(Pair.of("TABLE_CATALOG", null), Pair.of("TABLE_SCHEM", "druid"))
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
    final String varcharDescription = "VARCHAR(1) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL";
    Assert.assertEquals(
        ImmutableList.of(
            ROW(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("COLUMN_NAME", "__time"),
                Pair.of("DATA_TYPE", 93),
                Pair.of("TYPE_NAME", "TIMESTAMP(0) NOT NULL"),
                Pair.of("IS_NULLABLE", "NO")
            ),
            ROW(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("COLUMN_NAME", "cnt"),
                Pair.of("DATA_TYPE", -5),
                Pair.of("TYPE_NAME", "BIGINT NOT NULL"),
                Pair.of("IS_NULLABLE", "NO")
            ),
            ROW(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("COLUMN_NAME", "dim1"),
                Pair.of("DATA_TYPE", 12),
                Pair.of("TYPE_NAME", varcharDescription),
                Pair.of("IS_NULLABLE", "NO")
            ),
            ROW(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("COLUMN_NAME", "dim2"),
                Pair.of("DATA_TYPE", 12),
                Pair.of("TYPE_NAME", varcharDescription),
                Pair.of("IS_NULLABLE", "NO")
            ),
            ROW(
                Pair.of("TABLE_SCHEM", "druid"),
                Pair.of("TABLE_NAME", "foo"),
                Pair.of("COLUMN_NAME", "m1"),
                Pair.of("DATA_TYPE", 6),
                Pair.of("TYPE_NAME", "FLOAT NOT NULL"),
                Pair.of("IS_NULLABLE", "NO")
            )
        ),
        getRows(
            metaData.getColumns(null, "druid", "foo", "%"),
            ImmutableSet.of("IS_NULLABLE", "TABLE_NAME", "TABLE_SCHEM", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME")
        )
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
