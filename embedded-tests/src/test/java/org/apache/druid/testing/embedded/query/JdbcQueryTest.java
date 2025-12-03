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

package org.apache.druid.testing.embedded.query;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * JDBC query integration tests.
 * Note: we need to correspond queries with TLS support to fullfill the conversion
 */
public class JdbcQueryTest extends QueryTestBase
{
  private static final Logger LOG = new Logger(JdbcQueryTest.class);
  private static final String CONNECTION_TEMPLATE = "jdbc:avatica:remote:url=%s/druid/v2/sql/avatica/";
  private static final String TLS_CONNECTION_TEMPLATE =
      "jdbc:avatica:remote:url=%s/druid/v2/sql/avatica/;truststore=%s;truststore_password=%s;keystore=%s;keystore_password=%s;key_password=%s";

  private static final String QUERY_TEMPLATE =
      "SELECT \"item\", SUM(\"value\"), COUNT(*) "
      + "FROM \"%s\" "
      + "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '99' YEAR AND \"value\" < %s \n"
      + "GROUP BY 1 ORDER BY 3 DESC LIMIT 10";

  private String[] connections;
  private Properties connectionProperties;

  private String tableName;

  @Override
  protected void beforeAll()
  {
    connectionProperties = new Properties();
    connectionProperties.setProperty("user", "admin");
    connectionProperties.setProperty("password", "priest");
    connections = new String[]{
        StringUtils.format(CONNECTION_TEMPLATE, getServerUrl(router)),
        StringUtils.format(CONNECTION_TEMPLATE, getServerUrl(broker)),
        };

    tableName = ingestBasicData();
  }

  @Test
  public void testJdbcMetadata()
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
        DatabaseMetaData metadata = connection.getMetaData();

        List<String> catalogs = new ArrayList<>();
        ResultSet catalogsMetadata = metadata.getCatalogs();
        while (catalogsMetadata.next()) {
          final String catalog = catalogsMetadata.getString(1);
          catalogs.add(catalog);
        }
        LOG.info("catalogs %s", catalogs);
        Assertions.assertEquals(catalogs, ImmutableList.of("druid"));

        Set<String> schemas = new HashSet<>();
        ResultSet schemasMetadata = metadata.getSchemas("druid", null);
        while (schemasMetadata.next()) {
          final String schema = schemasMetadata.getString(1);
          schemas.add(schema);
        }
        LOG.info("'druid' catalog schemas %s", schemas);
        // maybe more schemas than this, but at least should have these
        Assertions.assertTrue(schemas.containsAll(ImmutableList.of("INFORMATION_SCHEMA", "druid", "lookup", "sys")));

        Set<String> druidTables = new HashSet<>();
        ResultSet tablesMetadata = metadata.getTables("druid", "druid", null, null);
        while (tablesMetadata.next()) {
          final String table = tablesMetadata.getString(3);
          druidTables.add(table);
        }
        LOG.info("'druid' schema tables %s", druidTables);
        // There may be more tables than this, but at least should have @tableName
        Assertions.assertTrue(
            druidTables.containsAll(ImmutableList.of(tableName))
        );

        Set<String> wikiColumns = new HashSet<>();
        ResultSet columnsMetadata = metadata.getColumns("druid", "druid", tableName, null);
        while (columnsMetadata.next()) {
          final String column = columnsMetadata.getString(4);
          wikiColumns.add(column);
        }
        LOG.info("'%s' columns %s", tableName, wikiColumns);
        // a lot more columns than this, but at least should have these
        Assertions.assertTrue(
            wikiColumns.containsAll(ImmutableList.of("__time", "item", "value"))
        );
      }
      catch (SQLException throwables) {
        Assertions.fail(throwables.getMessage());
      }
    }
  }

  @Test
  public void testJdbcStatementQuery()
  {
    String query = StringUtils.format(QUERY_TEMPLATE, tableName, "1000");
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
        try (Statement statement = connection.createStatement()) {
          final ResultSet resultSet = statement.executeQuery(query);
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
            LOG.info("%s,%s,%s", resultSet.getString(1), resultSet.getLong(2), resultSet.getLong(3));
          }
          Assertions.assertEquals(7, resultRowCount);
          resultSet.close();
        }
      }
      catch (SQLException throwables) {
        Assertions.fail(throwables.getMessage());
      }
    }
  }

  @Test
  public void testJdbcPrepareStatementQuery()
  {
    String query = StringUtils.format(QUERY_TEMPLATE, tableName, "?");
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
        try (PreparedStatement statement = connection.prepareStatement(query)) {
          statement.setLong(1, 1000);
          final ResultSet resultSet = statement.executeQuery();
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
            LOG.info("%s,%s,%s", resultSet.getString(1), resultSet.getLong(2), resultSet.getLong(3));
          }
          Assertions.assertEquals(7, resultRowCount);
          resultSet.close();
        }
      }
      catch (SQLException throwables) {
        Assertions.fail(throwables.getMessage());
      }
    }
  }

  @Test
  public void testJdbcPrepareStatementQueryMissingParameters()
  {
    String query = StringUtils.format(QUERY_TEMPLATE, tableName, "?");
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, connectionProperties);
           PreparedStatement statement = connection.prepareStatement(query);
           ResultSet resultSet = statement.executeQuery()) {
        // This won't actually run as we expect the exception to be thrown before it gets here
        Assertions.fail(resultSet.toString());
      }
      catch (SQLException e) {
        Assertions.assertInstanceOf(AvaticaSqlException.class, e);
        Assertions.assertTrue(e.getMessage().contains("No value bound for parameter (position [1])"));
      }
    }
  }
}
