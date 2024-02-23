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

package org.apache.druid.tests.query;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.apache.druid.https.SSLClientConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.DataLoaderHelper;
import org.apache.druid.tests.TestNGGroup;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

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

@Test(groups = {TestNGGroup.QUERY, TestNGGroup.CENTRALIZED_DATASOURCE_SCHEMA})
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITJdbcQueryTest
{
  private static final Logger LOG = new Logger(ITJdbcQueryTest.class);
  private static final String WIKIPEDIA_DATA_SOURCE = "wikipedia_editstream";
  private static final String CONNECTION_TEMPLATE = "jdbc:avatica:remote:url=%s/druid/v2/sql/avatica/";
  private static final String TLS_CONNECTION_TEMPLATE =
      "jdbc:avatica:remote:url=%s/druid/v2/sql/avatica/;truststore=%s;truststore_password=%s;keystore=%s;keystore_password=%s;key_password=%s";

  private static final String QUERY_TEMPLATE =
      "SELECT \"user\", SUM(\"added\"), COUNT(*)" +
      "FROM \"wikipedia\" " +
      "WHERE \"__time\" >= CURRENT_TIMESTAMP - INTERVAL '99' YEAR AND \"language\" = %s" +
      "GROUP BY 1 ORDER BY 3 DESC LIMIT 10";
  private static final String QUERY = StringUtils.format(QUERY_TEMPLATE, "'en'");

  private static final String QUERY_PARAMETERIZED = StringUtils.format(QUERY_TEMPLATE, "?");

  private String[] connections;
  private Properties connectionProperties;

  @Inject
  private IntegrationTestingConfig config;

  @Inject
  SSLClientConfig sslConfig;

  @Inject
  private DataLoaderHelper dataLoaderHelper;

  @BeforeMethod
  public void before()
  {
    connectionProperties = new Properties();
    connectionProperties.setProperty("user", "admin");
    connectionProperties.setProperty("password", "priest");
    connections = new String[]{
        StringUtils.format(CONNECTION_TEMPLATE, config.getRouterUrl()),
        StringUtils.format(CONNECTION_TEMPLATE, config.getBrokerUrl()),
        StringUtils.format(
            TLS_CONNECTION_TEMPLATE,
            config.getRouterTLSUrl(),
            sslConfig.getTrustStorePath(),
            sslConfig.getTrustStorePasswordProvider().getPassword(),
            sslConfig.getKeyStorePath(),
            sslConfig.getKeyStorePasswordProvider().getPassword(),
            sslConfig.getKeyManagerPasswordProvider().getPassword()
        ),
        StringUtils.format(
            TLS_CONNECTION_TEMPLATE,
            config.getBrokerTLSUrl(),
            sslConfig.getTrustStorePath(),
            sslConfig.getTrustStorePasswordProvider().getPassword(),
            sslConfig.getKeyStorePath(),
            sslConfig.getKeyStorePasswordProvider().getPassword(),
            sslConfig.getKeyManagerPasswordProvider().getPassword()
        )
    };
    // ensure that wikipedia segments are loaded completely
    dataLoaderHelper.waitUntilDatasourceIsReady(WIKIPEDIA_DATA_SOURCE);
    dataLoaderHelper.waitUntilDatasourceIsReady("wikipedia");
    dataLoaderHelper.waitUntilDatasourceIsReady("twitterstream");
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
        Assert.assertEquals(catalogs, ImmutableList.of("druid"));

        Set<String> schemas = new HashSet<>();
        ResultSet schemasMetadata = metadata.getSchemas("druid", null);
        while (schemasMetadata.next()) {
          final String schema = schemasMetadata.getString(1);
          schemas.add(schema);
        }
        LOG.info("'druid' catalog schemas %s", schemas);
        // maybe more schemas than this, but at least should have these
        Assert.assertTrue(schemas.containsAll(ImmutableList.of("INFORMATION_SCHEMA", "druid", "lookup", "sys")));

        Set<String> druidTables = new HashSet<>();
        ResultSet tablesMetadata = metadata.getTables("druid", "druid", null, null);
        while (tablesMetadata.next()) {
          final String table = tablesMetadata.getString(3);
          druidTables.add(table);
        }
        LOG.info("'druid' schema tables %s", druidTables);
        // maybe more tables than this, but at least should have these
        Assert.assertTrue(
            druidTables.containsAll(ImmutableList.of("twitterstream", "wikipedia", WIKIPEDIA_DATA_SOURCE))
        );

        Set<String> wikiColumns = new HashSet<>();
        ResultSet columnsMetadata = metadata.getColumns("druid", "druid", WIKIPEDIA_DATA_SOURCE, null);
        while (columnsMetadata.next()) {
          final String column = columnsMetadata.getString(4);
          wikiColumns.add(column);
        }
        LOG.info("'%s' columns %s", WIKIPEDIA_DATA_SOURCE, wikiColumns);
        // a lot more columns than this, but at least should have these
        Assert.assertTrue(
            wikiColumns.containsAll(ImmutableList.of("added", "city", "delta", "language"))
        );
      }
      catch (SQLException throwables) {
        Assert.fail(throwables.getMessage());
      }
    }
  }

  @Test
  public void testJdbcStatementQuery()
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
        try (Statement statement = connection.createStatement()) {
          final ResultSet resultSet = statement.executeQuery(QUERY);
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
            LOG.info("%s,%s,%s", resultSet.getString(1), resultSet.getLong(2), resultSet.getLong(3));
          }
          Assert.assertEquals(resultRowCount, 10);
          resultSet.close();
        }
      }
      catch (SQLException throwables) {
        Assert.fail(throwables.getMessage());
      }
    }
  }

  @Test
  public void testJdbcPrepareStatementQuery()
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
        try (PreparedStatement statement = connection.prepareStatement(QUERY_PARAMETERIZED)) {
          statement.setString(1, "en");
          final ResultSet resultSet = statement.executeQuery();
          int resultRowCount = 0;
          while (resultSet.next()) {
            resultRowCount++;
            LOG.info("%s,%s,%s", resultSet.getString(1), resultSet.getLong(2), resultSet.getLong(3));
          }
          Assert.assertEquals(resultRowCount, 10);
          resultSet.close();
        }
      }
      catch (SQLException throwables) {
        Assert.fail(throwables.getMessage());
      }
    }
  }

  @Test(expectedExceptions = AvaticaSqlException.class, expectedExceptionsMessageRegExp = ".* No value bound for parameter \\(position \\[1]\\)")
  public void testJdbcPrepareStatementQueryMissingParameters() throws SQLException
  {
    for (String url : connections) {
      try (Connection connection = DriverManager.getConnection(url, connectionProperties);
           PreparedStatement statement = connection.prepareStatement(QUERY_PARAMETERIZED);
           ResultSet resultSet = statement.executeQuery()) {
        // This won't actually run as we expect the exception to be thrown before it gets here
        throw new IllegalStateException(resultSet.toString());
      }
    }
  }
}
