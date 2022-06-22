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

package org.apache.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class SQLMetadataConnectorTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig;
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  @Before
  public void setUp()
  {
    connector = derbyConnectorRule.getConnector();
    tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
  }

  @Test
  public void testCreateTables()
  {
    final List<String> tables = new ArrayList<>();
    final String entryType = tablesConfig.getTaskEntryType();
    tables.add(tablesConfig.getConfigTable());
    tables.add(tablesConfig.getSegmentsTable());
    tables.add(tablesConfig.getRulesTable());
    tables.add(tablesConfig.getLockTable(entryType));
    tables.add(tablesConfig.getLogTable(entryType));
    tables.add(tablesConfig.getEntryTable(entryType));
    tables.add(tablesConfig.getAuditTable());
    tables.add(tablesConfig.getSupervisorTable());

    connector.createSegmentTable();
    connector.createConfigTable();
    connector.createRulesTable();
    connector.createTaskTables();
    connector.createAuditTable();
    connector.createSupervisorsTable();

    connector.getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle)
          {
            for (String table : tables) {
              Assert.assertTrue(
                  StringUtils.format("table %s was not created!", table),
                  connector.tableExists(handle, table)
              );
            }

            String taskTable = tablesConfig.getTasksTable();
            for (String column : Arrays.asList("type", "group_id")) {
              Assert.assertTrue(
                  StringUtils.format("Tasks table column %s was not created!", column),
                  connector.tableContainsColumn(handle, taskTable, column)
              );
            }

            return null;
          }
        }
    );

    for (String table : tables) {
      dropTable(table);
    }
  }

  @Test
  public void testInsertOrUpdate()
  {
    final String tableName = "test";
    connector.createConfigTable(tableName);

    Assert.assertNull(connector.lookup(tableName, "name", "payload", "emperor"));

    connector.insertOrUpdate(
        tableName,
        "name",
        "payload",
        "emperor",
        StringUtils.toUtf8("penguin")
    );
    Assert.assertArrayEquals(
        StringUtils.toUtf8("penguin"),
        connector.lookup(tableName, "name", "payload", "emperor")
    );

    connector.insertOrUpdate(
        tableName,
        "name",
        "payload",
        "emperor",
        StringUtils.toUtf8("penguin chick")
    );

    Assert.assertArrayEquals(
        StringUtils.toUtf8("penguin chick"),
        connector.lookup(tableName, "name", "payload", "emperor")
    );

    dropTable(tableName);
  }

  private void dropTable(final String tableName)
  {
    connector.getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle)
          {
            handle.createStatement(StringUtils.format("DROP TABLE %s", tableName))
                  .execute();
            return null;
          }
        }
    );
  }

  static class TestSQLMetadataConnector extends SQLMetadataConnector
  {
    public TestSQLMetadataConnector(
        Supplier<MetadataStorageConnectorConfig> config,
        Supplier<MetadataStorageTablesConfig> tablesConfigSupplier
    )
    {
      super(config, tablesConfigSupplier);
    }

    @Override
    public String getSerialType()
    {
      return null;
    }

    @Override
    public int getStreamingFetchSize()
    {
      return 0;
    }

    @Override
    public String limitClause(int limit)
    {
      return "";
    }

    @Override
    public String getQuoteString()
    {
      return null;
    }

    @Override
    public boolean tableExists(Handle handle, String tableName)
    {
      return false;
    }

    @Override
    public DBI getDBI()
    {
      return null;
    }

    @Override
    protected BasicDataSource getDatasource()
    {
      return super.getDatasource();
    }
  }

  private MetadataStorageConnectorConfig getDbcpPropertiesFile(
      boolean createTables,
      String host,
      int port,
      String connectURI,
      String user,
      String pwdString,
      String pwd
  ) throws Exception
  {
    return JSON_MAPPER.readValue(
        "{" +
        "\"createTables\": \"" + createTables + "\"," +
        "\"host\": \"" + host + "\"," +
        "\"port\": \"" + port + "\"," +
        "\"connectURI\": \"" + connectURI + "\"," +
        "\"user\": \"" + user + "\"," +
        "\"password\": " + pwdString + "," +
        "\"dbcp\": {\n" +
        "  \"maxConnLifetimeMillis\" : 1200000,\n" +
        "  \"defaultQueryTimeout\" : \"30000\"\n" +
        "}" +
        "}",
        MetadataStorageConnectorConfig.class
    );
  }

  @Test
  public void testBasicDataSourceCreation() throws Exception
  {
    MetadataStorageConnectorConfig config = getDbcpPropertiesFile(
        true,
        "host",
        1234,
        "connectURI",
        "user",
        "{\"type\":\"default\",\"password\":\"nothing\"}",
        "nothing"
    );
    TestSQLMetadataConnector testSQLMetadataConnector = new TestSQLMetadataConnector(
        Suppliers.ofInstance(config),
        Suppliers.ofInstance(tablesConfig)
    );
    BasicDataSource dataSource = testSQLMetadataConnector.getDatasource();
    Assert.assertEquals(dataSource.getMaxConnLifetimeMillis(), 1200000);
    Assert.assertEquals((long) dataSource.getDefaultQueryTimeout(), 30000);
  }

  private boolean verifyTaskTypeAndGroupId(String table, String id, String type, String groupId)
  {
    try {
      return connector.retryWithHandle(
          new HandleCallback<Boolean>()
          {
            @Override
            public Boolean withHandle(Handle handle) throws SQLException
            {
              Statement statement = handle.getConnection().createStatement();
              ResultSet resultSet = statement.executeQuery(
                  StringUtils.format("SELECT * FROM %1$s WHERE id = '%2$s'", table, id)
              );
              resultSet.next();
              boolean flag = type.equals(resultSet.getString("type"))
                             && groupId.equals(resultSet.getString("group_id"));
              statement.close();
              return flag;
            }
          }
      );
    }
    catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }
}
