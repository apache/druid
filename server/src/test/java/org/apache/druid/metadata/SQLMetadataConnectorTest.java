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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class SQLMetadataConnectorTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig;

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
        handle -> {
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
        handle -> handle.createStatement(StringUtils.format("DROP TABLE %s", tableName))
                        .execute()
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

  @Test
  public void testBasicDataSourceCreation()
  {
    Map<String, String> props = ImmutableMap.of(
        "maxConnLifetimeMillis", "1200000",
        "defaultQueryTimeout", "30000"
    );
    MetadataStorageConnectorConfig config =
        MetadataStorageConnectorConfig.create("connectURI", "user", "password", props);

    TestSQLMetadataConnector testSQLMetadataConnector = new TestSQLMetadataConnector(
        Suppliers.ofInstance(config),
        Suppliers.ofInstance(tablesConfig)
    );
    BasicDataSource dataSource = testSQLMetadataConnector.getDatasource();
    Assert.assertEquals(dataSource.getMaxConnLifetimeMillis(), 1200000);
    Assert.assertEquals(dataSource.getDefaultQueryTimeout().intValue(), 30000);
  }

  @Test
  public void testIsTransientException()
  {
    MetadataStorageConnectorConfig config =
        MetadataStorageConnectorConfig.create("connectURI", "user", "password", Collections.emptyMap());
    TestSQLMetadataConnector metadataConnector = new TestSQLMetadataConnector(
        Suppliers.ofInstance(config),
        Suppliers.ofInstance(tablesConfig)
    );

    // Transient exceptions
    Assert.assertTrue(metadataConnector.isTransientException(new RetryTransactionException("")));
    Assert.assertTrue(metadataConnector.isTransientException(new SQLRecoverableException()));
    Assert.assertTrue(metadataConnector.isTransientException(new SQLTransientException()));
    Assert.assertTrue(metadataConnector.isTransientException(new SQLTransientConnectionException()));

    // Non transient exceptions
    Assert.assertFalse(metadataConnector.isTransientException(null));
    Assert.assertFalse(metadataConnector.isTransientException(new SQLException()));
    Assert.assertFalse(metadataConnector.isTransientException(new UnableToExecuteStatementException("")));

    // Nested transient exceptions
    Assert.assertTrue(
        metadataConnector.isTransientException(
            new CallbackFailedException(new SQLTransientException())
        )
    );
    Assert.assertTrue(
        metadataConnector.isTransientException(
            new UnableToObtainConnectionException(new SQLException())
        )
    );
    Assert.assertTrue(
        metadataConnector.isTransientException(
            new UnableToExecuteStatementException(new SQLTransientException())
        )
    );

    // Nested non-transient exceptions
    Assert.assertFalse(
        metadataConnector.isTransientException(
            new CallbackFailedException(new SQLException())
        )
    );
    Assert.assertFalse(
        metadataConnector.isTransientException(
            new UnableToExecuteStatementException(new SQLException())
        )
    );

  }
}
