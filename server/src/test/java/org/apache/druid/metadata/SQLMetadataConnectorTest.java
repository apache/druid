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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


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
                connector.tableHasColumn(taskTable, column)
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
  public void testIndexCreationOnTaskTable()
  {
    final String entryType = tablesConfig.getTaskEntryType();
    String entryTableName = tablesConfig.getEntryTable(entryType);
    connector.createTaskTables();
    Set<String> createdIndexSet = connector.getIndexOnTable(entryTableName);
    Set<String> expectedIndexSet = Sets.newHashSet(
        StringUtils.format("idx_%1$s_active_created_date", entryTableName),
        StringUtils.format("idx_%1$s_datasource_active", entryTableName)
    ).stream().map(StringUtils::toUpperCase).collect(Collectors.toSet());

    for (String expectedIndex : expectedIndexSet) {
      Assert.assertTrue(
          StringUtils.format("Failed to find the expected Index %s on entry table", expectedIndex),
          createdIndexSet.contains(expectedIndex)
      );
    }
    connector.createTaskTables();
    dropTable(entryTableName);
  }

  @Test
  public void testCreateIndexOnNoTable()
  {
    String tableName = "noTable";
    try {
      connector.createIndex(
          tableName,
          "some_string",
          Lists.newArrayList("a", "b"),
          new HashSet<>()
      );
    }
    catch (Exception e) {
      Assert.fail("Index creation should never throw an exception");
    }
  }

  @Test
  public void testGeIndexOnNoTable()
  {
    String tableName = "noTable";
    try {
      Set<String> res = connector.getIndexOnTable(tableName);
      Assert.assertEquals(0, res.size());
    }
    catch (Exception e) {
      Assert.fail("getIndexOnTable should never throw an exception");
    }
  }

  /**
   * This is a test for the upgrade path where a cluster is upgrading from a version that did not have used_status_last_updated
   * in the segments table.
   */
  @Test
  public void testAlterSegmentTableAddLastUsed()
  {
    connector.createSegmentTable();
    derbyConnectorRule.segments().update("ALTER TABLE %1$s DROP COLUMN USED_STATUS_LAST_UPDATED");

    connector.alterSegmentTableAddUsedFlagLastUpdated();
    connector.tableHasColumn(
        derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
        "USED_STATUS_LAST_UPDATED"
    );
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
