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
import com.google.common.io.BaseEncoding;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
    tables.add(tablesConfig.getConfigTable());
    tables.add(tablesConfig.getSegmentsTable());
    tables.add(tablesConfig.getRulesTable());
    tables.add(tablesConfig.getTaskLockTable());
    tables.add(tablesConfig.getTasksTable());
    tables.add(tablesConfig.getAuditTable());
    tables.add(tablesConfig.getSupervisorTable());
    tables.add(tablesConfig.getIndexingStatesTable());

    connector.createSegmentTable();
    connector.createConfigTable();
    connector.createRulesTable();
    connector.createTaskTables();
    connector.createAuditTable();
    connector.createSupervisorsTable();
    connector.createIndexingStatesTable();

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
    String entryTableName = tablesConfig.getTasksTable();
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
          Lists.newArrayList("a", "b")
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

    connector.alterSegmentTable();
    Assert.assertTrue(connector.tableHasColumn(
        derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
        "USED_STATUS_LAST_UPDATED"
    ));

    Assert.assertFalse(connector.tableHasColumn(
        derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
        "SCHEMA_FINGERPRINT"
    ));

    Assert.assertFalse(connector.tableHasColumn(
        derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
        "NUM_ROWS"
    ));
  }

  /**
   * This is a test for the upgrade path where a cluster is upgrading from a version that did not have used_status_last_updated
   * in the segments table.
   */
  @Test
  public void testAlterSegmentTableAddIndexingStateFingerprint()
  {
    connector.createSegmentTable();
    derbyConnectorRule.segments().update("ALTER TABLE %1$s DROP COLUMN INDEXING_STATE_FINGERPRINT");
    connector.alterSegmentTable();
    Assert.assertTrue(connector.tableHasColumn(
        derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
        "INDEXING_STATE_FINGERPRINT"
    ));
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
        Suppliers.ofInstance(tablesConfig),
        CentralizedDatasourceSchemaConfig.create()
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
        Suppliers.ofInstance(tablesConfig),
        CentralizedDatasourceSchemaConfig.create()
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

  @Test
  public void test_useShortIndexNames_true_tableIndices_areNotAdded_ifExist()
  {
    tablesConfig = new MetadataStorageTablesConfig(
        "druidTest",
        null, null, null, null, null, null, null, null, null, null, null,
        true,
        null
    );
    connector = new TestDerbyConnector(new MetadataStorageConnectorConfig(), tablesConfig);

    final String segmentsTable = tablesConfig.getSegmentsTable();

    connector.createSegmentTable(segmentsTable);
    connector.alterSegmentTable();
    connector.getDBI().withHandle(handle -> {
      handle.execute("DROP INDEX IDX_8FE3D20EC8C9CA932EA3FF6AC497D1A9E75ADDA0");
      handle.execute("CREATE INDEX IDX_DRUIDTEST_SEGMENTS_USED ON druidTest_segments(used)");
      return null;
    });

    connector.createSegmentTable(segmentsTable);
    connector.alterSegmentTable();

    final Set<String> expectedIndices = Sets.newHashSet(
        "IDX_DRUIDTEST_SEGMENTS_USED",
        "IDX_D011BD6ED76268701273CE512704C5AFA060D672",
        "IDX_6381EF2DB4824C35C0E72EF9E166626ADB2B21A3"
    );
    assertIndicesPresentOnTable(segmentsTable, expectedIndices);

    dropTable(segmentsTable);
    connector.tearDown();
  }

  @Test
  public void test_useShortIndexNames_false_tableIndices_areNotAdded_ifExist()
  {
    tablesConfig = new MetadataStorageTablesConfig(
        "druidTest",
        null, null, null, null, null, null, null, null, null, null, null,
        false,
        null
    );
    connector = new TestDerbyConnector(new MetadataStorageConnectorConfig(), tablesConfig);

    final String segmentsTable = tablesConfig.getSegmentsTable();

    connector.createSegmentTable(segmentsTable);
    connector.alterSegmentTable();
    connector.getDBI().withHandle(handle -> {
      handle.execute("DROP INDEX IDX_DRUIDTEST_SEGMENTS_USED");
      handle.execute("CREATE INDEX IDX_8FE3D20EC8C9CA932EA3FF6AC497D1A9E75ADDA0 ON druidTest_segments(used)");
      return null;
    });

    connector.createSegmentTable(segmentsTable);
    connector.alterSegmentTable();

    final Set<String> expectedIndices = Sets.newHashSet(
        "IDX_8FE3D20EC8C9CA932EA3FF6AC497D1A9E75ADDA0",
        "IDX_DRUIDTEST_SEGMENTS_DATASOURCE_USED_END_START",
        "IDX_DRUIDTEST_SEGMENTS_DATASOURCE_UPGRADED_FROM_SEGMENT_ID"
    );
    assertIndicesPresentOnTable(segmentsTable, expectedIndices);

    dropTable(segmentsTable);
    connector.tearDown();
  }

  @Test
  public void test_useShortIndexNames_true_tableIndices_areAdded_IfNotExist()
  {
    tablesConfig = new MetadataStorageTablesConfig(
        "druidTest",
        null, null, null, null, null, null, null, null, null, null, null,
        true,
        null
    );
    connector = new TestDerbyConnector(new MetadataStorageConnectorConfig(), tablesConfig);

    final String segmentsTable = tablesConfig.getSegmentsTable();

    final Set<String> expectedIndices = Sets.newHashSet(
        "IDX_8FE3D20EC8C9CA932EA3FF6AC497D1A9E75ADDA0",
        "IDX_D011BD6ED76268701273CE512704C5AFA060D672",
        "IDX_6381EF2DB4824C35C0E72EF9E166626ADB2B21A3"
    );

    connector.createSegmentTable(segmentsTable);
    connector.alterSegmentTable();

    assertIndicesPresentOnTable(segmentsTable, expectedIndices);
    dropTable(segmentsTable);
    connector.tearDown();
  }

  @Test
  public void test_useShortIndexNames_false_tableIndices_areAdded_IfNotExist()
  {
    tablesConfig = new MetadataStorageTablesConfig(
        "druidTest",
        null, null, null, null, null, null, null, null, null, null, null,
        false,
        null
    );
    connector = new TestDerbyConnector(new MetadataStorageConnectorConfig(), tablesConfig);
    final String segmentsTable = tablesConfig.getSegmentsTable();

    final Set<String> expectedIndices = Sets.newHashSet(
        "IDX_DRUIDTEST_SEGMENTS_USED",
        "IDX_DRUIDTEST_SEGMENTS_DATASOURCE_USED_END_START",
        "IDX_DRUIDTEST_SEGMENTS_DATASOURCE_UPGRADED_FROM_SEGMENT_ID"
    );

    connector.createSegmentTable(segmentsTable);
    connector.alterSegmentTable();

    assertIndicesPresentOnTable(segmentsTable, expectedIndices);
    dropTable(segmentsTable);
    connector.tearDown();
  }

  private void assertIndicesPresentOnTable(String tableName, Set<String> expectedIndices)
  {
    // Fetch list of user-created indices, ignoring things like Derby-generated constraint indices, etc.
    final Set<String> actualIndices = connector.getIndexOnTable(tableName)
                                               .stream()
                                               .filter(name -> !name.startsWith("SQL"))
                                               .collect(Collectors.toSet());
    Assert.assertEquals(
        StringUtils.format(
            "Received unexpected table index set for table[%s]. Got [%s], expected [%s].",
            tableName,
            actualIndices,
            expectedIndices
        ),
        actualIndices,
        expectedIndices
    );
  }

  @Test
  public void testExportTable() throws IOException
  {
    final String tableName = "test_export";
    connector.getDBI().withHandle(
        handle -> {
          handle.execute(
              StringUtils.format(
                  "CREATE TABLE %s (name VARCHAR(255) NOT NULL, payload BLOB NOT NULL, active BOOLEAN NOT NULL, PRIMARY KEY(name))",
                  tableName
              )
          );
          handle.execute(
              StringUtils.format("INSERT INTO %s VALUES (?, ?, ?)", tableName),
              "key1",
              StringUtils.toUtf8("{\"type\":\"test\"}"),
              true
          );
          handle.execute(
              StringUtils.format("INSERT INTO %s VALUES (?, ?, ?)", tableName),
              "key2",
              StringUtils.toUtf8("{\"value\":42}"),
              false
          );
          return null;
        }
    );

    final File outputFile = File.createTempFile("export_test", ".csv");
    outputFile.deleteOnExit();

    // Call the base class exportTable (the generic JDBC path used by PostgreSQL)
    // rather than DerbyConnector's native SYSCS_EXPORT_TABLE override
    connector.exportTableGeneric(
        StringUtils.toUpperCase(tableName),
        outputFile.getAbsolutePath()
    );

    final List<String> lines = Files.readAllLines(outputFile.toPath(), StandardCharsets.UTF_8);
    Assert.assertEquals(2, lines.size());
    Collections.sort(lines);

    // Verify rows (sorted by name): hex-encoded payload, boolean as string
    final String expectedHex1 = BaseEncoding.base16().encode(StringUtils.toUtf8("{\"type\":\"test\"}"));
    Assert.assertEquals("key1," + expectedHex1 + ",true", lines.get(0));

    final String expectedHex2 = BaseEncoding.base16().encode(StringUtils.toUtf8("{\"value\":42}"));
    Assert.assertEquals("key2," + expectedHex2 + ",false", lines.get(1));

    dropTable(tableName);
  }

  @Test
  public void testExportTableWithSpecialCharacters() throws IOException
  {
    final String tableName = "test_export_special";
    connector.getDBI().withHandle(
        handle -> {
          handle.execute(
              StringUtils.format(
                  "CREATE TABLE %s (name VARCHAR(255) NOT NULL, description VARCHAR(1024), PRIMARY KEY(name))",
                  tableName
              )
          );
          handle.execute(
              StringUtils.format("INSERT INTO %s VALUES (?, ?)", tableName),
              "commas",
              "value,with,commas"
          );
          handle.execute(
              StringUtils.format("INSERT INTO %s VALUES (?, ?)", tableName),
              "quotes",
              "value\"with\"quotes"
          );
          handle.execute(
              StringUtils.format("INSERT INTO %s VALUES (?, ?)", tableName),
              "simple",
              "plain_value"
          );
          return null;
        }
    );

    final File outputFile = File.createTempFile("export_special_test", ".csv");
    outputFile.deleteOnExit();

    connector.exportTableGeneric(
        StringUtils.toUpperCase(tableName),
        outputFile.getAbsolutePath()
    );

    final List<String> lines = Files.readAllLines(outputFile.toPath(), StandardCharsets.UTF_8);
    Assert.assertEquals(3, lines.size());
    Collections.sort(lines);

    // Values with commas should be quoted (sorted order: commas, quotes, simple)
    Assert.assertEquals("commas,\"value,with,commas\"", lines.get(0));
    // Values with quotes should be quoted and quotes doubled
    Assert.assertEquals("quotes,\"value\"\"with\"\"quotes\"", lines.get(1));
    // Simple values should not be quoted
    Assert.assertEquals("simple,plain_value", lines.get(2));

    dropTable(tableName);
  }

  @Test
  public void testExportTableWithNullValues() throws IOException
  {
    final String tableName = "test_export_nulls";
    connector.getDBI().withHandle(
        handle -> {
          handle.execute(
              StringUtils.format(
                  "CREATE TABLE %s (name VARCHAR(255) NOT NULL, payload BLOB, description VARCHAR(255), PRIMARY KEY(name))",
                  tableName
              )
          );
          handle.execute(
              StringUtils.format("INSERT INTO %s VALUES (?, ?, ?)", tableName),
              "with_values",
              StringUtils.toUtf8("{\"key\":1}"),
              "has_desc"
          );
          handle.execute(
              StringUtils.format("INSERT INTO %s (name) VALUES (?)", tableName),
              "null_cols"
          );
          return null;
        }
    );

    final File outputFile = File.createTempFile("export_nulls_test", ".csv");
    outputFile.deleteOnExit();

    connector.exportTableGeneric(
        StringUtils.toUpperCase(tableName),
        outputFile.getAbsolutePath()
    );

    final List<String> lines = Files.readAllLines(outputFile.toPath(), StandardCharsets.UTF_8);
    Assert.assertEquals(2, lines.size());
    Collections.sort(lines);

    // Row with NULL payload and NULL description should have empty fields
    Assert.assertEquals("null_cols,,", lines.get(0));

    // Row with values
    final String expectedHex = BaseEncoding.base16().encode(StringUtils.toUtf8("{\"key\":1}"));
    Assert.assertEquals("with_values," + expectedHex + ",has_desc", lines.get(1));

    dropTable(tableName);
  }

  @Test
  public void testExportTablePreservesAllColumns() throws IOException
  {
    final String tableName = "test_export_allcols";
    connector.getDBI().withHandle(
        handle -> {
          // Simulate segments table structure with columns after payload
          handle.execute(
              StringUtils.format(
                  "CREATE TABLE %s ("
                  + "id VARCHAR(255) NOT NULL, "
                  + "used BOOLEAN NOT NULL, "
                  + "payload BLOB NOT NULL, "
                  + "used_status_last_updated VARCHAR(255), "
                  + "fingerprint VARCHAR(255), "
                  + "PRIMARY KEY(id))",
                  tableName
              )
          );
          handle.execute(
              StringUtils.format("INSERT INTO %s VALUES (?, ?, ?, ?, ?)", tableName),
              "seg1",
              true,
              StringUtils.toUtf8("{\"v\":1}"),
              "2024-01-01",
              "fp_abc"
          );
          handle.execute(
              StringUtils.format("INSERT INTO %s (id, used, payload) VALUES (?, ?, ?)", tableName),
              "seg2",
              false,
              StringUtils.toUtf8("{\"v\":2}")
          );
          return null;
        }
    );

    final File outputFile = File.createTempFile("export_allcols_test", ".csv");
    outputFile.deleteOnExit();

    connector.exportTableGeneric(
        StringUtils.toUpperCase(tableName),
        outputFile.getAbsolutePath()
    );

    final List<String> lines = Files.readAllLines(outputFile.toPath(), StandardCharsets.UTF_8);
    Assert.assertEquals(2, lines.size());
    Collections.sort(lines);

    // All 5 columns should be present, including those after payload
    final String hex1 = BaseEncoding.base16().encode(StringUtils.toUtf8("{\"v\":1}"));
    Assert.assertEquals("seg1,true," + hex1 + ",2024-01-01,fp_abc", lines.get(0));

    // NULL trailing columns should produce empty fields
    final String hex2 = BaseEncoding.base16().encode(StringUtils.toUtf8("{\"v\":2}"));
    Assert.assertEquals("seg2,false," + hex2 + ",,", lines.get(1));

    dropTable(tableName);
  }

  static class TestSQLMetadataConnector extends SQLMetadataConnector
  {
    public TestSQLMetadataConnector(
        Supplier<MetadataStorageConnectorConfig> config,
        Supplier<MetadataStorageTablesConfig> tablesConfigSupplier,
        CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
    )
    {
      super(config, tablesConfigSupplier, centralizedDatasourceSchemaConfig);
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
    public boolean isUniqueConstraintViolation(Throwable t)
    {
      return false;
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
}
