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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SQLMetadataConnectorSchemaPersistenceTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule =
      new TestDerbyConnector.DerbyConnectorRule(CentralizedDatasourceSchemaConfig.create(true));

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
    tables.add(tablesConfig.getSegmentSchemasTable());
    tables.add(tablesConfig.getSegmentsTable());
    tables.add(tablesConfig.getRulesTable());
    tables.add(tablesConfig.getLockTable(entryType));
    tables.add(tablesConfig.getLogTable(entryType));
    tables.add(tablesConfig.getEntryTable(entryType));
    tables.add(tablesConfig.getAuditTable());
    tables.add(tablesConfig.getSupervisorTable());

    final List<String> dropSequence = new ArrayList<>();
    dropSequence.add(tablesConfig.getConfigTable());
    dropSequence.add(tablesConfig.getSegmentsTable());
    dropSequence.add(tablesConfig.getSegmentSchemasTable());
    dropSequence.add(tablesConfig.getRulesTable());
    dropSequence.add(tablesConfig.getLockTable(entryType));
    dropSequence.add(tablesConfig.getLogTable(entryType));
    dropSequence.add(tablesConfig.getEntryTable(entryType));
    dropSequence.add(tablesConfig.getAuditTable());
    dropSequence.add(tablesConfig.getSupervisorTable());

    connector.createSegmentSchemasTable();
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

    for (String table : dropSequence) {
      dropTable(table);
    }
  }

  private void dropTable(final String tableName)
  {
    connector.getDBI().withHandle(
        handle -> handle.createStatement(StringUtils.format("DROP TABLE %s", tableName))
                        .execute()
    );
  }

  /**
   * This is a test for the upgrade path where a cluster is upgrading from a version that did not have used_status_last_updated,
   * schema_id, num_rows in the segments table.
   */
  @Test
  public void testAlterSegmentTable()
  {
    connector.createSegmentTable(tablesConfig.getSegmentsTable());

    // Drop column used_status_last_updated to bring us in line with pre-upgrade state
    derbyConnectorRule.segments().update("ALTER TABLE %1$s DROP COLUMN USED_STATUS_LAST_UPDATED");
    derbyConnectorRule.segments().update("ALTER TABLE %1$s DROP COLUMN SCHEMA_FINGERPRINT");
    derbyConnectorRule.segments().update("ALTER TABLE %1$s DROP COLUMN NUM_ROWS");

    connector.alterSegmentTable();
    Assert.assertTrue(connector.tableHasColumn(
        derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
        "USED_STATUS_LAST_UPDATED"
    ));
    Assert.assertTrue(connector.tableHasColumn(
        derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
        "SCHEMA_FINGERPRINT"
    ));
    Assert.assertTrue(connector.tableHasColumn(
        derbyConnectorRule.metadataTablesConfigSupplier().get().getSegmentsTable(),
        "NUM_ROWS"
    ));
  }
}
