/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.druid.metadata;

import com.google.common.base.Suppliers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.LinkedList;


public class SQLMetadataConnectorTest
{
  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig = MetadataStorageTablesConfig.fromBase("test");

  @Before
  public void setUp() throws Exception {
    connector = new TestDerbyConnector(
        Suppliers.ofInstance(new MetadataStorageConnectorConfig()),
        Suppliers.ofInstance(tablesConfig)
    );
  }

  @Test
  public void testCreateTables() throws Exception
  {
    final LinkedList<String> tables = new LinkedList<String>();
    final String entryType = tablesConfig.getTaskEntryType();
    tables.add(tablesConfig.getConfigTable());
    tables.add(tablesConfig.getSegmentsTable());
    tables.add(tablesConfig.getRulesTable());
    tables.add(tablesConfig.getLockTable(entryType));
    tables.add(tablesConfig.getLogTable(entryType));
    tables.add(tablesConfig.getEntryTable(entryType));
    tables.add(tablesConfig.getAuditTable());

    connector.createSegmentTable();
    connector.createConfigTable();
    connector.createRulesTable();
    connector.createTaskTables();
    connector.createAuditTable();

    connector.getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            for (String table : tables) {
              Assert.assertTrue(
                  String.format("table %s was not created!", table),
                  connector.tableExists(handle, table)
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
  public void testInsertOrUpdate() throws Exception
  {
    final String tableName = "test";
    connector.createConfigTable(connector.getDBI(), tableName);

    Assert.assertNull(connector.lookup(tableName, "name", "payload", "emperor"));

    connector.insertOrUpdate(tableName, "name", "payload", "emperor", "penguin".getBytes());
    Assert.assertArrayEquals(
        "penguin".getBytes(),
        connector.lookup(tableName, "name", "payload", "emperor")
    );

    connector.insertOrUpdate(tableName, "name", "payload", "emperor", "penguin chick".getBytes());

    Assert.assertArrayEquals(
        "penguin chick".getBytes(),
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
          public Void withHandle(Handle handle) throws Exception
          {
            handle.createStatement(String.format("DROP TABLE %s", tableName))
                  .execute();
            return null;
          }
        }
    );
  }
}
